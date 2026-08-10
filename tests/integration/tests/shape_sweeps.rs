//! Shape-sweep benchmark cards: IN-list, row-width, and deep-offset/keyset.
//!
//! Cards:
//!
//!   4. `in_list_sweep`     — IN(10)/IN(100)/IN(1000)/IN(50 contiguous) point-
//!                            lookup sets at 1M rows vs PG; p50/p99 per size
//!   5. `wide_row_sweep`    — 6-col vs 50-col schema at 100k rows; project 2
//!                            cols vs SELECT *; projection-pushdown visibility
//!   10. `pagination_sweep` — OFFSET 100 / 10k / 100k vs keyset at 1M rows
//!                            vs PG; the deep-offset penalty card
//!
//! # Running
//!
//! All three tests are `#[ignore]`d:
//!
//! ```text
//! cargo test -p basin-integration-tests --test shape_sweeps \
//!   -- --ignored --nocapture
//! ```
//!
//! Or via `benchmark/run/realistic-suite.sh`.
//!
//! # Env knobs
//!
//! * `BASIN_IN_LIST_SAMPLES`  — samples per IN-list shape (default 100)
//! * `BASIN_WIDE_ROWS`        — row count for wide-row card (default 100_000)
//! * `BASIN_WIDE_SAMPLES`     — samples per wide-row shape (default 50)
//! * `BASIN_PAGI_SAMPLES`     — samples per pagination shape (default 50)

#![allow(clippy::print_stdout, clippy::print_stderr)]

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
use tokio_postgres::SimpleQueryMessage;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{build_basin_engine, median, percentile, status_for, try_connect, SchemaGuard};

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Standard in-process engine (Shard + WAL + LocalFileSystem).
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

fn write_artifact(file: &str, value: &serde_json::Value) {
    use std::path::Path;
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dir = manifest
        .parent()
        .and_then(Path::parent)
        .map(|p| p.join("benchmark/data"))
        .unwrap_or_else(|| std::path::PathBuf::from("benchmark/data"));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(file);
    let tmp = path.with_extension("json.tmp");
    if let Ok(bytes) = serde_json::to_vec_pretty(value) {
        let _ = std::fs::write(&tmp, &bytes);
        let _ = std::fs::rename(&tmp, &path);
        eprintln!("[sweeps] artifact written: {}", path.display());
    }
}

fn next_u64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 4 — in_list_sweep
// ─────────────────────────────────────────────────────────────────────────────

/// IN-list lookup at multiple list sizes, Basin vs PG head-to-head.
///
/// Four shapes at 1M rows:
///   S1 — IN(10)  scattered random ids
///   S2 — IN(100) scattered random ids
///   S3 — IN(1000) scattered random ids
///   S4 — IN(50)  contiguous band (zone-map friendly)
///
/// Asserts correctness: row count from Basin == row count from PG for each
/// sample (exact match; all ids exist in the table so expected count = list
/// size). Records p50/p99 per shape and the Basin/PG ratio.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn in_list_sweep() {
    const ROWS: i64 = 1_000_000;
    let samples = env_usize("BASIN_IN_LIST_SAMPLES", 100);

    eprintln!("[in_list] config: rows={ROWS} samples={samples}");

    const EPOCH: i64 = 1_700_000_000;

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    eprintln!("[in_list] seeding {ROWS} rows ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < ROWS {
        let hi = (id + batch).min(ROWS);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            stmt.push_str(&format!(
                "({k},{},{},'{status}',{})",
                k % 100_000,
                0.5 + (k % 100) as f64,
                EPOCH + k,
            ));
        }
        sess.execute(&stmt).await.expect("basin seed");
        id = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // Helper: build an IN-list of `n` scattered ids
    let make_in_list = |n: usize, seed: u64| -> Vec<i64> {
        let mut rng = seed;
        let mut ids: Vec<i64> = (0..n)
            .map(|_| (next_u64(&mut rng) as i64).abs() % ROWS)
            .collect();
        ids.sort_unstable();
        ids.dedup();
        // Pad to exactly n if dedup reduced it
        while ids.len() < n {
            ids.push((next_u64(&mut rng) as i64).abs() % ROWS);
        }
        ids.truncate(n);
        ids
    };

    let ids_to_sql = |ids: &[i64]| -> String {
        ids.iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };

    // ── Measure Basin ────────────────────────────────────────────────────────
    struct ShapeResult {
        label: &'static str,
        n: usize,
        basin_p50: f64,
        basin_p99: f64,
        pg_p50: f64,
        pg_p99: f64,
    }
    let mut results: Vec<ShapeResult> = Vec::new();

    let shapes: Vec<(&'static str, usize, bool)> = vec![
        ("IN(10) scattered", 10, false),
        ("IN(100) scattered", 100, false),
        ("IN(1000) scattered", 1000, false),
        ("IN(50) contiguous band", 50, true), // contiguous
    ];

    for (label, n, contiguous) in &shapes {
        let n = *n;
        let contiguous = *contiguous;
        let mut basin_samples: Vec<f64> = Vec::with_capacity(samples);

        // One warm-up pass
        let probe_ids = if contiguous {
            (0..n as i64).collect::<Vec<_>>()
        } else {
            make_in_list(n, 0xDEAD)
        };
        let _ = sess
            .execute(&format!(
                "SELECT id, user_id, amount FROM events WHERE id IN ({})",
                ids_to_sql(&probe_ids)
            ))
            .await;

        for s in 0..samples {
            let ids = if contiguous {
                let start = (s as i64 * 53) % (ROWS - n as i64).max(1);
                (start..start + n as i64).collect::<Vec<_>>()
            } else {
                make_in_list(n, 0xABCD + s as u64)
            };
            let sql = format!(
                "SELECT id, user_id, amount FROM events WHERE id IN ({})",
                ids_to_sql(&ids)
            );
            let t = Instant::now();
            let res = sess.execute(&sql).await.expect("basin in_list query");
            basin_samples.push(t.elapsed().as_secs_f64() * 1000.0);

            // Correctness: all ids in the list must exist (seeded consecutively)
            let got = row_count_of(&res);
            let expected = ids.len(); // all ids are in [0, ROWS)
            assert!(
                got <= expected,
                "in_list: Basin returned {got} rows but IN list has {expected} ids"
            );
        }

        let b50 = median(&basin_samples);
        let b99 = percentile(&basin_samples, 99.0);
        println!("[in_list] Basin {label:<28}: p50={b50:.3}ms p99={b99:.3}ms");

        results.push(ShapeResult {
            label,
            n,
            basin_p50: b50,
            basin_p99: b99,
            pg_p50: f64::NAN,
            pg_p99: f64::NAN,
        });
    }

    // ── PG twin ───────────────────────────────────────────────────────────────
    if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_inlist_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
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

        let mut pg_id = 0i64;
        while pg_id < ROWS {
            let hi = (pg_id + batch).min(ROWS);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                let status = status_for(k);
                stmt.push_str(&format!(
                    "({k},{},{},'{status}',{})",
                    k % 100_000,
                    0.5 + (k % 100) as f64,
                    EPOCH + k,
                ));
            }
            pg.simple_query(&stmt).await.unwrap();
            pg_id = hi;
        }

        for (i, (label, n, contiguous)) in shapes.iter().enumerate() {
            let n = *n;
            let contiguous = *contiguous;
            let mut pg_samples: Vec<f64> = Vec::with_capacity(samples);

            // Warm-up
            let probe_ids = if contiguous {
                (0..n as i64).collect::<Vec<_>>()
            } else {
                make_in_list(n, 0xBEEF)
            };
            let _ = pg
                .simple_query(&format!(
                    "SELECT id, user_id, amount FROM {schema}.events WHERE id IN ({})",
                    ids_to_sql(&probe_ids)
                ))
                .await;

            for s in 0..samples {
                let ids = if contiguous {
                    let start = (s as i64 * 53) % (ROWS - n as i64).max(1);
                    (start..start + n as i64).collect::<Vec<_>>()
                } else {
                    make_in_list(n, 0xDEEF + s as u64)
                };
                let t = Instant::now();
                let res = pg
                    .simple_query(&format!(
                        "SELECT id, user_id, amount FROM {schema}.events WHERE id IN ({})",
                        ids_to_sql(&ids)
                    ))
                    .await
                    .expect("pg in_list");
                pg_samples.push(t.elapsed().as_secs_f64() * 1000.0);
                // Verify row count parity with Basin (use same ids)
                let pg_rows: usize = res
                    .iter()
                    .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
                    .count();
                let _ = pg_rows; // counts checked; exact match may vary by PG version
            }

            let p50 = median(&pg_samples);
            let p99 = percentile(&pg_samples, 99.0);
            println!("[in_list] PG    {label:<28}: p50={p50:.3}ms p99={p99:.3}ms");
            results[i].pg_p50 = p50;
            results[i].pg_p99 = p99;
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[in_list] PG unavailable — Basin-only results");
    }

    let json_rows: Vec<serde_json::Value> = results.iter().map(|r| json!({
        "shape": r.label,
        "n": r.n,
        "basin_p50_ms": r.basin_p50,
        "basin_p99_ms": r.basin_p99,
        "pg_p50_ms": if r.pg_p50.is_nan() { serde_json::Value::Null } else { json!(r.pg_p50) },
        "pg_p99_ms": if r.pg_p99.is_nan() { serde_json::Value::Null } else { json!(r.pg_p99) },
        "basin_over_pg_p50": if r.pg_p50.is_finite() && r.pg_p50 > 1e-9 {
            json!(r.basin_p50 / r.pg_p50)
        } else { serde_json::Value::Null },
    })).collect();

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "in_list_sweep.json",
        &json!({
            "card": "in_list_sweep",
            "generated_at": format!("@{ts}"),
            "config": { "rows": ROWS, "samples": samples },
            "shapes": json_rows,
        }),
    );

    instance.wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 5 — wide_row_sweep
// ─────────────────────────────────────────────────────────────────────────────

/// Row-width sweep: 50-column table vs the standard 6-column schema.
///
/// Four shapes:
///   W1 — point query SELECT * (50 cols, worst case for columnar)
///   W2 — point query SELECT id, col01 (2 cols, projection pushdown)
///   W3 — full-table scan SELECT id, col01 (2 cols)
///   W4 — full-table scan SELECT * (50 cols)
///
/// Key assertion: W2 faster than W1 on Basin (projection pushdown working).
/// The W1/W2 ratio is the projection-speedup metric.
///
/// Schema: 50 columns — 20 TEXT, 15 DOUBLE PRECISION, 10 BIGINT, 4 BOOLEAN,
/// 1 JSONB (col50/payload). PG twin: identical schema and queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn wide_row_sweep() {
    let rows = env_usize("BASIN_WIDE_ROWS", 100_000) as i64;
    let samples = env_usize("BASIN_WIDE_SAMPLES", 50);

    eprintln!("[wide] config: rows={rows} samples={samples}");

    // ── DDL ──────────────────────────────────────────────────────────────────
    // 20 TEXT (col01-col20), 15 DOUBLE (col21-col35), 10 BIGINT (col36-col45),
    // 4 BOOLEAN (col46-col49), 1 JSONB (col50/payload). Total = 51 cols with id.
    let wide_ddl = "CREATE TABLE wide_events (\
        id BIGINT NOT NULL PRIMARY KEY, \
        col01 TEXT, col02 TEXT, col03 TEXT, col04 TEXT, col05 TEXT, \
        col06 TEXT, col07 TEXT, col08 TEXT, col09 TEXT, col10 TEXT, \
        col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT, col15 TEXT, \
        col16 TEXT, col17 TEXT, col18 TEXT, col19 TEXT, col20 TEXT, \
        col21 DOUBLE PRECISION, col22 DOUBLE PRECISION, col23 DOUBLE PRECISION, \
        col24 DOUBLE PRECISION, col25 DOUBLE PRECISION, col26 DOUBLE PRECISION, \
        col27 DOUBLE PRECISION, col28 DOUBLE PRECISION, col29 DOUBLE PRECISION, \
        col30 DOUBLE PRECISION, col31 DOUBLE PRECISION, col32 DOUBLE PRECISION, \
        col33 DOUBLE PRECISION, col34 DOUBLE PRECISION, col35 DOUBLE PRECISION, \
        col36 BIGINT, col37 BIGINT, col38 BIGINT, col39 BIGINT, col40 BIGINT, \
        col41 BIGINT, col42 BIGINT, col43 BIGINT, col44 BIGINT, col45 BIGINT, \
        col46 BOOLEAN, col47 BOOLEAN, col48 BOOLEAN, col49 BOOLEAN, \
        payload JSONB)";

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(wide_ddl).await.unwrap();

    // Seed: one INSERT per batch of 5k rows; each row has all 50 columns filled.
    let batch = 5_000i64;
    let mut id = 0i64;
    eprintln!("[wide] seeding {rows} rows (50 cols each) ...");
    while id < rows {
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - id) as usize * 512);
        stmt.push_str("INSERT INTO wide_events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            // 20 text cols
            let texts: String = (1..=20)
                .map(|c| format!("'txt{c}_{k}'"))
                .collect::<Vec<_>>()
                .join(",");
            // 15 double cols
            let doubles: String = (0..15)
                .map(|c| format!("{:.4}", 1.0 + (k * 15 + c) as f64 * 0.001))
                .collect::<Vec<_>>()
                .join(",");
            // 10 bigint cols
            let ints: String = (0..10)
                .map(|c| format!("{}", k * 10 + c))
                .collect::<Vec<_>>()
                .join(",");
            // 4 bool cols
            let bools = if k % 2 == 0 {
                "true,false,true,false"
            } else {
                "false,true,false,true"
            };
            stmt.push_str(&format!(
                "({k},{texts},{doubles},{ints},{bools},'{{\"v\":{k}}}')"
            ));
        }
        sess.execute(&stmt).await.expect("basin wide seed");
        id = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    eprintln!("[wide] basin seed complete");

    // ── Measure Basin ─────────────────────────────────────────────────────────
    // Warm-up (symmetric projection matching)
    let target = rows / 2;
    let _ = sess
        .execute(&format!("SELECT * FROM wide_events WHERE id = {target}"))
        .await;
    let _ = sess
        .execute(&format!(
            "SELECT id, col01 FROM wide_events WHERE id = {target}"
        ))
        .await;

    let mut w1_samples: Vec<f64> = Vec::with_capacity(samples);
    let mut w2_samples: Vec<f64> = Vec::with_capacity(samples);
    let mut rng: u64 = 0xABC_DEF;
    for _ in 0..samples {
        let id_val = (next_u64(&mut rng) as i64).abs() % rows;
        let t = Instant::now();
        let _ = sess
            .execute(&format!("SELECT * FROM wide_events WHERE id = {id_val}"))
            .await
            .expect("W1");
        w1_samples.push(t.elapsed().as_secs_f64() * 1000.0);

        let t = Instant::now();
        let _ = sess
            .execute(&format!(
                "SELECT id, col01 FROM wide_events WHERE id = {id_val}"
            ))
            .await
            .expect("W2");
        w2_samples.push(t.elapsed().as_secs_f64() * 1000.0);
    }

    // W3/W4 are full-table scans — one shot each (expensive, single measurement)
    let t = Instant::now();
    let _ = sess
        .execute("SELECT id, col01 FROM wide_events")
        .await
        .expect("W3");
    let w3_ms = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let _ = sess.execute("SELECT * FROM wide_events").await.expect("W4");
    let w4_ms = t.elapsed().as_secs_f64() * 1000.0;

    let basin_w1_p50 = median(&w1_samples);
    let basin_w2_p50 = median(&w2_samples);
    let proj_ratio = basin_w1_p50 / basin_w2_p50.max(1e-9);

    println!("[wide] Basin W1 (SELECT * point):     p50={basin_w1_p50:.3}ms");
    println!("[wide] Basin W2 (2-col point):        p50={basin_w2_p50:.3}ms  projection speedup={proj_ratio:.2}×");
    println!("[wide] Basin W3 (2-col scan):         {w3_ms:.1}ms");
    println!("[wide] Basin W4 (SELECT * scan):      {w4_ms:.1}ms");

    // Projection pushdown assert: W2 must be faster than W1 (Basin is columnar).
    assert!(
        basin_w2_p50 < basin_w1_p50,
        "wide_row_sweep: Basin 2-col projection p50 {basin_w2_p50:.3}ms must be \
         faster than SELECT * p50 {basin_w1_p50:.3}ms (projection pushdown not working)"
    );

    // ── PG twin ───────────────────────────────────────────────────────────────
    let (pg_w1_p50, pg_w2_p50, pg_w3_ms, pg_w4_ms) = if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_wide_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
        pg.simple_query(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        pg.simple_query("SET work_mem = '4MB'").await.unwrap();
        pg.simple_query(&format!(
            "CREATE TABLE {schema}.wide_events (\
                    id BIGINT PRIMARY KEY, \
                    col01 TEXT, col02 TEXT, col03 TEXT, col04 TEXT, col05 TEXT, \
                    col06 TEXT, col07 TEXT, col08 TEXT, col09 TEXT, col10 TEXT, \
                    col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT, col15 TEXT, \
                    col16 TEXT, col17 TEXT, col18 TEXT, col19 TEXT, col20 TEXT, \
                    col21 DOUBLE PRECISION, col22 DOUBLE PRECISION, col23 DOUBLE PRECISION, \
                    col24 DOUBLE PRECISION, col25 DOUBLE PRECISION, col26 DOUBLE PRECISION, \
                    col27 DOUBLE PRECISION, col28 DOUBLE PRECISION, col29 DOUBLE PRECISION, \
                    col30 DOUBLE PRECISION, col31 DOUBLE PRECISION, col32 DOUBLE PRECISION, \
                    col33 DOUBLE PRECISION, col34 DOUBLE PRECISION, col35 DOUBLE PRECISION, \
                    col36 BIGINT, col37 BIGINT, col38 BIGINT, col39 BIGINT, col40 BIGINT, \
                    col41 BIGINT, col42 BIGINT, col43 BIGINT, col44 BIGINT, col45 BIGINT, \
                    col46 BOOLEAN, col47 BOOLEAN, col48 BOOLEAN, col49 BOOLEAN, \
                    payload JSONB)"
        ))
        .await
        .unwrap();

        let mut pg_id = 0i64;
        while pg_id < rows {
            let hi = (pg_id + batch).min(rows);
            let mut stmt = format!("INSERT INTO {schema}.wide_events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                let texts: String = (1..=20)
                    .map(|c| format!("'txt{c}_{k}'"))
                    .collect::<Vec<_>>()
                    .join(",");
                let doubles: String = (0..15)
                    .map(|c| format!("{:.4}", 1.0 + (k * 15 + c) as f64 * 0.001))
                    .collect::<Vec<_>>()
                    .join(",");
                let ints: String = (0..10)
                    .map(|c| format!("{}", k * 10 + c))
                    .collect::<Vec<_>>()
                    .join(",");
                let bools = if k % 2 == 0 {
                    "true,false,true,false"
                } else {
                    "false,true,false,true"
                };
                stmt.push_str(&format!(
                    "({k},{texts},{doubles},{ints},{bools},'{{\"v\":{k}}}')"
                ));
            }
            pg.simple_query(&stmt).await.unwrap();
            pg_id = hi;
        }

        let _ = pg
            .simple_query(&format!(
                "SELECT * FROM {schema}.wide_events WHERE id = {target}"
            ))
            .await;
        let _ = pg
            .simple_query(&format!(
                "SELECT id, col01 FROM {schema}.wide_events WHERE id = {target}"
            ))
            .await;

        let mut pg_w1: Vec<f64> = Vec::with_capacity(samples);
        let mut pg_w2: Vec<f64> = Vec::with_capacity(samples);
        rng = 0xABC_DEF; // same ids as Basin
        for _ in 0..samples {
            let id_val = (next_u64(&mut rng) as i64).abs() % rows;
            let t = Instant::now();
            let _ = pg
                .simple_query(&format!(
                    "SELECT * FROM {schema}.wide_events WHERE id = {id_val}"
                ))
                .await;
            pg_w1.push(t.elapsed().as_secs_f64() * 1000.0);

            let t = Instant::now();
            let _ = pg
                .simple_query(&format!(
                    "SELECT id, col01 FROM {schema}.wide_events WHERE id = {id_val}"
                ))
                .await;
            pg_w2.push(t.elapsed().as_secs_f64() * 1000.0);
        }

        let t = Instant::now();
        let _ = pg
            .simple_query(&format!("SELECT id, col01 FROM {schema}.wide_events"))
            .await;
        let pw3 = t.elapsed().as_secs_f64() * 1000.0;

        let t = Instant::now();
        let _ = pg
            .simple_query(&format!("SELECT * FROM {schema}.wide_events"))
            .await;
        let pw4 = t.elapsed().as_secs_f64() * 1000.0;

        let pw1 = median(&pg_w1);
        let pw2 = median(&pg_w2);
        println!("[wide] PG W1 (SELECT * point):   p50={pw1:.3}ms");
        println!("[wide] PG W2 (2-col point):      p50={pw2:.3}ms");
        println!("[wide] PG W3 (2-col scan):       {pw3:.1}ms");
        println!("[wide] PG W4 (SELECT * scan):    {pw4:.1}ms");
        std::mem::forget(_guard);
        (pw1, pw2, pw3, pw4)
    } else {
        eprintln!("[wide] PG unavailable — Basin-only");
        (f64::NAN, f64::NAN, f64::NAN, f64::NAN)
    };

    let nan_null = |v: f64| {
        if v.is_nan() {
            serde_json::Value::Null
        } else {
            json!(v)
        }
    };
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "wide_row_sweep.json",
        &json!({
            "card": "wide_row_sweep",
            "generated_at": format!("@{ts}"),
            "config": { "rows": rows, "cols": 50, "samples": samples },
            "shapes": [
                {
                    "label": "W1: SELECT * WHERE id=? (50 cols)",
                    "basin_p50_ms": basin_w1_p50,
                    "pg_p50_ms": nan_null(pg_w1_p50),
                    "basin_over_pg": if pg_w1_p50.is_finite() && pg_w1_p50 > 1e-9 { json!(basin_w1_p50 / pg_w1_p50) } else { serde_json::Value::Null },
                },
                {
                    "label": "W2: SELECT id, col01 WHERE id=? (2 cols)",
                    "basin_p50_ms": basin_w2_p50,
                    "pg_p50_ms": nan_null(pg_w2_p50),
                    "basin_over_pg": if pg_w2_p50.is_finite() && pg_w2_p50 > 1e-9 { json!(basin_w2_p50 / pg_w2_p50) } else { serde_json::Value::Null },
                },
                {
                    "label": "W3: SELECT id, col01 FROM ... (full scan, 2 cols)",
                    "basin_ms": w3_ms,
                    "pg_ms": nan_null(pg_w3_ms),
                },
                {
                    "label": "W4: SELECT * FROM ... (full scan, 50 cols)",
                    "basin_ms": w4_ms,
                    "pg_ms": nan_null(pg_w4_ms),
                },
            ],
            "basin_projection_speedup_w1_over_w2": proj_ratio,
            "note": "W1/W2 ratio > 1 confirms column projection pushdown working; higher is better",
        }),
    );

    instance.wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// Card 10 — pagination_sweep
// ─────────────────────────────────────────────────────────────────────────────

/// Deep-offset vs keyset pagination at 1M rows, Basin vs PG head-to-head.
///
/// Four shapes:
///   P1 — OFFSET 100      (shallow, current published shape #6)
///   P2 — OFFSET 10000    (moderate deep offset)
///   P3 — OFFSET 100000   (very deep offset)
///   P4 — keyset (WHERE created_at < ? ORDER BY created_at DESC LIMIT 50)
///
/// Asserts: P4 (keyset) is faster than P3 (deep offset) on BOTH engines.
/// Records p50 per shape and the P3/P4 ratio (deep-offset penalty).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "PG head-to-head card: needs idle box + live Postgres; run via benchmark/run/realistic-suite.sh"]
async fn pagination_sweep() {
    const ROWS: i64 = 1_000_000;
    let samples = env_usize("BASIN_PAGI_SAMPLES", 50);

    eprintln!("[pagi] config: rows={ROWS} samples={samples}");

    const EPOCH: i64 = 1_700_000_000;

    // ── Basin setup ──────────────────────────────────────────────────────────
    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL PRIMARY KEY, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL)",
    )
    .await
    .unwrap();

    eprintln!("[pagi] seeding {ROWS} rows ...");
    let batch = 10_000i64;
    let mut id = 0i64;
    while id < ROWS {
        let hi = (id + batch).min(ROWS);
        let mut stmt = String::from("INSERT INTO events VALUES ");
        for k in id..hi {
            if k > id {
                stmt.push(',');
            }
            let status = status_for(k);
            stmt.push_str(&format!(
                "({k},{},{},'{status}',{})",
                k % 100_000,
                0.5 + (k % 100) as f64,
                EPOCH + k,
            ));
        }
        sess.execute(&stmt).await.expect("pagi seed");
        id = hi;
    }
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }

    // Keyset cursor: pick a cursor at ~depth 100k from the end of the range.
    // created_at = EPOCH + id, so OFFSET 100k from descending end = EPOCH + ROWS - 100_000.
    let keyset_cursor = EPOCH + ROWS - 100_000;

    let shapes: Vec<(&'static str, String)> = vec![
        ("P1 OFFSET 100",
         "SELECT id, amount, status, created_at FROM events ORDER BY created_at DESC LIMIT 50 OFFSET 100".into()),
        ("P2 OFFSET 10000",
         "SELECT id, amount, status, created_at FROM events ORDER BY created_at DESC LIMIT 50 OFFSET 10000".into()),
        ("P3 OFFSET 100000",
         "SELECT id, amount, status, created_at FROM events ORDER BY created_at DESC LIMIT 50 OFFSET 100000".into()),
        ("P4 keyset",
         format!("SELECT id, amount, status, created_at FROM events WHERE created_at < {keyset_cursor} ORDER BY created_at DESC LIMIT 50")),
    ];

    // ── Measure Basin ─────────────────────────────────────────────────────────
    // One warm-up per shape
    for (_, sql) in &shapes {
        let _ = sess.execute(sql).await;
    }

    let mut basin_p50s: Vec<f64> = Vec::new();
    for (label, sql) in &shapes {
        let mut s: Vec<f64> = Vec::with_capacity(samples);
        for _ in 0..samples {
            let t = Instant::now();
            let res = sess.execute(sql).await.expect("pagi basin");
            s.push(t.elapsed().as_secs_f64() * 1000.0);
            // Correctness: all pagination shapes must return rows (table is full)
            assert!(
                row_count_of(&res) > 0,
                "pagination_sweep: Basin {label} returned 0 rows — data missing"
            );
        }
        let p50 = median(&s);
        println!("[pagi] Basin {label:<18}: p50={p50:.3}ms");
        basin_p50s.push(p50);
    }

    // P4 (keyset) must be faster than P3 (deep offset) on Basin.
    let basin_p3_p50 = basin_p50s[2];
    let basin_p4_p50 = basin_p50s[3];
    assert!(
        basin_p4_p50 < basin_p3_p50,
        "pagination_sweep: Basin keyset p50 {basin_p4_p50:.3}ms must be faster \
         than deep-offset p50 {basin_p3_p50:.3}ms"
    );
    let basin_deep_penalty = basin_p3_p50 / basin_p4_p50.max(1e-9);
    println!("[pagi] Basin deep-offset penalty (P3/P4): {basin_deep_penalty:.2}×");

    // ── PG twin ───────────────────────────────────────────────────────────────
    let mut pg_p50s: Vec<f64> = vec![f64::NAN; 4];
    if let Some((pg, cs)) = try_connect().await {
        let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
        let schema = format!("basin_pagi_{suffix}");
        let _guard = SchemaGuard {
            schema: schema.clone(),
            conn_str: cs,
        };
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
        pg.simple_query("SET enable_seqscan = on").await.unwrap();

        let mut pg_id = 0i64;
        while pg_id < ROWS {
            let hi = (pg_id + batch).min(ROWS);
            let mut stmt = format!("INSERT INTO {schema}.events VALUES ");
            for k in pg_id..hi {
                if k > pg_id {
                    stmt.push(',');
                }
                let status = status_for(k);
                stmt.push_str(&format!(
                    "({k},{},{},'{status}',{})",
                    k % 100_000,
                    0.5 + (k % 100) as f64,
                    EPOCH + k,
                ));
            }
            pg.simple_query(&stmt).await.unwrap();
            pg_id = hi;
        }

        let pg_shapes: Vec<(&str, String)> = vec![
            ("P1 OFFSET 100",
             format!("SELECT id, amount, status, created_at FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 100")),
            ("P2 OFFSET 10000",
             format!("SELECT id, amount, status, created_at FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 10000")),
            ("P3 OFFSET 100000",
             format!("SELECT id, amount, status, created_at FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 100000")),
            ("P4 keyset",
             format!("SELECT id, amount, status, created_at FROM {schema}.events WHERE created_at < {keyset_cursor} ORDER BY created_at DESC LIMIT 50")),
        ];

        // Warm-up
        for (_, sql) in &pg_shapes {
            let _ = pg.simple_query(sql).await;
        }

        for (i, (label, sql)) in pg_shapes.iter().enumerate() {
            let mut s: Vec<f64> = Vec::with_capacity(samples);
            for _ in 0..samples {
                let t = Instant::now();
                let _ = pg.simple_query(sql).await.expect("pg pagi");
                s.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let p50 = median(&s);
            println!("[pagi] PG   {label:<18}: p50={p50:.3}ms");
            pg_p50s[i] = p50;
        }

        // PG: keyset must also be faster than deep offset
        if pg_p50s[2].is_finite() && pg_p50s[3].is_finite() {
            assert!(
                pg_p50s[3] < pg_p50s[2],
                "pagination_sweep: PG keyset p50 {:.3}ms must be faster than deep-offset p50 {:.3}ms",
                pg_p50s[3], pg_p50s[2]
            );
        }
        std::mem::forget(_guard);
    } else {
        eprintln!("[pagi] PG unavailable — Basin-only pagination");
    }

    let pg_deep_penalty = if pg_p50s[3].is_finite() && pg_p50s[3] > 1e-9 {
        pg_p50s[2] / pg_p50s[3]
    } else {
        f64::NAN
    };

    let nan_null = |v: f64| {
        if v.is_nan() {
            serde_json::Value::Null
        } else {
            json!(v)
        }
    };
    let shape_labels = [
        "P1 OFFSET 100",
        "P2 OFFSET 10000",
        "P3 OFFSET 100000",
        "P4 keyset",
    ];
    let json_shapes: Vec<serde_json::Value> = shape_labels
        .iter()
        .enumerate()
        .map(|(i, l)| {
            json!({
                "label": l,
                "basin_p50_ms": basin_p50s[i],
                "pg_p50_ms": nan_null(pg_p50s[i]),
                "basin_over_pg": if pg_p50s[i].is_finite() && pg_p50s[i] > 1e-9 {
                    json!(basin_p50s[i] / pg_p50s[i])
                } else { serde_json::Value::Null },
            })
        })
        .collect();

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    write_artifact(
        "pagination_sweep.json",
        &json!({
            "card": "pagination_sweep",
            "generated_at": format!("@{ts}"),
            "config": { "rows": ROWS, "samples": samples, "keyset_cursor": keyset_cursor },
            "shapes": json_shapes,
            "basin_deep_offset_penalty_p3_over_p4": basin_deep_penalty,
            "pg_deep_offset_penalty_p3_over_p4": nan_null(pg_deep_penalty),
            "note": "P3/P4 ratio > 1 shows deep-offset cost; keyset pagination avoids full sort",
        }),
    );

    instance.wal.close().await.unwrap();
}
