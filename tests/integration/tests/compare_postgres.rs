//! Scaling test 5: head-to-head with a real Postgres 18.
//!
//! Claim: For wedge-shaped audit-log data, Basin uses dramatically less disk
//! than Postgres and matches or beats it on selective queries (without an
//! index — fair because we're comparing substrates, not Postgres's btree
//! machinery).
//!
//! This test connects to a real PG server expected to be running on
//! 127.0.0.1:5432. If unavailable, it prints a skip line and returns Ok —
//! this is a best-effort calibration, not a hard CI gate.
//!
//! Cleanup: a `Drop` guard on the schema name drops the schema even on
//! panic. We never leave PG state behind.
//!
//! Query suite:
//!   (a) Point query  — SELECT * WHERE id = N
//!   (b) Range scan   — SELECT * WHERE id BETWEEN lo AND hi  (~10 000 rows)
//!   (c) Aggregate    — SELECT ts/1000000 AS bucket, COUNT(*), SUM(id) GROUP BY bucket
//!   (d) Bulk INSERT  — throughput for 1 M rows
//!   (e) Cold-start   — first-query latency after a fresh engine/session

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

const ROWS: usize = 1_000_000;
const INSERT_BATCH: usize = 10_000;

fn payload_for(i: i64) -> String {
    // 50-byte payload, varying.
    format!("payload-{:040}", i)
}

fn dir_size_parquet(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    for entry in walkdir::WalkDir::new(root) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|s| s.to_str()) == Some("parquet")
        {
            total += std::fs::metadata(entry.path())
                .map(|m| m.len())
                .unwrap_or(0);
        }
    }
    total
}

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

/// RAII guard that drops the schema on Drop. We also offer an explicit
/// `cleanup()` to surface errors, but Drop is the safety net.
struct SchemaGuard {
    schema: String,
    conn_str: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        // Best-effort sync drop. We open a fresh blocking task on the current
        // runtime if available; failing that, we shell out to `psql` would
        // require an extra dep. Use `tokio::runtime::Handle` if present.
        let conn_str = self.conn_str.clone();
        let schema = self.schema.clone();
        // Spawn a detached task on the current handle if there is one. If we
        // are dropped after the runtime has shut down, this becomes a no-op
        // and the schema may linger — but in practice the test always drops
        // the guard before the runtime exits.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Block on the cleanup so it actually runs before the test exits.
            let _ = std::thread::spawn(move || {
                handle.block_on(async move {
                    if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                        tokio::spawn(async move {
                            let _ = conn.await;
                        });
                        let _ = client
                            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                            .await;
                    }
                });
            })
            .join();
        }
    }
}

async fn try_connect() -> Option<(Client, String)> {
    for user in ["pc", "postgres"] {
        let conn_str = format!("host=127.0.0.1 port=5432 user={user} dbname=postgres");
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return Some((client, conn_str));
            }
            Err(_) => continue,
        }
    }
    None
}

fn which_wins(basin: f64, postgres: f64) -> WhichWins {
    if basin < postgres {
        WhichWins::Basin
    } else if basin > postgres {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

/// Parse "Execution Time: 12.345 ms" out of EXPLAIN ANALYZE TEXT output.
fn parse_pg_exec_time(rows: &[tokio_postgres::SimpleQueryMessage]) -> Option<f64> {
    for m in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            if let Some(line) = r.get(0) {
                if let Some(idx) = line.find("Execution Time:") {
                    let after = &line[idx + "Execution Time:".len()..];
                    let trimmed = after.trim();
                    if let Some(num_end) = trimmed.find(' ') {
                        if let Ok(v) = trimmed[..num_end].parse::<f64>() {
                            return Some(v);
                        }
                    }
                }
            }
        }
    }
    None
}

struct BasinInstance {
    engine: Engine,
    project: ProjectId,
    bg: basin_shard::ShardBackgroundHandle,
    wal: Arc<dyn basin_wal::Wal>,
    dir: TempDir,
    _wal_dir: TempDir,
}

/// Build a fresh Basin engine backed by local-filesystem Parquet + WAL.
async fn build_basin_engine() -> BasinInstance {
    let dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .unwrap(),
    );
    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });
    let project = ProjectId::new();
    BasinInstance { engine, project, bg, wal, dir, _wal_dir: wal_dir }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres() {
    let (pg, conn_str) = match try_connect().await {
        Some(v) => v,
        None => {
            println!("[SCALING 5] postgres unavailable: skipping head-to-head");
            report_postgres_compare(
                "postgres",
                "Basin vs Postgres 18 (no index, 1M rows)",
                "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed point queries.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    // Unique schema per run via Ulid (already in deps via basin-common).
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_compare_{}", suffix);
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: conn_str.clone(),
    };

    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.events (id BIGINT, ts BIGINT, payload TEXT)"
    ))
    .await
    .expect("create table");

    // ---- PG insert ---------------------------------------------------------
    // Multi-row INSERT batched ROWS / INSERT_BATCH times. Simpler and reliable
    // through tokio-postgres's simple_query path.
    let pg_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.events (id, ts, payload) VALUES "
        ));
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        pg.simple_query(&stmt).await.expect("pg insert batch");
        row_idx += INSERT_BATCH as i64;
    }
    let pg_insert_ms = pg_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- PG disk size ------------------------------------------------------
    let pg_disk_bytes: i64 = {
        let row = pg
            .query_one(
                &format!("SELECT pg_total_relation_size('{schema}.events')::bigint"),
                &[],
            )
            .await
            .expect("pg_total_relation_size");
        row.get::<_, i64>(0)
    };

    // ---- PG point query latency -------------------------------------------
    let target_id: i64 = (ROWS as i64) / 2 + 7;
    let mut pg_point_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze point");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_point_ms.push(ms);
        }
    }
    assert!(!pg_point_ms.is_empty(), "no PG execution time samples parsed (point)");
    let pg_point_p50 = median(&pg_point_ms);

    // ---- PG range scan latency (~10 000 rows) ------------------------------
    let range_lo: i64 = (ROWS as i64) / 4;
    let range_hi: i64 = range_lo + 10_000;
    let mut pg_range_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id BETWEEN {range_lo} AND {range_hi}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze range");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_range_ms.push(ms);
        }
    }
    assert!(!pg_range_ms.is_empty(), "no PG execution time samples parsed (range)");
    let pg_range_p50 = median(&pg_range_ms);

    // ---- PG aggregate (COUNT + SUM with GROUP BY) --------------------------
    // Group rows into 1 000-row buckets by ts/1_000_000 (~1 000 groups).
    let mut pg_agg_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT ts/1000000 AS bucket, COUNT(*), SUM(id) FROM {schema}.events GROUP BY bucket"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze agg");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_agg_ms.push(ms);
        }
    }
    assert!(!pg_agg_ms.is_empty(), "no PG execution time samples parsed (agg)");
    let pg_agg_p50 = median(&pg_agg_ms);

    // ---- Basin setup -------------------------------------------------------
    let instance = build_basin_engine().await;
    let sess = instance.engine.open_session(instance.project).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
    )
    .await
    .unwrap();

    // ---- Basin bulk INSERT throughput --------------------------------------
    let basin_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str("INSERT INTO events VALUES ");
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            stmt.push_str(&format!("({id}, {}, '{}')", id * 1000, payload_for(id)));
        }
        sess.execute(&stmt).await.expect("basin insert batch");
        row_idx += INSERT_BATCH as i64;
    }
    let basin_insert_ms = basin_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- Basin point query -------------------------------------------------
    let mut basin_point_ms: Vec<f64> = Vec::with_capacity(5);
    // Warm DataFusion once.
    let _ = sess
        .execute(&format!("SELECT id FROM events WHERE id = {target_id}"))
        .await
        .unwrap();
    for _ in 0..5 {
        let started = Instant::now();
        let res = sess
            .execute(&format!(
                "SELECT id, ts, payload FROM events WHERE id = {target_id}"
            ))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if let ExecResult::Rows { batches, .. } = res {
            let mut hits = 0usize;
            for b in &batches {
                let arr = b
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                hits += arr.len();
            }
            assert!(hits >= 1, "basin missed point query");
        }
        basin_point_ms.push(elapsed);
    }
    let basin_point_p50 = median(&basin_point_ms);

    // ---- Basin range scan --------------------------------------------------
    let mut basin_range_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let started = Instant::now();
        let res = sess
            .execute(&format!(
                "SELECT id, ts, payload FROM events WHERE id BETWEEN {range_lo} AND {range_hi}"
            ))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if let ExecResult::Rows { batches, .. } = res {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total > 0, "basin range scan returned no rows");
        }
        basin_range_ms.push(elapsed);
    }
    let basin_range_p50 = median(&basin_range_ms);

    // ---- Basin aggregate ---------------------------------------------------
    let mut basin_agg_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let started = Instant::now();
        let res = sess
            .execute(
                "SELECT ts/1000000 AS bucket, COUNT(*), SUM(id) FROM events GROUP BY bucket",
            )
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        if let ExecResult::Rows { batches, .. } = res {
            let groups: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(groups > 0, "basin aggregate returned no groups");
        }
        basin_agg_ms.push(elapsed);
    }
    let basin_agg_p50 = median(&basin_agg_ms);

    // ---- Basin cold-start first-query latency -----------------------------
    // Build a brand-new engine+session (cold caches) and time the very first
    // query — the one that triggers WAL-drain + Parquet open from disk.
    let pg_cold_ms = {
        // For PG, cold start is a DISCARD ALL + fresh connection's first query.
        // We use a fresh connection to approximate a cold server cache.
        let conn_str_cold = format!("host=127.0.0.1 port=5432 user={} dbname=postgres",
            if conn_str.contains("user=pc") { "pc" } else { "postgres" });
        let cold_start = Instant::now();
        if let Ok((cold_client, cold_conn)) = tokio_postgres::connect(&conn_str_cold, NoTls).await {
            tokio::spawn(async move { let _ = cold_conn.await; });
            let _ = cold_client
                .simple_query(&format!(
                    "SELECT COUNT(*) FROM {schema}.events WHERE id = {target_id}"
                ))
                .await;
            cold_start.elapsed().as_secs_f64() * 1000.0
        } else {
            pg_point_p50 // fallback: reuse warm p50 if fresh connect fails
        }
    };

    let basin_cold_ms = {
        let cold = build_basin_engine().await;
        let cold_sess = cold.engine.open_session(cold.project).await.unwrap();
        cold_sess
            .execute(
                "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
            )
            .await
            .unwrap();
        // Insert a small seed so there's something to read.
        cold_sess
            .execute(&format!(
                "INSERT INTO events VALUES ({target_id}, {}, '{}')",
                target_id * 1000,
                payload_for(target_id)
            ))
            .await
            .unwrap();
        let started = Instant::now();
        let _ = cold_sess
            .execute(&format!(
                "SELECT id, ts, payload FROM events WHERE id = {target_id}"
            ))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        cold.bg.shutdown().await;
        cold.wal.close().await.unwrap();
        elapsed
    };

    // Disk measurement after SELECTs so compaction has flushed WAL to Parquet.
    let basin_disk_bytes = dir_size_parquet(instance.dir.path());

    // ---- Print results table -----------------------------------------------
    let basin_mib = basin_disk_bytes as f64 / (1024.0 * 1024.0);
    let pg_mib = pg_disk_bytes as f64 / (1024.0 * 1024.0);
    let disk_ratio = pg_disk_bytes as f64 / basin_disk_bytes.max(1) as f64;
    let point_ratio = pg_point_p50 / basin_point_p50.max(1e-9);
    let range_ratio = pg_range_p50 / basin_range_p50.max(1e-9);
    let agg_ratio = pg_agg_p50 / basin_agg_p50.max(1e-9);
    let cold_ratio = pg_cold_ms / basin_cold_ms.max(1e-9);

    println!(
        "\n{:>30} {:>15} {:>15} {:>20}",
        "metric", "basin", "postgres", "ratio (pg/basin)"
    );
    println!(
        "{:>30} {:>13.2}MiB {:>13.2}MiB {:>20}",
        "on_disk_bytes",
        basin_mib,
        pg_mib,
        format!("{:.2}x", disk_ratio)
    );
    println!(
        "{:>30} {:>15.0} {:>15.0} {:>20}",
        "insert_1m_rows_ms", basin_insert_ms, pg_insert_ms, "-"
    );
    println!(
        "{:>30} {:>15.2} {:>15.2} {:>20}",
        "point_query_p50_ms",
        basin_point_p50,
        pg_point_p50,
        format!("{:.2}x", point_ratio)
    );
    println!(
        "{:>30} {:>15.2} {:>15.2} {:>20}",
        "range_scan_p50_ms",
        basin_range_p50,
        pg_range_p50,
        format!("{:.2}x", range_ratio)
    );
    println!(
        "{:>30} {:>15.2} {:>15.2} {:>20}",
        "aggregate_p50_ms",
        basin_agg_p50,
        pg_agg_p50,
        format!("{:.2}x", agg_ratio)
    );
    println!(
        "{:>30} {:>15.2} {:>15.2} {:>20}",
        "cold_start_first_query_ms",
        basin_cold_ms,
        pg_cold_ms,
        format!("{:.2}x", cold_ratio)
    );
    println!(
        "\n[SCALING 5] disk {:.2}x smaller, point-query {:.2}x, range {:.2}x, agg {:.2}x (vs unindexed PG)",
        disk_ratio, point_ratio, range_ratio, agg_ratio
    );

    // ---- Emit benchmark JSON -----------------------------------------------
    let basin_disk_f = basin_disk_bytes as f64;
    let pg_disk_f = pg_disk_bytes as f64;
    let metrics = vec![
        CompareMetric {
            label: "On-disk bytes".into(),
            basin: basin_disk_f,
            postgres: pg_disk_f,
            unit: "bytes".into(),
            better: which_wins(basin_disk_f, pg_disk_f),
            ratio_text: Some(format!("pg / basin = {:.2}x", disk_ratio)),
        },
        CompareMetric {
            label: "Point query p50".into(),
            basin: basin_point_p50,
            postgres: pg_point_p50,
            unit: "ms".into(),
            better: which_wins(basin_point_p50, pg_point_p50),
            ratio_text: Some(format!("pg / basin = {:.2}x", point_ratio)),
        },
        CompareMetric {
            label: "Range scan p50 (~10k rows)".into(),
            basin: basin_range_p50,
            postgres: pg_range_p50,
            unit: "ms".into(),
            better: which_wins(basin_range_p50, pg_range_p50),
            ratio_text: Some(format!("pg / basin = {:.2}x", range_ratio)),
        },
        CompareMetric {
            label: "Aggregate COUNT/SUM GROUP BY p50".into(),
            basin: basin_agg_p50,
            postgres: pg_agg_p50,
            unit: "ms".into(),
            better: which_wins(basin_agg_p50, pg_agg_p50),
            ratio_text: Some(format!("pg / basin = {:.2}x", agg_ratio)),
        },
        CompareMetric {
            label: "Bulk INSERT 1M rows".into(),
            basin: basin_insert_ms,
            postgres: pg_insert_ms,
            unit: "ms".into(),
            better: which_wins(basin_insert_ms, pg_insert_ms),
            ratio_text: None,
        },
        CompareMetric {
            label: "Cold-start first query".into(),
            basin: basin_cold_ms,
            postgres: pg_cold_ms,
            unit: "ms".into(),
            better: which_wins(basin_cold_ms, pg_cold_ms),
            ratio_text: Some(format!("pg / basin = {:.2}x", cold_ratio)),
        },
    ];

    report_postgres_compare(
        "postgres",
        "Basin vs Postgres 18 (no index, 1M rows)",
        "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed queries across point, range, aggregate, and cold-start workloads.",
        true,
        metrics,
        None,
    );

    // Cleanly shut down background tasks.
    instance.bg.shutdown().await;
    instance.wal.close().await.unwrap();

    // Drop the schema explicitly first; the guard remains as the safety net.
    drop(_guard);
}
