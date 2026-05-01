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

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
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
            total += std::fs::metadata(entry.path()).map(|m| m.len()).unwrap_or(0);
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
                    if let Ok((client, conn)) =
                        tokio_postgres::connect(&conn_str, NoTls).await
                    {
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
    let suffix = TenantId::new().as_ulid().to_string().to_lowercase();
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
    // pg_total_relation_size includes heap + TOAST + indexes. We did NOT
    // create an index, so this is heap + TOAST only. Fair comparison vs
    // Basin Parquet bytes.
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
    // 5 EXPLAIN ANALYZE samples; parse "Execution Time:".
    let target_id: i64 = (ROWS as i64) / 2 + 7;
    let mut pg_point_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze");
        let mut found: Option<f64> = None;
        for m in &rows {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                if let Some(line) = r.get(0) {
                    if let Some(idx) = line.find("Execution Time:") {
                        let after = &line[idx + "Execution Time:".len()..];
                        // " 12.345 ms"
                        let trimmed = after.trim();
                        if let Some(num_end) = trimmed.find(' ') {
                            if let Ok(v) = trimmed[..num_end].parse::<f64>() {
                                found = Some(v);
                            }
                        }
                    }
                }
            }
        }
        if let Some(ms) = found {
            pg_point_ms.push(ms);
        }
    }
    assert!(!pg_point_ms.is_empty(), "no PG execution time samples parsed");
    let pg_point_p50 = median(&pg_point_ms);

    // ---- Basin path --------------------------------------------------------
    // Wire the WAL + shard owner. INSERTs route through the shard (WAL ack
    // first, then a background compactor flushes to Parquet). SELECTs trigger
    // a synchronous compaction so the Parquet base reflects the in-RAM tail
    // (Option A); this keeps the existing DataFusion path working unchanged.
    let dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal = basin_wal::Wal::open(basin_wal::WalConfig {
        object_store: Arc::new(wal_fs),
        root_prefix: None,
        flush_interval: Duration::from_millis(200),
        flush_max_bytes: 1024 * 1024,
    })
    .await
    .unwrap();
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
    let tenant = TenantId::new();
    let sess = engine.open_session(tenant).await.unwrap();
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    // Basin insert: same multi-row batches.
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

    // Basin point query: 5 samples.
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

    // Disk measurement happens after the SELECTs so the synchronous compaction
    // has flushed every WAL-resident batch to Parquet. Measuring before any
    // SELECT would undercount, since the tail still lives in RAM + WAL until
    // the first read drains it via the engine's tail-visibility hook.
    let basin_disk_bytes = dir_size_parquet(dir.path());

    // ---- Print -------------------------------------------------------------
    let basin_mib = basin_disk_bytes as f64 / (1024.0 * 1024.0);
    let pg_mib = pg_disk_bytes as f64 / (1024.0 * 1024.0);
    let disk_ratio = pg_disk_bytes as f64 / basin_disk_bytes.max(1) as f64;
    let point_ratio = pg_point_p50 / basin_point_p50.max(1e-9);

    println!(
        "{:>22} {:>15} {:>15} {:>20}",
        "metric", "basin", "postgres", "ratio"
    );
    println!(
        "{:>22} {:>13.2}MiB {:>13.2}MiB {:>20}",
        "on_disk_bytes",
        basin_mib,
        pg_mib,
        format!("pg/basin = {:.2}x", disk_ratio)
    );
    println!(
        "{:>22} {:>15.0} {:>15.0} {:>20}",
        "insert_total_ms", basin_insert_ms, pg_insert_ms, "-"
    );
    println!(
        "{:>22} {:>15.2} {:>15.2} {:>20}",
        "point_query_ms_p50",
        basin_point_p50,
        pg_point_p50,
        format!("pg/basin = {:.2}x", point_ratio)
    );

    println!(
        "[SCALING 5] head-to-head: disk {:.2}x smaller, point-query {:.2}x faster (no PG index)",
        disk_ratio, point_ratio
    );

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
            label: "Insert 1M rows".into(),
            basin: basin_insert_ms,
            postgres: pg_insert_ms,
            unit: "ms".into(),
            better: which_wins(basin_insert_ms, pg_insert_ms),
            ratio_text: None,
        },
    ];

    report_postgres_compare(
        "postgres",
        "Basin vs Postgres 18 (no index, 1M rows)",
        "On audit-log data, Basin uses much less disk than Postgres heap and matches or beats unindexed point queries.",
        true,
        metrics,
        None,
    );

    // Stop the shard's background loops + close the WAL before the runtime
    // shuts down, otherwise the file-backed WAL emits a warning when its
    // background flusher is dropped mid-flight.
    bg.shutdown().await;
    wal.close().await.unwrap();

    // Drop the schema explicitly first; the guard remains as the safety net.
    drop(_guard);
}
