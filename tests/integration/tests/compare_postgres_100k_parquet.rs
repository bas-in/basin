//! Parquet variant of `compare_postgres_100k.rs` — Basin pinned to
//! `basin.file_format='parquet'` for the 100k-row SaaS / OLTP workload, so
//! the dashboard renders BOTH Basin storage modes (Vortex default vs
//! Parquet legacy / read-compat option) head-to-head against PG 18.
//!
//! Same query suite, same shape of card, same fairness rules. The ONLY
//! difference is the `WITH (basin.file_format='parquet')` on each CREATE
//! TABLE and the report id / name so the cards live side-by-side.
//!
//! See `compare_postgres_100k.rs` for the full doc on the suite of 12
//! queries, the schema-drop guard, and the bench-card contract.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

const ROWS: usize = 100_000;
const INSERT_BATCH: usize = 5_000;

fn email_for(i: i64) -> String {
    let domain = match i % 10 {
        0 => "gmail.com",
        1 => "outlook.com",
        2 => "yahoo.com",
        3 => "proton.me",
        4 => "icloud.com",
        5 => "company.io",
        6 => "example.org",
        7 => "test.dev",
        8 => "fastmail.com",
        _ => "tutanota.com",
    };
    format!("user{:08}@{}", i, domain)
}

fn status_for(i: i64) -> &'static str {
    match i % 4 {
        0 => "pending",
        1 => "active",
        2 => "completed",
        _ => "archived",
    }
}

const EPOCH: i64 = 1_700_000_000;

/// Dual-extension data-file size counter — see `compare_postgres.rs` for
/// the long story on why this counts both `.vortex` and `.parquet`.
fn dir_size_data(root: &std::path::Path) -> u64 {
    let mut total = 0u64;
    for entry in walkdir::WalkDir::new(root) {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        if entry.file_type().is_file() {
            let ext = entry.path().extension().and_then(|s| s.to_str());
            if matches!(ext, Some("parquet") | Some("vortex")) {
                total += std::fs::metadata(entry.path())
                    .map(|m| m.len())
                    .unwrap_or(0);
            }
        }
    }
    total
}

fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (s.len() as f64 - 1.0)).round() as usize;
    s[idx.min(s.len() - 1)]
}

fn median(samples: &[f64]) -> f64 {
    percentile(samples, 50.0)
}

struct SchemaGuard {
    schema: String,
    conn_str: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let conn_str = self.conn_str.clone();
        let schema = self.schema.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
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
    BasinInstance {
        engine,
        project,
        bg,
        wal,
        dir,
        _wal_dir: wal_dir,
    }
}

async fn basin_timed(
    sess: &basin_engine::ProjectSession,
    sql: &str,
    expect_rows: bool,
) -> f64 {
    let started = Instant::now();
    let res = sess.execute(sql).await.unwrap();
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;
    if expect_rows {
        if let ExecResult::Rows { batches, .. } = res {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(total > 0, "basin query returned no rows: {sql}");
        }
    }
    elapsed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_100k_parquet() {
    let (pg, conn_str) = match try_connect().await {
        Some(v) => v,
        None => {
            println!("[COMPARE 100k / PARQUET] postgres unavailable: skipping head-to-head");
            report_postgres_compare(
                "postgres_100k_parquet",
                "Basin (Parquet) vs Postgres 18 (100k-row SaaS workload, no index)",
                "Same 100k-row SaaS workload as the Vortex card, but Basin is pinned to \
                 basin.file_format='parquet' so the dashboard can show both Basin storage \
                 modes side-by-side against PG.",
                false,
                vec![],
                Some("postgres unavailable on 127.0.0.1:5432"),
            );
            return;
        }
    };

    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_compare100kpq_{suffix}");
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: conn_str.clone(),
    };

    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.users (\
            id BIGINT PRIMARY KEY, \
            email TEXT, \
            created_at TIMESTAMPTZ)"
    ))
    .await
    .expect("pg create users");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.events (\
            id BIGINT PRIMARY KEY, \
            user_id BIGINT, \
            amount DOUBLE PRECISION, \
            status TEXT, \
            created_at TIMESTAMPTZ)"
    ))
    .await
    .expect("pg create events");
    pg.simple_query(&format!(
        "ALTER TABLE {schema}.users DROP CONSTRAINT users_pkey"
    ))
    .await
    .expect("drop users pkey");
    pg.simple_query(&format!(
        "ALTER TABLE {schema}.events DROP CONSTRAINT events_pkey"
    ))
    .await
    .expect("drop events pkey");

    const USERS: usize = 10_000;
    let user_count = USERS as i64;

    // ---- PG seed users -----------------------------------------------------
    {
        let mut stmt = String::with_capacity(USERS * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.users (id, email, created_at) VALUES "
        ));
        for i in 0..USERS as i64 {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!(
                "({i}, '{}', to_timestamp({}))",
                email_for(i),
                EPOCH + i
            ));
        }
        pg.simple_query(&stmt).await.expect("pg seed users");
    }

    // ---- PG bulk INSERT 100k events ----------------------------------------
    let pg_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < ROWS {
        let mut stmt = String::with_capacity(INSERT_BATCH * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.events (id, user_id, amount, status, created_at) VALUES "
        ));
        for j in 0..INSERT_BATCH {
            if j > 0 {
                stmt.push(',');
            }
            let id = row_idx + j as i64;
            let user_id = id % user_count;
            let amount = (id as f64) * 0.5;
            let status = status_for(id);
            stmt.push_str(&format!(
                "({id}, {user_id}, {amount}, '{status}', to_timestamp({}))",
                EPOCH + id
            ));
        }
        pg.simple_query(&stmt).await.expect("pg insert events batch");
        row_idx += INSERT_BATCH as i64;
    }
    let pg_insert_ms = pg_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- PG disk size ------------------------------------------------------
    let pg_disk_bytes: i64 = {
        let row = pg
            .query_one(
                &format!(
                    "SELECT pg_total_relation_size('{schema}.users')::bigint + \
                            pg_total_relation_size('{schema}.events')::bigint"
                ),
                &[],
            )
            .await
            .expect("pg_total_relation_size");
        row.get::<_, i64>(0)
    };

    let target_id: i64 = (ROWS as i64) / 2 + 7;
    let range_lo_ts = EPOCH + (ROWS as i64) / 4;
    let range_hi_ts = range_lo_ts + 1_000;
    let pagination_threshold = EPOCH + (ROWS as i64) / 3;
    let _ = pg
        .simple_query(&format!(
            "SELECT COUNT(*) FROM {schema}.events WHERE id = {target_id}"
        ))
        .await;

    // ---- PG measure: point query ------------------------------------------
    let mut pg_point_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze point");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_point_ms.push(ms);
        }
    }
    let pg_point_p50 = median(&pg_point_ms);
    let pg_point_p99 = percentile(&pg_point_ms, 99.0);

    // ---- PG measure: range scan -------------------------------------------
    let mut pg_range_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events \
             WHERE created_at BETWEEN to_timestamp({range_lo_ts}) AND to_timestamp({range_hi_ts})"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze range");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_range_ms.push(ms);
        }
    }
    let pg_range_p50 = median(&pg_range_ms);
    let pg_range_p99 = percentile(&pg_range_ms, 99.0);

    // ---- PG measure: aggregate --------------------------------------------
    let mut pg_agg_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT user_id, COUNT(*), SUM(amount) \
             FROM {schema}.events GROUP BY user_id ORDER BY 2 DESC LIMIT 10"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze agg");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_agg_ms.push(ms);
        }
    }
    let pg_agg_p50 = median(&pg_agg_ms);

    // ---- PG measure: 2-table JOIN -----------------------------------------
    let mut pg_join_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT u.email, COUNT(e.id) \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             GROUP BY u.email ORDER BY 2 DESC LIMIT 20"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze join");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_join_ms.push(ms);
        }
    }
    let pg_join_p50 = median(&pg_join_ms);

    // ---- PG measure: ILIKE pattern ----------------------------------------
    let mut pg_ilike_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, email FROM {schema}.users \
             WHERE email ILIKE '%@gmail.com'"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze ilike");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_ilike_ms.push(ms);
        }
    }
    let pg_ilike_p50 = median(&pg_ilike_ms);

    // ---- PG measure: pagination -------------------------------------------
    let mut pg_page_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, amount, status, created_at \
             FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 100"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze pagination");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_page_ms.push(ms);
        }
    }
    let pg_page_p50 = median(&pg_page_ms);

    // ---- PG measure: single-row UPDATE ------------------------------------
    let mut pg_upd1_ms: Vec<f64> = Vec::with_capacity(5);
    for i in 0..5 {
        let uid = i as i64;
        let new_email = format!("rotated{i}@example.org");
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) UPDATE {schema}.users \
             SET email = '{new_email}' WHERE id = {uid}"
        );
        let rows = pg.simple_query(&q).await.expect("explain analyze upd1");
        if let Some(ms) = parse_pg_exec_time(&rows) {
            pg_upd1_ms.push(ms);
        }
    }
    let pg_upd1_p50 = median(&pg_upd1_ms);

    // ---- PG measure: bulk UPDATE ------------------------------------------
    let pg_bulk_upd_ms: f64 = {
        let started = Instant::now();
        pg.simple_query(&format!(
            "UPDATE {schema}.events SET status = 'expired' \
             WHERE created_at < to_timestamp({pagination_threshold})"
        ))
        .await
        .expect("pg bulk update");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- PG measure: DELETE WHERE IN --------------------------------------
    let delete_ids: Vec<i64> = (0..10).map(|k| (ROWS as i64) - 1 - k).collect();
    let delete_in_list = delete_ids
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let pg_delete_ms: f64 = {
        let started = Instant::now();
        pg.simple_query(&format!(
            "DELETE FROM {schema}.events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("pg delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- Basin setup (Parquet-pinned) --------------------------------------
    let instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(
        "CREATE TABLE users (\
            id BIGINT NOT NULL, \
            email TEXT NOT NULL, \
            created_at BIGINT NOT NULL) \
         WITH (basin.file_format='parquet')",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE TABLE events (\
            id BIGINT NOT NULL, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL) \
         WITH (basin.file_format='parquet')",
    )
    .await
    .unwrap();

    // ---- Basin seed users -------------------------------------------------
    {
        let mut stmt = String::with_capacity(USERS * 70);
        stmt.push_str("INSERT INTO users VALUES ");
        for i in 0..USERS as i64 {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({i}, '{}', {})", email_for(i), EPOCH + i));
        }
        sess.execute(&stmt).await.expect("basin seed users");
    }

    // ---- Basin bulk INSERT 100k events ------------------------------------
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
            let user_id = id % user_count;
            let amount = (id as f64) * 0.5;
            let status = status_for(id);
            stmt.push_str(&format!(
                "({id}, {user_id}, {amount}, '{status}', {})",
                EPOCH + id
            ));
        }
        sess.execute(&stmt).await.expect("basin insert events batch");
        row_idx += INSERT_BATCH as i64;
    }
    let basin_insert_ms = basin_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- Basin warm-up ----------------------------------------------------
    let _ = sess
        .execute(&format!("SELECT id FROM events WHERE id = {target_id}"))
        .await;

    // ---- Basin measure: point query ---------------------------------------
    let mut basin_point_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        basin_point_ms.push(
            basin_timed(
                &sess,
                &format!(
                    "SELECT id, user_id, amount, status, created_at \
                     FROM events WHERE id = {target_id}"
                ),
                true,
            )
            .await,
        );
    }
    let basin_point_p50 = median(&basin_point_ms);
    let basin_point_p99 = percentile(&basin_point_ms, 99.0);

    // ---- Basin measure: range scan ----------------------------------------
    let mut basin_range_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        basin_range_ms.push(
            basin_timed(
                &sess,
                &format!(
                    "SELECT id, user_id, amount FROM events \
                     WHERE created_at BETWEEN {range_lo_ts} AND {range_hi_ts}"
                ),
                true,
            )
            .await,
        );
    }
    let basin_range_p50 = median(&basin_range_ms);
    let basin_range_p99 = percentile(&basin_range_ms, 99.0);

    // ---- Basin measure: aggregate -----------------------------------------
    let mut basin_agg_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        basin_agg_ms.push(
            basin_timed(
                &sess,
                "SELECT user_id, COUNT(*), SUM(amount) FROM events \
                 GROUP BY user_id ORDER BY 2 DESC LIMIT 10",
                true,
            )
            .await,
        );
    }
    let basin_agg_p50 = median(&basin_agg_ms);

    // ---- Basin measure: JOIN ----------------------------------------------
    let mut basin_join_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_join_ms.push(
            basin_timed(
                &sess,
                "SELECT u.email, COUNT(e.id) FROM users u \
                 JOIN events e ON e.user_id = u.id \
                 GROUP BY u.email ORDER BY 2 DESC LIMIT 20",
                true,
            )
            .await,
        );
    }
    let basin_join_p50 = median(&basin_join_ms);

    // ---- Basin measure: ILIKE ---------------------------------------------
    let mut basin_ilike_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_ilike_ms.push(
            basin_timed(
                &sess,
                "SELECT id, email FROM users WHERE email ILIKE '%@gmail.com'",
                true,
            )
            .await,
        );
    }
    let basin_ilike_p50 = median(&basin_ilike_ms);

    // ---- Basin measure: pagination ----------------------------------------
    let mut basin_page_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_page_ms.push(
            basin_timed(
                &sess,
                "SELECT id, amount, status, created_at FROM events \
                 ORDER BY created_at DESC LIMIT 50 OFFSET 100",
                true,
            )
            .await,
        );
    }
    let basin_page_p50 = median(&basin_page_ms);

    // ---- Basin measure: single-row UPDATE ---------------------------------
    let mut basin_upd1_ms: Vec<f64> = Vec::with_capacity(5);
    for i in 0..5 {
        let uid = i as i64;
        let new_email = format!("rotated{i}@example.org");
        let q = format!("UPDATE users SET email = '{new_email}' WHERE id = {uid}");
        let started = Instant::now();
        sess.execute(&q).await.expect("basin single-row update");
        basin_upd1_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    let basin_upd1_p50 = median(&basin_upd1_ms);

    // ---- Basin measure: bulk UPDATE ---------------------------------------
    let basin_bulk_upd_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "UPDATE events SET status = 'expired' WHERE created_at < {pagination_threshold}"
        ))
        .await
        .expect("basin bulk update");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- Basin measure: DELETE WHERE IN -----------------------------------
    let basin_delete_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "DELETE FROM events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("basin delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- Cold-start first query --------------------------------------------
    let pg_cold_ms = {
        let user_token = if conn_str.contains("user=pc") {
            "pc"
        } else {
            "postgres"
        };
        let conn_str_cold = format!("host=127.0.0.1 port=5432 user={user_token} dbname=postgres");
        let cold_start = Instant::now();
        if let Ok((cold_client, cold_conn)) = tokio_postgres::connect(&conn_str_cold, NoTls).await {
            tokio::spawn(async move {
                let _ = cold_conn.await;
            });
            let _ = cold_client
                .simple_query(&format!(
                    "SELECT COUNT(*) FROM {schema}.events WHERE id = {target_id}"
                ))
                .await;
            cold_start.elapsed().as_secs_f64() * 1000.0
        } else {
            pg_point_p50
        }
    };

    let basin_cold_ms = {
        let cold = build_basin_engine().await;
        let cold_sess = cold.engine.open_session(cold.project).await.unwrap();
        cold_sess
            .execute(
                "CREATE TABLE events (\
                    id BIGINT NOT NULL, \
                    user_id BIGINT NOT NULL, \
                    amount DOUBLE PRECISION NOT NULL, \
                    status TEXT NOT NULL, \
                    created_at BIGINT NOT NULL) \
                 WITH (basin.file_format='parquet')",
            )
            .await
            .unwrap();
        cold_sess
            .execute(&format!(
                "INSERT INTO events VALUES ({target_id}, 0, 1.5, 'pending', {})",
                EPOCH + target_id
            ))
            .await
            .unwrap();
        let started = Instant::now();
        let _ = cold_sess
            .execute(&format!(
                "SELECT id, user_id, amount, status, created_at \
                 FROM events WHERE id = {target_id}"
            ))
            .await
            .unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        cold.bg.shutdown().await;
        cold.wal.close().await.unwrap();
        elapsed
    };

    let basin_disk_bytes = dir_size_data(instance.dir.path());

    // ---- Print results table ----------------------------------------------
    let basin_mib = basin_disk_bytes as f64 / (1024.0 * 1024.0);
    let pg_mib = pg_disk_bytes as f64 / (1024.0 * 1024.0);
    let disk_ratio = pg_disk_bytes as f64 / basin_disk_bytes.max(1) as f64;

    println!(
        "\n[COMPARE 100k / PARQUET] Basin (Parquet) vs Postgres 18 — 100k-row SaaS workload"
    );
    println!(
        "{:>34} {:>14} {:>14} {:>16}",
        "metric", "basin(parquet)", "postgres", "pg/basin"
    );
    println!(
        "{:>34} {:>12.2}MiB {:>12.2}MiB {:>16}",
        "on_disk_bytes",
        basin_mib,
        pg_mib,
        format!("{:.2}x", disk_ratio)
    );
    let row = |label: &str, b: f64, p: f64| {
        println!(
            "{label:>34} {:>14.3} {:>14.3} {:>16}",
            b,
            p,
            format!("{:.2}x", p / b.max(1e-9))
        );
    };
    row("point_query_p50_ms", basin_point_p50, pg_point_p50);
    row("point_query_p99_ms", basin_point_p99, pg_point_p99);
    row("range_scan_p50_ms (~1k)", basin_range_p50, pg_range_p50);
    row("range_scan_p99_ms", basin_range_p99, pg_range_p99);
    row("aggregate_groupby_p50_ms", basin_agg_p50, pg_agg_p50);
    row("join_2table_p50_ms", basin_join_p50, pg_join_p50);
    row("ilike_pattern_p50_ms", basin_ilike_p50, pg_ilike_p50);
    row("pagination_p50_ms", basin_page_p50, pg_page_p50);
    row("single_row_update_p50_ms", basin_upd1_p50, pg_upd1_p50);
    row("bulk_update_ms (~33k rows)", basin_bulk_upd_ms, pg_bulk_upd_ms);
    row("delete_where_in_10_ms", basin_delete_ms, pg_delete_ms);
    row("bulk_insert_100k_ms", basin_insert_ms, pg_insert_ms);
    row("cold_start_first_query_ms", basin_cold_ms, pg_cold_ms);

    // ---- Emit benchmark JSON ----------------------------------------------
    let basin_disk_f = basin_disk_bytes as f64;
    let pg_disk_f = pg_disk_bytes as f64;
    let mk = |label: &str, basin: f64, postgres: f64, unit: &str, with_ratio: bool| -> CompareMetric {
        CompareMetric {
            label: label.into(),
            basin,
            postgres,
            unit: unit.into(),
            better: which_wins(basin, postgres),
            ratio_text: if with_ratio {
                Some(format!("pg / basin = {:.2}x", postgres / basin.max(1e-9)))
            } else {
                None
            },
        }
    };

    let metrics = vec![
        mk("On-disk bytes (users + events)", basin_disk_f, pg_disk_f, "bytes", true),
        mk("Point query p50", basin_point_p50, pg_point_p50, "ms", true),
        mk("Point query p99", basin_point_p99, pg_point_p99, "ms", true),
        mk("Range scan p50 (~1k rows)", basin_range_p50, pg_range_p50, "ms", true),
        mk("Range scan p99", basin_range_p99, pg_range_p99, "ms", true),
        mk("Aggregate GROUP BY user_id p50", basin_agg_p50, pg_agg_p50, "ms", true),
        mk("2-table JOIN GROUP BY p50", basin_join_p50, pg_join_p50, "ms", true),
        mk("ILIKE '%@gmail.com' p50", basin_ilike_p50, pg_ilike_p50, "ms", true),
        mk("Pagination ORDER BY LIMIT/OFFSET p50", basin_page_p50, pg_page_p50, "ms", true),
        mk("Single-row UPDATE p50", basin_upd1_p50, pg_upd1_p50, "ms", true),
        mk("Bulk UPDATE (~33k rows)", basin_bulk_upd_ms, pg_bulk_upd_ms, "ms", false),
        mk("DELETE WHERE id IN (10 rows)", basin_delete_ms, pg_delete_ms, "ms", false),
        mk("Bulk INSERT 100k rows", basin_insert_ms, pg_insert_ms, "ms", false),
        mk("Cold-start first query", basin_cold_ms, pg_cold_ms, "ms", true),
    ];

    report_postgres_compare(
        "postgres_100k_parquet",
        "Basin (Parquet) vs Postgres 18 (100k-row SaaS workload, no index)",
        "Sibling of the Vortex 100k card: same SaaS workload (users + events, joins, ILIKE, \
         pagination, bulk DML) but Basin is pinned to basin.file_format='parquet'. Shows how \
         the legacy / Iceberg-read-compat Parquet path compares against both PG heap and the \
         Vortex-default card.",
        true,
        metrics,
        None,
    );

    instance.bg.shutdown().await;
    instance.wal.close().await.unwrap();

    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    std::mem::forget(_guard);
}
