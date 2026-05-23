//! Shared scaffold for the `compare_postgres_*` benchmark family.
//!
//! Loaded via `#[path = "compare_postgres_common.rs"] mod common;` from
//! each `compare_postgres[_<rows>][_parquet].rs` per-scale wrapper. There
//! are six wrappers — {10k, 100k, 1M} × {Vortex, Parquet} — and they all
//! call `run_full_compare(...)` here.
//!
//! Why a single helper instead of six copies of an 800-line file?
//!
//! Prior to this refactor every (rows, format) pair was its own test file
//! that copy-pasted the schema, the warm-up dance, the PG EXPLAIN-ANALYZE
//! parser, the cold-start probe, and the JSON-emit block. The 1M cards
//! also had a SHALLOWER metric set (6) than the 100k cards (12), so the
//! dashboard rendered apples-to-oranges across scales. Centralising the
//! suite lets us run the SAME 15 metrics at all three scales and both
//! formats from a 30-line wrapper.
//!
//! The helper is loaded as a sibling test-tree module — NOT exported from
//! `basin-integration-tests/src/lib.rs` — because its dependencies
//! (`tokio-postgres`, `walkdir`) live under `[dev-dependencies]` in the
//! crate's Cargo.toml and are only available to the integration test
//! binaries, not to the library crate. The `#[path = …] mod common;`
//! pattern mirrors what `migration_tool_*.rs` already do here.
//!
//! Query suite (15 metrics per card):
//!   SaaS / OLTP shapes (12)
//!     (1)  Point query              — WHERE id = ?
//!     (2)  Range scan (~1 000 rows) — WHERE created_at BETWEEN ? AND ?
//!     (3)  Aggregate GROUP BY       — SUM/COUNT GROUP BY user_id LIMIT 10
//!     (4)  2-table JOIN             — users JOIN events GROUP BY email
//!     (5)  ILIKE pattern            — WHERE email ILIKE '%@gmail.com'
//!     (6)  Pagination               — ORDER BY DESC LIMIT 50 OFFSET 100
//!     (7)  Single-row UPDATE        — UPDATE users SET email WHERE id = ?
//!     (8)  Bulk UPDATE              — UPDATE events SET status … (~rows/3)
//!     (9)  DELETE WHERE IN (10)
//!    (10)  Bulk INSERT N            — batched multi-row VALUES
//!    (11)  Cold-start first query   — fresh engine, first query latency
//!    (12)  On-disk bytes
//!   OLAP / time-series shapes (3) — workloads Vortex should shine on
//!    (13)  COUNT(*) on whole table  — Vortex col stats vs PG seq scan
//!    (14)  DATE_TRUNC + GROUP BY    — time-series rollup
//!    (15)  JOIN + WHERE + GROUP BY  — analytics: "top spenders last N days"

#![allow(dead_code, clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

/// Which Basin storage format the card pins. Parquet sets
/// `WITH (basin.file_format='parquet')` on every CREATE TABLE; Vortex omits
/// the WITH clause and inherits the engine default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BasinFormat {
    Vortex,
    Parquet,
}

impl BasinFormat {
    fn with_clause(self) -> &'static str {
        match self {
            BasinFormat::Vortex => "",
            BasinFormat::Parquet => " WITH (basin.file_format='parquet')",
        }
    }

    fn label(self) -> &'static str {
        match self {
            BasinFormat::Vortex => "Vortex",
            BasinFormat::Parquet => "Parquet",
        }
    }
}

/// Synthetic clock anchor — `created_at` for row i is `EPOCH + i` seconds.
const EPOCH: i64 = 1_700_000_000;

/// Deterministic synthetic email. ~10% land in `@gmail.com` so the ILIKE
/// selectivity is meaningful but not the whole table.
pub fn email_for(i: i64) -> String {
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

pub fn status_for(i: i64) -> &'static str {
    match i % 4 {
        0 => "pending",
        1 => "active",
        2 => "completed",
        _ => "archived",
    }
}

/// Sum bytes of every Basin data file under `root`. Counts BOTH `.vortex`
/// and `.parquet` so the same counter works for either format.
pub fn dir_size_data(root: &std::path::Path) -> u64 {
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

pub fn percentile(samples: &[f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (s.len() as f64 - 1.0)).round() as usize;
    s[idx.min(s.len() - 1)]
}

pub fn median(samples: &[f64]) -> f64 {
    percentile(samples, 50.0)
}

/// RAII safety-net that drops the schema even on panic.
pub struct SchemaGuard {
    pub schema: String,
    pub conn_str: String,
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

pub async fn try_connect() -> Option<(Client, String)> {
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

pub struct BasinInstance {
    pub engine: Engine,
    pub project: ProjectId,
    pub bg: basin_shard::ShardBackgroundHandle,
    pub wal: Arc<dyn basin_wal::Wal>,
    pub dir: TempDir,
    pub _wal_dir: TempDir,
}

pub async fn build_basin_engine() -> BasinInstance {
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

/// Per-scale tuning of the multi-row INSERT batch size. At 10k rows we want a
/// single batch (warm cache fits the whole table), at 100k we pick 5k, at 1M
/// we pick 10k — the latter two match the pre-refactor per-file constants so
/// the timing comparison is apples-to-apples with the older cards.
fn insert_batch_for(rows: usize) -> usize {
    if rows <= 10_000 {
        rows.max(1)
    } else if rows <= 100_000 {
        5_000
    } else {
        10_000
    }
}

/// Single-replicated suite that runs the FULL 15-metric comparison and emits
/// the dashboard JSON card. Each `compare_postgres_*` test file is a 30-line
/// wrapper that calls this with the right (rows, format, id, name, claim).
pub async fn run_full_compare(
    rows: usize,
    basin_format: BasinFormat,
    id: &str,
    name: &str,
    claim: &str,
    schema_prefix: &str,
) {
    let (pg, conn_str) = match try_connect().await {
        Some(v) => v,
        None => {
            println!(
                "[COMPARE {rows} / {fmt}] postgres unavailable: skipping head-to-head",
                fmt = basin_format.label()
            );
            report_postgres_compare(
                id,
                name,
                claim,
                false,
                vec![],
                Some("postgres unavailable on 127.0.0.1:5432"),
            );
            return;
        }
    };

    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("{schema_prefix}_{suffix}");
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
    // Drop PKs so we measure heap-only performance (fair vs Basin substrate).
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

    // ~10 events per user across all scales.
    let users: usize = (rows / 10).max(100);
    let user_count = users as i64;
    let row_count = rows as i64;
    let insert_batch = insert_batch_for(rows);

    // ---- PG seed users -----------------------------------------------------
    {
        let mut stmt = String::with_capacity(users * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.users (id, email, created_at) VALUES "
        ));
        for i in 0..users as i64 {
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

    // ---- PG bulk INSERT N events ------------------------------------------
    let pg_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < rows {
        let remaining = rows - row_idx as usize;
        let batch = remaining.min(insert_batch);
        let mut stmt = String::with_capacity(batch * 80);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.events (id, user_id, amount, status, created_at) VALUES "
        ));
        for j in 0..batch {
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
        row_idx += batch as i64;
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

    // ---- Pre-compute query targets ----------------------------------------
    let target_id: i64 = row_count / 2 + 7;
    let range_lo_ts = EPOCH + row_count / 4;
    let range_hi_ts = range_lo_ts + 1_000; // ~1 000-row range
    // Used as a "rows older than N" cut for bulk UPDATE / pagination filter.
    let pagination_threshold = EPOCH + row_count / 3;
    // OLAP "last N days" filter — the synthetic clock advances 1 second per
    // row, so "last 30 days" only matches anything when ROWS ≥ 30·86400. At
    // smaller scales we widen the window to the last 1/3 of the inserted
    // range so the query still touches a non-empty slice.
    let thirty_days: i64 = 30 * 86_400;
    let olap_window: i64 = if row_count > thirty_days {
        thirty_days
    } else {
        (row_count / 3).max(1)
    };
    let olap_cutoff_ts = EPOCH + row_count - olap_window;
    // bulk UPDATE / DELETE counts scale with ROWS so that "1/3 of the table"
    // is always the unit of work, regardless of scale.
    let bulk_update_rows = row_count / 3; // status='expired' rows
    let delete_ids: Vec<i64> = (0..10).map(|k| row_count - 1 - k).collect();
    let delete_in_list = delete_ids
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");

    let _ = pg
        .simple_query(&format!(
            "SELECT COUNT(*) FROM {schema}.events WHERE id = {target_id}"
        ))
        .await;

    // ---- PG: 12 SaaS metrics + 3 OLAP metrics ------------------------------
    let mut pg_point_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let r = pg.simple_query(&q).await.expect("explain point");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_point_ms.push(ms);
        }
    }
    let pg_point_p50 = median(&pg_point_ms);
    let pg_point_p99 = percentile(&pg_point_ms, 99.0);

    let mut pg_range_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events \
             WHERE created_at BETWEEN to_timestamp({range_lo_ts}) AND to_timestamp({range_hi_ts})"
        );
        let r = pg.simple_query(&q).await.expect("explain range");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_range_ms.push(ms);
        }
    }
    let pg_range_p50 = median(&pg_range_ms);
    let pg_range_p99 = percentile(&pg_range_ms, 99.0);

    let mut pg_agg_ms: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT user_id, COUNT(*), SUM(amount) \
             FROM {schema}.events GROUP BY user_id ORDER BY 2 DESC LIMIT 10"
        );
        let r = pg.simple_query(&q).await.expect("explain agg");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_agg_ms.push(ms);
        }
    }
    let pg_agg_p50 = median(&pg_agg_ms);

    let mut pg_join_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT u.email, COUNT(e.id) \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             GROUP BY u.email ORDER BY 2 DESC LIMIT 20"
        );
        let r = pg.simple_query(&q).await.expect("explain join");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_join_ms.push(ms);
        }
    }
    let pg_join_p50 = median(&pg_join_ms);

    let mut pg_ilike_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, email FROM {schema}.users \
             WHERE email ILIKE '%@gmail.com'"
        );
        let r = pg.simple_query(&q).await.expect("explain ilike");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_ilike_ms.push(ms);
        }
    }
    let pg_ilike_p50 = median(&pg_ilike_ms);

    let mut pg_page_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, amount, status, created_at \
             FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 100"
        );
        let r = pg.simple_query(&q).await.expect("explain pagination");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_page_ms.push(ms);
        }
    }
    let pg_page_p50 = median(&pg_page_ms);

    let mut pg_upd1_ms: Vec<f64> = Vec::with_capacity(5);
    for i in 0..5 {
        let uid = i as i64;
        let new_email = format!("rotated{i}@example.org");
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) UPDATE {schema}.users \
             SET email = '{new_email}' WHERE id = {uid}"
        );
        let r = pg.simple_query(&q).await.expect("explain upd1");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_upd1_ms.push(ms);
        }
    }
    let pg_upd1_p50 = median(&pg_upd1_ms);

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

    let pg_delete_ms: f64 = {
        let started = Instant::now();
        pg.simple_query(&format!(
            "DELETE FROM {schema}.events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("pg delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- PG OLAP (3) ------------------------------------------------------
    let mut pg_count_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT COUNT(*) FROM {schema}.events"
        );
        let r = pg.simple_query(&q).await.expect("explain count");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_count_ms.push(ms);
        }
    }
    let pg_count_p50 = median(&pg_count_ms);

    let mut pg_trunc_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT DATE_TRUNC('day', created_at) AS d, \
                    SUM(amount) FROM {schema}.events GROUP BY 1 ORDER BY 1"
        );
        let r = pg.simple_query(&q).await.expect("explain date_trunc");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_trunc_ms.push(ms);
        }
    }
    let pg_trunc_p50 = median(&pg_trunc_ms);

    let mut pg_olap_join_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT u.email, SUM(e.amount) \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             WHERE e.created_at > to_timestamp({olap_cutoff_ts}) \
             GROUP BY u.email ORDER BY 2 DESC LIMIT 10"
        );
        let r = pg.simple_query(&q).await.expect("explain olap join");
        if let Some(ms) = parse_pg_exec_time(&r) {
            pg_olap_join_ms.push(ms);
        }
    }
    let pg_olap_join_p50 = median(&pg_olap_join_ms);

    // ---- Basin setup ------------------------------------------------------
    let instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    let with_clause = basin_format.with_clause();
    sess.execute(&format!(
        "CREATE TABLE users (\
            id BIGINT NOT NULL, \
            email TEXT NOT NULL, \
            created_at BIGINT NOT NULL){with_clause}"
    ))
    .await
    .unwrap();
    sess.execute(&format!(
        "CREATE TABLE events (\
            id BIGINT NOT NULL, \
            user_id BIGINT NOT NULL, \
            amount DOUBLE PRECISION NOT NULL, \
            status TEXT NOT NULL, \
            created_at BIGINT NOT NULL){with_clause}"
    ))
    .await
    .unwrap();

    // ---- Basin seed users -------------------------------------------------
    {
        let mut stmt = String::with_capacity(users * 70);
        stmt.push_str("INSERT INTO users VALUES ");
        for i in 0..users as i64 {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({i}, '{}', {})", email_for(i), EPOCH + i));
        }
        sess.execute(&stmt).await.expect("basin seed users");
    }

    // ---- Basin bulk INSERT N events ---------------------------------------
    let basin_insert_started = Instant::now();
    let mut row_idx: i64 = 0;
    while (row_idx as usize) < rows {
        let remaining = rows - row_idx as usize;
        let batch = remaining.min(insert_batch);
        let mut stmt = String::with_capacity(batch * 80);
        stmt.push_str("INSERT INTO events VALUES ");
        for j in 0..batch {
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
        row_idx += batch as i64;
    }
    let basin_insert_ms = basin_insert_started.elapsed().as_secs_f64() * 1000.0;

    // ---- Basin warm-up ----------------------------------------------------
    let _ = sess
        .execute(&format!("SELECT id FROM events WHERE id = {target_id}"))
        .await;

    // ---- Basin: 12 SaaS metrics -------------------------------------------
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

    let basin_bulk_upd_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "UPDATE events SET status = 'expired' WHERE created_at < {pagination_threshold}"
        ))
        .await
        .expect("basin bulk update");
        started.elapsed().as_secs_f64() * 1000.0
    };

    let basin_delete_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "DELETE FROM events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("basin delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    // ---- Basin OLAP (3) ---------------------------------------------------
    let mut basin_count_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_count_ms.push(
            basin_timed(&sess, "SELECT COUNT(*) FROM events", true).await,
        );
    }
    let basin_count_p50 = median(&basin_count_ms);

    // Basin stores `created_at` as BIGINT seconds-since-epoch (vs PG's
    // TIMESTAMPTZ). Basin's `to_timestamp(numeric)` 1-arg form is gapped
    // (v0.2 roadmap), so we bucket directly with integer division:
    // `created_at / 86400` = days since unix epoch. Same group cardinality
    // and same scan cost as PG's `DATE_TRUNC('day', created_at)` over the
    // synthetic clock (1 second per row, EPOCH = 2023-11-14), so the
    // measurement is apples-to-apples on the substrate axis. When the
    // 1-arg `to_timestamp` lands this can flip to `DATE_TRUNC('day',
    // to_timestamp(created_at))` without changing the timing story.
    let mut basin_trunc_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_trunc_ms.push(
            basin_timed(
                &sess,
                "SELECT created_at / 86400 AS day_bucket, \
                        SUM(amount) FROM events GROUP BY 1 ORDER BY 1",
                true,
            )
            .await,
        );
    }
    let basin_trunc_p50 = median(&basin_trunc_ms);

    let mut basin_olap_join_ms: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        basin_olap_join_ms.push(
            basin_timed(
                &sess,
                &format!(
                    "SELECT u.email, SUM(e.amount) FROM users u \
                     JOIN events e ON e.user_id = u.id \
                     WHERE e.created_at > {olap_cutoff_ts} \
                     GROUP BY u.email ORDER BY 2 DESC LIMIT 10"
                ),
                true,
            )
            .await,
        );
    }
    let basin_olap_join_p50 = median(&basin_olap_join_ms);

    // ---- Cold-start first query -------------------------------------------
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
            .execute(&format!(
                "CREATE TABLE events (\
                    id BIGINT NOT NULL, \
                    user_id BIGINT NOT NULL, \
                    amount DOUBLE PRECISION NOT NULL, \
                    status TEXT NOT NULL, \
                    created_at BIGINT NOT NULL){with_clause}"
            ))
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
        "\n[COMPARE {rows} / {fmt}] Basin vs Postgres 18 — {rows}-row SaaS+OLAP workload (no index)",
        fmt = basin_format.label()
    );
    println!(
        "{:>34} {:>14} {:>14} {:>16}",
        "metric", "basin", "postgres", "pg/basin"
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
    row("bulk_update_ms (~1/3 rows)", basin_bulk_upd_ms, pg_bulk_upd_ms);
    row("delete_where_in_10_ms", basin_delete_ms, pg_delete_ms);
    row(&format!("bulk_insert_{rows}_ms"), basin_insert_ms, pg_insert_ms);
    row("cold_start_first_query_ms", basin_cold_ms, pg_cold_ms);
    row("count_star_p50_ms", basin_count_p50, pg_count_p50);
    row("date_trunc_groupby_p50_ms", basin_trunc_p50, pg_trunc_p50);
    row("analytics_join_p50_ms", basin_olap_join_p50, pg_olap_join_p50);

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

    let bulk_label = format!("Bulk UPDATE (~{bulk_update_rows} rows)");
    let insert_label = format!("Bulk INSERT {rows} rows");
    let olap_label = format!(
        "Analytics JOIN+WHERE (last {olap_window}s window)"
    );
    let metrics = vec![
        // On-disk
        mk("On-disk bytes (users + events)", basin_disk_f, pg_disk_f, "bytes", true),
        // SaaS / OLTP
        mk("Point query p50", basin_point_p50, pg_point_p50, "ms", true),
        mk("Point query p99", basin_point_p99, pg_point_p99, "ms", true),
        mk("Range scan p50 (~1k rows)", basin_range_p50, pg_range_p50, "ms", true),
        mk("Range scan p99", basin_range_p99, pg_range_p99, "ms", true),
        mk("Aggregate GROUP BY user_id p50", basin_agg_p50, pg_agg_p50, "ms", true),
        mk("2-table JOIN GROUP BY p50", basin_join_p50, pg_join_p50, "ms", true),
        mk("ILIKE '%@gmail.com' p50", basin_ilike_p50, pg_ilike_p50, "ms", true),
        mk("Pagination ORDER BY LIMIT/OFFSET p50", basin_page_p50, pg_page_p50, "ms", true),
        mk("Single-row UPDATE p50", basin_upd1_p50, pg_upd1_p50, "ms", true),
        mk(&bulk_label, basin_bulk_upd_ms, pg_bulk_upd_ms, "ms", false),
        mk("DELETE WHERE id IN (10 rows)", basin_delete_ms, pg_delete_ms, "ms", false),
        mk(&insert_label, basin_insert_ms, pg_insert_ms, "ms", false),
        mk("Cold-start first query", basin_cold_ms, pg_cold_ms, "ms", true),
        // OLAP
        mk("COUNT(*) full table p50", basin_count_p50, pg_count_p50, "ms", true),
        mk("DATE_TRUNC day + SUM GROUP BY p50", basin_trunc_p50, pg_trunc_p50, "ms", true),
        mk(&olap_label, basin_olap_join_p50, pg_olap_join_p50, "ms", true),
    ];

    report_postgres_compare(id, name, claim, true, metrics, None);

    instance.bg.shutdown().await;
    instance.wal.close().await.unwrap();

    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    std::mem::forget(_guard);
}
