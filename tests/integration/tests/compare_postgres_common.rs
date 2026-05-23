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
//! Query suite (29 metrics per card):
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
//!   Extended-shape coverage (12) — for perf-issue triangulation
//!    (16)  COUNT(DISTINCT user_id) — column-stats / dictionary win for Vortex
//!    (17)  LIKE prefix (status)    — sargable prefix; parity check
//!    (18)  Multi-col GROUP BY + HAVING — high-cardinality grouping w/ filter
//!    (19)  Window LAG OVER PARTITION — window-function plan cost
//!    (20)  Recursive CTE fib(30)   — engine compat + recursive plan
//!    (21)  Correlated subquery     — n+1-style select-list subquery
//!    (22)  EXISTS in WHERE         — semi-join planning
//!    (23)  3-table JOIN w/ BETWEEN — categories ⋈ events ⋈ users
//!    (24)  UNION ALL of two scans  — branch-union cost
//!    (25)  ORDER BY NULLS LAST     — nullable column sort + LIMIT
//!    (26)  Top-N per group (MAX)   — analytics "best customer" pattern
//!    (27)  Numeric range filter    — double-precision BETWEEN pushdown

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

/// Like `basin_timed`, but returns `None` if the query errors. Used by the
/// extended-shape suite (#16-#27) so a single unsupported feature (e.g.
/// recursive CTE) becomes a NaN-equivalent metric row rather than panicking
/// the whole 29-row card. The caller decides how to surface the gap.
///
/// Row-count assertion is skipped for empty-allowed shapes (LIKE prefix,
/// EXISTS) — the timing is still meaningful even on a zero-row result.
async fn basin_timed_try(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Option<f64> {
    let started = Instant::now();
    match sess.execute(sql).await {
        Ok(_) => Some(started.elapsed().as_secs_f64() * 1000.0),
        Err(_) => None,
    }
}

/// Median of N samples, retrying on per-sample failure. If FEWER than half
/// the samples succeed, returns `None` (treat the whole shape as unsupported).
async fn basin_p50_try(
    sess: &basin_engine::ProjectSession,
    sql: &str,
    n: usize,
) -> Option<f64> {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        if let Some(ms) = basin_timed_try(sess, sql).await {
            samples.push(ms);
        }
    }
    if samples.len() * 2 < n {
        return None;
    }
    Some(median(&samples))
}

/// Same as `basin_p50_try` but for PG via EXPLAIN ANALYZE. Returns `None`
/// if Postgres also fails (kept symmetric so the row gets a sentinel on
/// both sides rather than a misleading "PG = 0").
async fn pg_p50_explain(pg: &Client, sql_inner: &str, n: usize) -> Option<f64> {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let q = format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql_inner}");
        if let Ok(r) = pg.simple_query(&q).await {
            if let Some(ms) = parse_pg_exec_time(&r) {
                samples.push(ms);
            }
        }
    }
    if samples.is_empty() {
        return None;
    }
    Some(median(&samples))
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

/// Postgres-side results for the original 14 SaaS+OLAP measurements (mirror
/// of `BasinCoreResults`). Extracted for stack-budget reasons.
struct PgCoreResults {
    point_p50: f64,
    point_p99: f64,
    range_p50: f64,
    range_p99: f64,
    agg_p50: f64,
    join_p50: f64,
    ilike_p50: f64,
    page_p50: f64,
    upd1_p50: f64,
    bulk_upd_ms: f64,
    delete_ms: f64,
    count_p50: f64,
    trunc_p50: f64,
    olap_join_p50: f64,
}

/// Run the PG-side 14 SaaS+OLAP measurements via `EXPLAIN (ANALYZE, FORMAT
/// TEXT)` to capture engine time (excludes the network/protocol roundtrip
/// that would otherwise dominate small queries). Extracted from
/// `run_full_compare` for the same stack-budget reason as the Basin twin.
#[allow(clippy::too_many_arguments)]
async fn run_pg_core_suite(
    pg: &Client,
    schema: &str,
    target_id: i64,
    range_lo_ts: i64,
    range_hi_ts: i64,
    pagination_threshold: i64,
    olap_cutoff_ts: i64,
    delete_in_list: &str,
) -> PgCoreResults {
    let mut point: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events WHERE id = {target_id}"
        );
        let r = pg.simple_query(&q).await.expect("explain point");
        if let Some(ms) = parse_pg_exec_time(&r) { point.push(ms); }
    }

    let mut range: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT * FROM {schema}.events \
             WHERE created_at BETWEEN to_timestamp({range_lo_ts}) AND to_timestamp({range_hi_ts})"
        );
        let r = pg.simple_query(&q).await.expect("explain range");
        if let Some(ms) = parse_pg_exec_time(&r) { range.push(ms); }
    }

    let mut agg: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT user_id, COUNT(*), SUM(amount) \
             FROM {schema}.events GROUP BY user_id ORDER BY 2 DESC LIMIT 10"
        );
        let r = pg.simple_query(&q).await.expect("explain agg");
        if let Some(ms) = parse_pg_exec_time(&r) { agg.push(ms); }
    }

    let mut join: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT u.email, COUNT(e.id) \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             GROUP BY u.email ORDER BY 2 DESC LIMIT 20"
        );
        let r = pg.simple_query(&q).await.expect("explain join");
        if let Some(ms) = parse_pg_exec_time(&r) { join.push(ms); }
    }

    let mut ilike: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, email FROM {schema}.users \
             WHERE email ILIKE '%@gmail.com'"
        );
        let r = pg.simple_query(&q).await.expect("explain ilike");
        if let Some(ms) = parse_pg_exec_time(&r) { ilike.push(ms); }
    }

    let mut page: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id, amount, status, created_at \
             FROM {schema}.events ORDER BY created_at DESC LIMIT 50 OFFSET 100"
        );
        let r = pg.simple_query(&q).await.expect("explain pagination");
        if let Some(ms) = parse_pg_exec_time(&r) { page.push(ms); }
    }

    let mut upd1: Vec<f64> = Vec::with_capacity(5);
    for i in 0..5 {
        let uid = i as i64;
        let new_email = format!("rotated{i}@example.org");
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) UPDATE {schema}.users \
             SET email = '{new_email}' WHERE id = {uid}"
        );
        let r = pg.simple_query(&q).await.expect("explain upd1");
        if let Some(ms) = parse_pg_exec_time(&r) { upd1.push(ms); }
    }

    let bulk_upd_ms: f64 = {
        let started = Instant::now();
        pg.simple_query(&format!(
            "UPDATE {schema}.events SET status = 'expired' \
             WHERE created_at < to_timestamp({pagination_threshold})"
        ))
        .await
        .expect("pg bulk update");
        started.elapsed().as_secs_f64() * 1000.0
    };

    let delete_ms: f64 = {
        let started = Instant::now();
        pg.simple_query(&format!(
            "DELETE FROM {schema}.events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("pg delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    let mut count: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT COUNT(*) FROM {schema}.events"
        );
        let r = pg.simple_query(&q).await.expect("explain count");
        if let Some(ms) = parse_pg_exec_time(&r) { count.push(ms); }
    }

    let mut trunc: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT DATE_TRUNC('day', created_at) AS d, \
                    SUM(amount) FROM {schema}.events GROUP BY 1 ORDER BY 1"
        );
        let r = pg.simple_query(&q).await.expect("explain date_trunc");
        if let Some(ms) = parse_pg_exec_time(&r) { trunc.push(ms); }
    }

    let mut olap_join: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        let q = format!(
            "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT u.email, SUM(e.amount) \
             FROM {schema}.users u JOIN {schema}.events e ON e.user_id = u.id \
             WHERE e.created_at > to_timestamp({olap_cutoff_ts}) \
             GROUP BY u.email ORDER BY 2 DESC LIMIT 10"
        );
        let r = pg.simple_query(&q).await.expect("explain olap join");
        if let Some(ms) = parse_pg_exec_time(&r) { olap_join.push(ms); }
    }

    PgCoreResults {
        point_p50: median(&point),
        point_p99: percentile(&point, 99.0),
        range_p50: median(&range),
        range_p99: percentile(&range, 99.0),
        agg_p50: median(&agg),
        join_p50: median(&join),
        ilike_p50: median(&ilike),
        page_p50: median(&page),
        upd1_p50: median(&upd1),
        bulk_upd_ms,
        delete_ms,
        count_p50: median(&count),
        trunc_p50: median(&trunc),
        olap_join_p50: median(&olap_join),
    }
}

/// Basin-side results for the original 14 SaaS+OLAP measurements (#1-#11,
/// #13-#15). Bundled into a struct so the helper can run all of them in its
/// own async-fn frame (off the `run_full_compare` worker stack).
struct BasinCoreResults {
    point_p50: f64,
    point_p99: f64,
    range_p50: f64,
    range_p99: f64,
    agg_p50: f64,
    join_p50: f64,
    ilike_p50: f64,
    page_p50: f64,
    upd1_p50: f64,
    bulk_upd_ms: f64,
    delete_ms: f64,
    count_p50: f64,
    trunc_p50: f64,
    olap_join_p50: f64,
}

/// Runs the original 14-measurement Basin SaaS+OLAP suite. Returns a flat
/// struct of p50/p99 results. Extracted from `run_full_compare` so the
/// outer function's state machine doesn't accumulate ~14 separate `Vec<f64>`
/// locals on the worker-thread stack.
#[allow(clippy::too_many_arguments)]
async fn run_basin_core_suite(
    sess: &basin_engine::ProjectSession,
    target_id: i64,
    range_lo_ts: i64,
    range_hi_ts: i64,
    pagination_threshold: i64,
    olap_cutoff_ts: i64,
    delete_in_list: &str,
) -> BasinCoreResults {
    // Warm-up: triggers any first-query plan caching so the p99 column
    // reflects steady-state latency rather than first-touch overhead.
    let _ = sess
        .execute(&format!("SELECT id FROM events WHERE id = {target_id}"))
        .await;

    let mut point: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        point.push(
            basin_timed(
                sess,
                &format!(
                    "SELECT id, user_id, amount, status, created_at \
                     FROM events WHERE id = {target_id}"
                ),
                true,
            )
            .await,
        );
    }

    let mut range: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        range.push(
            basin_timed(
                sess,
                &format!(
                    "SELECT id, user_id, amount FROM events \
                     WHERE created_at BETWEEN {range_lo_ts} AND {range_hi_ts}"
                ),
                true,
            )
            .await,
        );
    }

    let mut agg: Vec<f64> = Vec::with_capacity(7);
    for _ in 0..7 {
        agg.push(
            basin_timed(
                sess,
                "SELECT user_id, COUNT(*), SUM(amount) FROM events \
                 GROUP BY user_id ORDER BY 2 DESC LIMIT 10",
                true,
            )
            .await,
        );
    }

    let mut join: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        join.push(
            basin_timed(
                sess,
                "SELECT u.email, COUNT(e.id) FROM users u \
                 JOIN events e ON e.user_id = u.id \
                 GROUP BY u.email ORDER BY 2 DESC LIMIT 20",
                true,
            )
            .await,
        );
    }

    let mut ilike: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        ilike.push(
            basin_timed(
                sess,
                "SELECT id, email FROM users WHERE email ILIKE '%@gmail.com'",
                true,
            )
            .await,
        );
    }

    let mut page: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        page.push(
            basin_timed(
                sess,
                "SELECT id, amount, status, created_at FROM events \
                 ORDER BY created_at DESC LIMIT 50 OFFSET 100",
                true,
            )
            .await,
        );
    }

    let mut upd1: Vec<f64> = Vec::with_capacity(5);
    for i in 0..5 {
        let uid = i as i64;
        let new_email = format!("rotated{i}@example.org");
        let q = format!("UPDATE users SET email = '{new_email}' WHERE id = {uid}");
        let started = Instant::now();
        sess.execute(&q).await.expect("basin single-row update");
        upd1.push(started.elapsed().as_secs_f64() * 1000.0);
    }

    let bulk_upd_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "UPDATE events SET status = 'expired' WHERE created_at < {pagination_threshold}"
        ))
        .await
        .expect("basin bulk update");
        started.elapsed().as_secs_f64() * 1000.0
    };

    let delete_ms: f64 = {
        let started = Instant::now();
        sess.execute(&format!(
            "DELETE FROM events WHERE id IN ({delete_in_list})"
        ))
        .await
        .expect("basin delete");
        started.elapsed().as_secs_f64() * 1000.0
    };

    let mut count: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        count.push(basin_timed(sess, "SELECT COUNT(*) FROM events", true).await);
    }

    // Basin stores `created_at` as BIGINT seconds-since-epoch (vs PG's
    // TIMESTAMPTZ). Basin's `to_timestamp(numeric)` 1-arg form is gapped
    // (v0.2 roadmap), so we bucket directly with integer division —
    // `created_at / 86400` = days since unix epoch — same group cardinality
    // and same scan cost as PG's `DATE_TRUNC('day', created_at)`.
    let mut trunc: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        trunc.push(
            basin_timed(
                sess,
                "SELECT created_at / 86400 AS day_bucket, \
                        SUM(amount) FROM events GROUP BY 1 ORDER BY 1",
                true,
            )
            .await,
        );
    }

    let mut olap_join: Vec<f64> = Vec::with_capacity(5);
    for _ in 0..5 {
        olap_join.push(
            basin_timed(
                sess,
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

    BasinCoreResults {
        point_p50: median(&point),
        point_p99: percentile(&point, 99.0),
        range_p50: median(&range),
        range_p99: percentile(&range, 99.0),
        agg_p50: median(&agg),
        join_p50: median(&join),
        ilike_p50: median(&ilike),
        page_p50: median(&page),
        upd1_p50: median(&upd1),
        bulk_upd_ms,
        delete_ms,
        count_p50: median(&count),
        trunc_p50: median(&trunc),
        olap_join_p50: median(&olap_join),
    }
}

/// Carrier for the 12 extended-shape (PG, Basin) p50 pairs.
///
/// PG values are `f64::INFINITY` when the query errored out (kept finite
/// 99.99% of the time in practice); Basin values are `Option<f64>` because
/// the engine may legitimately not support a shape yet (e.g. recursive CTE
/// in pre-roadmap builds) and we want to surface that as a "(basin gap)"
/// card row rather than fail the whole comparison.
struct ExtendedResults {
    count_distinct: (Option<f64>, f64),
    like_prefix: (Option<f64>, f64),
    groupby_having: (Option<f64>, f64),
    window_lag: (Option<f64>, f64),
    recursive_cte: (Option<f64>, f64),
    correlated_sub: (Option<f64>, f64),
    exists_in_where: (Option<f64>, f64),
    join3_between: (Option<f64>, f64),
    union_all: (Option<f64>, f64),
    order_nulls_last: (Option<f64>, f64),
    top_n_per_group: (Option<f64>, f64),
    numeric_range: (Option<f64>, f64),
}

/// Run all 12 extended-shape probes (#16-#27) and return their (basin, pg)
/// p50 pairs. Pulled out of `run_full_compare` to keep the outer function's
/// stack-frame size within the worker-thread default — the 12 inline shape
/// blocks otherwise overflow on the multi_thread tokio test runtime.
///
/// Each shape:
///   - PG: `EXPLAIN (ANALYZE, FORMAT TEXT) <sql>` × EXT_SAMPLES, median.
///   - Basin: `sess.execute(<sql>)` × EXT_SAMPLES, median (Option::None if
///     >half the samples errored — caller treats as a "(basin gap)" row).
///   - Query text matches PG and Basin modulo schema qualification and the
///     `created_at BIGINT` clock convention used in Basin's seed.
async fn run_extended_suite(
    pg: &Client,
    sess: &basin_engine::ProjectSession,
    schema: &str,
) -> ExtendedResults {
    const EXT_SAMPLES: usize = 5;

    async fn pair(
        pg: &Client,
        sess: &basin_engine::ProjectSession,
        pg_sql: String,
        basin_sql: &str,
    ) -> (Option<f64>, f64) {
        let p = pg_p50_explain(pg, &pg_sql, EXT_SAMPLES)
            .await
            .unwrap_or(f64::INFINITY);
        let b = basin_p50_try(sess, basin_sql, EXT_SAMPLES).await;
        (b, p)
    }

    // -- #16 COUNT(DISTINCT user_id) ----------------------------------------
    let count_distinct = pair(
        pg, sess,
        format!("SELECT COUNT(DISTINCT user_id) FROM {schema}.events"),
        "SELECT COUNT(DISTINCT user_id) FROM events",
    ).await;

    // -- #17 LIKE prefix ----------------------------------------------------
    let like_prefix = pair(
        pg, sess,
        format!("SELECT id FROM {schema}.events WHERE status LIKE 'pending%' LIMIT 100"),
        "SELECT id FROM events WHERE status LIKE 'pending%' LIMIT 100",
    ).await;

    // -- #18 Multi-col GROUP BY + HAVING + ORDER + LIMIT --------------------
    let groupby_having = pair(
        pg, sess,
        format!(
            "SELECT user_id, status, COUNT(*) FROM {schema}.events \
             GROUP BY 1, 2 HAVING COUNT(*) > 5 ORDER BY 3 DESC LIMIT 20"
        ),
        "SELECT user_id, status, COUNT(*) FROM events \
         GROUP BY 1, 2 HAVING COUNT(*) > 5 ORDER BY 3 DESC LIMIT 20",
    ).await;

    // -- #19 Window LAG OVER (PARTITION BY ... ORDER BY ...) ----------------
    let window_lag = pair(
        pg, sess,
        format!(
            "SELECT id, amount, LAG(amount) OVER (PARTITION BY user_id ORDER BY created_at) \
             FROM {schema}.events LIMIT 1000"
        ),
        "SELECT id, amount, LAG(amount) OVER (PARTITION BY user_id ORDER BY created_at) \
         FROM events LIMIT 1000",
    ).await;

    // -- #20 Recursive CTE (Fibonacci to n=30) ------------------------------
    let rec_cte = "WITH RECURSIVE fib(n, a, b) AS (\
                     SELECT 1, 0, 1 \
                     UNION ALL \
                     SELECT n+1, b, a+b FROM fib WHERE n < 30) \
                   SELECT n, a FROM fib";
    let recursive_cte = pair(pg, sess, rec_cte.to_string(), rec_cte).await;

    // -- #21 Correlated subquery in SELECT list -----------------------------
    let correlated_sub = pair(
        pg, sess,
        format!(
            "SELECT u.email, (SELECT COUNT(*) FROM {schema}.events e WHERE e.user_id = u.id) \
                AS n_events \
             FROM {schema}.users u LIMIT 100"
        ),
        "SELECT u.email, (SELECT COUNT(*) FROM events e WHERE e.user_id = u.id) AS n_events \
         FROM users u LIMIT 100",
    ).await;

    // -- #22 EXISTS in WHERE ------------------------------------------------
    let exists_in_where = pair(
        pg, sess,
        format!(
            "SELECT u.id FROM {schema}.users u \
             WHERE EXISTS (SELECT 1 FROM {schema}.events e \
                           WHERE e.user_id = u.id AND e.amount > 90)"
        ),
        "SELECT u.id FROM users u \
         WHERE EXISTS (SELECT 1 FROM events e WHERE e.user_id = u.id AND e.amount > 90)",
    ).await;

    // -- #23 3-table JOIN (categories ⋈ events ⋈ users via BETWEEN) ---------
    // BETWEEN-join is intentionally expensive — measures predicate-pushdown
    // coverage when the join key is a range, not equality.
    let join3_between = pair(
        pg, sess,
        format!(
            "SELECT c.name, SUM(e.amount) FROM {schema}.categories c \
             JOIN {schema}.events e ON e.amount BETWEEN c.min_amt AND c.max_amt \
             JOIN {schema}.users u ON e.user_id = u.id \
             GROUP BY 1"
        ),
        "SELECT c.name, SUM(e.amount) FROM categories c \
         JOIN events e ON e.amount BETWEEN c.min_amt AND c.max_amt \
         JOIN users u ON e.user_id = u.id \
         GROUP BY 1",
    ).await;

    // -- #24 UNION ALL of two filtered scans --------------------------------
    let union_all = pair(
        pg, sess,
        format!(
            "SELECT id, 'paid' AS kind FROM {schema}.events WHERE status = 'paid' \
             UNION ALL \
             SELECT id, 'pending' FROM {schema}.events WHERE status = 'pending'"
        ),
        "SELECT id, 'paid' AS kind FROM events WHERE status = 'paid' \
         UNION ALL \
         SELECT id, 'pending' FROM events WHERE status = 'pending'",
    ).await;

    // -- #25 ORDER BY NULLS LAST + LIMIT ------------------------------------
    let order_nulls_last = pair(
        pg, sess,
        format!(
            "SELECT id, last_login FROM {schema}.users \
             ORDER BY last_login DESC NULLS LAST LIMIT 50"
        ),
        "SELECT id, last_login FROM users \
         ORDER BY last_login DESC NULLS LAST LIMIT 50",
    ).await;

    // -- #26 Top-N per group (MAX) ------------------------------------------
    let top_n_per_group = pair(
        pg, sess,
        format!(
            "SELECT user_id, MAX(amount) FROM {schema}.events \
             GROUP BY user_id ORDER BY 2 DESC LIMIT 10"
        ),
        "SELECT user_id, MAX(amount) FROM events \
         GROUP BY user_id ORDER BY 2 DESC LIMIT 10",
    ).await;

    // -- #27 Numeric range filter on doubles --------------------------------
    let numeric_range = pair(
        pg, sess,
        format!("SELECT COUNT(*) FROM {schema}.events WHERE amount BETWEEN 25.5 AND 75.5"),
        "SELECT COUNT(*) FROM events WHERE amount BETWEEN 25.5 AND 75.5",
    ).await;

    ExtendedResults {
        count_distinct,
        like_prefix,
        groupby_having,
        window_lag,
        recursive_cte,
        correlated_sub,
        exists_in_where,
        join3_between,
        union_all,
        order_nulls_last,
        top_n_per_group,
        numeric_range,
    }
}

/// Single-replicated suite that runs the FULL 29-metric comparison and emits
/// the dashboard JSON card. Each `compare_postgres_*` test file is a 30-line
/// wrapper that calls this with the right (rows, format, id, name, claim).
///
/// Stack budget: the inner body's combined async state-machine size exceeds
/// tokio's default 2 MiB worker-thread stack (29 metrics × ~all-on-one-frame
/// state-machine variants), so we hop onto a dedicated `std::thread` with a
/// 32 MiB stack and a fresh current-thread tokio runtime. The wrapper
/// remains `async fn` so the per-scale `#[tokio::test]` files don't need to
/// change. spawn_blocking would also work but doesn't let us pin the stack
/// size, and the inner future borrows `&str` arguments that aren't `'static`.
pub async fn run_full_compare(
    rows: usize,
    basin_format: BasinFormat,
    id: &str,
    name: &str,
    claim: &str,
    schema_prefix: &str,
) {
    // Move the &str args into owned Strings so they outlive the wrapper
    // frame and can cross the thread boundary.
    let id = id.to_string();
    let name = name.to_string();
    let claim = claim.to_string();
    let schema_prefix = schema_prefix.to_string();
    let handle = std::thread::Builder::new()
        .name("compare-postgres-runner".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build current-thread runtime");
            rt.block_on(run_full_compare_inner(
                rows,
                basin_format,
                &id,
                &name,
                &claim,
                &schema_prefix,
            ));
        })
        .expect("spawn runner thread");
    // Bridge the std::thread join back to async land. join() blocks, but
    // we're already on a tokio worker — spawn_blocking keeps the worker
    // free for other tasks.
    tokio::task::spawn_blocking(move || handle.join().expect("runner thread panicked"))
        .await
        .expect("await runner join");
}

async fn run_full_compare_inner(
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
            created_at TIMESTAMPTZ, \
            last_login TIMESTAMPTZ)"
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
    // Small fixed-shape lookup table used by the 3-table JOIN shape (#23).
    // Five buckets that together cover the full events.amount domain — Basin
    // and PG both seed it with the same 5 rows so the join cardinality is
    // identical on each side.
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.categories (\
            id BIGINT PRIMARY KEY, \
            name TEXT, \
            min_amt DOUBLE PRECISION, \
            max_amt DOUBLE PRECISION)"
    ))
    .await
    .expect("pg create categories");
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
    pg.simple_query(&format!(
        "ALTER TABLE {schema}.categories DROP CONSTRAINT categories_pkey"
    ))
    .await
    .expect("drop categories pkey");

    // ~10 events per user across all scales.
    let users: usize = (rows / 10).max(100);
    let user_count = users as i64;
    let row_count = rows as i64;
    let insert_batch = insert_batch_for(rows);

    // ---- PG seed users -----------------------------------------------------
    // `last_login` is NULL for every 10th row so the ORDER BY NULLS LAST
    // shape (#25) has a non-trivial null cluster.
    {
        let mut stmt = String::with_capacity(users * 90);
        stmt.push_str(&format!(
            "INSERT INTO {schema}.users (id, email, created_at, last_login) VALUES "
        ));
        for i in 0..users as i64 {
            if i > 0 {
                stmt.push(',');
            }
            let last_login = if i % 10 == 0 {
                "NULL".to_string()
            } else {
                format!("to_timestamp({})", EPOCH + i + 100_000)
            };
            stmt.push_str(&format!(
                "({i}, '{}', to_timestamp({}), {last_login})",
                email_for(i),
                EPOCH + i
            ));
        }
        pg.simple_query(&stmt).await.expect("pg seed users");
    }

    // ---- PG seed categories (5 fixed rows) --------------------------------
    // Buckets span [0, max_amount] where max_amount = (rows-1) * 0.5. Each
    // event row lands in exactly one bucket — so `JOIN ... ON e.amount
    // BETWEEN c.min_amt AND c.max_amt` has rows-cardinality output and the
    // GROUP BY produces exactly 5 groups.
    let max_amt = ((row_count - 1) as f64) * 0.5;
    let cat_rows: [(i64, &str, f64, f64); 5] = [
        (1, "micro",  0.0,            max_amt * 0.20),
        (2, "small",  max_amt * 0.20 + 0.001, max_amt * 0.40),
        (3, "medium", max_amt * 0.40 + 0.001, max_amt * 0.60),
        (4, "large",  max_amt * 0.60 + 0.001, max_amt * 0.80),
        (5, "xlarge", max_amt * 0.80 + 0.001, max_amt + 1.0),
    ];
    {
        let mut stmt = String::new();
        stmt.push_str(&format!(
            "INSERT INTO {schema}.categories (id, name, min_amt, max_amt) VALUES "
        ));
        for (i, (id, name, lo, hi)) in cat_rows.iter().enumerate() {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({id}, '{name}', {lo}, {hi})"));
        }
        pg.simple_query(&stmt).await.expect("pg seed categories");
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

    // ---- PG: 12 SaaS metrics + 3 OLAP metrics (extracted) ----------------
    let pg_core = run_pg_core_suite(
        &pg,
        &schema,
        target_id,
        range_lo_ts,
        range_hi_ts,
        pagination_threshold,
        olap_cutoff_ts,
        delete_in_list.as_str(),
    )
    .await;
    let pg_point_p50 = pg_core.point_p50;
    let pg_point_p99 = pg_core.point_p99;
    let pg_range_p50 = pg_core.range_p50;
    let pg_range_p99 = pg_core.range_p99;
    let pg_agg_p50 = pg_core.agg_p50;
    let pg_join_p50 = pg_core.join_p50;
    let pg_ilike_p50 = pg_core.ilike_p50;
    let pg_page_p50 = pg_core.page_p50;
    let pg_upd1_p50 = pg_core.upd1_p50;
    let pg_bulk_upd_ms = pg_core.bulk_upd_ms;
    let pg_delete_ms = pg_core.delete_ms;
    let pg_count_p50 = pg_core.count_p50;
    let pg_trunc_p50 = pg_core.trunc_p50;
    let pg_olap_join_p50 = pg_core.olap_join_p50;

    // ---- Basin setup ------------------------------------------------------
    let instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    let with_clause = basin_format.with_clause();
    // `last_login` is BIGINT and nullable so ~10% of rows can be NULL — that
    // drives shape #25 (ORDER BY NULLS LAST + LIMIT). Basin stores all clock
    // columns as seconds-since-epoch BIGINT, see the DATE_TRUNC note further
    // down for why we don't use TIMESTAMPTZ here.
    sess.execute(&format!(
        "CREATE TABLE users (\
            id BIGINT NOT NULL, \
            email TEXT NOT NULL, \
            created_at BIGINT NOT NULL, \
            last_login BIGINT){with_clause}"
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
    sess.execute(&format!(
        "CREATE TABLE categories (\
            id BIGINT NOT NULL, \
            name TEXT NOT NULL, \
            min_amt DOUBLE PRECISION NOT NULL, \
            max_amt DOUBLE PRECISION NOT NULL){with_clause}"
    ))
    .await
    .unwrap();

    // ---- Basin seed users -------------------------------------------------
    // Mirrors the PG seed: `last_login` is NULL for every 10th row.
    {
        let mut stmt = String::with_capacity(users * 80);
        stmt.push_str("INSERT INTO users VALUES ");
        for i in 0..users as i64 {
            if i > 0 {
                stmt.push(',');
            }
            let last_login = if i % 10 == 0 {
                "NULL".to_string()
            } else {
                (EPOCH + i + 100_000).to_string()
            };
            stmt.push_str(&format!(
                "({i}, '{}', {}, {last_login})",
                email_for(i),
                EPOCH + i
            ));
        }
        sess.execute(&stmt).await.expect("basin seed users");
    }

    // ---- Basin seed categories (same 5 buckets as PG) ---------------------
    {
        let mut stmt = String::new();
        stmt.push_str("INSERT INTO categories VALUES ");
        for (i, (id, name, lo, hi)) in cat_rows.iter().enumerate() {
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({id}, '{name}', {lo}, {hi})"));
        }
        sess.execute(&stmt).await.expect("basin seed categories");
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

    // ---- Basin SaaS + OLAP measurements (extracted) -----------------------
    // Pulled out into its own async fn for the same stack-budget reason as
    // run_extended_suite — see BasinSaasOlapResults docs.
    let basin_core = run_basin_core_suite(
        &sess,
        target_id,
        range_lo_ts,
        range_hi_ts,
        pagination_threshold,
        olap_cutoff_ts,
        &delete_in_list,
    )
    .await;
    let basin_point_p50 = basin_core.point_p50;
    let basin_point_p99 = basin_core.point_p99;
    let basin_range_p50 = basin_core.range_p50;
    let basin_range_p99 = basin_core.range_p99;
    let basin_agg_p50 = basin_core.agg_p50;
    let basin_join_p50 = basin_core.join_p50;
    let basin_ilike_p50 = basin_core.ilike_p50;
    let basin_page_p50 = basin_core.page_p50;
    let basin_upd1_p50 = basin_core.upd1_p50;
    let basin_bulk_upd_ms = basin_core.bulk_upd_ms;
    let basin_delete_ms = basin_core.delete_ms;
    let basin_count_p50 = basin_core.count_p50;
    let basin_trunc_p50 = basin_core.trunc_p50;
    let basin_olap_join_p50 = basin_core.olap_join_p50;

    // =======================================================================
    // Extended-shape suite (#16-#27) — 12 perf-coverage probes.
    // Extracted into its own async fn so the outer state machine stays small
    // (a flat block here would balloon `run_full_compare` past tokio's
    // default 2 MiB worker-thread stack budget — see ExtendedResults).
    // =======================================================================
    let ext = run_extended_suite(&pg, &sess, &schema).await;

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

    // Extended-shape rows: print the value if Basin succeeded, else "GAP".
    let row_opt = |label: &str, b: Option<f64>, p: f64| {
        match b {
            Some(bv) => println!(
                "{label:>34} {:>14.3} {:>14.3} {:>16}",
                bv,
                p,
                format!("{:.2}x", p / bv.max(1e-9))
            ),
            None => println!(
                "{label:>34} {:>14} {:>14.3} {:>16}",
                "GAP", p, "-"
            ),
        }
    };
    row_opt("count_distinct_p50_ms", ext.count_distinct.0, ext.count_distinct.1);
    row_opt("like_prefix_p50_ms", ext.like_prefix.0, ext.like_prefix.1);
    row_opt("groupby_having_p50_ms", ext.groupby_having.0, ext.groupby_having.1);
    row_opt("window_lag_p50_ms", ext.window_lag.0, ext.window_lag.1);
    row_opt("recursive_cte_fib30_p50_ms", ext.recursive_cte.0, ext.recursive_cte.1);
    row_opt("correlated_subq_p50_ms", ext.correlated_sub.0, ext.correlated_sub.1);
    row_opt("exists_in_where_p50_ms", ext.exists_in_where.0, ext.exists_in_where.1);
    row_opt("join_3table_between_p50_ms", ext.join3_between.0, ext.join3_between.1);
    row_opt("union_all_p50_ms", ext.union_all.0, ext.union_all.1);
    row_opt("order_by_nulls_last_p50_ms", ext.order_nulls_last.0, ext.order_nulls_last.1);
    row_opt("top_n_per_group_p50_ms", ext.top_n_per_group.0, ext.top_n_per_group.1);
    row_opt("numeric_range_p50_ms", ext.numeric_range.0, ext.numeric_range.1);

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

    // mk_ext: extended-shape variant. If Basin succeeded → normal row.
    // If Basin errored (Option::None) → label gets a "(basin gap)" suffix,
    // basin field is -1.0 (JSON-safe sentinel; the dashboard renderer can
    // skip ratio computation), and `better` is set to Postgres so the card
    // visibly flags the gap. Postgres failure is mirrored with f64::INFINITY
    // upstream and yields the same sentinel handling here.
    let mk_ext = |label: &str, basin: Option<f64>, postgres: f64| -> CompareMetric {
        match (basin, postgres.is_finite()) {
            (Some(b), true) => CompareMetric {
                label: label.into(),
                basin: b,
                postgres,
                unit: "ms".into(),
                better: which_wins(b, postgres),
                ratio_text: Some(format!("pg / basin = {:.2}x", postgres / b.max(1e-9))),
            },
            (None, true) => CompareMetric {
                label: format!("{label} (basin gap)"),
                basin: -1.0,
                postgres,
                unit: "ms".into(),
                better: WhichWins::Postgres,
                ratio_text: Some("basin: unsupported".into()),
            },
            (Some(b), false) => CompareMetric {
                label: format!("{label} (pg failed)"),
                basin: b,
                postgres: -1.0,
                unit: "ms".into(),
                better: WhichWins::Basin,
                ratio_text: Some("postgres: failed".into()),
            },
            (None, false) => CompareMetric {
                label: format!("{label} (both failed)"),
                basin: -1.0,
                postgres: -1.0,
                unit: "ms".into(),
                better: WhichWins::Tie,
                ratio_text: Some("both: failed".into()),
            },
        }
    };

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
        // Extended shapes (perf-coverage probes)
        mk_ext("COUNT(DISTINCT user_id) p50", ext.count_distinct.0, ext.count_distinct.1),
        mk_ext("LIKE 'pending%' prefix p50", ext.like_prefix.0, ext.like_prefix.1),
        mk_ext("Multi-col GROUP BY + HAVING p50", ext.groupby_having.0, ext.groupby_having.1),
        mk_ext("Window LAG OVER PARTITION p50", ext.window_lag.0, ext.window_lag.1),
        mk_ext("Recursive CTE Fibonacci(30) p50", ext.recursive_cte.0, ext.recursive_cte.1),
        mk_ext("Correlated subquery in SELECT p50", ext.correlated_sub.0, ext.correlated_sub.1),
        mk_ext("EXISTS in WHERE p50", ext.exists_in_where.0, ext.exists_in_where.1),
        mk_ext("3-table JOIN BETWEEN p50", ext.join3_between.0, ext.join3_between.1),
        mk_ext("UNION ALL two scans p50", ext.union_all.0, ext.union_all.1),
        mk_ext("ORDER BY NULLS LAST + LIMIT p50", ext.order_nulls_last.0, ext.order_nulls_last.1),
        mk_ext("Top-N per group (MAX) p50", ext.top_n_per_group.0, ext.top_n_per_group.1),
        mk_ext("Numeric range BETWEEN p50", ext.numeric_range.0, ext.numeric_range.1),
    ];

    report_postgres_compare(id, name, claim, true, metrics, None);

    instance.bg.shutdown().await;
    instance.wal.close().await.unwrap();

    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    std::mem::forget(_guard);
}
