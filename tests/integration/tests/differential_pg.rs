//! PG-oracle differential test harness.
//!
//! Runs identical SQL against both an in-process Basin pgwire server and a
//! real PostgreSQL instance, failing on any divergence.
//!
//! # Running
//!
//! Without a real PG instance every test is skipped cleanly (exit 0):
//! ```
//! cargo test -p basin-integration-tests --test differential_pg --release
//! ```
//!
//! With a real PG (e.g. via Docker):
//! ```sh
//! docker run -d -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
//! PG_DIFF_TEST_DSN=postgres://postgres:postgres@127.0.0.1:5432/postgres \
//!   cargo test -p basin-integration-tests --test differential_pg --release
//! ```
//!
//! See `tests/integration/tests/DIFFERENTIAL_README.md` for full docs.
//!
//! # Architecture
//!
//! - `start_basin_server()` spins up a fresh in-process Basin pgwire server
//!   on a random local port using the same `start_server()` pattern as every
//!   other integration test in this suite.
//! - `DifferentialRunner` wraps both clients and exposes:
//!   - `run_setup` — run DDL/DML on both sides (panics on harness failure)
//!   - `run_assert_match` — run SELECT on both, compare cell-by-cell
//!   - `run_assert_both_error` — assert both sides error with same SQLSTATE
//!   - `run_assert_both_ok` — assert both sides succeed (loose DDL check)
//! - Each test gets an isolated table namespace via a UUID prefix.
//! - Tests marked `#[ignore]` are gated on specific issues; they serve as
//!   guards that flip green automatically when the fix lands.
//!
//! # Comparison Rules (DifferentialRunner::run_assert_match)
//!
//! - One side errors + other succeeds → `DivergenceKind::OneErrored`
//! - Both error → SQLSTATE codes must match
//! - Both succeed:
//!   - Row counts must match
//!   - Column names must match (case-insensitive)
//!   - Each (row, col) cell:
//!     1. Exact string equality (text protocol, covers most types)
//!     2. JSONB: parse + normalize (sort keys recursively), then compare
//!     3. FLOAT: epsilon comparison (default 1e-9, relative to magnitude)
//!     4. TIMESTAMP: parse to microseconds, tolerance ≤ 1 µs
//!   - CommandComplete tag: prefix + numeric count must match

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, Error as PgError, NoTls, SimpleQueryMessage};
use uuid::Uuid;

// =============================================================================
// DifferentialRunner — comparison engine
// =============================================================================

/// Float comparison epsilon (relative to max(|a|, |b|, 1)).
const FLOAT_EPSILON: f64 = 1e-9;

/// Timestamp microsecond tolerance.
const TIMESTAMP_TOLERANCE_MICROS: i64 = 1;

/// A divergence discovered between Basin and PG.
#[derive(Debug)]
#[allow(dead_code)]
enum DivergenceKind {
    OneErrored {
        basin_error: Option<String>,
        pg_error: Option<String>,
    },
    DifferentSqlstate {
        basin_state: String,
        pg_state: String,
    },
    RowCountMismatch {
        basin: usize,
        pg: usize,
    },
    SchemaMismatch {
        basin: Vec<String>,
        pg: Vec<String>,
    },
    CellMismatch {
        row_idx: usize,
        col_name: String,
        basin_value: String,
        pg_value: String,
    },
    TagMismatch {
        basin_tag: String,
        pg_tag: String,
    },
}

impl std::fmt::Display for DivergenceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Extract SQLSTATE from a tokio-postgres error; falls back to "00000".
fn sqlstate_of(e: &PgError) -> String {
    if let Some(dbe) = e.as_db_error() {
        dbe.code().code().to_owned()
    } else {
        "00000".to_owned()
    }
}

/// Display an option cell value for error messages.
fn display_opt(v: Option<&str>) -> String {
    match v {
        None => "NULL".to_owned(),
        Some(s) => format!("{s:?}"),
    }
}

/// Normalize a JSON value for comparison: sort object keys recursively.
fn normalize_json(v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<String, serde_json::Value> = map
                .iter()
                .map(|(k, vv)| (k.clone(), normalize_json(vv)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(normalize_json).collect()),
        other => other.clone(),
    }
}

/// Attempt to parse two strings as timestamps and compare within tolerance.
fn try_compare_timestamps(a: &str, b: &str) -> Option<bool> {
    use chrono::{DateTime, NaiveDateTime, Utc};

    fn parse_ts(s: &str) -> Option<i64> {
        if let Ok(dt) = s.parse::<DateTime<Utc>>() {
            return Some(dt.timestamp_micros());
        }
        if let Ok(ndt) = s.parse::<NaiveDateTime>() {
            return Some(ndt.and_utc().timestamp_micros());
        }
        let formats = [
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
        ];
        for fmt in &formats {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Some(ndt.and_utc().timestamp_micros());
            }
        }
        None
    }

    let ta = parse_ts(a)?;
    let tb = parse_ts(b)?;
    Some((ta - tb).abs() <= TIMESTAMP_TOLERANCE_MICROS)
}

/// Compare two optional cell strings using type-aware rules.
fn cells_equal(basin: Option<&str>, pg: Option<&str>) -> bool {
    match (basin, pg) {
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
        (Some(b), Some(p)) => {
            if b == p {
                return true;
            }
            // JSONB: parse + normalize.
            if let (Ok(bj), Ok(pj)) = (
                serde_json::from_str::<serde_json::Value>(b),
                serde_json::from_str::<serde_json::Value>(p),
            ) {
                if normalize_json(&bj) == normalize_json(&pj) {
                    return true;
                }
            }
            // Float: epsilon comparison.
            if let (Ok(bf), Ok(pf)) = (b.parse::<f64>(), p.parse::<f64>()) {
                let tol = FLOAT_EPSILON * bf.abs().max(pf.abs()).max(1.0);
                if (bf - pf).abs() <= tol {
                    return true;
                }
            }
            // Timestamp: microsecond tolerance.
            if let Some(result) = try_compare_timestamps(b, p) {
                return result;
            }
            false
        }
    }
}

/// Extract column names from a SimpleQueryMessage::Row.
fn col_names_of(msg: &SimpleQueryMessage) -> Vec<String> {
    match msg {
        SimpleQueryMessage::Row(row) => (0..row.len())
            .map(|i| {
                row.columns()
                    .get(i)
                    .map(|c| c.name().to_owned())
                    .unwrap_or_else(|| format!("col{i}"))
            })
            .collect(),
        _ => vec![],
    }
}

/// Extract CommandComplete numeric count from a message list.
fn extract_command_tag(msgs: &[SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let SimpleQueryMessage::CommandComplete(n) = msg {
            return Some(n.to_string());
        }
    }
    None
}

/// Loosely compare CommandComplete tags.
fn tags_compatible(a: &str, b: &str) -> bool {
    fn split_tag(t: &str) -> (String, Option<u64>) {
        let parts: Vec<&str> = t.split_whitespace().collect();
        match parts.as_slice() {
            [kw] => (kw.to_uppercase(), None),
            [kw, n] => (kw.to_uppercase(), n.parse::<u64>().ok()),
            [kw, _zero, n] => (kw.to_uppercase(), n.parse::<u64>().ok()),
            _ => (t.to_uppercase(), None),
        }
    }
    let (ak, an) = split_tag(a);
    let (bk, bn) = split_tag(b);
    if ak != bk {
        return false;
    }
    match (an, bn) {
        (Some(x), Some(y)) => x == y,
        _ => true,
    }
}

/// Compare simple-query message lists from both sides.
fn compare_results(
    sql: &str,
    basin_msgs: Vec<SimpleQueryMessage>,
    pg_msgs: Vec<SimpleQueryMessage>,
) -> anyhow::Result<()> {
    let basin_rows: Vec<&SimpleQueryMessage> = basin_msgs
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .collect();
    let pg_rows: Vec<&SimpleQueryMessage> = pg_msgs
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .collect();

    if basin_rows.len() != pg_rows.len() {
        anyhow::bail!(
            "DIVERGENCE on: {sql}\n  {:?}",
            DivergenceKind::RowCountMismatch {
                basin: basin_rows.len(),
                pg: pg_rows.len(),
            }
        );
    }

    if !basin_rows.is_empty() {
        let basin_cols = col_names_of(basin_rows[0]);
        let pg_cols = col_names_of(pg_rows[0]);
        let basin_lower: Vec<_> = basin_cols.iter().map(|s| s.to_lowercase()).collect();
        let pg_lower: Vec<_> = pg_cols.iter().map(|s| s.to_lowercase()).collect();

        if basin_lower != pg_lower {
            anyhow::bail!(
                "DIVERGENCE on: {sql}\n  {:?}",
                DivergenceKind::SchemaMismatch {
                    basin: basin_cols,
                    pg: pg_cols,
                }
            );
        }

        for (row_idx, (basin_msg, pg_msg)) in basin_rows.iter().zip(pg_rows.iter()).enumerate() {
            let SimpleQueryMessage::Row(basin_row) = basin_msg else {
                unreachable!()
            };
            let SimpleQueryMessage::Row(pg_row) = pg_msg else {
                unreachable!()
            };

            for (col_idx, col_name) in basin_cols.iter().enumerate() {
                let bv = basin_row.get(col_idx);
                let pv = pg_row.get(col_idx);
                if !cells_equal(bv, pv) {
                    anyhow::bail!(
                        "DIVERGENCE on: {sql}\n  {:?}",
                        DivergenceKind::CellMismatch {
                            row_idx,
                            col_name: col_name.clone(),
                            basin_value: display_opt(bv),
                            pg_value: display_opt(pv),
                        }
                    );
                }
            }
        }
    }

    let basin_tag = extract_command_tag(&basin_msgs);
    let pg_tag = extract_command_tag(&pg_msgs);
    if let (Some(bt), Some(pt)) = (&basin_tag, &pg_tag) {
        if !tags_compatible(bt, pt) {
            anyhow::bail!(
                "DIVERGENCE on: {sql}\n  {:?}",
                DivergenceKind::TagMismatch {
                    basin_tag: bt.clone(),
                    pg_tag: pt.clone(),
                }
            );
        }
    }

    Ok(())
}

/// Runs SQL against both Basin and PG and compares results.
struct DifferentialRunner {
    basin: Client,
    pg: Client,
}

impl DifferentialRunner {
    fn new(basin: Client, pg: Client) -> Self {
        Self { basin, pg }
    }

    /// Run identical DDL/DML setup on both sides. Panics on any harness failure.
    async fn run_setup(&self, sqls: &[&str]) -> anyhow::Result<()> {
        for sql in sqls {
            self.basin
                .simple_query(sql)
                .await
                .unwrap_or_else(|e| panic!("Basin setup failed on [{sql}]: {e}"));
            self.pg
                .simple_query(sql)
                .await
                .unwrap_or_else(|e| panic!("PG setup failed on [{sql}]: {e}"));
        }
        Ok(())
    }

    /// Assert both sides succeed; ignore result shape. Useful for DDL.
    #[allow(dead_code)]
    async fn run_assert_both_ok(&self, sql: &str) -> anyhow::Result<()> {
        let basin_res = self.basin.simple_query(sql).await;
        let pg_res = self.pg.simple_query(sql).await;
        match (basin_res, pg_res) {
            (Ok(_), Ok(_)) => Ok(()),
            (Err(be), Ok(_)) => anyhow::bail!("Basin errored on [{sql}] but PG succeeded: {be}"),
            (Ok(_), Err(pe)) => anyhow::bail!("PG errored on [{sql}] but Basin succeeded: {pe}"),
            (Err(be), Err(pe)) => {
                anyhow::bail!("Both errored on [{sql}]: basin={be}, pg={pe}")
            }
        }
    }

    /// Assert that both sides return an error with the same SQLSTATE.
    /// If `expected_sqlstate` is `Some("XXXXX")`, both sides must produce that code.
    async fn run_assert_both_error(
        &self,
        sql: &str,
        expected_sqlstate: Option<&str>,
    ) -> anyhow::Result<()> {
        let basin_res = self.basin.simple_query(sql).await;
        let pg_res = self.pg.simple_query(sql).await;

        let basin_err = match basin_res {
            Err(e) => e,
            Ok(_) => anyhow::bail!("Basin succeeded on [{sql}] but expected an error"),
        };
        let pg_err = match pg_res {
            Err(e) => e,
            Ok(_) => anyhow::bail!("PG succeeded on [{sql}] but expected an error"),
        };

        let basin_state = sqlstate_of(&basin_err);
        let pg_state = sqlstate_of(&pg_err);

        if basin_state != pg_state {
            anyhow::bail!(
                "DIVERGENCE on: {sql}\n  {:?}",
                DivergenceKind::DifferentSqlstate {
                    basin_state: basin_state.clone(),
                    pg_state: pg_state.clone(),
                }
            );
        }

        if let Some(expected) = expected_sqlstate {
            if basin_state != expected {
                anyhow::bail!(
                    "Both sides returned SQLSTATE {basin_state} but expected {expected} for [{sql}]"
                );
            }
        }

        Ok(())
    }

    /// Run SQL on both sides and compare results cell-by-cell.
    async fn run_assert_match(&self, sql: &str) -> anyhow::Result<()> {
        let basin_res = self.basin.simple_query(sql).await;
        let pg_res = self.pg.simple_query(sql).await;

        match (basin_res, pg_res) {
            (Err(be), Err(pe)) => {
                let basin_state = sqlstate_of(&be);
                let pg_state = sqlstate_of(&pe);
                if basin_state != pg_state {
                    anyhow::bail!(
                        "DIVERGENCE on: {sql}\n  {:?}",
                        DivergenceKind::DifferentSqlstate {
                            basin_state,
                            pg_state,
                        }
                    );
                }
                Ok(())
            }
            (Err(be), Ok(_)) => anyhow::bail!(
                "DIVERGENCE on: {sql}\n  {:?}",
                DivergenceKind::OneErrored {
                    basin_error: Some(be.to_string()),
                    pg_error: None,
                }
            ),
            (Ok(_), Err(pe)) => anyhow::bail!(
                "DIVERGENCE on: {sql}\n  {:?}",
                DivergenceKind::OneErrored {
                    basin_error: None,
                    pg_error: Some(pe.to_string()),
                }
            ),
            (Ok(basin_msgs), Ok(pg_msgs)) => compare_results(sql, basin_msgs, pg_msgs),
        }
    }
}

// =============================================================================
// Basin test server (identical shape to param_bind_smoke.rs / orm_compat.rs)
// =============================================================================

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_basin_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("difftest".to_owned(), project);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: None,
    })
    .await
    .expect("basin server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect_basin(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=difftest password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to basin");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("[differential] basin conn driver: {e}");
        }
    });
    client
}

async fn connect_pg_dsn(dsn: &str) -> Client {
    let (client, conn) = tokio_postgres::connect(dsn, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect to PG DSN {dsn}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("[differential] PG conn driver: {e}");
        }
    });
    client
}

// =============================================================================
// Harness factory
// =============================================================================

/// Build a `DifferentialRunner`.  Returns `None` (and logs a skip message) if
/// `PG_DIFF_TEST_DSN` is not set in the environment.
async fn make_runner(server: &TestServer) -> Option<DifferentialRunner> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(v) if !v.is_empty() => v,
        _ => {
            eprintln!(
                "[differential] PG_DIFF_TEST_DSN not set — skipping differential tests"
            );
            return None;
        }
    };
    let basin_client = connect_basin(server.addr).await;
    let pg_client = connect_pg_dsn(&dsn).await;
    Some(DifferentialRunner::new(basin_client, pg_client))
}

/// Generate a unique table-name prefix for per-test isolation.
fn table_prefix() -> String {
    let id = Uuid::new_v4().simple().to_string();
    format!("diff_{}", &id[..12])
}

/// Drop a table on both sides (best-effort cleanup).
async fn drop_table(runner: &DifferentialRunner, table: &str) {
    let sql = format!("DROP TABLE IF EXISTS {table}");
    let _ = runner.basin.simple_query(&sql).await;
    let _ = runner.pg.simple_query(&sql).await;
}

// =============================================================================
// SANITY — pipeline floor (should pass on day 1)
// =============================================================================

/// Test 1: SELECT 1 — the simplest possible query.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_sanity_select_1() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner.run_assert_match("SELECT 1").await.unwrap();
}

/// Test 2: SELECT 1, 2, 3 — multi-column literal projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_sanity_select_multi_column() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner.run_assert_match("SELECT 1, 2, 3").await.unwrap();
}

/// Test 3: SELECT NULL — NULL literal projection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_sanity_select_null() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner.run_assert_match("SELECT NULL").await.unwrap();
}

/// Test 4: INT4 arithmetic — SELECT 1::int4 + 2::int4.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_sanity_int4_arithmetic() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT 1::int4 + 2::int4")
        .await
        .unwrap();
}

/// Test 5: Text concatenation — SELECT 'hello'::text || ' world'::text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_sanity_text_concat() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT 'hello'::text || ' world'::text")
        .await
        .unwrap();
}

// =============================================================================
// NULL SEMANTICS
// =============================================================================

/// Test 6: WHERE col IS NULL vs WHERE col = NULL.
/// PG: `col = NULL` always yields UNKNOWN (three-valued logic) → 0 rows even
/// when col IS NULL. Basin must match this behaviour.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_null_is_null_vs_eq_null() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_nullsem");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'a'), (2, NULL), (3, 'c')"),
        ])
        .await
        .unwrap();

    // IS NULL — should return 1 row (id=2).
    runner
        .run_assert_match(&format!("SELECT id FROM {t} WHERE val IS NULL ORDER BY id"))
        .await
        .unwrap();

    // = NULL — should return 0 rows (three-valued logic).
    runner
        .run_assert_match(&format!("SELECT id FROM {t} WHERE val = NULL ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 7: NOT IN with NULL in the subquery list.
/// PG: `x NOT IN (1, NULL)` yields NULL (unknown) for all rows → 0 rows.
/// DataFusion matches this semantically, but must be verified concretely.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_null_not_in_with_null() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_notin");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (n INT)"),
            &format!("INSERT INTO {t} VALUES (1), (2), (3)"),
        ])
        .await
        .unwrap();

    // NOT IN with NULL in list → should return 0 rows.
    runner
        .run_assert_match(&format!(
            "SELECT n FROM {t} WHERE n NOT IN (1, NULL) ORDER BY n"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 8: COALESCE and NULLIF behaviours.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_null_coalesce_nullif() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match("SELECT COALESCE(NULL, NULL, 42)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT COALESCE(NULL, 'first', 'second')")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT NULLIF(5, 5)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT NULLIF(5, 6)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT NULLIF(NULL::int, NULL::int)")
        .await
        .unwrap();
}

// =============================================================================
// TYPE COERCION
// =============================================================================

/// Test 9: INT4 + INT8 — result type promotion.
/// PG promotes INT4 + INT8 → INT8. Result should be 2147483648.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_type_int4_plus_int8() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT 2147483647::int4 + 1::int8")
        .await
        .unwrap();
}

/// Test 10: NUMERIC arithmetic precision.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_type_numeric_precision() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT (1.1::numeric + 2.2::numeric)::text")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT (0.1::numeric * 0.1::numeric)::text")
        .await
        .unwrap();
}

/// Test 11: text-to-int implicit cast is an error in both PG and Basin.
/// PG: `operator does not exist: text + integer` → SQLSTATE 42883.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_type_text_to_int_implicit_cast_errors() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_both_error("SELECT 'abc' + 1", Some("42883"))
        .await
        .unwrap();
}

/// Test 12: Date + INTERVAL arithmetic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_type_date_interval_arithmetic() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT '2024-01-15'::date + INTERVAL '10 days'")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT '2024-03-01'::date - '2024-01-01'::date")
        .await
        .unwrap();
}

// =============================================================================
// JSONB OPERATORS
// =============================================================================

/// Test 13: jsonb -> 'key' (field extraction returning jsonb).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_arrow_op() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_jb1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, data JSONB)"),
            &format!("INSERT INTO {t} VALUES (1, '{{\"a\":1,\"b\":\"hello\"}}')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT data -> 'a' FROM {t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 14: jsonb ->> 'key' (text extraction).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_double_arrow_op() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_jb2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, data JSONB)"),
            &format!("INSERT INTO {t} VALUES (1, '{{\"name\":\"alice\",\"score\":42}}')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT data ->> 'name' FROM {t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 15: jsonb @> containment operator.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_containment_op() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_jb3");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, data JSONB)"),
            &format!(
                "INSERT INTO {t} VALUES (1, '{{\"a\":1,\"b\":2}}'), (2, '{{\"a\":1,\"c\":3}}')"
            ),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id FROM {t} WHERE data @> '{{\"a\":1}}' ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 16: jsonb #> path extraction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_path_op() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_jb4");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, data JSONB)"),
            &format!("INSERT INTO {t} VALUES (1, '{{\"a\":{{\"b\":42}}}}')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT data #> '{{a,b}}' FROM {t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 17: jsonb_each() set-returning function.
///
/// Basin currently returns a scalar from `jsonb_each` instead of multiple rows
/// (issue #139). Ignored until the SRF fix lands — at that point it will flip
/// green automatically, confirming the fix is correct end-to-end.
// KNOWN DIVERGENCE — gated on #139. Test runs and FAILS honestly today: basin's
// jsonb_each is a scalar stub returning a single text value, while PG returns
// N rows (one per JSON object key). The failure proves the harness catches the
// bug. This will flip to passing automatically when #139 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_each_srf() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_jb5");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, data JSONB)"),
            &format!(
                "INSERT INTO {t} VALUES (1, '{{\"x\":1,\"y\":2,\"z\":3}}')"
            ),
        ])
        .await
        .unwrap();

    // PG: returns 3 rows (one per key). Basin currently returns wrong shape.
    runner
        .run_assert_match(&format!(
            "SELECT key, value FROM {t}, jsonb_each(data) ORDER BY key"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// WINDOW FUNCTIONS
// =============================================================================

/// Test 18: SUM(x) OVER (ORDER BY id) — cumulative running sum.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_window_running_sum() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_wf1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, x BIGINT)"),
            &format!("INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30), (4, 5)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id, SUM(x) OVER (ORDER BY id) AS running FROM {t} ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 19: lag(x, 1) OVER (ORDER BY id).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_window_lag() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_wf2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, x BIGINT)"),
            &format!("INSERT INTO {t} VALUES (1, 100), (2, 200), (3, 300)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id, lag(x, 1) OVER (ORDER BY id) AS prev FROM {t} ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 20: percentile_disc(0.5) WITHIN GROUP (ORDER BY x).
/// Basin's #77 implementation should produce the same result as PG.
/// PG: percentile_disc(0.5) on [1,2,3,4,5] = 3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_window_percentile_disc() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_wf3");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, x BIGINT)"),
            &format!("INSERT INTO {t} VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM {t}"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// AGGREGATE FILTER
// =============================================================================

/// Test 21: SUM(x) FILTER (WHERE x > 0) — plain aggregate FILTER clause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_agg_filter_plain() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_af1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, x INT)"),
            &format!(
                "INSERT INTO {t} VALUES (1, 10), (2, -5), (3, 20), (4, -1), (5, 15)"
            ),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT SUM(x) FILTER (WHERE x > 0) FROM {t}"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 22: SUM(x) FILTER (WHERE x > 0) OVER (PARTITION BY g) — window FILTER.
///
/// Issue #110 investigation: aggregate FILTER in window position may diverge
/// today. Ignored until confirmed correct; acts as a guard-in-waiting.
// KNOWN-POTENTIAL DIVERGENCE — basin's aggregate FILTER in window position
// may produce different results than PG due to the CASE-WHEN rewrite path
// (#110 investigation flagged this). Running honestly — if it passes, the
// rewrite is correct; if it fails, the failure pinpoints the exact divergence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_agg_filter_window() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_af2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, g TEXT, x INT)"),
            &format!(
                "INSERT INTO {t} VALUES (1, 'a', 10), (2, 'a', -5), (3, 'b', 20), (4, 'b', -1)"
            ),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id, g, SUM(x) FILTER (WHERE x > 0) OVER (PARTITION BY g) AS pos_sum \
             FROM {t} ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// ERROR SQLSTATE CODES
// =============================================================================

/// Test 23: Division by zero — both must return SQLSTATE 22012.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_error_division_by_zero() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_both_error("SELECT 1 / 0", Some("22012"))
        .await
        .unwrap();
}

/// Test 24: Unique violation on duplicate INSERT — both must return SQLSTATE 23505.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_error_unique_violation() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_uq");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT PRIMARY KEY)"),
            &format!("INSERT INTO {t} VALUES (1)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_both_error(&format!("INSERT INTO {t} VALUES (1)"), Some("23505"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 25: NOT NULL violation — both must return SQLSTATE 23502.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_error_not_null_violation() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_nn");

    runner
        .run_setup(&[&format!("CREATE TABLE {t} (id INT NOT NULL)")])
        .await
        .unwrap();

    runner
        .run_assert_both_error(&format!("INSERT INTO {t} VALUES (NULL)"), Some("23502"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// SCHEMA ISOLATION (KNOWN DIVERGENCE)
// =============================================================================

/// Test 26: Multi-schema isolation.
///
/// PG supports `CREATE SCHEMA` and fully qualified `schema.table` names.
/// Basin currently does NOT have full multi-schema isolation (issue #116-125):
/// two tables with the same name in different schemas collide silently.
///
/// KNOWN DIVERGENCE — this test runs and FAILS honestly today. The failure
/// IS the proof the harness catches the bug. When #116-125 lands the test
/// will flip to passing automatically — making it a real correctness guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_schema_isolation_multi_schema() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let sa = format!("{pfx}_sa");
    let sb = format!("{pfx}_sb");

    runner
        .run_setup(&[
            &format!("CREATE SCHEMA {sa}"),
            &format!("CREATE SCHEMA {sb}"),
            &format!("CREATE TABLE {sa}.t (id INT)"),
            &format!("CREATE TABLE {sb}.t (id INT)"),
            &format!("INSERT INTO {sa}.t VALUES (1)"),
            &format!("INSERT INTO {sb}.t VALUES (2)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT id FROM {sa}.t ORDER BY id"))
        .await
        .unwrap();
    runner
        .run_assert_match(&format!("SELECT id FROM {sb}.t ORDER BY id"))
        .await
        .unwrap();

    // Best-effort cleanup.
    for side in [&runner.basin, &runner.pg] {
        let _ = side
            .simple_query(&format!("DROP TABLE IF EXISTS {sa}.t"))
            .await;
        let _ = side
            .simple_query(&format!("DROP TABLE IF EXISTS {sb}.t"))
            .await;
        let _ = side
            .simple_query(&format!("DROP SCHEMA IF EXISTS {sa}"))
            .await;
        let _ = side
            .simple_query(&format!("DROP SCHEMA IF EXISTS {sb}"))
            .await;
    }
}
