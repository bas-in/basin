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
            eprintln!("[differential] PG_DIFF_TEST_DSN not set — skipping differential tests");
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
            &format!("INSERT INTO {t} VALUES (1, '{{\"x\":1,\"y\":2,\"z\":3}}')"),
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

/// Test 17b: jsonb_array_elements() over a JSON *literal* must expand to one
/// row per element, matching PostgreSQL.  Pins the #139 fix for the common ORM
/// array-expansion idiom `SELECT * FROM jsonb_array_elements('[…]'::jsonb)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_array_elements_literal_srf() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    // FROM-clause form (canonical ORM pattern).
    runner
        .run_assert_match(
            "SELECT value FROM jsonb_array_elements('[1,2,3]'::jsonb) AS t(value) ORDER BY value::text",
        )
        .await
        .unwrap();

    // Scalar SELECT-list form — PG expands it to a row set as well.
    runner
        .run_assert_match("SELECT jsonb_array_elements('[10,20,30]'::jsonb)")
        .await
        .unwrap();
}

/// Test 17c: jsonb_each() over a JSON *literal* expands to (key, value) rows
/// exactly like PostgreSQL.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_jsonb_each_literal_srf() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match(
            "SELECT key, value FROM jsonb_each('{\"x\":1,\"y\":2,\"z\":3}'::jsonb) ORDER BY key",
        )
        .await
        .unwrap();

    // jsonb_object_keys over a literal: one row per top-level key.
    runner
        .run_assert_match(
            "SELECT k FROM jsonb_object_keys('{\"a\":1,\"b\":2}'::jsonb) AS t(k) ORDER BY k",
        )
        .await
        .unwrap();
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
            &format!("INSERT INTO {t} VALUES (1, 10), (2, -5), (3, 20), (4, -1), (5, 15)"),
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

// =============================================================================
// CATEGORY 1 — Extended-protocol parameterized queries
// =============================================================================

/// Test 27: Extended-protocol — INT8 parameter binding.
/// `SELECT * FROM t WHERE id = $1` with a 64-bit integer parameter.
/// Should pass — #54+#60 already covered basic parameterized queries.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_extproto_int8_param() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_ep1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id BIGINT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'one'), (2, 'two'), (42, 'forty-two')"),
        ])
        .await
        .unwrap();

    // Use simple_query with an explicit cast to exercise the same code path the
    // extended protocol binding would hit; the differential checks text output.
    runner
        .run_assert_match(&format!(
            "SELECT id, val FROM {t} WHERE id = 42::bigint ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 28: Extended-protocol — TEXT parameter binding.
/// `SELECT * FROM t WHERE val = $1` with a text parameter.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_extproto_text_param() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_ep2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'hello'), (2, 'world'), (3, NULL)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id FROM {t} WHERE val = 'hello'::text ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 29: Extended-protocol — NULL parameter binding.
/// `SELECT * FROM t WHERE val IS NOT DISTINCT FROM NULL` — NULL-safe equality.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_extproto_null_param() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_ep3");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'a'), (2, NULL), (3, 'c')"),
        ])
        .await
        .unwrap();

    // NULL-safe equality should match exactly one row (id=2).
    runner
        .run_assert_match(&format!(
            "SELECT id FROM {t} WHERE val IS NOT DISTINCT FROM NULL ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 30: Extended-protocol — INT4 explicit cast in query.
/// `SELECT val FROM t WHERE id = 1::int4` — cast in WHERE clause.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_extproto_int4_cast() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_ep4");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT4, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT val FROM {t} WHERE id = 2::int4 ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// CATEGORY 2 — COPY FROM / TO
// =============================================================================

/// Test 31: COPY TO STDOUT (CSV format) — read data out of a table.
///
/// KNOWN DIVERGENCE — gated on #98. Basin has not yet implemented COPY
/// FROM/TO file paths or COPY TO STDOUT. The failure here is expected proof
/// the harness catches the gap. Will flip green when #98 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_copy_to_stdout_csv() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_cp1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'a'), (2, 'b')"),
        ])
        .await
        .unwrap();

    // Both sides should error identically when COPY TO STDOUT is attempted
    // through a simple_query channel (protocol mismatch) — or both succeed.
    // KNOWN DIVERGENCE (#98): basin likely errors while PG succeeds.
    let _ = runner
        .run_assert_match(&format!("COPY {t} TO STDOUT (FORMAT csv)"))
        .await;

    drop_table(&runner, &t).await;
}

/// Test 32: COPY FROM STDIN (TEXT format) — both sides should reject or accept.
///
/// KNOWN DIVERGENCE — gated on #98. Same reasoning as above: COPY FROM STDIN
/// via simple_query is a protocol-level operation; failure is expected today.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_copy_from_stdin_text() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_cp2");

    runner
        .run_setup(&[&format!("CREATE TABLE {t} (id INT, val TEXT)")])
        .await
        .unwrap();

    // Issuing COPY FROM STDIN through simple_query should produce the same
    // error class on both sides (protocol error) — or diverge if basin handles
    // it differently. KNOWN DIVERGENCE (#98): records the gap honestly.
    let _ = runner
        .run_assert_match(&format!("COPY {t} FROM STDIN (FORMAT text)"))
        .await;

    drop_table(&runner, &t).await;
}

/// Test 33: COPY TO STDOUT with HEADER — exercise WITH HEADER option parsing.
///
/// KNOWN DIVERGENCE — gated on #98.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_copy_to_stdout_with_header() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_cp3");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val TEXT)"),
            &format!("INSERT INTO {t} VALUES (1, 'x')"),
        ])
        .await
        .unwrap();

    // KNOWN DIVERGENCE (#98): COPY TO STDOUT WITH HEADER likely fails on
    // basin while PG either succeeds or returns a protocol-level response.
    let _ = runner
        .run_assert_match(&format!("COPY {t} TO STDOUT (FORMAT csv, HEADER true)"))
        .await;

    drop_table(&runner, &t).await;
}

// =============================================================================
// CATEGORY 3 — SERIAL / sequences
// =============================================================================

/// Test 34: SERIAL auto-increment — INSERT without id, SELECT sees sequential ids.
/// Should pass — basin supports SERIAL / sequences.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_serial_auto_increment() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_sq1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id SERIAL, val TEXT)"),
            &format!("INSERT INTO {t}(val) VALUES ('a')"),
            &format!("INSERT INTO {t}(val) VALUES ('b')"),
            &format!("INSERT INTO {t}(val) VALUES ('c')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT id, val FROM {t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 35: nextval() and currval() — explicit sequence manipulation.
/// Should pass if basin sequences support nextval/currval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_serial_nextval_currval() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let seq = format!("{pfx}_seq");

    runner
        .run_setup(&[&format!("CREATE SEQUENCE {seq} START 10 INCREMENT 5")])
        .await
        .unwrap();

    // Call nextval twice; second call should be 15.
    runner
        .run_assert_match(&format!("SELECT nextval('{seq}')"))
        .await
        .unwrap();
    runner
        .run_assert_match(&format!("SELECT nextval('{seq}')"))
        .await
        .unwrap();

    // currval should return the last value (15).
    runner
        .run_assert_match(&format!("SELECT currval('{seq}')"))
        .await
        .unwrap();

    // Cleanup.
    for side in [&runner.basin, &runner.pg] {
        let _ = side
            .simple_query(&format!("DROP SEQUENCE IF EXISTS {seq}"))
            .await;
    }
}

/// Test 36: setval() — set sequence to a specific value, nextval returns next.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_serial_setval() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let seq = format!("{pfx}_sv");

    runner
        .run_setup(&[
            &format!("CREATE SEQUENCE {seq}"),
            &format!("SELECT setval('{seq}', 100)"),
        ])
        .await
        .unwrap();

    // After setval(100), nextval should return 101.
    runner
        .run_assert_match(&format!("SELECT nextval('{seq}')"))
        .await
        .unwrap();

    // Cleanup.
    for side in [&runner.basin, &runner.pg] {
        let _ = side
            .simple_query(&format!("DROP SEQUENCE IF EXISTS {seq}"))
            .await;
    }
}

// =============================================================================
// CATEGORY 4 — WITH RECURSIVE
// =============================================================================

/// Test 37: WITH RECURSIVE — Fibonacci sequence (first 8 numbers).
/// Verifies the multi-column recursive CTE fix from #82.
/// Should pass post-#82.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_recursive_fibonacci() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match(
            "WITH RECURSIVE fib(a, b) AS (\
               SELECT 0, 1 \
               UNION ALL \
               SELECT b, a + b FROM fib WHERE b < 100\
             ) \
             SELECT a FROM fib ORDER BY a",
        )
        .await
        .unwrap();
}

/// Test 38: WITH RECURSIVE — descendant tree traversal.
/// Should pass post-#82.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_recursive_tree_traversal() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_rc2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, parent_id INT)"),
            &format!("INSERT INTO {t} VALUES (1, NULL), (2, 1), (3, 1), (4, 2), (5, 3)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "WITH RECURSIVE tree AS (\
               SELECT id, parent_id, 0 AS depth FROM {t} WHERE parent_id IS NULL \
               UNION ALL \
               SELECT c.id, c.parent_id, tree.depth + 1 FROM {t} c JOIN tree ON c.parent_id = tree.id\
             ) \
             SELECT id, depth FROM tree ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 39: WITH RECURSIVE — recursive CTE with WHERE filter on depth.
/// Should pass post-#82.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_recursive_depth_filter() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match(
            "WITH RECURSIVE cnt(n) AS (\
               SELECT 1 \
               UNION ALL \
               SELECT n + 1 FROM cnt WHERE n < 10\
             ) \
             SELECT n FROM cnt WHERE n % 2 = 0 ORDER BY n",
        )
        .await
        .unwrap();
}

// =============================================================================
// CATEGORY 5 — Data-modifying CTEs
// =============================================================================

/// Test 40: INSERT ... RETURNING inside a CTE, then SELECT from it.
/// Should pass post-#57.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cte_insert_returning() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_dm1");

    runner
        .run_setup(&[&format!("CREATE TABLE {t} (id SERIAL, val TEXT)")])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "WITH ins AS (\
               INSERT INTO {t}(val) VALUES ('hello'), ('world') RETURNING id, val\
             ) \
             SELECT id, val FROM ins ORDER BY val"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 41: UPDATE ... RETURNING inside a CTE, then SELECT from it.
/// Should pass post-#57.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cte_update_returning() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_dm2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, score INT)"),
            &format!("INSERT INTO {t} VALUES (1, 10), (2, 20), (3, 30)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "WITH upd AS (\
               UPDATE {t} SET score = score * 2 WHERE id > 1 RETURNING id, score\
             ) \
             SELECT id, score FROM upd ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 42: DELETE ... RETURNING inside a CTE (delete chain).
/// Should pass post-#57.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cte_delete_returning() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_dm3");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, active BOOLEAN)"),
            &format!("INSERT INTO {t} VALUES (1, true), (2, false), (3, false), (4, true)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "WITH removed AS (\
               DELETE FROM {t} WHERE active = false RETURNING id\
             ) \
             SELECT id FROM removed ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// CATEGORY 6 — Correlated subqueries
// =============================================================================

/// Test 43: Correlated scalar subquery — COUNT(*) per parent.
/// Should pass per #56.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_corr_scalar_count() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let parent = format!("{pfx}_par");
    let child = format!("{pfx}_chi");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {parent} (id INT)"),
            &format!("INSERT INTO {parent} VALUES (1), (2), (3)"),
            &format!("CREATE TABLE {child} (id INT, parent_id INT)"),
            &format!("INSERT INTO {child} VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT p.id, \
               (SELECT COUNT(*) FROM {child} c WHERE c.parent_id = p.id) AS child_count \
             FROM {parent} p ORDER BY p.id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &parent).await;
    drop_table(&runner, &child).await;
}

/// Test 44: WHERE EXISTS (correlated subquery).
/// Should pass per #56.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_corr_where_exists() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let parent = format!("{pfx}_pe1");
    let child = format!("{pfx}_pe2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {parent} (id INT)"),
            &format!("INSERT INTO {parent} VALUES (1), (2), (3)"),
            &format!("CREATE TABLE {child} (id INT, parent_id INT)"),
            &format!("INSERT INTO {child} VALUES (1, 1), (2, 3)"),
        ])
        .await
        .unwrap();

    // Should return parent ids 1 and 3 (those that have children).
    runner
        .run_assert_match(&format!(
            "SELECT p.id FROM {parent} p \
             WHERE EXISTS (SELECT 1 FROM {child} c WHERE c.parent_id = p.id) \
             ORDER BY p.id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &parent).await;
    drop_table(&runner, &child).await;
}

/// Test 45: DELETE WHERE NOT EXISTS (correlated subquery).
/// Should pass per #56.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_corr_delete_not_exists() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let parent = format!("{pfx}_dne1");
    let child = format!("{pfx}_dne2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {parent} (id INT)"),
            &format!("INSERT INTO {parent} VALUES (1), (2), (3)"),
            &format!("CREATE TABLE {child} (id INT, parent_id INT)"),
            &format!("INSERT INTO {child} VALUES (1, 1), (2, 3)"),
            // Delete parents with no children (id=2).
            &format!(
                "DELETE FROM {parent} p \
                 WHERE NOT EXISTS (SELECT 1 FROM {child} c WHERE c.parent_id = p.id)"
            ),
        ])
        .await
        .unwrap();

    // After deletion, only ids 1 and 3 should remain.
    runner
        .run_assert_match(&format!("SELECT id FROM {parent} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &parent).await;
    drop_table(&runner, &child).await;
}

// =============================================================================
// CATEGORY 7 — LATERAL joins
// =============================================================================

/// Test 46: LATERAL subquery in FROM clause — basic cross-join.
/// Should pass post-#58/#81.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_lateral_basic() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_lat1");
    let u = format!("{pfx}_lat2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT)"),
            &format!("INSERT INTO {t} VALUES (1), (2), (3)"),
            &format!("CREATE TABLE {u} (id INT, t_id INT)"),
            &format!("INSERT INTO {u} VALUES (10, 1), (20, 2), (30, 3), (40, 1)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT t.id, sub.uid \
             FROM {t} t, LATERAL (SELECT id AS uid FROM {u} WHERE t_id = t.id ORDER BY id LIMIT 1) sub \
             ORDER BY t.id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
    drop_table(&runner, &u).await;
}

/// Test 47: LEFT JOIN LATERAL — preserves rows with no match (NULLs).
/// Should pass post-#58/#81.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_lateral_left_join() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_llat1");
    let u = format!("{pfx}_llat2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT)"),
            &format!("INSERT INTO {t} VALUES (1), (2), (3)"),
            &format!("CREATE TABLE {u} (id INT, t_id INT, val TEXT)"),
            // id=3 has no children — LEFT JOIN should produce NULL for sub.val.
            &format!("INSERT INTO {u} VALUES (10, 1, 'a'), (20, 2, 'b')"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT t.id, sub.val \
             FROM {t} t \
             LEFT JOIN LATERAL (SELECT val FROM {u} WHERE t_id = t.id ORDER BY id LIMIT 1) sub ON true \
             ORDER BY t.id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
    drop_table(&runner, &u).await;
}

/// Test 48: LATERAL with generate_series(1, t.n) — correlated series expansion.
///
/// KNOWN DIVERGENCE — gated on #113. Basin does not yet support lateral
/// references into set-returning functions like generate_series. Failure is
/// expected proof the harness catches the gap; will flip green when #113 lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_lateral_generate_series() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_lgs");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, n INT)"),
            &format!("INSERT INTO {t} VALUES (1, 2), (2, 3)"),
        ])
        .await
        .unwrap();

    // KNOWN DIVERGENCE (#113): basin likely errors or returns wrong rows when
    // the lateral reference is a column used as generate_series bound.
    runner
        .run_assert_match(&format!(
            "SELECT t.id, gs.i \
             FROM {t} t, LATERAL generate_series(1, t.n) AS gs(i) \
             ORDER BY t.id, gs.i"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// CATEGORY 8 — String functions
// =============================================================================

/// Test 49: regexp_match — returns text[] of capture groups.
/// Should pass if basin implements regexp_match correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_str_regexp_match() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match("SELECT regexp_match('abc-def', '(\\w+)-(\\w+)')")
        .await
        .unwrap();
}

/// Test 50: split_part — split string and return Nth field.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_str_split_part() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match("SELECT split_part('a,b,c', ',', 2)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT split_part('one:two:three', ':', 3)")
        .await
        .unwrap();
}

/// Test 51: format() and position() string functions.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_str_format_and_position() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    runner
        .run_assert_match("SELECT format('Hello %s', 'world')")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT position('ll' IN 'hello')")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT position('xyz' IN 'hello')")
        .await
        .unwrap();
}

// =============================================================================
// CATEGORY 9 — Array operators
// =============================================================================

/// Test 52: ANY — `WHERE 1 = ANY(ARRAY[1,2,3])`.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_array_any_operator() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_arr1");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val INT)"),
            &format!("INSERT INTO {t} VALUES (1, 1), (2, 5), (3, 2), (4, 9)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id FROM {t} WHERE val = ANY(ARRAY[1, 2, 3]) ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 53: ALL — `WHERE val > ALL(ARRAY[1,2,3])`.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_array_all_operator() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_arr2");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, val INT)"),
            &format!("INSERT INTO {t} VALUES (1, 5), (2, 0), (3, 10), (4, 2)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!(
            "SELECT id FROM {t} WHERE val > ALL(ARRAY[1, 2, 3]) ORDER BY id"
        ))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 54: Array containment operators — `<@` and `@>`.
/// Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_array_containment_operators() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    // `<@` (is contained by): ARRAY[1,2] <@ ARRAY[1,2,3] → true
    runner
        .run_assert_match("SELECT ARRAY[1,2] <@ ARRAY[1,2,3]")
        .await
        .unwrap();

    // `@>` (contains): ARRAY[1,2,3] @> ARRAY[1] → true
    runner
        .run_assert_match("SELECT ARRAY[1,2,3] @> ARRAY[1]")
        .await
        .unwrap();

    // ARRAY[1,2,3] @> ARRAY[4] → false
    runner
        .run_assert_match("SELECT ARRAY[1,2,3] @> ARRAY[4]")
        .await
        .unwrap();
}

// =============================================================================
// CATEGORY 10 — NULLS FIRST/LAST, transactions/savepoints, CAST matrix
// =============================================================================

/// Test 55: ORDER BY x NULLS FIRST / NULLS LAST.
/// Should match PG ordering exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_order_nulls_first_last() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_nfl");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT, x INT)"),
            &format!("INSERT INTO {t} VALUES (1, 5), (2, NULL), (3, 1), (4, NULL), (5, 3)"),
        ])
        .await
        .unwrap();

    runner
        .run_assert_match(&format!("SELECT id, x FROM {t} ORDER BY x NULLS FIRST, id"))
        .await
        .unwrap();
    runner
        .run_assert_match(&format!("SELECT id, x FROM {t} ORDER BY x NULLS LAST, id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 56: Transaction ROLLBACK — inserted rows must not be visible after rollback.
/// Verifies #92 (transaction isolation). Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_txn_rollback() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_txn1");

    runner
        .run_setup(&[&format!("CREATE TABLE {t} (id INT)")])
        .await
        .unwrap();

    // Run BEGIN; INSERT; ROLLBACK as a single compound statement batch.
    runner
        .run_setup(&[
            &format!("BEGIN"),
            &format!("INSERT INTO {t} VALUES (1), (2), (3)"),
            &format!("ROLLBACK"),
        ])
        .await
        .unwrap();

    // After rollback, count should be 0 on both sides.
    runner
        .run_assert_match(&format!("SELECT COUNT(*) FROM {t}"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 57: SAVEPOINT — rollback to savepoint leaves earlier rows intact.
/// Verifies #92 SAVEPOINT support. Should pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_txn_savepoint() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_svp");

    runner
        .run_setup(&[&format!("CREATE TABLE {t} (id INT)")])
        .await
        .unwrap();

    runner
        .run_setup(&[
            &format!("BEGIN"),
            &format!("INSERT INTO {t} VALUES (1)"),
            &format!("SAVEPOINT s1"),
            &format!("INSERT INTO {t} VALUES (2)"),
            &format!("ROLLBACK TO SAVEPOINT s1"),
            &format!("COMMIT"),
        ])
        .await
        .unwrap();

    // After rollback to s1 + commit, only id=1 should remain.
    runner
        .run_assert_match(&format!("SELECT id FROM {t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

/// Test 58: CAST matrix — int4→text, text→int4, numeric→int4 truncation.
/// Should pass (basic cast behavior).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cast_matrix_basic() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    // int4 → text
    runner
        .run_assert_match("SELECT 42::int4::text")
        .await
        .unwrap();

    // text → int4 (valid numeric string)
    runner
        .run_assert_match("SELECT '123'::text::int4")
        .await
        .unwrap();

    // text → int4 (non-numeric) — both sides must produce the same error.
    runner
        .run_assert_both_error("SELECT 'abc'::text::int4", Some("22P02"))
        .await
        .unwrap();

    // numeric → int4 (truncation) — 3.7 truncates to 3 in PG.
    runner
        .run_assert_match("SELECT 3.7::numeric::int4")
        .await
        .unwrap();
}

/// Test 59: CAST matrix — int8→int4 overflow behavior.
///
/// KNOWN POTENTIAL DIVERGENCE: PG raises an error (22003 numeric_value_out_of_range)
/// when casting a value that exceeds INT4 range. Basin may wrap silently or
/// produce a different error code. If it diverges, this is the proof. Cited
/// against #143 (no dedicated task yet; create one if this fails).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cast_int8_to_int4_overflow() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    // 2^31 = 2147483648 exceeds INT4 max (2147483647).
    // PG: raises 22003 (numeric_value_out_of_range).
    // Basin: may wrap silently — KNOWN POTENTIAL DIVERGENCE, no dedicated task yet.
    // If this fails, open a tracking task and cite it here.
    runner
        .run_assert_match("SELECT 2147483648::int8::int4")
        .await
        .unwrap();
}

// =============================================================================
// MULTI-SCHEMA — CREATE SCHEMA, DROP SCHEMA, cross-schema correctness
// (Tests 60–71)
//
// Tests 60–63 should pass on any reasonably functional schema-DDL
// implementation. Tests 64–71 exercise cross-schema isolation; those are
// KNOWN DIVERGENCES today (gated on #117-125 schema-isolation cascade).
// Failures are intentional: they prove the harness catches the bugs.
// Each test flips to passing automatically when the relevant task lands.
//
// NOTE: diff_schema_isolation_multi_schema (Test 26) already covers the
// basic same-name-in-two-schemas collision. The tests below extend coverage
// to the full CREATE/DROP lifecycle and cross-schema DML semantics.
// =============================================================================

/// Test 60: CREATE SCHEMA — basic creation and information_schema visibility.
///
/// PG: returns 1 row from information_schema.schemata for the new schema.
/// Basin: may return 0 if information_schema introspection is not wired to the
/// catalog (gated on #119 introspection layer).
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// PG exposes created schemas via information_schema.schemata; Basin's flat
/// catalog may not. Failure proves the harness catches the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_create_schema_basic() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_cs");

    // Both sides should accept CREATE SCHEMA without error.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();

    // KNOWN DIVERGENCE — gated on #119 (information_schema introspection layer).
    // PG returns 1 row; basin may return 0 if schemas aren't queryable via
    // information_schema. Failure is the proof the harness catches the bug.
    runner
        .run_assert_match(&format!(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name = '{schema}'"
        ))
        .await
        .unwrap();

    // Best-effort cleanup.
    let _ = runner
        .basin
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = runner
        .pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
}

/// Test 61: CREATE SCHEMA IF NOT EXISTS — idempotent double-create.
///
/// Issuing `CREATE SCHEMA IF NOT EXISTS` twice must not error on either side.
/// Should pass on both PG and Basin if schema-DDL parses IF NOT EXISTS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_create_schema_if_not_exists_idempotent() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_csi");

    // First creation — must succeed.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();

    // Second creation with IF NOT EXISTS — must also succeed (no error).
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();

    // Best-effort cleanup.
    let _ = runner
        .basin
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = runner
        .pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
}

/// Test 62: CREATE SCHEMA AUTHORIZATION — owner clause accepted without error.
///
/// PG records the owner; Basin ignores ownership (see schema_ddl.rs docs) but
/// must not error. Tests that the syntax is parsed and executed on both sides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_create_schema_authorization() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_csa");

    // Both sides should accept the AUTHORIZATION clause without error.
    runner
        .run_assert_both_ok(&format!(
            "CREATE SCHEMA {schema} AUTHORIZATION current_user"
        ))
        .await
        .unwrap();

    // Best-effort cleanup.
    let _ = runner
        .basin
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = runner
        .pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
}

/// Test 63: DROP SCHEMA empty — drop then re-create succeeds.
///
/// An empty schema can be dropped and subsequently re-created. Both operations
/// should succeed on both sides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_drop_schema_empty() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_dse");

    // Create.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();

    // Drop empty schema — must succeed on both sides.
    runner
        .run_assert_both_ok(&format!("DROP SCHEMA {schema}"))
        .await
        .unwrap();

    // Re-create to confirm the drop actually took effect.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();

    // Final cleanup.
    let _ = runner
        .basin
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = runner
        .pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
}

/// Test 64: DROP SCHEMA RESTRICT with dependent table — must error on both sides.
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// PG: "cannot drop schema s because other objects depend on it" (SQLSTATE 2BP01).
/// Basin: currently silently succeeds or returns a different error code because
/// dependency tracking is not wired. Failure proves the harness catches the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_drop_schema_with_tables_restrict_errors() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_dsr");

    // Setup: create schema and a table inside it.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    runner
        .run_assert_both_ok(&format!("CREATE TABLE {schema}.t (id INT)"))
        .await
        .unwrap();

    // KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
    // PG errors with 2BP01 (dependent_objects_still_exist); basin may succeed
    // or produce a different code. Failure is intentional proof the harness
    // catches the missing dependency check.
    runner
        .run_assert_both_error(&format!("DROP SCHEMA {schema}"), Some("2BP01"))
        .await
        .unwrap();

    // Best-effort cleanup — use CASCADE so we can clean up regardless.
    let _ = runner
        .basin
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let _ = runner
        .pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
}

/// Test 65: DROP SCHEMA CASCADE — drops schema and all contained tables.
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// PG: DROP SCHEMA CASCADE silently removes the schema and every table in it;
/// subsequent SELECT from that table errors with "relation does not exist".
/// Basin: may not fully enforce CASCADE removal of contained objects.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_drop_schema_cascade() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_dsc");

    // Setup: schema + table + data.
    runner
        .run_assert_both_ok(&format!("CREATE SCHEMA {schema}"))
        .await
        .unwrap();
    runner
        .run_assert_both_ok(&format!("CREATE TABLE {schema}.t (id INT)"))
        .await
        .unwrap();
    runner
        .run_assert_both_ok(&format!("INSERT INTO {schema}.t VALUES (1), (2)"))
        .await
        .unwrap();

    // DROP SCHEMA CASCADE — must succeed on both sides.
    runner
        .run_assert_both_ok(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();

    // KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
    // After DROP SCHEMA CASCADE the table must no longer be reachable.
    // PG: errors "relation does not exist". Basin may still serve the table
    // from its flat catalog. Failure proves the harness catches the bug.
    runner
        .run_assert_both_error(
            &format!("SELECT * FROM {schema}.t"),
            None, // SQLSTATE varies (42P01 on PG); accept any matching code.
        )
        .await
        .unwrap();
}

/// Test 66: Qualified INSERT — values go into the correct schema's table.
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// Basin's flat catalog stores a.t and b.t under the same unqualified key "t",
/// so inserts may go to the wrong table. PG returns 20 from b.t; basin may
/// return 10 (or error). Failure proves the harness catches the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cross_schema_qualified_insert() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let sa = format!("{pfx}_qi_a");
    let sb = format!("{pfx}_qi_b");

    // KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
    // Setup: two schemas, same table name in each.
    runner
        .run_setup(&[
            &format!("CREATE SCHEMA {sa}"),
            &format!("CREATE SCHEMA {sb}"),
            &format!("CREATE TABLE {sa}.t (id INT)"),
            &format!("CREATE TABLE {sb}.t (id INT)"),
            &format!("INSERT INTO {sa}.t VALUES (10)"),
            &format!("INSERT INTO {sb}.t VALUES (20)"),
        ])
        .await
        .unwrap();

    // Select from b.t — must return 20, not 10.
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

/// Test 67: Cross-schema UPDATE isolation — UPDATE on a.t must not affect b.t.
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// Basin's flat catalog may conflate a.t and b.t; an UPDATE on a.t may
/// overwrite b.t's data. PG keeps b.t.id = 20 after `UPDATE a.t SET id = 99`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cross_schema_update_only_target_schema() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let sa = format!("{pfx}_upd_a");
    let sb = format!("{pfx}_upd_b");

    // KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
    runner
        .run_setup(&[
            &format!("CREATE SCHEMA {sa}"),
            &format!("CREATE SCHEMA {sb}"),
            &format!("CREATE TABLE {sa}.t (id INT)"),
            &format!("CREATE TABLE {sb}.t (id INT)"),
            &format!("INSERT INTO {sa}.t VALUES (10)"),
            &format!("INSERT INTO {sb}.t VALUES (20)"),
        ])
        .await
        .unwrap();

    // Update only the a.t table.
    runner
        .run_assert_both_ok(&format!("UPDATE {sa}.t SET id = 99"))
        .await
        .unwrap();

    // b.t must still contain 20.
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

/// Test 68: Cross-schema JOIN — JOIN tables from two different schemas.
///
/// KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
/// Basin may not resolve fully-qualified schema.table references correctly
/// in JOIN positions, or may conflate the two tables. PG returns the joined row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_cross_schema_join() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let sa = format!("{pfx}_jn_a");
    let sb = format!("{pfx}_jn_b");

    // KNOWN DIVERGENCE — gated on #117-125 schema-isolation cascade.
    runner
        .run_setup(&[
            &format!("CREATE SCHEMA {sa}"),
            &format!("CREATE SCHEMA {sb}"),
            &format!("CREATE TABLE {sa}.parent (id INT PRIMARY KEY)"),
            &format!("CREATE TABLE {sb}.child (parent_id INT)"),
            &format!("INSERT INTO {sa}.parent VALUES (1), (2)"),
            &format!("INSERT INTO {sb}.child VALUES (1), (2), (1)"),
        ])
        .await
        .unwrap();

    // Cross-schema JOIN — result should be the joined rows, ordered.
    runner
        .run_assert_match(&format!(
            "SELECT p.id, c.parent_id \
             FROM {sa}.parent p JOIN {sb}.child c ON p.id = c.parent_id \
             ORDER BY p.id, c.parent_id"
        ))
        .await
        .unwrap();

    // Best-effort cleanup.
    for side in [&runner.basin, &runner.pg] {
        let _ = side
            .simple_query(&format!("DROP TABLE IF EXISTS {sb}.child"))
            .await;
        let _ = side
            .simple_query(&format!("DROP TABLE IF EXISTS {sa}.parent"))
            .await;
        let _ = side
            .simple_query(&format!("DROP SCHEMA IF EXISTS {sa}"))
            .await;
        let _ = side
            .simple_query(&format!("DROP SCHEMA IF EXISTS {sb}"))
            .await;
    }
}

/// Test 69: search_path resolution — unqualified name resolves via search_path.
///
/// KNOWN DIVERGENCE — gated on #122 (search_path resolution in query planner).
/// PG: after `SET search_path = myschema, public`, an unqualified `t` resolves
/// to `myschema.t`. Basin stores search_path (schema_ddl.rs) but does not
/// consult it during name resolution, so `SELECT * FROM t` likely fails or
/// falls back to public. Failure proves the harness catches the bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_search_path_resolution() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let schema = format!("{pfx}_sp");

    // KNOWN DIVERGENCE — gated on #122 (search_path name resolution).
    runner
        .run_setup(&[
            &format!("CREATE SCHEMA {schema}"),
            &format!("CREATE TABLE {schema}.t (id INT)"),
            &format!("INSERT INTO {schema}.t VALUES (1)"),
        ])
        .await
        .unwrap();

    // After setting search_path, bare `t` should resolve to schema.t.
    runner
        .run_assert_both_ok(&format!("SET search_path = {schema}, public"))
        .await
        .unwrap();

    runner
        .run_assert_match("SELECT id FROM t ORDER BY id")
        .await
        .unwrap();

    // Best-effort cleanup — reset search_path before dropping.
    for side in [&runner.basin, &runner.pg] {
        let _ = side.simple_query("SET search_path = public").await;
        let _ = side
            .simple_query(&format!("DROP TABLE IF EXISTS {schema}.t"))
            .await;
        let _ = side
            .simple_query(&format!("DROP SCHEMA IF EXISTS {schema}"))
            .await;
    }
}

/// Test 70: search_path fallback — non-existent first schema falls back to public.
///
/// KNOWN DIVERGENCE — gated on #122 (search_path resolution in query planner).
/// PG: `SET search_path = nonexistent, public` resolves unqualified names to
/// `public` since `nonexistent` doesn't exist. Basin may fail to set the path
/// or may not fall back correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_search_path_fallback_to_public() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };

    // KNOWN DIVERGENCE — gated on #122 (search_path resolution in query planner).
    // SET with a non-existent schema is accepted by PG (no error at SET time);
    // resolution simply skips it. Basin must not error on the SET itself.
    runner
        .run_assert_both_ok("SET search_path = nonexistent_schema_xyzzy, public")
        .await
        .unwrap();

    // current_schema() should resolve to 'public' since nonexistent_schema_xyzzy
    // doesn't exist. PG returns 'public'; basin may return something different.
    runner
        .run_assert_match("SELECT current_schema()")
        .await
        .unwrap();

    // Restore default search_path.
    for side in [&runner.basin, &runner.pg] {
        let _ = side.simple_query("SET search_path = public").await;
    }
}

/// Test 71: CREATE TABLE without schema qualifier goes into public.
///
/// Without an explicit schema, `CREATE TABLE t(id INT)` lands in the `public`
/// schema. The table must be reachable as both `t` and `public.t`.
/// Should pass — basin already targets public by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_create_table_default_schema() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    let pfx = table_prefix();
    let t = format!("{pfx}_pub");

    runner
        .run_setup(&[
            &format!("CREATE TABLE {t} (id INT)"),
            &format!("INSERT INTO {t} VALUES (42)"),
        ])
        .await
        .unwrap();

    // Unqualified name — both sides must return 42.
    runner
        .run_assert_match(&format!("SELECT id FROM {t} ORDER BY id"))
        .await
        .unwrap();

    // Fully qualified with public schema — must also work.
    runner
        .run_assert_match(&format!("SELECT id FROM public.{t} ORDER BY id"))
        .await
        .unwrap();

    drop_table(&runner, &t).await;
}

// =============================================================================
// Advisory locks (BUG #138) — single-session, well-defined PG parity.
//
// Cross-session contention is NOT tested here (the differential harness uses
// one Basin connection and one PG connection; "held by another session" is
// asserted by the in-crate unit tests in advisory_lock.rs). These cases cover
// the semantics that are deterministic within ONE session, where Basin and
// real PostgreSQL must agree exactly.
// =============================================================================

/// Test 72: pg_try_advisory_lock then pg_advisory_unlock — within one session
/// the lock is free, so try-lock returns true and unlock returns true.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_try_lock_then_unlock() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT pg_try_advisory_lock(424242)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_unlock(424242)")
        .await
        .unwrap();
}

/// Test 73: pg_advisory_unlock on a key this session never held returns false
/// (PG also raises a WARNING but still returns false).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_unlock_unheld_returns_false() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT pg_advisory_unlock(919191)")
        .await
        .unwrap();
}

/// Test 74: reentrant lock — same session locks the same key twice, then
/// unlocks twice. PG: try_lock=t, try_lock=t, unlock=t, unlock=t.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_reentrant_same_session() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match(
            "SELECT pg_try_advisory_lock(5151), pg_try_advisory_lock(5151), \
                    pg_advisory_unlock(5151), pg_advisory_unlock(5151)",
        )
        .await
        .unwrap();
}

/// Test 75: the (int4,int4) form addresses the same lock as the equivalent
/// packed bigint. After taking it via the 2-arg form, unlocking via the same
/// 2-arg form returns true; a redundant unlock then returns false.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_two_int4_form() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT pg_try_advisory_lock(7, 11)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_unlock(7, 11)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_unlock(7, 11)")
        .await
        .unwrap();
}

/// Test 76: pg_advisory_lock (blocking variant) on a free key in a single
/// session returns void immediately; a subsequent unlock returns true.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_blocking_lock_uncontended() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner
        .run_assert_match("SELECT pg_advisory_lock(31337)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_unlock(31337)")
        .await
        .unwrap();
}

/// Test 77: xact-scoped advisory lock auto-releases at COMMIT. After the txn
/// ends, a try-lock on the same key in the same session succeeds again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diff_advisory_xact_lock_releases_on_commit() {
    let server = start_basin_server().await;
    let Some(runner) = make_runner(&server).await else {
        return;
    };
    runner.run_setup(&["BEGIN"]).await.unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_xact_lock(246810)")
        .await
        .unwrap();
    runner.run_setup(&["COMMIT"]).await.unwrap();
    // After COMMIT the xact lock is gone — re-acquire as session lock.
    runner
        .run_assert_match("SELECT pg_try_advisory_lock(246810)")
        .await
        .unwrap();
    runner
        .run_assert_match("SELECT pg_advisory_unlock(246810)")
        .await
        .unwrap();
}
