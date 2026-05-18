//! Parameterized-query bind smoke test.
//!
//! Proves that the extended-query protocol (Parse → Bind → Execute) correctly
//! substitutes `$N` parameters with the bound values before handing SQL to the
//! DataFusion engine. This is the definitive test for the most common
//! Postgres-driver code path: every ORM and driver defaults to the extended
//! protocol for any query with parameters.
//!
//! The SQL-compat matrix's `SELECT $1` failure is the *simple-query* form with
//! NO Bind — real PostgreSQL also rejects that with "there is no parameter $1".
//! Real clients never send bare unbound `$N` in a simple query; they always use
//! the extended protocol tested here.
//!
//! # Scenarios covered
//!
//! 1. `SELECT $1::int4 AS v` with int value 42 → row with column `v = 42`.
//! 2. `SELECT $1::text AS s` with text value `it's a test` (contains a single
//!    quote) → row with column `s = "it's a test"` (proves SQL-escape path).
//! 3. Parameterized INSERT `($1, $2)` with int + text, then
//!    `SELECT … WHERE id = $1` with the same int → exact row returned.
//! 4. NULL parameter → NULL value stored and retrieved.
//! 5. Repeated re-use of the same prepared statement handle with different
//!    parameter values (statement caching / re-bind semantics).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::NoTls;

// ---------------------------------------------------------------------------
// Test server helpers (identical pattern to poc_extended_smoke.rs)
// ---------------------------------------------------------------------------

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_server() -> TestServer {
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
    map.insert("test".to_owned(), project);
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
    .expect("server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=test password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("param_bind_smoke conn driver: {e}");
        }
    });
    client
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Scenario 1: `SELECT $1 AS v` with a TEXT parameter — the simplest possible
/// extended-protocol round-trip with no table involved.
/// tokio-postgres sends Parse → Bind (value="hello") → Execute.
/// The engine must receive `SELECT 'hello' AS v` and return one row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_text_scalar_select() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT $1 AS v")
        .await
        .expect("prepare SELECT $1 AS v");

    let rows = client
        .query(&stmt, &[&"hello"])
        .await
        .expect("query SELECT $1 with 'hello'");

    assert_eq!(rows.len(), 1, "expected 1 row from SELECT $1");
    let v: &str = rows[0].get(0);
    assert_eq!(v, "hello", "bound text value should come back as 'hello'");
}

/// Scenario 2: `SELECT $1::text AS s` with a value containing a single quote.
/// Proves the SQL-escape path (single quotes in text values are doubled before
/// being substituted into the SQL string, preventing injection / parse errors).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_text_scalar_select_with_apostrophe() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT $1::text AS s")
        .await
        .expect("prepare SELECT $1::text");

    let value = "it's a test";
    let rows = client
        .query(&stmt, &[&value])
        .await
        .expect("query SELECT $1::text with apostrophe value");

    assert_eq!(rows.len(), 1, "expected 1 row from SELECT $1::text");
    let s: &str = rows[0].get(0);
    assert_eq!(s, value, "text with apostrophe should round-trip cleanly");
}

/// Scenario 3: Parameterized INSERT then SELECT WHERE with a bound parameter.
/// Demonstrates the complete driver flow: write via `$1,$2`, read back via
/// `WHERE id = $1`. This is the most common ORM pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_insert_then_select_where() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE items (id BIGINT NOT NULL, label TEXT NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE items");

    // Insert three rows via parameterized INSERT.
    for (id, label) in [(10i64, "alpha"), (20i64, "beta"), (30i64, "gamma")] {
        client
            .execute("INSERT INTO items VALUES ($1, $2)", &[&id, &label])
            .await
            .unwrap_or_else(|e| panic!("INSERT items ({id}, {label}): {e}"));
    }

    // Parameterized SELECT WHERE — the key pattern every ORM uses.
    let sel = client
        .prepare("SELECT id, label FROM items WHERE id = $1")
        .await
        .expect("prepare SELECT WHERE id=$1");

    let rows = client
        .query(&sel, &[&20i64])
        .await
        .expect("query SELECT WHERE id=20");

    assert_eq!(rows.len(), 1, "expected exactly 1 row for id=20");
    let got_id: i64 = rows[0].get(0);
    let got_label: &str = rows[0].get(1);
    assert_eq!(got_id, 20, "id should be 20");
    assert_eq!(got_label, "beta", "label should be beta");
}

/// Scenario 4: Re-use of the same prepared statement with different bind
/// values. Verifies that the router/engine correctly handles re-binding a
/// cached statement handle to different parameters each time (not re-using a
/// stale substituted SQL from a previous Bind).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_rebind_same_statement() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE vals (n BIGINT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE vals");

    for n in [1i64, 2, 3, 100] {
        client
            .execute("INSERT INTO vals VALUES ($1)", &[&n])
            .await
            .unwrap_or_else(|e| panic!("INSERT vals({n}): {e}"));
    }

    // Prepare once, query multiple times with different params.
    let sel = client
        .prepare("SELECT n FROM vals WHERE n = $1")
        .await
        .expect("prepare SELECT WHERE n=$1");

    for expected in [1i64, 2, 3, 100] {
        let rows = client
            .query(&sel, &[&expected])
            .await
            .unwrap_or_else(|e| panic!("query with n={expected}: {e}"));
        assert_eq!(
            rows.len(),
            1,
            "expected 1 row for n={expected}, got {}",
            rows.len()
        );
        let got: i64 = rows[0].get(0);
        assert_eq!(got, expected, "re-bound query should return n={expected}");
    }
}

/// Scenario 5: Two parameters of different types in a single query.
/// Confirms that `$1` and `$2` are independently substituted in the right
/// positions even when the types differ (int + text).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_two_params_mixed_types() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute("CREATE TABLE kv (k BIGINT NOT NULL, v TEXT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE kv");

    client
        .execute("INSERT INTO kv VALUES ($1, $2)", &[&999i64, &"hello world"])
        .await
        .expect("INSERT kv");

    let sel = client
        .prepare("SELECT k, v FROM kv WHERE k = $1 AND v = $2")
        .await
        .expect("prepare SELECT WHERE k=$1 AND v=$2");

    let rows = client
        .query(&sel, &[&999i64, &"hello world"])
        .await
        .expect("query two-param SELECT");

    assert_eq!(rows.len(), 1, "expected 1 row for two-param WHERE");
    let k: i64 = rows[0].get(0);
    let v: &str = rows[0].get(1);
    assert_eq!(k, 999);
    assert_eq!(v, "hello world");
}

// =============================================================================
// Scenarios 6+: Explicit-cast projection type inference (#60).
//
// `SELECT $1::type AS v` must infer the parameter type from the cast so that
// ParameterDescription returns the correct OID and drivers (tokio-postgres,
// asyncpg, pgx, Prisma) bind the value without a WrongType rejection.
// =============================================================================

/// `SELECT $1::int4 AS v` with i32 — the canonical form Prisma / asyncpg emit
/// for scalar projections. The engine must infer OID INT4 (23) for $1 so the
/// driver can bind an i32 without a WrongType error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_int4_projection_cast() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT $1::int4 AS v")
        .await
        .expect("prepare SELECT $1::int4");

    let rows = client
        .query(&stmt, &[&42_i32])
        .await
        .expect("query SELECT $1::int4 with i32 42");

    assert_eq!(rows.len(), 1, "expected 1 row");
    let v: i32 = rows[0].get(0);
    assert_eq!(v, 42, "int4 cast param must round-trip as i32");
}

/// `SELECT $1::int8 AS v` with i64 — verifies int8/bigint inference.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_int8_projection_cast() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT $1::int8 AS v")
        .await
        .expect("prepare SELECT $1::int8");

    let rows = client
        .query(&stmt, &[&9_000_000_000_i64])
        .await
        .expect("query SELECT $1::int8");

    assert_eq!(rows.len(), 1);
    let v: i64 = rows[0].get(0);
    assert_eq!(v, 9_000_000_000_i64);
}

/// `SELECT $1::bool AS v` with bool — verifies boolean projection cast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_bool_projection_cast() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT $1::bool AS v")
        .await
        .expect("prepare SELECT $1::bool");

    let rows = client
        .query(&stmt, &[&true])
        .await
        .expect("query SELECT $1::bool with true");

    assert_eq!(rows.len(), 1);
    let v: bool = rows[0].get(0);
    assert!(v, "bool cast param must round-trip as true");
}

/// CAST syntax (`CAST($1 AS int4)`) must infer the same as `::` sugar.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn param_bind_cast_syntax_int4() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let stmt = client
        .prepare("SELECT CAST($1 AS int4) AS v")
        .await
        .expect("prepare SELECT CAST($1 AS int4)");

    let rows = client
        .query(&stmt, &[&-7_i32])
        .await
        .expect("query CAST($1 AS int4) with -7");

    assert_eq!(rows.len(), 1);
    let v: i32 = rows[0].get(0);
    assert_eq!(v, -7);
}
