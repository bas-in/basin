//! Smoke tests for INSERT/UPDATE/DELETE … RETURNING over the extended protocol.
//!
//! These tests reproduce the exact gap described in issue #73: when a DML
//! statement carrying a RETURNING clause is sent via the extended-query protocol
//! (Parse / Bind / Execute — the path every Postgres driver uses for
//! parameterised queries), the projected RETURNING columns must reach the client
//! as properly-typed DataRow messages, with a matching RowDescription delivered
//! during the preceding Describe phase.
//!
//! Before the fix, `basin-engine` returned the correct RETURNING rows but the
//! router's extended-protocol handler always sent `NoData` during Describe for
//! DML statements (because `probe_schema` in the engine short-circuits at any
//! non-SELECT SQL), causing drivers to cache a 0-column schema. The DataRows
//! that followed were parsed with that 0-column schema, producing "invalid
//! column `0`" errors in every ORM that uses parameterised RETURNING.
//!
//! Fix landed in `crates/basin-router/src/protocol.rs` (function
//! `probe_returning_schema`): at Parse time the router detects a RETURNING
//! clause, synthesises a `SELECT <returning_items> FROM <table> WHERE false`
//! probe query, executes it (0 rows touched), and patches `StatementSchema.columns`
//! so Describe returns the correct RowDescription. The CommandComplete tag is
//! also corrected to "INSERT 0 N" / "UPDATE N" / "DELETE N".

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// ---------------------------------------------------------------------------
// Harness
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

async fn connect(addr: SocketAddr) -> Client {
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
            eprintln!("extended_returning_smoke conn driver: {e}");
        }
    });
    client
}

/// Reproduce the exact #73 bug: `INSERT … RETURNING` via `client.query()`
/// (extended protocol) must return a DataRow with the correct columns.
///
/// Before the fix this panicked with "invalid column `0`" because the driver
/// decoded the row using a 0-column schema (Describe returned NoData instead
/// of RowDescription).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_returning_extended_protocol_yields_correct_columns() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query(
            "CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL, qty BIGINT NOT NULL)",
        )
        .await
        .expect("create table");

    // Single-row INSERT … RETURNING id, name via extended protocol
    // (tokio-postgres::query == Parse+Bind+Execute, not simple_query).
    let rows = client
        .query(
            "INSERT INTO items (id, name, qty) VALUES ($1, $2, $3) RETURNING id, name",
            &[&42_i64, &"widget", &10_i64],
        )
        .await
        .expect("INSERT ... RETURNING via extended protocol must not error");

    assert_eq!(rows.len(), 1, "one inserted row → one RETURNING row");
    // Before the fix: `row.get::<_, i64>(0)` panics with "invalid column `0`"
    // because the RowDescription was never sent (Describe returned NoData).
    let id: i64 = rows[0].get(0);
    let name: &str = rows[0].get(1);
    assert_eq!(id, 42, "RETURNING id must equal the inserted value");
    assert_eq!(name, "widget", "RETURNING name must equal the inserted value");
}

/// `UPDATE … RETURNING` via extended protocol — fix must surface the new
/// column values, not a 0-column result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_returning_extended_protocol_yields_correct_columns() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query(
            "CREATE TABLE counters (id BIGINT NOT NULL, val BIGINT NOT NULL)",
        )
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO counters (id, val) VALUES (1, 100), (2, 200)")
        .await
        .expect("seed");

    // UPDATE … RETURNING id, val — match one row.
    let rows = client
        .query(
            "UPDATE counters SET val = $1 WHERE id = $2 RETURNING id, val",
            &[&999_i64, &1_i64],
        )
        .await
        .expect("UPDATE ... RETURNING via extended protocol must not error");

    assert_eq!(rows.len(), 1, "one matched row → one RETURNING row");
    let id: i64 = rows[0].get(0);
    let val: i64 = rows[0].get(1);
    assert_eq!(id, 1);
    assert_eq!(val, 999, "RETURNING val must reflect the new value");
}

/// `DELETE … RETURNING` via extended protocol — fix must surface the deleted
/// row's columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_returning_extended_protocol_yields_correct_columns() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE events (id BIGINT NOT NULL, kind TEXT NOT NULL)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO events (id, kind) VALUES (7, 'click'), (8, 'scroll')")
        .await
        .expect("seed");

    let rows = client
        .query(
            "DELETE FROM events WHERE id = $1 RETURNING id",
            &[&7_i64],
        )
        .await
        .expect("DELETE ... RETURNING via extended protocol must not error");

    assert_eq!(rows.len(), 1, "one deleted row → one RETURNING row");
    let id: i64 = rows[0].get(0);
    assert_eq!(id, 7, "RETURNING id must equal the deleted row's id");

    // Verify the row was actually deleted.
    let remaining = client
        .query("SELECT COUNT(*) FROM events", &[])
        .await
        .expect("count");
    let n: i64 = remaining[0].get(0);
    assert_eq!(n, 1, "only one event remains after DELETE");
}

/// Zero-row case: UPDATE with RETURNING but WHERE matches nothing → 0
/// DataRows + CommandComplete. This must succeed (not error) because ORMs
/// like Ecto / ActiveRecord treat a zero-row RETURNING as a normal empty set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returning_zero_rows_is_ok_not_error() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE things (id BIGINT NOT NULL, v BIGINT NOT NULL)")
        .await
        .expect("create table");
    client
        .simple_query("INSERT INTO things VALUES (1, 10)")
        .await
        .expect("seed");

    // WHERE id = 999 matches nothing → 0 RETURNING rows.
    let rows = client
        .query(
            "UPDATE things SET v = $1 WHERE id = $2 RETURNING id",
            &[&0_i64, &999_i64],
        )
        .await
        .expect("zero-row UPDATE RETURNING must succeed with 0 rows, not error");
    assert_eq!(rows.len(), 0, "no rows matched → RETURNING is empty");

    // Plain DML without RETURNING must still work (no regression).
    let affected = client
        .execute("UPDATE things SET v = $1 WHERE id = $2", &[&99_i64, &1_i64])
        .await
        .expect("plain UPDATE must still work");
    assert_eq!(affected, 1, "plain UPDATE without RETURNING affected 1 row");
}
