//! Multi-statement simple-query (`Q` message) support.
//!
//! PG's pgwire spec says a single `Q` carries N semicolon-separated
//! statements; clients like `tokio_postgres::Client::batch_execute` and
//! `psql -f setup.sql` rely on it. Earlier revisions of Basin rejected
//! anything that produced more than one parsed statement with SQLSTATE
//! `XX000` ("expected exactly one statement, got N"); the router-side
//! splitter now dispatches each statement individually and emits one
//! response group per statement. The shape is asserted here end-to-end
//! through `tokio-postgres` against an in-process Basin server.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

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
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let tenant = TenantId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), tenant);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
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
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

/// `batch_execute` of two DDLs in one `Q` message succeeds; both tables are
/// visible to a follow-up SELECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_multiple_statements_succeeds() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .batch_execute(
            "CREATE TABLE t (id BIGINT NOT NULL); \
             CREATE TABLE s (val TEXT NOT NULL);",
        )
        .await
        .expect("batch_execute of two CREATE TABLEs");

    let names: Vec<String> = client
        .query(
            "SELECT c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' AND n.nspname = 'public'",
            &[],
        )
        .await
        .expect("pg_class lookup")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    assert!(names.iter().any(|n| n == "t"), "missing t in {names:?}");
    assert!(names.iter().any(|n| n == "s"), "missing s in {names:?}");
}

/// Two SELECTs in one batch — both result sets must come back in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_select_then_select() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let messages = client
        .simple_query("SELECT 1; SELECT 2;")
        .await
        .expect("simple_query of two SELECTs");

    // Each SELECT yields a `RowDescription` (no separate variant in the
    // tokio-postgres surface), N `Row` messages, and a `CommandComplete`
    // (surfaced as `SimpleQueryMessage::CommandComplete`). We expect to
    // see at least two CommandComplete tags, one per SELECT, with the
    // row payloads in order: 1 then 2.
    let mut row_values: Vec<String> = Vec::new();
    let mut complete_tags: Vec<u64> = Vec::new();
    for m in &messages {
        match m {
            SimpleQueryMessage::Row(row) => {
                row_values.push(row.get(0).unwrap_or("").to_string());
            }
            SimpleQueryMessage::CommandComplete(n) => complete_tags.push(*n),
            _ => {}
        }
    }
    assert_eq!(row_values, vec!["1".to_string(), "2".to_string()]);
    assert_eq!(complete_tags.len(), 2, "expected two CommandCompletes, got {complete_tags:?}");
}

/// Mid-batch failure — the failing statement reports an error AND every
/// statement after it is silently skipped. Earlier successful statements
/// stand (no rollback semantics in non-transactional simple-query).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_error_mid_batch_stops_processing() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let result = client
        .batch_execute(
            "CREATE TABLE t (id BIGINT NOT NULL); \
             CREATE TABLE t (val TEXT NOT NULL); \
             CREATE TABLE u (other BIGINT NOT NULL);",
        )
        .await;
    assert!(result.is_err(), "second CREATE TABLE t must fail");

    // Verify the assertion: the FIRST CREATE did succeed; the THIRD did
    // NOT execute. We check by looking at pg_class — if `u` is present
    // the dispatcher kept going past the error.
    let names: Vec<String> = client
        .query(
            "SELECT c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind = 'r' AND n.nspname = 'public'",
            &[],
        )
        .await
        .expect("pg_class lookup")
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    assert!(
        names.iter().any(|n| n == "t"),
        "first CREATE TABLE t should have succeeded before the failure: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "u"),
        "third CREATE TABLE u must NOT have been dispatched after the mid-batch error: {names:?}"
    );
}

/// Empty batch — `batch_execute("")` should succeed (no error, connection
/// stays open). PG sends `EmptyQueryResponse` + `ReadyForQuery`; tokio-postgres
/// surfaces this as `Ok(())`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_empty_input() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .batch_execute("")
        .await
        .expect("empty batch should not error");

    // Connection still usable.
    client
        .simple_query("SELECT 1")
        .await
        .expect("connection still usable after empty batch");
}

/// Single-statement batch — must behave identically to a direct query, no
/// regression in the common path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simple_query_single_statement_unchanged() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .batch_execute("CREATE TABLE only (id BIGINT NOT NULL)")
        .await
        .expect("single-statement batch_execute");

    let messages = client
        .simple_query("SELECT 1")
        .await
        .expect("single-statement simple_query");
    let row_values: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(row_values, vec!["1".to_string()]);
}
