//! Integration tests for schema-level DDL.
//!
//! Covers:
//!   - `CREATE SCHEMA [IF NOT EXISTS] name [AUTHORIZATION role]`
//!   - `DROP SCHEMA [IF EXISTS] name [CASCADE|RESTRICT]`
//!   - `ALTER SCHEMA name RENAME TO new_name`
//!   - `SET search_path = …` / `SHOW search_path`
//!   - Schema-qualified table names (`schema.table`) in SELECT / INSERT

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _data_dir: TempDir,
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
    map.insert("alice".to_owned(), project);
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
        _data_dir: dir,
    }
}

async fn connect(addr: SocketAddr) -> tokio_postgres::Client {
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

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<Vec<Option<String>>> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => {
                let mut row = Vec::with_capacity(r.len());
                for i in 0..r.len() {
                    row.push(r.get(i).map(|s| s.to_string()));
                }
                Some(row)
            }
            _ => None,
        })
        .collect()
}

/// Extract the full error message from a tokio-postgres error, walking
/// the error-source chain to find a `DbError` message when the top-level
/// is a generic "db error".
fn db_error_message(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        return db.message().to_string();
    }
    // Fall back to string representation (includes source chain on some versions)
    format!("{e}")
}

// ── CREATE SCHEMA ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_schema_basic() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_schema_if_not_exists() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS myschema")
        .await
        .expect("CREATE SCHEMA IF NOT EXISTS should succeed");

    // Second call with IF NOT EXISTS is idempotent.
    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS myschema")
        .await
        .expect("idempotent CREATE SCHEMA IF NOT EXISTS should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_schema_duplicate_errors() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("first CREATE SCHEMA should succeed");

    let err = client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect_err("duplicate CREATE SCHEMA should fail");
    let msg = db_error_message(&err);
    assert!(
        msg.contains("already exists"),
        "expected 'already exists' in error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_schema_public_is_idempotent() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // 'public' always exists; CREATE SCHEMA public is always idempotent.
    client
        .simple_query("CREATE SCHEMA public")
        .await
        .expect("CREATE SCHEMA public should be idempotent");

    client
        .simple_query("CREATE SCHEMA IF NOT EXISTS public")
        .await
        .expect("CREATE SCHEMA IF NOT EXISTS public should be idempotent");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_schema_authorization() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // AUTHORIZATION clause is accepted; owner is ignored in Basin's JWT model.
    client
        .simple_query("CREATE SCHEMA myschema AUTHORIZATION alice")
        .await
        .expect("CREATE SCHEMA … AUTHORIZATION should succeed");
}

// ── DROP SCHEMA ───────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_basic() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA");
    client
        .simple_query("DROP SCHEMA myschema")
        .await
        .expect("DROP SCHEMA should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_if_exists() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    // Dropping a non-existent schema with IF EXISTS is a no-op.
    client
        .simple_query("DROP SCHEMA IF EXISTS nonexistent")
        .await
        .expect("DROP SCHEMA IF EXISTS on missing schema should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_not_found_errors() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let err = client
        .simple_query("DROP SCHEMA nonexistent")
        .await
        .expect_err("DROP SCHEMA on missing schema should fail");
    let msg = db_error_message(&err);
    assert!(
        msg.contains("does not exist"),
        "expected 'does not exist', got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_cascade_accepted() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA");
    client
        .simple_query("DROP SCHEMA myschema CASCADE")
        .await
        .expect("DROP SCHEMA CASCADE should succeed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_schema_public_rejected() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let err = client
        .simple_query("DROP SCHEMA public")
        .await
        .expect_err("DROP SCHEMA public should fail");
    let msg = db_error_message(&err);
    assert!(
        msg.contains("cannot drop"),
        "expected 'cannot drop', got: {msg}"
    );
}

// ── ALTER SCHEMA RENAME TO ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_schema_rename_basic() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA");
    client
        .simple_query("ALTER SCHEMA myschema RENAME TO newschema")
        .await
        .expect("ALTER SCHEMA RENAME TO should succeed");

    // Old name is gone — DROP should fail.
    let err = client
        .simple_query("DROP SCHEMA myschema")
        .await
        .expect_err("old schema name should no longer exist");
    let msg = db_error_message(&err);
    assert!(msg.contains("does not exist"), "{msg}");

    // New name is present — DROP should succeed.
    client
        .simple_query("DROP SCHEMA newschema")
        .await
        .expect("new schema name should exist after rename");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_schema_rename_nonexistent_errors() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let err = client
        .simple_query("ALTER SCHEMA ghost RENAME TO other")
        .await
        .expect_err("renaming a non-existent schema should fail");
    let msg = db_error_message(&err);
    assert!(msg.contains("does not exist"), "{msg}");
}

// ── SET search_path / SHOW search_path ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn show_search_path_default() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    let msgs = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path should succeed");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 1, "expected one row from SHOW search_path");
    let val = data[0][0].as_deref().unwrap_or("");
    assert!(
        val.contains("public"),
        "default search_path should contain 'public', got: {val:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_and_show_search_path() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA");
    client
        .simple_query("SET search_path = myschema, public")
        .await
        .expect("SET search_path should succeed");

    let msgs = client
        .simple_query("SHOW search_path")
        .await
        .expect("SHOW search_path after SET should succeed");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 1);
    let val = data[0][0].as_deref().unwrap_or("");
    assert!(
        val.contains("myschema"),
        "search_path should contain 'myschema' after SET, got: {val:?}"
    );
    assert!(
        val.contains("public"),
        "search_path should still contain 'public', got: {val:?}"
    );
}

// ── Schema-qualified table names ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_qualified_create_and_select() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE SCHEMA myschema")
        .await
        .expect("CREATE SCHEMA");

    // CREATE TABLE with schema qualifier.
    client
        .simple_query(
            "CREATE TABLE myschema.items (id BIGINT NOT NULL, label TEXT NOT NULL)",
        )
        .await
        .expect("CREATE TABLE myschema.items should succeed");

    // INSERT using schema-qualified name.
    client
        .simple_query("INSERT INTO myschema.items VALUES (1, 'hello'), (2, 'world')")
        .await
        .expect("INSERT INTO myschema.items should succeed");

    // SELECT using schema-qualified name.
    let msgs = client
        .simple_query("SELECT id, label FROM myschema.items ORDER BY id")
        .await
        .expect("SELECT FROM myschema.items should succeed");
    let data = rows_of(&msgs);
    assert_eq!(data.len(), 2, "expected 2 rows");
    assert_eq!(data[0][0].as_deref(), Some("1"));
    assert_eq!(data[0][1].as_deref(), Some("hello"));
    assert_eq!(data[1][0].as_deref(), Some("2"));
    assert_eq!(data[1][1].as_deref(), Some("world"));
}
