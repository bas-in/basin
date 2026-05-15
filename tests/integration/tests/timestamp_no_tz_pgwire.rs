//! Bare `TIMESTAMP` (without time zone) over the pgwire boundary.
//!
//! The DDL gate (`crates/basin-engine/src/types.rs::arrow_data_type`) was
//! flipped to accept TIMESTAMP. The downstream layers (`basin-router`'s
//! `arrow_to_pg_type`, `info_schema.rs`'s `pg_type_for_field`,
//! `convert.rs`'s Arrow bridge) were already wired for the no-tz variant.
//! This test pins the end-to-end behaviour: a real tokio-postgres client
//! issues `CREATE TABLE … (ts TIMESTAMP)` plus `INSERT` plus `SELECT`,
//! and the `RowDescription` must advertise OID 1114 (TIMESTAMP) — not
//! 1184 (TIMESTAMPTZ), which the column-shape would silently produce if
//! the timezone string ever leaked through.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

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

    let alice = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
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
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamp_without_tz_routes_through_pgwire() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE evt (id BIGINT NOT NULL, ts TIMESTAMP NOT NULL)")
        .await
        .expect("CREATE TABLE with bare TIMESTAMP must succeed at pgwire layer");
    client
        .simple_query("INSERT INTO evt VALUES (1, '2026-03-04T05:06:07Z')")
        .await
        .expect("INSERT timestamp value");

    // Extended-protocol query → RowDescription carries the advertised OID.
    let row = client
        .query_one("SELECT ts FROM evt WHERE id = 1", &[])
        .await
        .expect("SELECT ts");
    let col = row.columns().first().expect("at least one column");
    assert_eq!(
        col.type_().oid(),
        1114,
        "TIMESTAMP column must advertise OID 1114 (TIMESTAMP), got {} \
         — if 1184 (TIMESTAMPTZ), the timezone string leaked through",
        col.type_().oid()
    );

    // Side-by-side: TIMESTAMPTZ in the same connection still advertises 1184.
    client
        .simple_query("CREATE TABLE evtz (id BIGINT NOT NULL, ts TIMESTAMPTZ NOT NULL)")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO evtz VALUES (1, '2026-03-04T05:06:07Z')")
        .await
        .unwrap();
    let row_tz = client
        .query_one("SELECT ts FROM evtz WHERE id = 1", &[])
        .await
        .unwrap();
    let col_tz = row_tz.columns().first().expect("at least one column");
    assert_eq!(
        col_tz.type_().oid(),
        1184,
        "TIMESTAMPTZ column unchanged: must still advertise OID 1184"
    );

    drop(client);
    drop(server);
}
