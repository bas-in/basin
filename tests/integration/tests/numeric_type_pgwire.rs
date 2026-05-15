//! `NUMERIC` over the pgwire boundary.
//!
//! End-to-end check: a real tokio-postgres client issues
//! `CREATE TABLE … (price NUMERIC(10, 2))`, INSERTs fractional literals,
//! and SELECTs them back. The `RowDescription` must advertise OID 1700
//! (NUMERIC) and the text-format payload must round-trip the column's
//! `(precision, scale)` shape (e.g. `1.50` not `1.5`).
//!
//! Binary numeric encoding is deferred to v0.2; lenient drivers like
//! `tokio-postgres` accept the text form in a binary slot via the
//! fallback path in `basin-router::types::encode_value_binary`.

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
async fn numeric_round_trip_through_pgwire() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .simple_query("CREATE TABLE t (id BIGINT NOT NULL, price NUMERIC(10, 2))")
        .await
        .expect("CREATE TABLE with NUMERIC must succeed at pgwire layer");
    client
        .simple_query("INSERT INTO t VALUES (1, 1.50), (2, 99.99)")
        .await
        .expect("INSERT NUMERIC values");

    // simple_query path uses the text protocol — confirm the rendered cells
    // preserve the column's scale (no accidental f64 rendering as `1.5`).
    let rows = client
        .simple_query("SELECT id, price FROM t WHERE id = 1")
        .await
        .expect("simple-query SELECT");
    let mut found = false;
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let price = row.get(1).expect("price cell");
            assert_eq!(
                price, "1.50",
                "NUMERIC text form must preserve scale (got {price:?})"
            );
            found = true;
        }
    }
    assert!(found, "expected a SimpleQueryMessage::Row");

    // Extended-query path: confirm RowDescription advertises OID 1700.
    // tokio-postgres requests binary for NUMERIC by default; our v0.1
    // binary fallback emits the text form, which lenient drivers tolerate
    // — but the OID assertion is what guards drivers / introspection
    // from seeing TEXT (25) instead of NUMERIC (1700).
    let stmt = client
        .prepare("SELECT price FROM t WHERE id = $1")
        .await
        .expect("prepare");
    let cols = stmt.columns();
    assert_eq!(cols.len(), 1);
    assert_eq!(
        cols[0].type_().oid(),
        1700,
        "NUMERIC column must advertise OID 1700 (numeric), got {}",
        cols[0].type_().oid()
    );

    drop(client);
    drop(server);
}
