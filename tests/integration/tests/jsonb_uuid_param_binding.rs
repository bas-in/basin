//! Round-trip JSONB + UUID parameter binding through the pgwire extended
//! protocol with `tokio-postgres`'s native binary encoders.
//!
//! Phase 5.10 wire-protocol fix: Basin's `ParameterDescription` previously
//! emitted BYTEA / TEXT for JSONB / UUID columns, which made strict
//! client-side encoders (asyncpg, pgx, tokio-postgres with their
//! `with-uuid-1` / `with-serde_json-1` features) refuse to encode native
//! `uuid::Uuid` / `serde_json::Value` values. This test pins the corrected
//! behaviour: `Uuid` and `Value` flow straight through the binary BIND path
//! and come back identical.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::ProjectId;
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::NoTls;
use uuid::Uuid;

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
    map.insert("alice".to_owned(), project);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jsonb_uuid_param_binding_round_trip() {
    let server = start_server().await;
    let client = connect(server.addr).await;

    client
        .execute(
            "CREATE TABLE foo (id UUID NOT NULL, payload JSONB NOT NULL)",
            &[],
        )
        .await
        .expect("CREATE TABLE");

    let id = Uuid::new_v4();
    let payload = json!({
        "k": "v",
        "n": 42,
        "nested": [1, 2, 3],
        "obj": {"b": 2, "a": 1},
    });

    // tokio-postgres encodes Uuid + serde_json::Value in BINARY by default
    // when the `with-uuid-1` / `with-serde_json-1` features are on. This is
    // exactly the encoder path asyncpg and pgx exercise; if our
    // ParameterDescription advertises the wrong OIDs (BYTEA / TEXT) the
    // client-side encoder rejects the call before it ever hits the wire.
    client
        .execute(
            "INSERT INTO foo (id, payload) VALUES ($1, $2)",
            &[&id, &payload],
        )
        .await
        .expect("INSERT with native UUID + JSONB params");

    // SELECT round-trip via the extended protocol. We avoid `WHERE id = $1`
    // here because the storage-side fast-select predicate evaluator pushes
    // a `Utf8` ScalarValue against a `FixedSizeBinary(16)` column (a
    // pre-existing limitation tracked separately in basin-storage). Reading
    // the row back unfiltered still proves the parameter-binding wire-format
    // round-trip — the OID advertisement and binary-format BIND are the
    // whole point of this test.
    let rows = client
        .query("SELECT id, payload FROM foo", &[])
        .await
        .expect("SELECT all rows");
    assert_eq!(rows.len(), 1, "expected exactly 1 row, got {}", rows.len());

    let got_id: Uuid = rows[0].get(0);
    assert_eq!(got_id, id, "UUID round-trip mismatch");

    let got_payload: serde_json::Value = rows[0].get(1);
    // Canonicalisation reorders object keys; `serde_json::Value`'s
    // PartialEq is structural and order-independent, so this still
    // catches semantic drift.
    assert_eq!(got_payload, payload, "JSONB round-trip mismatch");
}
