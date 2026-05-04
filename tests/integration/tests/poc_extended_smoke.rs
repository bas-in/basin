//! Extended-protocol smoke test for the Basin server.
//!
//! Drives `tokio-postgres`'s default extended-protocol API (`client.execute`,
//! `client.prepare`, `client.query`) end-to-end. Asserts:
//!
//! 1. CREATE / INSERT / prepared SELECT round-trip via `Parse`/`Bind`/`Execute`.
//! 2. Two tenants in parallel see only their own rows.
//! 3. The result-set of a parameterised SELECT matches what was inserted.
//!
//! This is the test that proves the wedge-1 work — the moment this is green,
//! every popular Postgres driver works against Basin instead of failing on
//! `Parse` with `0A000`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::NoTls;

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
    });
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let alice = TenantId::new();
    let bob = TenantId::new();
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    map.insert("bob".to_owned(), bob);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
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

async fn connect(addr: SocketAddr, user: &str) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user={user} password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("conn driver: {e}");
        }
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_extended_protocol_end_to_end() {
    let server = start_server().await;
    let alice = connect(server.addr, "alice").await;

    let mut total = 0u32;
    let mut passed = 0u32;

    // 1) DDL via extended protocol (`execute` defaults to extended).
    total += 1;
    if alice
        .execute(
            "CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)",
            &[],
        )
        .await
        .is_ok()
    {
        passed += 1;
    }

    // 2..4) Parameterised inserts. Each call sends Parse+Bind+Execute under
    // the hood. Three calls => three pass increments.
    for (id, body) in [(1i64, "first"), (2, "second"), (3, "third")] {
        total += 1;
        if alice
            .execute("INSERT INTO events VALUES ($1, $2)", &[&id, &body])
            .await
            .is_ok()
        {
            passed += 1;
        }
    }

    // 5) Prepared SELECT, then query it. `client.prepare` triggers a real
    // Parse+Describe round-trip; `client.query` then issues Bind+Execute.
    total += 1;
    let stmt = alice
        .prepare("SELECT id, body FROM events WHERE id = $1")
        .await
        .expect("prepare failed");
    let rows = alice
        .query(&stmt, &[&2i64])
        .await
        .expect("query failed");
    if rows.len() == 1 {
        let id: i64 = rows[0].get(0);
        let body: &str = rows[0].get(1);
        if id == 2 && body == "second" {
            passed += 1;
        }
    }

    // 6) Multi-row prepared SELECT to confirm we stream all data rows.
    total += 1;
    let all = alice
        .prepare("SELECT id, body FROM events")
        .await
        .expect("prepare all failed");
    let rs = alice.query(&all, &[]).await.expect("query all failed");
    if rs.len() == 3 {
        passed += 1;
    }

    // 7..8) Tenant isolation through the extended protocol.
    let bob = connect(server.addr, "bob").await;
    total += 1;
    if bob
        .execute(
            "CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)",
            &[],
        )
        .await
        .is_ok()
    {
        passed += 1;
    }
    total += 1;
    if bob
        .execute("INSERT INTO events VALUES ($1, $2)", &[&100i64, &"bob1"])
        .await
        .is_ok()
    {
        passed += 1;
    }

    // 9) Bob sees only his row.
    total += 1;
    let bobs = bob
        .query("SELECT id, body FROM events", &[])
        .await
        .expect("bob select failed");
    if bobs.len() == 1 {
        let id: i64 = bobs[0].get(0);
        if id == 100 {
            passed += 1;
        }
    }

    // 10) Alice still sees her three rows after bob's insert.
    total += 1;
    let alices = alice
        .query("SELECT id FROM events", &[])
        .await
        .expect("alice re-select failed");
    if alices.len() == 3 {
        passed += 1;
    }

    let ratio = passed as f64 / total as f64;
    let pass = (ratio - 1.0).abs() < f64::EPSILON;

    report_viability(
        "extended_protocol",
        "Postgres extended-query protocol works end-to-end",
        "tokio-postgres / asyncpg / JDBC default extended-query path runs CREATE / INSERT($1,$2) / prepared SELECT against Basin without falling back to simple_query.",
        pass,
        PrimaryMetric {
            label: "Extended-protocol queries that succeed".into(),
            value: ratio,
            unit: "fraction".into(),
            bar: BarOp::ge(1.0),
        },
        json!({
            "passed_queries": passed,
            "total_queries": total,
            "driver": "tokio-postgres 0.7",
        }),
    );

    assert_eq!(passed, total, "{passed}/{total} extended-protocol queries passed");
}
