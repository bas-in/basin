//! S3 port of `poc_extended_smoke.rs`.
//!
//! Drives `tokio-postgres`'s extended-protocol API (Parse / Bind / Execute)
//! end-to-end against an in-process Basin server backed by real-S3 `Storage`.
//! The protocol surface doesn't depend on the storage backend, but every
//! INSERT/CREATE here actually writes to S3, so this card surfaces protocol
//! correctness over S3-resident parquet.
//!
//! Skips cleanly when `[s3]` is missing.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::{ServerConfig, StaticTenantResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio_postgres::NoTls;

const TEST_NAME: &str = "s3_viability_extended_protocol";

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _cleanup: CleanupOnDrop,
}

async fn start_server(
    s3_cfg: &basin_integration_tests::test_config::S3Config,
) -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix.as_str())),
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
        _cleanup: cleanup,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_extended_protocol() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let server = start_server(&s3_cfg).await;
    let alice = connect(server.addr, "alice").await;

    let mut total = 0u32;
    let mut passed = 0u32;

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

    total += 1;
    let all = alice
        .prepare("SELECT id, body FROM events")
        .await
        .expect("prepare all failed");
    let rs = alice.query(&all, &[]).await.expect("query all failed");
    if rs.len() == 3 {
        passed += 1;
    }

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

    report_real_viability(
        "extended_protocol",
        "Postgres extended-query protocol works end-to-end (real S3)",
        "tokio-postgres / asyncpg / JDBC default extended-query path runs CREATE / INSERT($1,$2) / prepared SELECT against Basin (real-S3 storage) without falling back to simple_query.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert_eq!(
        passed, total,
        "{passed}/{total} extended-protocol queries passed"
    );
}
