//! S3 port of `durable_catalog_smoke.rs`.
//!
//! Boots an in-process Basin server (via `basin_router::run_until_bound`)
//! backed by:
//!   - a Postgres-backed catalog (unique schema per run)
//!   - a real-S3 `Storage` rooted at a per-run prefix
//!
//! Inserts rows, drops the server, brings up a fresh server pointed at the
//! same Postgres schema and the same S3 prefix, and asserts the rows are
//! still readable.
//!
//! Skips cleanly when `[postgres]` or `[s3]` are missing.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{
    BasinTestConfig, CleanupOnDrop, PostgresConfig,
};
use basin_router::{RunningServer, ServerConfig, StaticTenantResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use ulid::Ulid;

const TEST_NAME: &str = "s3_viability_durable_catalog";

fn pg_url(pg: &PostgresConfig) -> String {
    format!(
        "host={} port={} user={} password={} dbname={}",
        pg.host, pg.port, pg.user, pg.password, pg.dbname
    )
}

struct SchemaGuard {
    schema: String,
    url: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let url = self.url.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("schema cleanup runtime: {e}");
                    return;
                }
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(&url, NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(pair)) => pair,
                    _ => return,
                };
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
                drop(client);
                let _ = tokio::time::timeout(Duration::from_millis(200), driver).await;
            });
        })
        .join();
    }
}

async fn pg_reachable(url: &str) -> bool {
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_postgres::connect(url, NoTls),
    )
    .await;
    matches!(result, Ok(Ok(_)))
}

async fn boot_server(
    object_store: Arc<dyn object_store::ObjectStore>,
    run_prefix: &str,
    pg_url: &str,
    schema_name: &str,
    user: &str,
    tenant: TenantId,
) -> RunningServer {
    basin_common::telemetry::try_init_for_tests();

    let storage = Storage::new(StorageConfig {
        object_store,
        root_prefix: Some(ObjectPath::from(run_prefix)),
    });
    let pg_catalog =
        basin_catalog::PostgresCatalog::connect_with_schema(pg_url, schema_name)
            .await
            .expect("connect postgres catalog");
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(pg_catalog);
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let mut map = HashMap::new();
    map.insert(user.to_owned(), tenant);
    let resolver = Arc::new(StaticTenantResolver::new(map));

    basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
        pool: None,
    })
    .await
    .expect("server failed to bind")
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
            eprintln!("pg conn driver: {e}");
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

async fn shutdown(server: RunningServer) {
    let RunningServer {
        shutdown, join, ..
    } = server;
    drop(shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_viability_durable_catalog() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };
    let pg_cfg = match cfg.pg_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => {
            report_real_viability(
                "durable_catalog",
                "Durable catalog: rows survive restart (real S3)",
                "Catalog metadata persists across basin-server restarts (postgres-backed catalog + real-S3 storage)",
                false,
                PrimaryMetric {
                    label: "rows survived restart".into(),
                    value: 0.0,
                    unit: "bool".into(),
                    bar: BarOp::eq(1.0),
                },
                serde_json::json!({
                    "skipped": true,
                    "reason": "[postgres] not in test config",
                }),
            );
            return;
        }
    };
    let url = pg_url(&pg_cfg);
    if !pg_reachable(&url).await {
        eprintln!("postgres not reachable at {url}; skipping {TEST_NAME}");
        report_real_viability(
            "durable_catalog",
            "Durable catalog: rows survive restart (real S3)",
            "Catalog metadata persists across basin-server restarts (postgres-backed catalog + real-S3 storage)",
            false,
            PrimaryMetric {
                label: "rows survived restart".into(),
                value: 0.0,
                unit: "bool".into(),
                bar: BarOp::eq(1.0),
            },
            serde_json::json!({
                "skipped": true,
                "reason": "postgres unreachable",
                "endpoint": s3_cfg.endpoint.clone(),
                "bucket": s3_cfg.bucket,
            }),
        );
        return;
    }

    let schema_name = format!(
        "basin_s3_durable_catalog_{}",
        Ulid::new().to_string().to_lowercase()
    );
    let _schema_guard = SchemaGuard {
        schema: schema_name.clone(),
        url: url.clone(),
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let user = "alice";
    let tenant = TenantId::new();

    // === Phase 1 ===
    let server1 = boot_server(
        object_store.clone(),
        &run_prefix,
        &url,
        &schema_name,
        user,
        tenant,
    )
    .await;
    let addr1 = server1.local_addr;
    let client1 = connect(addr1, user).await;

    client1
        .simple_query(
            "CREATE TABLE durable_events (id BIGINT NOT NULL, body TEXT NOT NULL)",
        )
        .await
        .expect("create table");
    client1
        .simple_query(
            "INSERT INTO durable_events VALUES (1, 'first'), (2, 'second'), (3, 'third')",
        )
        .await
        .expect("insert");

    let pre_rows = rows_of(
        &client1
            .simple_query("SELECT id, body FROM durable_events ORDER BY id")
            .await
            .expect("pre-restart select"),
    );
    assert_eq!(pre_rows.len(), 3, "pre-restart row count");

    drop(client1);
    shutdown(server1).await;

    // === Phase 2 ===
    let server2 = boot_server(
        object_store.clone(),
        &run_prefix,
        &url,
        &schema_name,
        user,
        tenant,
    )
    .await;
    let addr2 = server2.local_addr;
    let client2 = connect(addr2, user).await;

    let post_rows = rows_of(
        &client2
            .simple_query("SELECT id, body FROM durable_events ORDER BY id")
            .await
            .expect("post-restart select"),
    );

    drop(client2);
    shutdown(server2).await;

    let survived = post_rows.len() == 3
        && post_rows[0] == vec![Some("1".into()), Some("first".into())]
        && post_rows[1] == vec![Some("2".into()), Some("second".into())]
        && post_rows[2] == vec![Some("3".into()), Some("third".into())];

    report_real_viability(
        "durable_catalog",
        "Durable catalog: rows survive restart (real S3)",
        "Catalog metadata + S3-resident parquet survives a basin-server restart: rows written through pgwire come back identical after the server is dropped and a fresh server is started against the same Postgres schema and the same S3 prefix.",
        survived,
        PrimaryMetric {
            label: "rows survived restart".into(),
            value: if survived { 1.0 } else { 0.0 },
            unit: "bool".into(),
            bar: BarOp::eq(1.0),
        },
        serde_json::json!({
            "rows_inserted": 3,
            "rows_after_restart": post_rows.len(),
            "schema": schema_name,
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    assert!(
        survived,
        "rows did not survive simulated restart: {post_rows:?}"
    );
}
