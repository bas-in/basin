//! Cross-restart smoke test for [`basin_catalog::PostgresCatalog`].
//!
//! Boots a Basin server backed by a fresh Postgres schema, writes rows
//! through the pgwire simple-query path, drops the server, brings up a new
//! server pointed at the *same* schema and the *same* on-disk data
//! directory, and asserts the rows are still readable. Proves that catalog
//! state survives process restart — which is the whole point of WEDGE.md
//! item #2.
//!
//! Hermeticity:
//!
//! * Each run allocates a unique Postgres schema name (`basin_catalog_smoke
//!   _<ulid>`). A `Drop` guard runs `DROP SCHEMA ... CASCADE` on a fresh
//!   client so a panicking test still cleans up after itself.
//! * The on-disk parquet path lives under a `TempDir` shared by both server
//!   instances (so the second instance sees the first instance's files).
//!
//! Skip-if-unavailable: if Postgres isn't reachable on `127.0.0.1:5432`, the
//! test prints a one-line skip message and returns success. The integration
//! suite must remain runnable in environments without local Postgres.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_router::{RunningServer, ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use ulid::Ulid;

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

/// Drops the schema (and every table inside) when the test ends. Uses a
/// fresh connection on a fresh runtime so cleanup runs even from sync
/// `Drop`.
struct SchemaGuard {
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("smoke schema cleanup runtime: {e}");
                    return;
                }
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
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

/// True if the local Postgres responds to a TCP connect within a short
/// timeout. Returns false on any error so callers can `eprintln!` and skip.
async fn pg_reachable() -> bool {
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        tokio_postgres::connect(PG_URL, NoTls),
    )
    .await;
    matches!(result, Ok(Ok(_)))
}

/// Boot a Basin server pointed at the given Postgres schema and on-disk
/// data dir. Returns the running handle.
async fn boot_server(
    data_dir: &Path,
    schema_name: &str,
    user: &str,
    tenant: TenantId,
) -> RunningServer {
    basin_common::telemetry::try_init_for_tests();

    let fs = LocalFileSystem::new_with_prefix(data_dir).expect("local fs");
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let pg_catalog =
        basin_catalog::PostgresCatalog::connect_with_schema(PG_URL, schema_name)
            .await
            .expect("connect postgres catalog");
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(pg_catalog);
    let engine =
        basin_engine::Engine::new(basin_engine::EngineConfig {
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

/// Tear the running server down: drop the shutdown sender (= the test
/// stops accepting new connections) and await the join handle so we don't
/// race tokio task cancellation with the rest of the test. Mirrors what the
/// task spec calls "drop the `RunningServer.shutdown` channel; await
/// `join`".
async fn shutdown(server: RunningServer) {
    let RunningServer {
        shutdown, join, ..
    } = server;
    drop(shutdown);
    // Bound the wait — if the join hangs (it shouldn't), we don't want the
    // test to hang either.
    let _ = tokio::time::timeout(Duration::from_secs(5), join).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_catalog_survives_restart() {
    if !pg_reachable().await {
        eprintln!(
            "postgres not reachable at {PG_URL}, skipping durable_catalog_survives_restart"
        );
        // Emit a viability sidecar reflecting that the test was skipped so
        // the dashboard reports "unavailable" instead of stale data.
        report_viability(
            "durable_catalog",
            "Durable catalog: rows survive restart",
            "Catalog metadata persists across basin-server restarts",
            false,
            PrimaryMetric {
                label: "rows survived restart".into(),
                value: 0.0,
                unit: "bool".into(),
                bar: BarOp::eq(1.0),
            },
            serde_json::json!({
                "skipped": true,
                "reason": "postgres unreachable on 127.0.0.1:5432",
            }),
        );
        return;
    }

    let schema_name = format!(
        "basin_catalog_smoke_{}",
        Ulid::new().to_string().to_lowercase()
    );
    let _guard = SchemaGuard {
        schema: schema_name.clone(),
    };
    let data_dir = TempDir::new().expect("tempdir");
    let user = "alice";
    let tenant = TenantId::new();

    // === Phase 1: first server lifetime ===
    let server1 = boot_server(data_dir.path(), &schema_name, user, tenant).await;
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

    // Sanity: the rows are visible to the first server.
    let pre_rows = rows_of(
        &client1
            .simple_query("SELECT id, body FROM durable_events ORDER BY id")
            .await
            .expect("pre-restart select"),
    );
    assert_eq!(pre_rows.len(), 3, "pre-restart row count");

    // Drop the client so the connection driver shuts down before we drop
    // the server (otherwise tokio_postgres logs noisy "connection closed"
    // errors on shutdown that would mask real failures).
    drop(client1);
    shutdown(server1).await;

    // === Phase 2: bring up a fresh server pointed at the same schema +
    // same data dir, then read the rows back. ===
    let server2 = boot_server(data_dir.path(), &schema_name, user, tenant).await;
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

    report_viability(
        "durable_catalog",
        "Durable catalog: rows survive restart",
        "Catalog metadata persists across basin-server restarts",
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
        }),
    );

    assert!(survived, "rows did not survive simulated restart: {post_rows:?}");
}
