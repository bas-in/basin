//! Diagnostic: extended-protocol (prepared statement) vs simple-protocol
//! overhead measured through Basin's in-process pgwire server and, when
//! available, a local Postgres instance.
//!
//! Basin side: an in-process pgwire listener is started via
//! `basin_router::run_until_bound`, mirroring the pattern used in
//! `prepared_statements.rs`, `poc_extended_smoke.rs`, and `schema_e2e.rs`.
//! `tokio_postgres` connects to it and exercises both `simple_query` (simple
//! protocol, no Parse/Bind) and `client.query(&stmt, &[&k])` (extended
//! protocol, reusing a prepared `Statement` handle across all iterations).
//!
//! Postgres side: same two shapes run against `127.0.0.1:5432` via the same
//! `try_connect` convention as `compare_postgres_common.rs`. If Postgres is
//! unreachable the PG numbers are skipped.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_router::{ServerConfig, StaticProjectResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio_postgres::{Client, NoTls};

// ── in-process Basin server ──────────────────────────────────────────────────

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _dir: TempDir,
}

async fn start_basin_server() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });

    let project = ProjectId::new();
    let mut map = HashMap::new();
    map.insert("probe".to_owned(), project);
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
    .expect("basin pgwire server failed to bind");

    TestServer {
        addr: running.local_addr,
        _shutdown: running.shutdown,
        _join: running.join,
        _dir: dir,
    }
}

async fn connect_basin(addr: SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=probe password=ignored",
        addr.ip(),
        addr.port()
    );
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to Basin pgwire");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

// ── Postgres helpers (mirrors compare_postgres_common try_connect) ────────────

async fn try_connect_pg() -> Option<Client> {
    let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_owned());
    for u in [user.as_str(), "postgres"] {
        let conn_str = format!("host=127.0.0.1 port=5432 user={u} dbname=postgres");
        if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            return Some(client);
        }
    }
    None
}

// ── percentile helper (same logic as update_latency_probe) ───────────────────

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

// ── seed + measure helpers ───────────────────────────────────────────────────

/// Seed 10k rows (id BIGINT PK, v BIGINT) into a table named `t` on `client`.
/// For Postgres the INSERT is issued in one multi-VALUES batch; for Basin we
/// use the same shape.
async fn seed_table(client: &Client, table: &str) {
    client
        .simple_query(&format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE TABLE {table}: {e}"));

    // Build a single multi-row INSERT with all 10k rows.
    let mut stmt = format!("INSERT INTO {table} VALUES ");
    for k in 0i64..10_000 {
        if k > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({k},{k})"));
    }
    client
        .simple_query(&stmt)
        .await
        .unwrap_or_else(|e| panic!("seed INSERT into {table}: {e}"));
}

/// Measure p50 over `n` iterations of a simple-protocol SELECT.
/// Each iteration picks a distinct id spread across [0, 10_000).
async fn measure_simple(client: &Client, table: &str, n: usize) -> f64 {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k = ((i as i64) * (10_000 / n as i64)) % 10_000;
        let sql = format!("SELECT * FROM {table} WHERE id = {k}");
        let t0 = Instant::now();
        let _ = client.simple_query(&sql).await.expect("simple_query");
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    percentile(&times, 0.50)
}

/// Measure p50 over `n` iterations of an extended-protocol SELECT, reusing a
/// single prepared `Statement` handle.
async fn measure_extended(client: &Client, table: &str, n: usize) -> f64 {
    let sql = format!("SELECT * FROM {table} WHERE id = $1");
    let stmt = client
        .prepare(&sql)
        .await
        .unwrap_or_else(|e| panic!("PREPARE on {table}: {e}"));

    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k: i64 = ((i as i64) * (10_000 / n as i64)) % 10_000;
        let t0 = Instant::now();
        let _ = client
            .query(&stmt, &[&k])
            .await
            .expect("extended query");
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    percentile(&times, 0.50)
}

// ── probe ─────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn prepared_stmt_probe() {
    const N: usize = 500;
    const TABLE: &str = "probe_t";

    // ── Basin side ────────────────────────────────────────────────────────────
    let server = start_basin_server().await;
    let basin_client = connect_basin(server.addr).await;

    seed_table(&basin_client, TABLE).await;

    // Warm-up: one pass of each protocol to prime plan caches.
    let _ = basin_client
        .simple_query(&format!("SELECT * FROM {TABLE} WHERE id = 1"))
        .await;
    {
        let warm_stmt = basin_client
            .prepare(&format!("SELECT * FROM {TABLE} WHERE id = $1"))
            .await
            .unwrap();
        let k: i64 = 1;
        let _ = basin_client.query(&warm_stmt, &[&k]).await;
    }

    let basin_simple_p50 = measure_simple(&basin_client, TABLE, N).await;
    let basin_extended_p50 = measure_extended(&basin_client, TABLE, N).await;

    println!(
        "[prepared-stmt] basin simple p50={basin_simple_p50:.3}ms  \
         extended p50={basin_extended_p50:.3}ms  \
         ratio={:.2}x  (extended/simple)",
        basin_extended_p50 / basin_simple_p50
    );

    // ── Postgres side ─────────────────────────────────────────────────────────
    let Some(pg_client) = try_connect_pg().await else {
        println!("[prepared-stmt] SKIP no postgres");
        return;
    };

    // Unique schema so parallel probe runs don't collide.
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("prepared_stmt_probe_{suffix}");

    // Schema guard: DROP on probe completion (or panic unwind).
    struct SchemaGuard(String);
    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            let schema = self.0.clone();
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("rt");
                rt.block_on(async move {
                    let user = std::env::var("USER").unwrap_or_else(|_| "postgres".to_owned());
                    for u in [user.as_str(), "postgres"] {
                        let conn_str =
                            format!("host=127.0.0.1 port=5432 user={u} dbname=postgres");
                        if let Ok((cl, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                            tokio::spawn(async move { let _ = conn.await; });
                            let _ = cl
                                .simple_query(&format!(
                                    "DROP SCHEMA IF EXISTS {schema} CASCADE"
                                ))
                                .await;
                            break;
                        }
                    }
                });
            })
            .join();
        }
    }
    let _guard = SchemaGuard(schema.clone());

    pg_client
        .simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("pg CREATE SCHEMA");

    // Create table inside the throwaway schema.
    pg_client
        .simple_query(&format!(
            "CREATE TABLE {schema}.{TABLE} (id BIGINT PRIMARY KEY, v BIGINT)"
        ))
        .await
        .expect("pg CREATE TABLE");

    // Seed 10k rows.
    let mut ins = format!("INSERT INTO {schema}.{TABLE} VALUES ");
    for k in 0i64..10_000 {
        if k > 0 {
            ins.push(',');
        }
        ins.push_str(&format!("({k},{k})"));
    }
    pg_client.simple_query(&ins).await.expect("pg seed INSERT");

    // Warm-up.
    let _ = pg_client
        .simple_query(&format!("SELECT * FROM {schema}.{TABLE} WHERE id = 1"))
        .await;
    {
        let ws = pg_client
            .prepare(&format!(
                "SELECT * FROM {schema}.{TABLE} WHERE id = $1"
            ))
            .await
            .unwrap();
        let k: i64 = 1;
        let _ = pg_client.query(&ws, &[&k]).await;
    }

    // Use schema-qualified queries for Postgres.
    let pg_simple_p50 = measure_simple_qualified(&pg_client, &schema, TABLE, N).await;
    let pg_extended_p50 = measure_extended_qualified(&pg_client, &schema, TABLE, N).await;

    println!(
        "[prepared-stmt] pg    simple p50={pg_simple_p50:.3}ms  \
         extended p50={pg_extended_p50:.3}ms  \
         ratio={:.2}x  (extended/simple)",
        pg_extended_p50 / pg_simple_p50
    );
    println!(
        "[prepared-stmt] basin-vs-pg simple={:.2}x  extended={:.2}x  (basin/pg)",
        basin_simple_p50 / pg_simple_p50,
        basin_extended_p50 / pg_extended_p50
    );
}

// Schema-qualified variants for Postgres (the Basin server has its own
// namespace so unqualified names work there).

async fn measure_simple_qualified(
    client: &Client,
    schema: &str,
    table: &str,
    n: usize,
) -> f64 {
    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k = ((i as i64) * (10_000 / n as i64)) % 10_000;
        let sql = format!("SELECT * FROM {schema}.{table} WHERE id = {k}");
        let t0 = Instant::now();
        let _ = client.simple_query(&sql).await.expect("pg simple_query");
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    percentile(&times, 0.50)
}

async fn measure_extended_qualified(
    client: &Client,
    schema: &str,
    table: &str,
    n: usize,
) -> f64 {
    let sql = format!("SELECT * FROM {schema}.{table} WHERE id = $1");
    let stmt = client
        .prepare(&sql)
        .await
        .unwrap_or_else(|e| panic!("pg PREPARE: {e}"));

    let mut times = Vec::with_capacity(n);
    for i in 0..n {
        let k: i64 = ((i as i64) * (10_000 / n as i64)) % 10_000;
        let t0 = Instant::now();
        let _ = client.query(&stmt, &[&k]).await.expect("pg extended query");
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    percentile(&times, 0.50)
}
