//! Integration test for the WAL + shard-owner write path.
//!
//! Boots a Basin server with `BASIN_SHARD_ENABLED=1` semantics (constructed
//! programmatically in-process: WAL + Shard built and wired into
//! `EngineConfig`). Through `tokio-postgres`, runs CREATE/INSERT/SELECT and
//! asserts:
//!
//! 1. Rows inserted via the shard-acked path are visible to the very next
//!    SELECT — the SELECT-after-INSERT correctness check that the engine's
//!    "force a synchronous compaction before SELECT" tail-visibility strategy
//!    must satisfy.
//! 2. 1_000 sequential single-row INSERTs complete inside a generous bound
//!    (30 s; that's 30 ms/insert, well above PG-comparable inserts on a debug
//!    build). The WAL itself benches at <0.01 ms/insert; everything above that
//!    is sqlparser + DataFusion overhead.
//!
//! Emits a `shard_insert_path` viability JSON whose primary metric is
//! `inserts_per_sec`, with a lenient `>= 50` bar that should clear comfortably
//! once the path is wired. The bar is intentionally loose: it asserts
//! viability of the architecture (not a microbenchmark), and lets debug-build
//! CI runs pass without flaking.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::ProjectId;
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_router::{ServerConfig, StaticProjectResolver};
use object_store::local::LocalFileSystem;
use serde_json::json;
use tempfile::TempDir;
use tokio_postgres::{NoTls, SimpleQueryMessage};

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    _data_dir: TempDir,
    _wal_dir: TempDir,
    bg: Option<basin_shard::ShardBackgroundHandle>,
    wal: Arc<dyn basin_wal::Wal>,
}

async fn start_server_with_shard() -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let data_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage_fs = LocalFileSystem::new_with_prefix(data_dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(storage_fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());

    // WAL rooted at a separate tempdir, mirroring the BASIN_WAL_DIR layout the
    // server uses when the shard is enabled.
    let wal_fs = LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap();
    let wal: Arc<dyn basin_wal::Wal> = Arc::new(
        basin_wal::LocalWal::open(basin_wal::WalConfig {
            object_store: Arc::new(wal_fs),
            root_prefix: None,
            flush_interval: Duration::from_millis(200),
            flush_max_bytes: 1024 * 1024,
        })
        .await
        .expect("open WAL"),
    );

    let shard = basin_shard::Shard::new(basin_shard::ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();

    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog,
        shard: Some(shard),
    });

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), ProjectId::new());
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
        _data_dir: data_dir,
        _wal_dir: wal_dir,
        bg: Some(bg),
        wal,
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

/// Recursively collect the distinct partition-directory names that hold at
/// least one data file, i.e. the path segment immediately after a `data`
/// segment (`projects/{p}/tables/{t}/data/{partition}/yyyy/mm/dd/{file}`).
/// Extension-agnostic on purpose: works for Parquet and Vortex files alike.
fn partitions_with_files(root: &std::path::Path) -> std::collections::BTreeSet<String> {
    fn walk(dir: &std::path::Path, out: &mut std::collections::BTreeSet<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                let comps: Vec<&str> = path
                    .components()
                    .filter_map(|c| c.as_os_str().to_str())
                    .collect();
                if let Some(i) = comps.iter().position(|c| *c == "data") {
                    if let Some(part) = comps.get(i + 1) {
                        out.insert((*part).to_string());
                    }
                }
            }
        }
    }
    let mut out = std::collections::BTreeSet::new();
    walk(root, &mut out);
    out
}

/// W4 statement-affine striping: every multi-row INSERT statement writes its
/// WHOLE batch to ONE stripe partition selected by the session's pid
/// (`session_pid % BASIN_WRITE_STRIPES`), instead of slicing each statement
/// across all stripes. Cross-session lock fan-out — the only thing striping
/// ever bought — must survive: concurrent sessions get consecutive pids from
/// the `ConnectionRegistry`, so they must land in distinct stripe partitions.
///
/// Asserts:
/// 1. ≥ 4 concurrent sessions spread their statements across ≥ 2 distinct
///    stripe partitions on disk (session-stable selection; consecutive pids
///    within a window smaller than the stripe count can never all collide).
/// 2. `count(*)` and full-scan row counts are exact after the SELECT-forced
///    `flush_to_parquet` compacts every stripe tail — no rows lost or
///    duplicated by routing whole statements to single stripes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_sessions_fan_out_across_stripe_partitions() {
    let mut server = start_server_with_shard().await;
    let setup = connect(server.addr, "alice").await;

    setup
        .simple_query("CREATE TABLE striped (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("create");

    const SESSIONS: usize = 6;
    const STMTS_PER_SESSION: usize = 5;
    const ROWS_PER_STMT: usize = 4;

    // One connection (= one engine session = one pid) per writer; each issues
    // multi-row statements so the striped path engages (single-row statements
    // intentionally keep the historical default_key fallback).
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    for s in 0..SESSIONS {
        let addr = server.addr;
        set.spawn(async move {
            let client = connect(addr, "alice").await;
            for stmt in 0..STMTS_PER_SESSION {
                let base = (s * 1_000 + stmt * 10) as i64;
                let sql = format!(
                    "INSERT INTO striped VALUES \
                     ({0}, 's{4}-a'), ({1}, 's{4}-b'), ({2}, 's{4}-c'), ({3}, 's{4}-d')",
                    base,
                    base + 1,
                    base + 2,
                    base + 3,
                    s,
                );
                client
                    .simple_query(&sql)
                    .await
                    .unwrap_or_else(|e| panic!("session {s} stmt {stmt}: {e}"));
            }
        });
    }
    while let Some(r) = set.join_next().await {
        r.expect("writer session panicked");
    }

    let expected = (SESSIONS * STMTS_PER_SESSION * ROWS_PER_STMT) as i64;

    // Full scan first: forces the synchronous flush_to_parquet (Option A
    // tail-visibility), draining every stripe's WAL tail into data files.
    let res = setup
        .simple_query("SELECT id FROM striped")
        .await
        .expect("full select");
    let rows = rows_of(&res);
    assert_eq!(
        rows.len() as i64,
        expected,
        "full scan after flush: want {expected} rows, got {}",
        rows.len()
    );

    // count(*) across multiple statements after the flush must agree.
    let res = setup
        .simple_query("SELECT count(*) FROM striped")
        .await
        .expect("count");
    let rows = rows_of(&res);
    assert_eq!(
        rows[0][0].as_deref(),
        Some(expected.to_string().as_str()),
        "count(*) after flush_to_parquet disagrees"
    );

    // Stripe fan-out: the flushed files must span >= 2 distinct partitions.
    let parts = partitions_with_files(server._data_dir.path());
    assert!(
        parts.len() >= 2,
        "expected >= 2 distinct stripe partitions for {SESSIONS} concurrent \
         sessions, got {parts:?} — session-pid stripe selection has collapsed \
         to a single partition"
    );

    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_path_select_after_insert_visible() {
    let mut server = start_server_with_shard().await;
    let alice = connect(server.addr, "alice").await;

    alice
        .simple_query("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("create");

    alice
        .simple_query("INSERT INTO events VALUES (1, 'first'), (2, 'second'), (3, 'third')")
        .await
        .expect("insert");

    let res = alice
        .simple_query("SELECT id, body FROM events ORDER BY id")
        .await
        .expect("select");
    let rows = rows_of(&res);
    assert_eq!(rows.len(), 3, "want 3 rows, got {}: {:?}", rows.len(), rows);
    assert_eq!(rows[0][1].as_deref(), Some("first"));
    assert_eq!(rows[1][1].as_deref(), Some("second"));
    assert_eq!(rows[2][1].as_deref(), Some("third"));

    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_path_thousand_inserts_within_bound() {
    let mut server = start_server_with_shard().await;
    let alice = connect(server.addr, "alice").await;

    alice
        .simple_query("CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)")
        .await
        .expect("create");

    const N: usize = 1_000;

    let started = Instant::now();
    for i in 0..N {
        let sql = format!("INSERT INTO events VALUES ({i}, 'row-{i}')", i = i,);
        alice
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert {i}: {e}"));
    }
    let elapsed = started.elapsed();

    // Must complete inside 30 s — generous on debug; in practice this should
    // run an order of magnitude faster once the WAL path replaces the
    // synchronous Parquet write.
    assert!(
        elapsed < Duration::from_secs(30),
        "1k inserts took {elapsed:?} (>= 30 s)"
    );
    let inserts_per_sec = N as f64 / elapsed.as_secs_f64();

    // Sanity check: the rows survived. SELECT trips the synchronous compaction
    // (Option A), so DataFusion sees the just-written tail through Parquet.
    let res = alice
        .simple_query("SELECT id FROM events")
        .await
        .expect("select");
    let rows = rows_of(&res);
    assert_eq!(rows.len(), N, "want {N} rows, got {}", rows.len());

    let pass = inserts_per_sec >= 50.0;
    println!(
        "[VIABILITY shard_insert_path] {} inserts in {:?} = {:.1} inserts/sec (bar >= 50) {}",
        N,
        elapsed,
        inserts_per_sec,
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "shard_insert_path",
        "Shard-acked INSERT path",
        "Single-row INSERTs route through the WAL + shard owner and acks fast enough that point INSERT throughput is no longer Parquet-bound.",
        pass,
        PrimaryMetric {
            label: "inserts_per_sec".into(),
            value: inserts_per_sec,
            unit: "inserts/sec".into(),
            bar: BarOp::ge(50.0),
        },
        json!({
            "n_inserts": N,
            "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
            "ms_per_insert": elapsed.as_secs_f64() * 1000.0 / N as f64,
        }),
    );

    assert!(
        pass,
        "inserts_per_sec {inserts_per_sec:.1} < 50 (1k inserts in {elapsed:?})"
    );

    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();
}
