//! S3 port of `insert_through_shard.rs` (shard_insert_path viability).
//!
//! Storage on S3, so background Parquet flushes go to S3. WAL stays in RAM
//! (we use an in-memory object store for the WAL — the WAL's whole point is
//! sub-millisecond ack latency, which a real S3 WAL would defeat).
//!
//! Bar: 1000 single-row INSERTs >= 50 inserts/sec (same as LocalFS — the WAL
//! ack absorbs S3 latency, so the user-visible INSERT throughput should hold).

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_real_viability, BarOp, PrimaryMetric};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use serde_json::json;
use tokio_postgres::{NoTls, SimpleQueryMessage};

const TEST_NAME: &str = "s3_shard_insert_path";

struct TestServer {
    addr: SocketAddr,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _join: tokio::task::JoinHandle<basin_common::Result<()>>,
    bg: Option<basin_shard::ShardBackgroundHandle>,
    wal: basin_wal::Wal,
}

async fn start_server_with_shard(
    storage_object_store: Arc<dyn object_store::ObjectStore>,
    storage_root_prefix: ObjectPath,
) -> TestServer {
    basin_common::telemetry::try_init_for_tests();

    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: storage_object_store,
        root_prefix: Some(storage_root_prefix),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> =
        Arc::new(basin_catalog::InMemoryCatalog::new());

    // WAL is in-memory — sub-ms ack latency is the WAL's job; an S3-backed
    // WAL would crater INSERT throughput in this single-shard PoC.
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let wal = basin_wal::Wal::open(basin_wal::WalConfig {
        object_store: wal_store,
        root_prefix: None,
        flush_interval: Duration::from_millis(200),
        flush_max_bytes: 1024 * 1024,
    })
    .await
    .expect("open WAL");

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
    map.insert("alice".to_owned(), TenantId::new());
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn s3_shard_insert_path() {
    let cfg = match BasinTestConfig::load() {
        Ok(c) => c,
        Err(e) => panic!("parse .basin-test.toml: {e}"),
    };
    let s3_cfg = match cfg.s3_or_skip(TEST_NAME) {
        Some(c) => c.clone(),
        None => return,
    };

    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
        store: object_store.clone(),
        prefix: run_prefix.clone(),
    };

    let mut server = start_server_with_shard(
        object_store,
        ObjectPath::from(run_prefix.as_str()),
    )
    .await;
    let alice = connect(server.addr, "alice").await;

    alice
        .simple_query(
            "CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL)",
        )
        .await
        .expect("create");

    const N: usize = 1_000;

    let started = Instant::now();
    for i in 0..N {
        let sql = format!("INSERT INTO events VALUES ({i}, 'row-{i}')");
        alice
            .simple_query(&sql)
            .await
            .unwrap_or_else(|e| panic!("insert {i}: {e}"));
    }
    let elapsed = started.elapsed();

    let inserts_per_sec = N as f64 / elapsed.as_secs_f64();

    let res = alice
        .simple_query("SELECT id FROM events")
        .await
        .expect("select");
    let rows = rows_of(&res);
    assert_eq!(rows.len(), N, "want {N} rows, got {}", rows.len());

    let pass = inserts_per_sec >= 50.0;
    println!(
        "[S3 shard_insert_path] {} inserts in {:?} = {:.1} inserts/sec (bar >= 50) {}",
        N,
        elapsed,
        inserts_per_sec,
        if pass { "PASS" } else { "FAIL" }
    );

    report_real_viability(
        "shard_insert_path",
        "Shard-acked INSERT path (real S3 storage)",
        "Single-row INSERTs route through the WAL + shard owner; WAL absorbs S3 latency so user-visible throughput stays >= 50 inserts/sec.",
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
            "endpoint": s3_cfg.endpoint.clone(),
            "bucket": s3_cfg.bucket,
        }),
    );

    if let Some(bg) = server.bg.take() {
        bg.shutdown().await;
    }
    server.wal.close().await.unwrap();

    assert!(
        pass,
        "inserts_per_sec {inserts_per_sec:.1} < 50 (1k inserts in {elapsed:?})"
    );
}
