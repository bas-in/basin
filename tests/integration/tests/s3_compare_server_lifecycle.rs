//! S3 port of `compare_server_lifecycle.rs`.
//!
//! The server-lifecycle metrics (accept latency, connection-scaling ceiling,
//! RSS per held-open connection) are wire-protocol / process-model metrics
//! that don't depend on the storage backend. We still wire the in-process
//! basin-server to a real-S3 `Storage` so the dashboard's "real S3" story
//! covers the full server stack, not just the storage layer.
//!
//! For the RSS subprocess metric: basin-server's CLI today only takes a
//! LocalFS data dir (S3 backend in the binary is gated to v0.2). The
//! subprocess therefore still uses LocalFS for that one metric — which is
//! honest, because the metric measures process/socket overhead per held-
//! open connection, not storage I/O.
//!
//! Skips cleanly when `[s3]` or `[postgres]` are missing.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::TenantId;
use basin_integration_tests::benchmark::{
    report_real_postgres_compare, CompareMetric, WhichWins,
};
use basin_integration_tests::test_config::{BasinTestConfig, CleanupOnDrop};
use basin_router::{ServerConfig, StaticTenantResolver};
use basin_storage::{Storage, StorageConfig};
use object_store::path::Path as ObjectPath;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls};

const TEST_NAME: &str = "s3_compare_server_lifecycle";
const ACCEPT_ITERS: usize = 50;
const MAX_CONNS: usize = 1000;
const RSS_CONNS: usize = 100;

fn median(samples: &[f64]) -> f64 {
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn which_wins_lower(basin: f64, postgres: f64) -> WhichWins {
    if basin < postgres {
        WhichWins::Basin
    } else if basin > postgres {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

fn which_wins_higher(basin: f64, postgres: f64) -> WhichWins {
    if basin > postgres {
        WhichWins::Basin
    } else if basin < postgres {
        WhichWins::Postgres
    } else {
        WhichWins::Tie
    }
}

async fn try_pg_connect(
    pg_cfg: &basin_integration_tests::test_config::PostgresConfig,
) -> Option<(Client, String)> {
    let conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        pg_cfg.host, pg_cfg.port, pg_cfg.user, pg_cfg.password, pg_cfg.dbname
    );
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            Some((client, conn_str))
        }
        Err(_) => None,
    }
}

fn rss_kib_one(pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    let Ok(out) = out else { return 0 };
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u64>().unwrap_or(0)
}

fn rss_kib_tree(root_pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "pid=,ppid=,rss=", "-ax"])
        .output();
    let Ok(out) = out else { return 0 };
    let s = String::from_utf8_lossy(&out.stdout);
    let mut total: u64 = 0;
    for line in s.lines() {
        let mut it = line.split_whitespace();
        let Some(pid) = it.next().and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = it.next().and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };
        let Some(rss) = it.next().and_then(|v| v.parse::<u64>().ok()) else {
            continue;
        };
        if pid == root_pid || ppid == root_pid {
            total += rss;
        }
    }
    total
}

fn find_postmaster_pid() -> Option<u32> {
    let out = std::process::Command::new("ps")
        .args(["-o", "pid=,ppid=,command=", "-ax"])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    for line in s.lines() {
        let mut it = line.split_whitespace();
        let Some(pid) = it.next().and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };
        let Some(ppid) = it.next().and_then(|v| v.parse::<u32>().ok()) else {
            continue;
        };
        let cmd: String = it.collect::<Vec<_>>().join(" ");
        if ppid == 1 && cmd.contains("postgres") && !cmd.contains(": ") {
            return Some(pid);
        }
    }
    None
}

fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let mut p = exe.clone();
    p.pop();
    p.pop();
    let candidate = p.join("basin-server");
    if candidate.exists() {
        return Some(candidate);
    }
    let parent = p.parent()?;
    for prof in ["debug", "release"] {
        let c = parent.join(prof).join("basin-server");
        if c.exists() {
            return Some(c);
        }
    }
    None
}

struct ChildGuard {
    child: Option<tokio::process::Child>,
}

impl ChildGuard {
    fn pid(&self) -> Option<u32> {
        self.child.as_ref().and_then(|c| c.id())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
    }
}

async fn spawn_basin_server() -> Option<(ChildGuard, std::net::SocketAddr, TempDir, TempDir)> {
    let bin = find_basin_server_binary()?;
    let data_dir = TempDir::new().ok()?;
    let wal_dir = TempDir::new().ok()?;

    let probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let addr = probe.local_addr().ok()?;
    drop(probe);

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.env("BASIN_BIND", addr.to_string())
        .env("BASIN_DATA_DIR", data_dir.path())
        .env("BASIN_WAL_DIR", wal_dir.path())
        .env("BASIN_TENANTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().ok()?;
    let stderr = child.stderr.take().expect("piped stderr");
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(_)) = reader.next_line().await {}
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).await.is_ok() {
            return Some((ChildGuard { child: Some(child) }, addr, data_dir, wal_dir));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.start_kill();
    None
}

async fn measure_accept_latency(conn_str: &str) -> Vec<f64> {
    let mut samples = Vec::with_capacity(ACCEPT_ITERS);
    for _ in 0..ACCEPT_ITERS {
        let started = Instant::now();
        let Ok((client, conn)) = tokio_postgres::connect(conn_str, NoTls).await else {
            continue;
        };
        let join = tokio::spawn(async move {
            let _ = conn.await;
        });
        let _ = client.simple_query("SELECT 1").await;
        drop(client);
        let _ = join.await;
        samples.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    samples
}

async fn measure_conn_ceiling(
    conn_str: String,
    target: usize,
) -> (usize, Vec<tokio::sync::oneshot::Sender<()>>) {
    let success = Arc::new(AtomicUsize::new(0));
    let mut holders = Vec::with_capacity(target);
    let mut joins = Vec::with_capacity(target);

    for _ in 0..target {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        holders.push(tx);
        let success = success.clone();
        let conn_str = conn_str.clone();
        joins.push(tokio::spawn(async move {
            match tokio_postgres::connect(&conn_str, NoTls).await {
                Ok((client, conn)) => {
                    success.fetch_add(1, Ordering::Relaxed);
                    let driver = tokio::spawn(async move {
                        let _ = conn.await;
                    });
                    let _ = rx.await;
                    drop(client);
                    driver.abort();
                }
                Err(_) => {}
            }
        }));
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    let count = success.load(Ordering::Relaxed);

    drop(holders);
    for j in joins {
        let _ = j.await;
    }
    (count, Vec::new())
}

struct HeldConns {
    _clients: Vec<Client>,
    drivers: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for HeldConns {
    fn drop(&mut self) {
        for d in self.drivers.drain(..) {
            d.abort();
        }
    }
}

async fn open_idle(conn_str: &str, n: usize) -> HeldConns {
    let mut clients = Vec::with_capacity(n);
    let mut drivers = Vec::with_capacity(n);
    for _ in 0..n {
        if let Ok((client, conn)) = tokio_postgres::connect(conn_str, NoTls).await {
            drivers.push(tokio::spawn(async move {
                let _ = conn.await;
            }));
            clients.push(client);
        }
    }
    HeldConns {
        _clients: clients,
        drivers,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn s3_compare_server_lifecycle() {
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
            report_real_postgres_compare(
                "server_lifecycle",
                "Server lifecycle: accept, conn-scaling, RSS/conn (real S3)",
                "Basin's tokio-task model handles dramatically more connections at lower memory than PG's process-per-connection.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };

    let (pg, pg_conn_str) = match try_pg_connect(&pg_cfg).await {
        Some(v) => v,
        None => {
            println!("[S3 server_lifecycle] postgres unavailable: skipping");
            report_real_postgres_compare(
                "server_lifecycle",
                "Server lifecycle: accept, conn-scaling, RSS/conn (real S3)",
                "Basin's tokio-task model handles dramatically more connections at lower memory than PG's process-per-connection.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };
    drop(pg);

    // ---- In-process Basin server backed by real S3 ---------------------
    let object_store = s3_cfg
        .build_object_store()
        .unwrap_or_else(|e| panic!("build object store: {e}"));
    let run_prefix = s3_cfg.run_prefix(TEST_NAME);
    let _cleanup = CleanupOnDrop {
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
    .expect("basin server failed to bind");
    let basin_addr = running.local_addr;
    let basin_conn_str = format!(
        "host=127.0.0.1 port={} user=alice password=ignored",
        basin_addr.port()
    );

    // ---- Metric A: accept latency ------------------------------------
    let basin_accept = measure_accept_latency(&basin_conn_str).await;
    let pg_accept = measure_accept_latency(&pg_conn_str).await;
    assert!(!basin_accept.is_empty(), "basin accept samples empty");
    assert!(!pg_accept.is_empty(), "pg accept samples empty");
    let basin_accept_p50 = median(&basin_accept);
    let pg_accept_p50 = median(&pg_accept);

    // ---- Metric B: connection-scaling ceiling ------------------------
    let (basin_conns, _) = measure_conn_ceiling(basin_conn_str.clone(), MAX_CONNS).await;
    let (pg_conns, _) = measure_conn_ceiling(pg_conn_str.clone(), MAX_CONNS).await;
    let basin_refused = MAX_CONNS.saturating_sub(basin_conns) as f64;
    let pg_refused = MAX_CONNS.saturating_sub(pg_conns) as f64;

    // ---- Metric C: RSS per held-open connection ----------------------
    // basin-server's binary today only supports LocalFS storage backend;
    // S3 wiring is v0.2. This metric is process/socket overhead per held
    // connection and doesn't depend on the storage backend. We accept
    // the LocalFS subprocess for this one number.
    let basin_kib_per_conn: Option<f64> = match spawn_basin_server().await {
        Some((child_guard, child_addr, _data, _wal)) => {
            let child_pid = child_guard.pid().expect("child PID");
            let child_conn_str = format!(
                "host=127.0.0.1 port={} user=alice password=ignored",
                child_addr.port()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
            let before = rss_kib_one(child_pid);
            let held = open_idle(&child_conn_str, RSS_CONNS).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = rss_kib_one(child_pid);
            let delta = after.saturating_sub(before);
            drop(held);
            drop(child_guard);
            Some(delta as f64 / RSS_CONNS as f64)
        }
        None => {
            println!("[S3 server_lifecycle] basin-server binary not found; skipping RSS metric");
            None
        }
    };

    let pg_kib_per_conn: Option<f64> = match find_postmaster_pid() {
        Some(pm) => {
            let before = rss_kib_tree(pm);
            let held = open_idle(&pg_conn_str, RSS_CONNS).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = rss_kib_tree(pm);
            drop(held);
            let delta = after.saturating_sub(before);
            Some(delta as f64 / RSS_CONNS as f64)
        }
        None => {
            println!("[S3 server_lifecycle] postmaster PID not found; skipping RSS metric");
            None
        }
    };

    let accept_ratio = pg_accept_p50 / basin_accept_p50.max(1e-9);
    let conn_ratio = (basin_conns as f64) / (pg_conns.max(1) as f64);

    println!(
        "{:>34} {:>16} {:>16} {:>22}",
        "metric", "basin", "postgres", "ratio"
    );
    println!(
        "{:>34} {:>14.3}ms {:>14.3}ms {:>22}",
        "accept_latency_p50",
        basin_accept_p50,
        pg_accept_p50,
        format!("pg/basin = {:.2}x", accept_ratio)
    );
    println!(
        "{:>34} {:>16} {:>16} {:>22}",
        "connections_held_of_1000",
        basin_conns,
        pg_conns,
        format!("basin/pg = {:.2}x", conn_ratio)
    );
    println!(
        "{:>34} {:>16} {:>16} {:>22}",
        "refused_under_1000_conn_flood", basin_refused as u64, pg_refused as u64, "-"
    );

    let mut metrics = vec![
        CompareMetric {
            label: "Connection accept latency p50".into(),
            basin: basin_accept_p50,
            postgres: pg_accept_p50,
            unit: "ms".into(),
            better: which_wins_lower(basin_accept_p50, pg_accept_p50),
            ratio_text: Some(format!("pg / basin = {:.2}x", accept_ratio)),
        },
        CompareMetric {
            label: "Connections held under 1000-conn flood".into(),
            basin: basin_conns as f64,
            postgres: pg_conns as f64,
            unit: "conns".into(),
            better: which_wins_higher(basin_conns as f64, pg_conns as f64),
            ratio_text: Some(format!("basin / pg = {:.2}x", conn_ratio)),
        },
        CompareMetric {
            label: "Refused conns under 1000-conn flood".into(),
            basin: basin_refused,
            postgres: pg_refused,
            unit: "conns".into(),
            better: which_wins_lower(basin_refused, pg_refused),
            ratio_text: None,
        },
    ];

    let rss_ratio_text = match (basin_kib_per_conn, pg_kib_per_conn) {
        (Some(basin), Some(pg)) => {
            let ratio = pg / basin.max(1e-9);
            println!(
                "{:>34} {:>14.1}KiB {:>14.1}KiB {:>22}",
                "rss_per_held_conn",
                basin,
                pg,
                format!("pg/basin = {:.2}x", ratio)
            );
            metrics.push(CompareMetric {
                label: "RSS per held-open connection".into(),
                basin,
                postgres: pg,
                unit: "KiB".into(),
                better: which_wins_lower(basin, pg),
                ratio_text: Some(format!("pg / basin = {:.2}x", ratio)),
            });
            format!("{:.2}x", ratio)
        }
        _ => "n/a".to_string(),
    };

    println!(
        "[S3 server_lifecycle] basin/pg accept ratio={:.2}x, conn-scaling ratio={:.2}x, RSS-per-conn ratio={}",
        accept_ratio, conn_ratio, rss_ratio_text
    );

    report_real_postgres_compare(
        "server_lifecycle",
        "Server lifecycle: accept, conn-scaling, RSS/conn (real S3)",
        "Basin's tokio-task model handles dramatically more connections at lower memory than PG's process-per-connection. Basin in-process server is wired to real S3; the subprocess RSS sample uses LocalFS because basin-server's S3 backend is v0.2.",
        true,
        metrics,
        None,
    );

    drop(running.shutdown);
    let _ = running.join.await;
}
