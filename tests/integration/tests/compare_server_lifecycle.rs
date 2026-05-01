//! Compare 2: server lifecycle — accept latency, connection-scaling ceiling,
//! and RSS per held-open connection. Postgres on 127.0.0.1:5432 vs an
//! in-process Basin server bound to 127.0.0.1:0.
//!
//! Three metrics, all measured serially so each one stands alone:
//!   A. Connection-accept latency (median of 50 single-conn round-trips).
//!   B. Connection-scaling ceiling (open MAX_CONNS=1000 concurrently, count
//!      successful startups; PG's max_connections=100 default is what we're
//!      surfacing).
//!   C. RSS per held-open connection (open 100 idle conns, divide RSS delta).
//!      Basin runs as a `tokio::process::Command` subprocess so we can
//!      measure its PID independently. Postgres RSS is the sum across the
//!      whole postmaster tree (`ps -o rss= --ppid` + the postmaster line).
//!
//! Skip-rather-than-fail: if PG is unreachable the test emits a `compare`
//! report with `available=false` and returns Ok.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_common::TenantId;
use basin_integration_tests::benchmark::{report_postgres_compare, CompareMetric, WhichWins};
use basin_router::{ServerConfig, StaticTenantResolver};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::{Client, NoTls};

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

async fn try_pg_connect() -> Option<(Client, String)> {
    for user in ["pc", "postgres"] {
        let conn_str = format!("host=127.0.0.1 port=5432 user={user} dbname=postgres");
        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                return Some((client, conn_str));
            }
            Err(_) => continue,
        }
    }
    None
}

/// Read RSS in KiB for one PID via `ps -o rss=`. Returns 0 if the process
/// is gone (the caller treats that as "process exited").
fn rss_kib_one(pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output();
    let Ok(out) = out else { return 0 };
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u64>().unwrap_or(0)
}

/// Sum RSS across a postmaster + every direct child. macOS `ps` doesn't
/// support `--ppid`, but `-o pid=,ppid=,rss= -ax` works on both macOS and
/// Linux and lets us filter in-process.
fn rss_kib_tree(root_pid: u32) -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "pid=,ppid=,rss=", "-ax"])
        .output();
    let Ok(out) = out else { return 0 };
    let s = String::from_utf8_lossy(&out.stdout);
    let mut total: u64 = 0;
    for line in s.lines() {
        let mut it = line.split_whitespace();
        let Some(pid) = it.next().and_then(|v| v.parse::<u32>().ok()) else { continue };
        let Some(ppid) = it.next().and_then(|v| v.parse::<u32>().ok()) else { continue };
        let Some(rss) = it.next().and_then(|v| v.parse::<u64>().ok()) else { continue };
        if pid == root_pid || ppid == root_pid {
            total += rss;
        }
    }
    total
}

/// Locate the postmaster PID (the parent of every PG backend) by walking
/// `ps` output for processes whose ppid is 1 and whose command contains
/// `postgres`. macOS's launchd is PID 1 and that's where Homebrew's
/// `postgres@18` ends up; on Linux it's systemd which is also PID 1. If
/// nothing matches, returns None and the test will skip the RSS metric.
fn find_postmaster_pid() -> Option<u32> {
    let out = std::process::Command::new("ps")
        .args(["-o", "pid=,ppid=,command=", "-ax"])
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    for line in s.lines() {
        // `ps` right-pads pid columns with spaces. split_whitespace eats
        // leading/repeated spaces; we manually re-join the tail to keep
        // any spaces inside the command line.
        let mut it = line.split_whitespace();
        let Some(pid) = it.next().and_then(|v| v.parse::<u32>().ok()) else { continue };
        let Some(ppid) = it.next().and_then(|v| v.parse::<u32>().ok()) else { continue };
        let cmd: String = it.collect::<Vec<_>>().join(" ");
        // The postmaster's command line is the absolute path to the
        // `postgres` binary, possibly with `-D <datadir>`. Backends look
        // like `postgres: <state>` and have ppid != 1, so the ppid==1
        // filter is enough to discriminate.
        if ppid == 1 && cmd.contains("postgres") && !cmd.contains(": ") {
            return Some(pid);
        }
    }
    None
}

/// Walk up from the test executable to find `target/{debug,release}` and
/// return the path to `basin-server`. We never want to invoke `cargo build`
/// from inside a test — too slow and too error-prone on shared CI.
fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    // exe = .../target/{debug,release}/deps/compare_server_lifecycle-<hash>
    let mut p = exe.clone();
    p.pop(); // deps/
    p.pop(); // debug or release
    let candidate = p.join("basin-server");
    if candidate.exists() {
        return Some(candidate);
    }
    // Fallback: try the sibling profile.
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
            // start_kill is non-blocking; the OS will reap the zombie when
            // the test runtime tears down.
            let _ = c.start_kill();
        }
    }
}

/// Spawn `basin-server` as a subprocess and wait until its TCP port accepts
/// connections. Picks an ephemeral port via TcpListener, releases it, then
/// passes it to the child — small race window, but the alternative is
/// parsing the child's stderr for the actual `local_addr`, which the
/// current binary doesn't print.
async fn spawn_basin_server() -> Option<(ChildGuard, std::net::SocketAddr, TempDir, TempDir)> {
    let bin = find_basin_server_binary()?;
    let data_dir = TempDir::new().ok()?;
    let wal_dir = TempDir::new().ok()?;

    // Reserve an ephemeral port, then drop the listener so the child can
    // bind. There is a tiny race here; mitigated by the immediate handoff.
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
    // Drain stderr in the background so the child doesn't block on a full
    // pipe buffer if it ever gets chatty.
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(_)) = reader.next_line().await {}
    });

    // Poll until the child accepts a TCP connection (max ~5s).
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).await.is_ok() {
            return Some((ChildGuard { child: Some(child) }, addr, data_dir, wal_dir));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Couldn't bind in time. Tear the child down.
    let _ = child.start_kill();
    None
}

/// Median end-to-end latency of: TCP connect + startup + `SELECT 1` + close.
/// Run serially — parallelism would measure something different.
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

/// Open `target` connections concurrently, hold them, and return the count
/// that completed startup. Each task parks on a oneshot the caller never
/// sends; dropping the returned `_holders` releases everything.
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
                    // Park until cancellation, keeping `client` alive so
                    // the connection is genuinely held.
                    let _ = rx.await;
                    drop(client);
                    driver.abort();
                }
                Err(_) => {
                    // Even an errored startup will be discarded.
                }
            }
        }));
    }

    // Give the cluster a moment to plateau, then read the count.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let count = success.load(Ordering::Relaxed);

    // Drop the senders; each parked task wakes and exits.
    drop(holders);
    for j in joins {
        let _ = j.await;
    }
    (count, Vec::new())
}

/// Open `n` idle connections, hold them, return their handles + driver
/// joins so the caller can drop them after measuring RSS.
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
async fn compare_server_lifecycle() {
    let (pg, pg_conn_str) = match try_pg_connect().await {
        Some(v) => v,
        None => {
            println!("[COMPARE server_lifecycle] postgres unavailable: skipping");
            report_postgres_compare(
                "server_lifecycle",
                "Server lifecycle: accept, conn-scaling, RSS/conn",
                "Basin's tokio-task model handles dramatically more connections at lower memory than PG's process-per-connection.",
                false,
                vec![],
                Some("postgres unavailable"),
            );
            return;
        }
    };
    // Hold the warm-up handle just long enough to know PG works.
    drop(pg);

    // ---- Bring up an in-process Basin server for metrics A and B --------
    let dir = TempDir::new().unwrap();
    let _wal_dir = TempDir::new().unwrap();
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
    let mut map = HashMap::new();
    map.insert("alice".to_owned(), TenantId::new());
    let resolver = Arc::new(StaticTenantResolver::new(map));
    let running = basin_router::run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        tenant_resolver: resolver,
    })
    .await
    .expect("basin server failed to bind");
    let basin_addr = running.local_addr;
    let basin_conn_str = format!(
        "host=127.0.0.1 port={} user=alice password=ignored",
        basin_addr.port()
    );

    // ---- Metric A: accept latency ---------------------------------------
    let basin_accept = measure_accept_latency(&basin_conn_str).await;
    let pg_accept = measure_accept_latency(&pg_conn_str).await;
    assert!(!basin_accept.is_empty(), "basin accept samples empty");
    assert!(!pg_accept.is_empty(), "pg accept samples empty");
    let basin_accept_p50 = median(&basin_accept);
    let pg_accept_p50 = median(&pg_accept);

    // ---- Metric B: connection-scaling ceiling ---------------------------
    let (basin_conns, _) = measure_conn_ceiling(basin_conn_str.clone(), MAX_CONNS).await;
    let (pg_conns, _) = measure_conn_ceiling(pg_conn_str.clone(), MAX_CONNS).await;
    let basin_refused = MAX_CONNS.saturating_sub(basin_conns) as f64;
    let pg_refused = MAX_CONNS.saturating_sub(pg_conns) as f64;

    // ---- Metric C: RSS per held-open connection -------------------------
    // Spin up basin-server as a subprocess so we have an isolated PID. PG's
    // postmaster PID is discovered via `ps`; the tree-RSS sum captures
    // every backend the test opens.
    let basin_kib_per_conn: Option<f64> = match spawn_basin_server().await {
        Some((child_guard, child_addr, _data, _wal)) => {
            let child_pid = child_guard.pid().expect("child PID");
            let child_conn_str = format!(
                "host=127.0.0.1 port={} user=alice password=ignored",
                child_addr.port()
            );
            // Let the child stabilise before sampling.
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
            println!("[COMPARE server_lifecycle] basin-server binary not found; skipping RSS metric");
            None
        }
    };

    let pg_kib_per_conn: Option<f64> = match find_postmaster_pid() {
        Some(pm) => {
            let before = rss_kib_tree(pm);
            let held = open_idle(&pg_conn_str, RSS_CONNS).await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let after = rss_kib_tree(pm);
            // Drop the conns AFTER reading; PG's per-backend RSS is the
            // very thing we're measuring.
            drop(held);
            let delta = after.saturating_sub(before);
            Some(delta as f64 / RSS_CONNS as f64)
        }
        None => {
            println!("[COMPARE server_lifecycle] could not find postmaster PID; skipping RSS metric");
            None
        }
    };

    // ---- Print + report --------------------------------------------------
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
        "[COMPARE server_lifecycle] basin/pg accept ratio={:.2}x, conn-scaling ratio={:.2}x, RSS-per-conn ratio={}",
        accept_ratio, conn_ratio, rss_ratio_text
    );

    report_postgres_compare(
        "server_lifecycle",
        "Server lifecycle: accept, conn-scaling, RSS/conn",
        "Basin's tokio-task model handles dramatically more connections at lower memory than PG's process-per-connection.",
        true,
        metrics,
        None,
    );

    drop(running.shutdown);
    let _ = running.join.await;
}
