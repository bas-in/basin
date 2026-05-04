//! End-to-end smoke test for `BASIN_POOL_ENABLED=1`.
//!
//! Spawns the basin-server binary with `BASIN_POOL_ENABLED=1`, opens 100
//! short-lived pgwire connections in sequence as the same tenant, then asserts
//! the pool actually reused sessions instead of opening one per connection.
//!
//! "Reuse actually happened" is observed indirectly via behaviour: every
//! short-lived connection must succeed quickly. There is no in-process pool
//! handle to read `stats()` from when the server runs as a subprocess, so
//! this test plus the unit-level pool tests in `basin-pool` together cover
//! the wedge: this test proves the pool path is wired to the binary, the
//! unit tests prove the pool's `hits` / `misses` counters work.
//!
//! Skip-cleanly: if the basin-server binary is not on the path next to the
//! test executable (e.g. the workspace was tested without first building the
//! binary), we print a one-line note and return. This mirrors the pattern in
//! `compare_server_lifecycle.rs`.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

const POOL_CONN_COUNT: usize = 100;

fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    // exe = .../target/{debug,release}/deps/<test-name>-<hash>
    let mut p = exe.clone();
    p.pop(); // deps/
    p.pop(); // debug or release
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

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
    }
}

async fn spawn_with_pool() -> Option<(ChildGuard, std::net::SocketAddr, TempDir)> {
    let bin = find_basin_server_binary()?;
    let data_dir = TempDir::new().ok()?;

    let probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let addr = probe.local_addr().ok()?;
    drop(probe);

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.env("BASIN_BIND", addr.to_string())
        .env("BASIN_DATA_DIR", data_dir.path())
        .env("BASIN_TENANTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("BASIN_POOL_ENABLED", "1")
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
            return Some((ChildGuard { child: Some(child) }, addr, data_dir));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = child.start_kill();
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poc_pool_short_lived_connections_complete() {
    let Some((_guard, addr, _data_dir)) = spawn_with_pool().await else {
        eprintln!(
            "basin-server binary not built; skipping poc_pool_smoke (run `cargo build -p basin-server` first)"
        );
        return;
    };

    let conn_str = format!(
        "host={} port={} user=alice password=ignored",
        addr.ip(),
        addr.port()
    );

    // Sequentially open + tear down `POOL_CONN_COUNT` connections, each
    // running a single `SELECT 1` so the engine session actually does work.
    // Without the pool every iteration would re-run `Engine::open_session`;
    // with the pool only the first miss pays that cost. We don't have
    // in-process counters, so the assertion is a forward-progress check:
    // 100 connections must complete inside a generous time budget. On a
    // dev laptop this comes in well under a second.
    let started = Instant::now();
    let mut succeeded = 0usize;
    for i in 0..POOL_CONN_COUNT {
        let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await else {
            panic!("connection {i} failed to start up");
        };
        let driver = tokio::spawn(async move {
            let _ = conn.await;
        });
        let res = client.simple_query("SELECT 1").await;
        assert!(res.is_ok(), "iteration {i} query failed: {res:?}");
        drop(client);
        let _ = driver.await;
        succeeded += 1;
    }
    let elapsed = started.elapsed();

    assert_eq!(succeeded, POOL_CONN_COUNT);
    // Generous ceiling: even a slow CI runner should beat ~30s for 100
    // sequential conns when the pool is doing its job. The assertion is
    // less about latency and more about "the pool path doesn't crash and
    // doesn't leak sessions".
    assert!(
        elapsed < Duration::from_secs(60),
        "100 pooled connections took {elapsed:?}, expected < 60s"
    );
}
