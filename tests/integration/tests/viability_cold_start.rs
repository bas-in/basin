//! Viability test: cold-start latency.
//!
//! Card: `viability_cold_start`
//! Bar: from process spawn → first `SELECT 1` returning Ok over pgwire is
//! < 2000 ms with LocalFS storage.
//!
//! This is the load-bearing "Basin starts fast" claim. A bucket-native DB
//! that takes minutes to come up isn't a serverless wedge; it's a regular
//! database with a fancier storage layer. The test:
//!
//! 1. Reserves an ephemeral TCP port (and releases it for the child to bind).
//! 2. Records `Instant::now()` as the spawn instant.
//! 3. Spawns `basin-server` with LocalFS storage rooted in a tempdir.
//! 4. Polls TCP connect on the bound port until it succeeds OR a 5s deadline.
//! 5. On first connect, opens a `tokio_postgres` client and runs `SELECT 1`.
//! 6. The total elapsed from the spawn instant to `SELECT 1` returning Ok
//!    is `cold_start_ms`.
//!
//! Note: the test depends on the `basin-server` binary already being built
//! (`cargo build -p basin-server` or via a prior `cargo test --tests`). If
//! the binary is missing, the test prints a `[skip]` line, emits a viability
//! report with `passed=false, available=false`-equivalent (we just put 0.0
//! in the metric and a clear note in details), and returns Ok. This avoids
//! kicking off a multi-minute build inside a benchmark.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;

/// LocalFS bar. The S3 variant has its own (looser) bar.
const BAR_COLD_START_MS: f64 = 2_000.0;

/// Walk up from the test executable to find `target/{debug,release}` and
/// return the path to `basin-server`. Mirrors the helper in
/// `compare_server_lifecycle.rs`.
fn find_basin_server_binary() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
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

/// Cold-start measurement.
///
/// Returns `(cold_start_ms, port)` on success, or `None` on a hard failure
/// (binary missing, child failed to bind in time). Caller takes the child
/// process by ownership and is responsible for tearing it down.
async fn measure_cold_start_local(
    bin: &std::path::Path,
    data_dir: &std::path::Path,
    wal_dir: &std::path::Path,
) -> Option<(f64, std::net::SocketAddr, tokio::process::Child)> {
    // Reserve an ephemeral port, then drop the listener so the child can
    // bind. There is a tiny race here; mitigated by the immediate handoff.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let addr = probe.local_addr().ok()?;
    drop(probe);

    let mut cmd = tokio::process::Command::new(bin);
    cmd.env("BASIN_BIND", addr.to_string())
        .env("BASIN_DATA_DIR", data_dir)
        .env("BASIN_WAL_DIR", wal_dir)
        .env("BASIN_PROJECTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    // Spawn — clock starts NOW.
    let started = Instant::now();
    let mut child = cmd.spawn().ok()?;
    let stderr = child.stderr.take().expect("piped stderr");
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(_)) = reader.next_line().await {}
    });

    // Poll TCP connect until success or 10s deadline. The deadline is
    // generous so a slow CI host never falsely "fails to start"; the
    // bar (2s for LocalFS / 5s for S3) is checked separately on the
    // returned ms value.
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(addr).await.is_ok() {
            // Connect succeeded — but the pgwire startup handshake is what
            // we actually care about. Run `SELECT 1` to confirm the server
            // is fully responsive, not just listening on the socket.
            let conn_str = format!(
                "host=127.0.0.1 port={} user=alice password=ignored",
                addr.port()
            );
            if let Ok((client, conn)) = tokio_postgres::connect(&conn_str, NoTls).await {
                let driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                if client.simple_query("SELECT 1").await.is_ok() {
                    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                    drop(client);
                    driver.abort();
                    return Some((elapsed_ms, addr, child));
                }
                drop(client);
                driver.abort();
            }
            // Connect succeeded but pgwire didn't — keep polling. The
            // server may still be wiring up its project resolver.
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = child.start_kill();
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_cold_start() {
    let bin = match find_basin_server_binary() {
        Some(b) => b,
        None => {
            println!(
                "[VIABILITY cold_start] basin-server binary not found; \
                 build with `cargo build --release -p basin-server` and re-run"
            );
            report_viability(
                "cold_start",
                "Cold-start latency (LocalFS)",
                "From process spawn to first `SELECT 1` over pgwire is < 2 s on LocalFS storage.",
                false,
                PrimaryMetric {
                    label: "cold_start_ms".into(),
                    value: 0.0,
                    unit: "ms".into(),
                    bar: BarOp::lt(BAR_COLD_START_MS),
                },
                json!({
                    "skip_reason": "basin-server binary not found",
                }),
            );
            return;
        }
    };

    let data_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let (cold_start_ms, addr, child) =
        match measure_cold_start_local(&bin, data_dir.path(), wal_dir.path()).await {
            Some(v) => v,
            None => {
                panic!("[VIABILITY cold_start] basin-server failed to come up within 10s")
            }
        };

    let pass = cold_start_ms < BAR_COLD_START_MS;
    println!(
        "[VIABILITY cold_start] cold_start_ms={:.1} (bar <{}ms) {} addr={addr}",
        cold_start_ms,
        BAR_COLD_START_MS,
        if pass { "PASS" } else { "FAIL" }
    );

    report_viability(
        "cold_start",
        "Cold-start latency (LocalFS)",
        "From process spawn to first `SELECT 1` over pgwire is < 2 s on LocalFS storage.",
        pass,
        PrimaryMetric {
            label: "cold_start_ms".into(),
            value: cold_start_ms,
            unit: "ms".into(),
            bar: BarOp::lt(BAR_COLD_START_MS),
        },
        json!({
            "cold_start_ms": cold_start_ms,
            "bind_addr": addr.to_string(),
            "storage": "localfs",
        }),
    );

    // Tear down: kill_on_drop is set, but be explicit.
    drop(child);
    drop(data_dir);
    drop(wal_dir);

    assert!(
        pass,
        "cold_start_ms={cold_start_ms:.1} >= bar {BAR_COLD_START_MS}ms"
    );
}
