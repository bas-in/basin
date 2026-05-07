//! Unit test: outbound request hits the configured timeout.
//!
//! We spin up a TCP listener that accepts the connection but never sends a
//! response. The client's `tokio::time::timeout` wrapper must trip before
//! reqwest's own infinite default would.

use std::time::Duration;

use basin_common::TenantId;
use basin_net::{AllowList, GuardConfig, HttpClient, RateLimit};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// Spawn a TCP server that accepts and stalls. Returns the bound address
/// so the test can point the client at it.
async fn spawn_stalling_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut sock, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            tokio::spawn(async move {
                // Read the request and then stall indefinitely. The 4 KiB
                // buffer is plenty for a typical GET line + headers.
                let mut buf = vec![0u8; 4096];
                let _ = sock.read(&mut buf).await;
                // Sleep beyond the test's outer timeout so the connection
                // is held open for the duration of the test.
                tokio::time::sleep(Duration::from_secs(60)).await;
                drop(sock);
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_fires_on_slow_server() {
    let addr = spawn_stalling_server().await;
    let cfg = GuardConfig {
        max_body_bytes: 1024,
        timeout: Duration::from_millis(150),
    };
    let client = HttpClient::with_config(cfg, AllowList::new(), RateLimit::new());
    let tenant = TenantId::new();
    client.allow_host(&tenant, "127.0.0.1").await;
    let url = format!("http://{addr}/slow");
    let started = std::time::Instant::now();
    let err = client.http_get(&tenant, &url).await.unwrap_err();
    let elapsed = started.elapsed();
    let msg = format!("{err}");
    assert!(msg.contains("timed out"), "got {msg}");
    // The wall-clock should be on the order of the configured timeout
    // (150 ms), with some slack for OS scheduling. Anything > 5s indicates
    // we accidentally waited on reqwest's built-in.
    assert!(
        elapsed < Duration::from_secs(5),
        "timeout took too long: {elapsed:?}"
    );
}
