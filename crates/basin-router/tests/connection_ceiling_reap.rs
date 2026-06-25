//! Per-project connection-ceiling slot is released on EVERY disconnect —
//! clean (pgwire `Terminate`) or unclean (client vanishes, no `Terminate`,
//! engine-side socket left silently open as if an L4 proxy were holding it).
//!
//! Regression test for the connection-ceiling leak: a session blocked forever
//! on a dead-but-open socket would pin the project's `ConnectionGuard`, so the
//! live count never returned to zero and the project eventually locked itself
//! out of all new connections. The idle watchdog in `handle_connection` reaps
//! such a session, dropping the guard and freeing the slot.
//!
//! The unclean case is simulated faithfully: a raw TCP socket completes the
//! pgwire startup + cleartext-password handshake (so the limiter has admitted
//! the connection and incremented the count) and is then *leaked* — never
//! closed, never sent `Terminate`, no FIN. TCP keepalive cannot help (there is
//! no real client to stop answering probes); only the application idle
//! watchdog can release the slot.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use basin_catalog::Catalog;
use basin_common::ProjectId;
use basin_router::{
    run_until_bound, CatalogConnectionLimitProvider, ConnectionLimiter, ServerConfig,
    StaticProjectResolver,
};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Boot a router with a per-project connection ceiling we can read back via
/// the returned `ConnectionLimiter`. `cap` is the project's `max_connections`.
async fn spawn_with_ceiling(
    cap: u32,
) -> (
    SocketAddr,
    ProjectId,
    Arc<ConnectionLimiter>,
    TempDir,
    basin_router::RunningServer,
) {
    basin_common::telemetry::try_init_for_tests();
    let dir = TempDir::new().unwrap();
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(basin_catalog::InMemoryCatalog::new());
    let engine = basin_engine::Engine::new(basin_engine::EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });

    let alice = ProjectId::new();
    catalog
        .set_project_max_connections(&alice, cap)
        .await
        .unwrap();

    let mut map = HashMap::new();
    map.insert("alice".to_owned(), alice);
    let resolver = Arc::new(StaticProjectResolver::new(map));

    let limiter = Arc::new(ConnectionLimiter::new(Arc::new(
        CatalogConnectionLimitProvider::new(catalog.clone()),
    )));

    let running = run_until_bound(ServerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        project_resolver: resolver,
        pool: None,
        shard_endpoints: None,
        tls: None,
        connection_limiter: Some(limiter.clone()),
    })
    .await
    .expect("server failed to bind");

    (running.local_addr, alice, limiter, dir, running)
}

/// Drive the pgwire startup + cleartext-password handshake on `sock` as `user`.
/// Returns once the server has sent `ReadyForQuery` ('Z'), i.e. the connection
/// is authenticated and counted by the limiter.
async fn pg_handshake(sock: &mut TcpStream, user: &str) {
    // StartupMessage: len(i32) | proto(196608) | "user\0<user>\0" | "\0"
    let mut params = Vec::new();
    params.extend_from_slice(b"user\0");
    params.extend_from_slice(user.as_bytes());
    params.push(0);
    params.push(0); // terminating empty key
    let len = (4 + 4 + params.len()) as i32;
    let mut startup = Vec::new();
    startup.extend_from_slice(&len.to_be_bytes());
    startup.extend_from_slice(&196608i32.to_be_bytes()); // protocol 3.0
    startup.extend_from_slice(&params);
    sock.write_all(&startup).await.expect("send startup");
    sock.flush().await.unwrap();

    // Expect Authentication ('R'). We don't strictly parse the auth subtype;
    // the server's flow is CleartextPassword, so reply with a PasswordMessage.
    let tag = read_message_tag(sock).await;
    assert_eq!(tag, b'R', "expected Authentication message, got {tag:?}");

    // PasswordMessage: 'p' | len(i32) | "ignored\0"
    let pw = b"ignored\0";
    let plen = (4 + pw.len()) as i32;
    let mut msg = vec![b'p'];
    msg.extend_from_slice(&plen.to_be_bytes());
    msg.extend_from_slice(pw);
    sock.write_all(&msg).await.expect("send password");
    sock.flush().await.unwrap();

    // Read messages until ReadyForQuery ('Z').
    loop {
        let tag = read_message_tag(sock).await;
        if tag == b'Z' {
            return;
        }
        if tag == b'E' {
            panic!("server sent ErrorResponse during handshake");
        }
    }
}

/// Read one backend message: 1-byte tag + i32 length (incl. self) + body.
/// Returns the tag; the body is consumed and discarded.
async fn read_message_tag(sock: &mut TcpStream) -> u8 {
    let mut tag = [0u8; 1];
    sock.read_exact(&mut tag).await.expect("read tag");
    let mut len = [0u8; 4];
    sock.read_exact(&mut len).await.expect("read len");
    let body_len = i32::from_be_bytes(len) as usize - 4;
    if body_len > 0 {
        let mut body = vec![0u8; body_len];
        sock.read_exact(&mut body).await.expect("read body");
    }
    tag[0]
}

/// Poll `f` until it returns true or `deadline` elapses.
async fn wait_until<F: Fn() -> bool>(deadline: Duration, f: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if f() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    f()
}

// NOTE: both scenarios live in ONE `#[tokio::test]` because the idle ceiling
// is read from a process-global env var at server-boot time. Running them as
// separate parallel tests would let one test's `set_var` race the other's
// server boot. Sequencing them in a single test keeps each hermetic.
#[tokio::test]
async fn ceiling_slot_released_on_clean_and_unclean_disconnect() {
    // ── Unclean disconnect ────────────────────────────────────────────────
    // Aggressive idle ceiling so the watchdog fires fast. The reaper checks
    // at ceiling/4, so detection is within ~ceiling + tick.
    std::env::set_var("BASIN_PGWIRE_IDLE_TIMEOUT_SECS", "2");
    let (addr, alice, limiter, _dir, running) = spawn_with_ceiling(1).await;

    assert_eq!(limiter.live_count(alice), 0, "no connections yet");

    let mut sock = TcpStream::connect(addr).await.expect("connect");
    pg_handshake(&mut sock, "alice").await;

    // Connection is authenticated → counted.
    assert!(
        wait_until(Duration::from_secs(2), || limiter.live_count(alice) == 1).await,
        "live count should reach 1 after auth, got {}",
        limiter.live_count(alice)
    );

    // Simulate the client vanishing behind a proxy that keeps the engine-side
    // socket open: leak the socket so no FIN/Terminate is ever sent.
    std::mem::forget(sock);

    // The idle watchdog (2s ceiling, ~0.5s tick) must reap it and free the slot.
    assert!(
        wait_until(Duration::from_secs(8), || limiter.live_count(alice) == 0).await,
        "leaked session not reaped: live count still {}",
        limiter.live_count(alice)
    );

    // No lockout: a brand-new connection on a cap-of-1 project succeeds.
    let mut sock2 = TcpStream::connect(addr).await.expect("reconnect");
    pg_handshake(&mut sock2, "alice").await;
    assert!(
        wait_until(Duration::from_secs(2), || limiter.live_count(alice) == 1).await,
        "fresh connect after reap should be admitted (slot was freed)"
    );
    drop(sock2);
    let _ = running.shutdown.send(());
    let _ = running.join.await;

    // ── Clean disconnect ──────────────────────────────────────────────────
    // Generous idle ceiling so the watchdog is NOT what frees the slot — the
    // clean Terminate path is. Proves the decrement fires exactly once.
    std::env::set_var("BASIN_PGWIRE_IDLE_TIMEOUT_SECS", "3600");
    let (addr, alice, limiter, _dir, running) = spawn_with_ceiling(2).await;

    let mut sock = TcpStream::connect(addr).await.expect("connect");
    pg_handshake(&mut sock, "alice").await;
    assert!(
        wait_until(Duration::from_secs(2), || limiter.live_count(alice) == 1).await,
        "live count should reach 1 after auth"
    );

    // Clean termination: send pgwire 'X' (Terminate) then close the socket.
    let term = [b'X', 0, 0, 0, 4];
    sock.write_all(&term).await.expect("send Terminate");
    sock.flush().await.unwrap();
    sock.shutdown().await.ok();
    drop(sock);

    // Decrement fires exactly once → back to 0 (not negative, not stuck at 1).
    assert!(
        wait_until(Duration::from_secs(5), || limiter.live_count(alice) == 0).await,
        "clean disconnect should decrement to 0, got {}",
        limiter.live_count(alice)
    );

    // And it never went negative / wrapped: a fresh pair of connections both
    // admit (cap 2) and the count is exactly 2.
    let mut a = TcpStream::connect(addr).await.expect("connect a");
    pg_handshake(&mut a, "alice").await;
    let mut b = TcpStream::connect(addr).await.expect("connect b");
    pg_handshake(&mut b, "alice").await;
    assert!(
        wait_until(Duration::from_secs(2), || limiter.live_count(alice) == 2).await,
        "two fresh connections should yield exactly 2, got {}",
        limiter.live_count(alice)
    );

    std::env::remove_var("BASIN_PGWIRE_IDLE_TIMEOUT_SECS");
    drop(a);
    drop(b);
    let _ = running.shutdown.send(());
    let _ = running.join.await;
}
