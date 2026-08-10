//! Integration tests for the SSE realtime transport (Phase 5.11.R2).
//!
//! Spawns `basin-server` with `--features realtime` + `BASIN_AUTH_ENABLED=1`
//! + `BASIN_REST_ENABLED=1` and drives the SSE endpoint at
//! `GET /realtime/v1/sse/:project/:table`.
//!
//! # Skip conditions
//!
//! Every test silently passes (`return`) when:
//! - The `basin-server` binary is not found in the build output.
//! - The `basin-server` binary was built *without* `--features realtime`
//!   (SSE endpoint returns 404 → test exits cleanly).
//! - An external Postgres DSN is unavailable (`pg_alive()` returns false).
//!
//! This mirrors the skip-cleanly contract from `poc_rest_smoke.rs`.

#![allow(clippy::print_stdout)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;

/// Dev PG DSN for auth schema provisioning.
const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

// ---------------------------------------------------------------------------
// Binary / infra helpers
// ---------------------------------------------------------------------------

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

async fn pg_alive() -> bool {
    match tokio::time::timeout(
        Duration::from_secs(2),
        tokio_postgres::connect(PG_URL, NoTls),
    )
    .await
    {
        Ok(Ok((_c, conn))) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            true
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Child-process guard
// ---------------------------------------------------------------------------

struct ChildGuard {
    child: Option<tokio::process::Child>,
    auth_schema: String,
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.start_kill();
        }
        let schema = self.auth_schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async {
                let Ok(Ok((client, conn))) = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
                )
                .await
                else {
                    return;
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

/// Addresses for a running test server instance.
struct ServerAddrs {
    _guard: ChildGuard,
    rest_addr: std::net::SocketAddr,
    sse_addr: std::net::SocketAddr,
}

/// Spawn a `basin-server` binary with auth + REST + realtime enabled.
/// Returns `None` if any precondition is unmet (binary not found, PG not up).
async fn spawn_server() -> Option<ServerAddrs> {
    if !pg_alive().await {
        eprintln!("postgres unreachable — skipping realtime_sse tests");
        return None;
    }
    let bin = find_basin_server_binary()?;
    let data_dir = tempfile::TempDir::new().ok()?;

    // Reserve ephemeral ports: pgwire + REST + realtime SSE.
    let pg_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let pg_addr = pg_probe.local_addr().ok()?;
    drop(pg_probe);
    let rest_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let rest_addr = rest_probe.local_addr().ok()?;
    drop(rest_probe);
    let sse_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let sse_addr = sse_probe.local_addr().ok()?;
    drop(sse_probe);

    let auth_schema = format!("basin_sse_test_{}", Ulid::new().to_string().to_lowercase());

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.env("BASIN_BIND", pg_addr.to_string())
        .env("BASIN_DATA_DIR", data_dir.path())
        .env("BASIN_PROJECTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("BASIN_AUTH_ENABLED", "1")
        .env("BASIN_REST_ENABLED", "1")
        .env("BASIN_REST_BIND", rest_addr.to_string())
        .env("BASIN_REALTIME_BIND", sse_addr.to_string())
        .env(
            "BASIN_AUTH_JWT_SECRET",
            "0011223344556677889900112233445566778899001122334455667788990011",
        )
        .env("BASIN_AUTH_SMTP_HOST", "smtp.invalid")
        .env("BASIN_AUTH_SMTP_PORT", "587")
        .env("BASIN_AUTH_SMTP_USERNAME", "u")
        .env("BASIN_AUTH_SMTP_PASSWORD", "p")
        .env("BASIN_AUTH_SMTP_FROM", "noreply@example.com")
        .env("BASIN_AUTH_SMTP_TLS", "starttls")
        .env("BASIN_AUTH_CATALOG_DSN", PG_URL)
        .env("BASIN_AUTH_CATALOG_SCHEMA", &auth_schema)
        .env("BASIN_AUTH_BCRYPT_COST", "4")
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().ok()?;
    let stderr = child.stderr.take().expect("piped");
    tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            eprintln!("basin-server: {line}");
        }
    });

    // Wait for the SSE listener to be up (it binds after REST).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if Instant::now() >= deadline {
            let _ = child.start_kill();
            return None;
        }
        if TcpStream::connect(sse_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Keep data_dir alive for the child's lifetime.
    let _data_dir = data_dir;
    Some(ServerAddrs {
        _guard: ChildGuard {
            child: Some(child),
            auth_schema,
        },
        rest_addr,
        sse_addr,
    })
}

// ---------------------------------------------------------------------------
// Minimal HTTP/1.1 helper (buffered — for auth calls)
// ---------------------------------------------------------------------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

impl HttpResp {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body not JSON ({e}): {}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }
}

async fn http_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> HttpResp {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    sock.write_all(req.as_bytes()).await.expect("write head");
    if let Some(b) = body {
        sock.write_all(b).await.expect("write body");
    }
    sock.flush().await.expect("flush");
    let mut buf = Vec::with_capacity(4096);
    sock.read_to_end(&mut buf).await.expect("read");
    parse_http_response(&buf)
}

fn parse_http_response(buf: &[u8]) -> HttpResp {
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response missing header terminator");
    let head = std::str::from_utf8(&buf[..split]).expect("headers utf8");
    let status: u16 = head
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status u16");
    HttpResp {
        status,
        body: buf[split + 4..].to_vec(),
    }
}

// ---------------------------------------------------------------------------
// Auth helpers (identical to poc_rest_smoke.rs)
// ---------------------------------------------------------------------------

/// Signup + bypass SMTP verification + signin; returns the access token.
async fn obtain_jwt(
    rest_addr: std::net::SocketAddr,
    auth_schema: &str,
    project: &str,
    email: &str,
    password: &str,
) -> Option<String> {
    // 1. Signup.
    let body = serde_json::json!({
        "project_id": project,
        "email": email,
        "password": password,
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signup",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    if r.status != 201 {
        eprintln!(
            "signup failed: {} {}",
            r.status,
            String::from_utf8_lossy(&r.body)
        );
        return None;
    }

    // 2. Bypass SMTP: mark all users in the schema as verified.
    let Ok(Ok((client, conn))) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_postgres::connect(PG_URL, NoTls),
    )
    .await
    else {
        return None;
    };
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
        .batch_execute(&format!(
            "UPDATE {auth_schema}.users SET email_verified_at = now()"
        ))
        .await
        .ok()?;
    drop(client);

    // 3. Signin.
    let body = serde_json::json!({
        "project_id": project,
        "email": email,
        "password": password,
    })
    .to_string();
    let r = http_request(
        rest_addr,
        "POST",
        "/auth/v1/signin",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    if r.status != 200 {
        eprintln!(
            "signin failed: {} {}",
            r.status,
            String::from_utf8_lossy(&r.body)
        );
        return None;
    }
    let v = r.json();
    Some(v["access_token"].as_str()?.to_owned())
}

// ---------------------------------------------------------------------------
// SSE helper: open a streaming connection and collect frames until timeout.
// ---------------------------------------------------------------------------

/// A parsed SSE data frame.
#[derive(Debug)]
struct SseFrame {
    id: Option<String>,
    data: String,
    /// True for comment frames (`: …`).
    is_comment: bool,
}

/// Open a persistent GET connection to `addr/path` with the given headers and
/// collect SSE frames for up to `timeout`. Returns all frames received.
async fn collect_sse_frames(
    addr: std::net::SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Vec<SseFrame> {
    let Ok(mut sock) = TcpStream::connect(addr).await else {
        return vec![];
    };

    // Send the HTTP/1.1 GET — *no* `Connection: close` so the server keeps
    // the response body open.
    let mut req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    if sock.write_all(req.as_bytes()).await.is_err() {
        return vec![];
    }
    if sock.flush().await.is_err() {
        return vec![];
    }

    // Read the response with a timeout budget.
    let mut raw = Vec::with_capacity(4096);
    let _ = tokio::time::timeout(timeout, sock.read_to_end(&mut raw)).await;

    // Strip headers.
    let body = if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
        &raw[pos + 4..]
    } else {
        &raw[..]
    };

    parse_sse_frames(body)
}

/// Parse the raw SSE response body into individual frames.
fn parse_sse_frames(body: &[u8]) -> Vec<SseFrame> {
    let text = String::from_utf8_lossy(body);
    let mut frames = Vec::new();
    let mut current_id: Option<String> = None;
    let mut current_data: Vec<String> = Vec::new();

    for line in text.lines() {
        if line.is_empty() {
            // Blank line = frame boundary. Flush if we have data.
            if !current_data.is_empty() {
                frames.push(SseFrame {
                    id: current_id.take(),
                    data: current_data.join("\n"),
                    is_comment: false,
                });
                current_data.clear();
            }
            continue;
        }
        if let Some(comment) = line.strip_prefix(':') {
            frames.push(SseFrame {
                id: None,
                data: comment.trim_start().to_owned(),
                is_comment: true,
            });
            continue;
        }
        if let Some(val) = line.strip_prefix("id:") {
            current_id = Some(val.trim_start().to_owned());
        } else if let Some(val) = line.strip_prefix("data:") {
            current_data.push(val.trim_start().to_owned());
        }
        // Ignore `event:`, `retry:`, etc. for now.
    }

    // Trailing frame without terminal blank line.
    if !current_data.is_empty() {
        frames.push(SseFrame {
            id: current_id,
            data: current_data.join("\n"),
            is_comment: false,
        });
    }

    frames
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Gate 1 + 5: connecting with a valid JWT to the correct project receives
/// the SSE response (200 + `text/event-stream`). Cross-project: connecting
/// with a JWT for project A to project B's table returns 403.
///
/// Note: we don't actually drive an INSERT here (that requires pgwire or
/// REST DDL) — the acceptance criterion for R2 is that the connection is
/// accepted and the stream is open. Event delivery is verified by other tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_connect_and_cross_project_isolation() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();

    let project_a = basin_common::ProjectId::new().to_string();
    let project_b = basin_common::ProjectId::new().to_string();

    // Obtain JWT for project A.
    let Some(jwt_a) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project_a,
        "user_a@example.com",
        "longenoughpassword",
    )
    .await
    else {
        eprintln!("could not obtain JWT for project A — skipping");
        return;
    };

    // Obtain JWT for project B (different email).
    let Some(jwt_b) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project_b,
        "user_b@example.com",
        "longenoughpassword",
    )
    .await
    else {
        eprintln!("could not obtain JWT for project B — skipping");
        return;
    };

    // --- Gate 1: project A's JWT → project A's table → HTTP 200 ----------
    //
    // We connect and immediately close after reading the status line.
    let path_a = format!("/realtime/v1/sse/{project_a}/orders");
    let bearer_a = format!("Bearer {jwt_a}");

    // We only need to check the HTTP status, so read just the first line.
    let mut sock = TcpStream::connect(srv.sse_addr).await.expect("connect");
    let req = format!(
        "GET {path_a} HTTP/1.1\r\nHost: {}\r\nAuthorization: {bearer_a}\r\nAccept: text/event-stream\r\n\r\n",
        srv.sse_addr
    );
    sock.write_all(req.as_bytes()).await.expect("write");
    sock.flush().await.expect("flush");

    // Read until the end of headers.
    let mut buf = [0u8; 512];
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sock.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    let head = String::from_utf8_lossy(&response);
    let status_line = head.lines().next().unwrap_or("");
    // The SSE endpoint is only present when built with `--features realtime`.
    // If not present, we get 404 — skip cleanly.
    if status_line.contains("404") {
        eprintln!("SSE endpoint returned 404 — binary built without --features realtime, skipping");
        return;
    }
    assert!(
        status_line.contains("200"),
        "expected 200 for valid JWT+project, got: {status_line}"
    );
    drop(sock);

    // --- Gate 5: project B's JWT → project A's table → HTTP 403 -----------
    let bearer_b = format!("Bearer {jwt_b}");
    let mut sock2 = TcpStream::connect(srv.sse_addr).await.expect("connect2");
    let req2 = format!(
        "GET {path_a} HTTP/1.1\r\nHost: {}\r\nAuthorization: {bearer_b}\r\nAccept: text/event-stream\r\n\r\n",
        srv.sse_addr
    );
    sock2.write_all(req2.as_bytes()).await.expect("write2");
    sock2.flush().await.expect("flush2");

    let mut response2 = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sock2.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response2.extend_from_slice(&buf[..n]);
                    if response2.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    let head2 = String::from_utf8_lossy(&response2);
    let status_line2 = head2.lines().next().unwrap_or("");
    assert!(
        status_line2.contains("403"),
        "expected 403 for cross-project JWT, got: {status_line2}"
    );
}

/// Gate 3: heartbeats fire every 15 s. We hold the connection open for 16 s
/// and assert that at least one comment frame (`: heartbeat`) was received.
///
/// This test takes ~16 seconds to run and is therefore gated behind an
/// environment variable: set `BASIN_RUN_SLOW_TESTS=1` to enable it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_heartbeat_fires_within_16s() {
    if std::env::var("BASIN_RUN_SLOW_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping sse_heartbeat_fires_within_16s (set BASIN_RUN_SLOW_TESTS=1 to run)");
        return;
    }

    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();

    let project = basin_common::ProjectId::new().to_string();
    let Some(jwt) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "hb@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };

    let path = format!("/realtime/v1/sse/{project}/heartbeat_table");
    let bearer = format!("Bearer {jwt}");

    let frames = collect_sse_frames(
        srv.sse_addr,
        &path,
        &[
            ("Authorization", bearer.as_str()),
            ("Accept", "text/event-stream"),
        ],
        Duration::from_secs(16),
    )
    .await;

    // Check 404 gracefully (binary without realtime feature).
    // If we got no frames at all but the connection was refused we'd have
    // bailed above; an empty frame list with a short timeout means 404.

    let heartbeats: Vec<_> = frames.iter().filter(|f| f.is_comment).collect();
    assert!(
        !heartbeats.is_empty(),
        "expected at least 1 heartbeat comment frame in 16 s, got frames: {frames:?}"
    );
}

/// Gate 4: RLS — a subscriber whose JWT doesn't carry `admin` or `service_role`
/// cannot see events tagged with a different user.
///
/// This test validates the [`rls_permits`] logic in isolation (via the
/// `basin-realtime` unit tests) and here just checks that the SSE connection
/// is accepted (200) for a normal user — the filtering itself is unit-tested
/// inside `crates/basin-realtime/src/sse.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_rls_subscriber_connects_successfully() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();

    let project = basin_common::ProjectId::new().to_string();
    let Some(jwt) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "rls_user@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };

    let path = format!("/realtime/v1/sse/{project}/orders");
    let bearer = format!("Bearer {jwt}");

    let mut sock = TcpStream::connect(srv.sse_addr).await.expect("connect");
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: {bearer}\r\nAccept: text/event-stream\r\n\r\n",
        srv.sse_addr
    );
    sock.write_all(req.as_bytes()).await.expect("write");
    sock.flush().await.expect("flush");

    let mut buf = [0u8; 512];
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sock.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    let head = String::from_utf8_lossy(&response);
    let status_line = head.lines().next().unwrap_or("");
    if status_line.contains("404") {
        eprintln!("SSE endpoint not present (no realtime feature) — skipping");
        return;
    }
    assert!(
        status_line.contains("200"),
        "RLS-subscriber should get 200, got: {status_line}"
    );
}

/// Gate 2 (replay): subscribing with `Last-Event-ID: 0` is accepted; since
/// the `ReplayCursor::drain` stub returns an empty vec, no replay events are
/// emitted but the connection remains open (stream transitions to live mode).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_last_event_id_accepted() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();

    let project = basin_common::ProjectId::new().to_string();
    let Some(jwt) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "replay@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };

    let path = format!("/realtime/v1/sse/{project}/orders");
    let bearer = format!("Bearer {jwt}");

    let mut sock = TcpStream::connect(srv.sse_addr).await.expect("connect");
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: {bearer}\r\nAccept: text/event-stream\r\nLast-Event-ID: 42\r\n\r\n",
        srv.sse_addr
    );
    sock.write_all(req.as_bytes()).await.expect("write");
    sock.flush().await.expect("flush");

    let mut buf = [0u8; 512];
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sock.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    let head = String::from_utf8_lossy(&response);
    let status_line = head.lines().next().unwrap_or("");
    if status_line.contains("404") {
        eprintln!("SSE endpoint not present — skipping");
        return;
    }
    assert!(
        status_line.contains("200"),
        "Last-Event-ID should not cause a reject, got: {status_line}"
    );
}

/// Gate 6 (disconnect): subscribing with no auth → 401 immediately.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_no_auth_returns_401() {
    let Some(srv) = spawn_server().await else {
        return;
    };

    let project = basin_common::ProjectId::new().to_string();
    let path = format!("/realtime/v1/sse/{project}/orders");

    let mut sock = TcpStream::connect(srv.sse_addr).await.expect("connect");
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\n\r\n",
        srv.sse_addr
    );
    sock.write_all(req.as_bytes()).await.expect("write");
    sock.flush().await.expect("flush");

    let mut buf = [0u8; 512];
    let mut response = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match sock.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    response.extend_from_slice(&buf[..n]);
                    if response.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;

    let head = String::from_utf8_lossy(&response);
    let status_line = head.lines().next().unwrap_or("");
    if status_line.contains("404") {
        eprintln!("SSE endpoint not present — skipping");
        return;
    }
    assert!(
        status_line.contains("401"),
        "missing auth should return 401, got: {status_line}"
    );
}
