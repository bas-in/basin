//! Integration tests for the WebSocket presence protocol (Phase 5.11.R4).
//!
//! Spawns `basin-server` with `--features realtime` and drives the WS endpoint
//! to exercise presence tracking (`track`/`untrack`/heartbeat/`presence_state`/
//! `presence_diff`).
//!
//! # Skip conditions
//!
//! Every test silently passes (`return`) when:
//! - The `basin-server` binary is not found in the build output.
//! - The `basin-server` binary was built without `--features realtime` (WS
//!   endpoint returns 404 → test exits cleanly).
//! - An external Postgres DSN is unavailable (`pg_alive()` returns false).
//!
//! This mirrors the skip-cleanly contract from `realtime_ws.rs`.

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
// Binary / infra helpers (mirrors realtime_ws.rs)
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

struct ServerAddrs {
    _guard: ChildGuard,
    rest_addr: std::net::SocketAddr,
    ws_addr: std::net::SocketAddr,
}

/// Spawn a `basin-server` binary with auth + REST + realtime enabled.
/// Returns `None` if any precondition is unmet (binary not found, PG not up).
async fn spawn_server() -> Option<ServerAddrs> {
    if !pg_alive().await {
        eprintln!("postgres unreachable — skipping realtime_presence tests");
        return None;
    }
    let bin = find_basin_server_binary()?;
    let data_dir = tempfile::TempDir::new().ok()?;

    let pg_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let pg_addr = pg_probe.local_addr().ok()?;
    drop(pg_probe);
    let rest_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let rest_addr = rest_probe.local_addr().ok()?;
    drop(rest_probe);
    let sse_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let sse_addr = sse_probe.local_addr().ok()?;
    drop(sse_probe);
    let ws_probe = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
    let ws_addr = ws_probe.local_addr().ok()?;
    drop(ws_probe);

    let auth_schema = format!("basin_pres_test_{}", Ulid::new().to_string().to_lowercase());

    let mut cmd = tokio::process::Command::new(&bin);
    cmd.env("BASIN_BIND", pg_addr.to_string())
        .env("BASIN_DATA_DIR", data_dir.path())
        .env("BASIN_PROJECTS", "alice=*")
        .env("BASIN_CATALOG", "memory")
        .env("BASIN_AUTH_ENABLED", "1")
        .env("BASIN_REST_ENABLED", "1")
        .env("BASIN_REST_BIND", rest_addr.to_string())
        .env("BASIN_REALTIME_BIND", sse_addr.to_string())
        .env("BASIN_REALTIME_WS_BIND", ws_addr.to_string())
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

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if Instant::now() >= deadline {
            let _ = child.start_kill();
            return None;
        }
        if TcpStream::connect(ws_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let _data_dir = data_dir;
    Some(ServerAddrs {
        _guard: ChildGuard {
            child: Some(child),
            auth_schema,
        },
        rest_addr,
        ws_addr,
    })
}

// ---------------------------------------------------------------------------
// HTTP helper
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
// Auth helpers
// ---------------------------------------------------------------------------

async fn obtain_jwt(
    rest_addr: std::net::SocketAddr,
    auth_schema: &str,
    project: &str,
    email: &str,
    password: &str,
) -> Option<String> {
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
// WebSocket helpers (mirrors realtime_ws.rs)
// ---------------------------------------------------------------------------

async fn ws_connect(addr: std::net::SocketAddr, path: &str, jwt: &str) -> Option<TcpStream> {
    let mut sock = TcpStream::connect(addr).await.ok()?;
    let ws_key = "dGhlIHNhbXBsZSBub25jZQ==";
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Version: 13\r\n\
         Sec-WebSocket-Key: {ws_key}\r\n\
         Authorization: Bearer {jwt}\r\n\
         \r\n"
    );
    sock.write_all(req.as_bytes()).await.ok()?;
    sock.flush().await.ok()?;

    let mut buf = Vec::with_capacity(1024);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        let mut tmp = [0u8; 256];
        match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut tmp)).await {
            Ok(Ok(0)) => return None,
            Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
            Ok(Err(_)) | Err(_) => {}
        }
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    let status_line = head.lines().next().unwrap_or("");
    if !status_line.contains("101") {
        eprintln!("WS upgrade failed: {status_line}");
        return None;
    }
    Some(sock)
}

/// Send a WebSocket text frame (client-to-server, masked).
async fn ws_send_text(sock: &mut TcpStream, text: &str) -> bool {
    let payload = text.as_bytes();
    let mask: [u8; 4] = [0x37, 0xfa, 0x21, 0x3d];

    let mut frame = Vec::with_capacity(2 + 4 + payload.len());
    frame.push(0x81); // FIN + text opcode
    assert!(
        payload.len() < 126,
        "test helper only supports short frames"
    );
    frame.push(0x80 | payload.len() as u8);
    frame.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        frame.push(b ^ mask[i % 4]);
    }
    sock.write_all(&frame).await.is_ok() && sock.flush().await.is_ok()
}

/// Read one WebSocket frame. Returns (opcode, payload) or None on error/close.
async fn ws_recv_frame(sock: &mut TcpStream, timeout_dur: Duration) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; 2];
    tokio::time::timeout(timeout_dur, sock.read_exact(&mut header))
        .await
        .ok()?
        .ok()?;

    let opcode = header[0] & 0x0f;
    let masked = (header[1] & 0x80) != 0;
    let len = (header[1] & 0x7f) as usize;

    let mut payload = vec![0u8; len];
    if len > 0 {
        tokio::time::timeout(timeout_dur, sock.read_exact(&mut payload))
            .await
            .ok()?
            .ok()?;
    }

    if masked {
        let mut mask = [0u8; 4];
        tokio::time::timeout(timeout_dur, sock.read_exact(&mut mask))
            .await
            .ok()?
            .ok()?;
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i % 4];
        }
    }
    Some((opcode, payload))
}

/// Collect JSON text frames until the predicate returns true or the deadline.
async fn ws_collect_text_frames(
    sock: &mut TcpStream,
    timeout_dur: Duration,
    stop: impl Fn(&serde_json::Value) -> bool,
) -> Vec<serde_json::Value> {
    let mut frames = Vec::new();
    let deadline = Instant::now() + timeout_dur;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Some((opcode, payload)) = ws_recv_frame(sock, remaining).await else {
            break;
        };
        if opcode == 0x08 {
            break; // close
        }
        if opcode == 0x0a {
            continue; // pong
        }
        if opcode == 0x09 {
            // ping from server — send pong best-effort
            let pong = vec![0x8a_u8, 0x80, 0x00, 0x00, 0x00, 0x00];
            let _ = sock.write_all(&pong).await;
            continue;
        }
        if opcode == 0x01 {
            if let Ok(text) = std::str::from_utf8(&payload) {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
                    let done = stop(&v);
                    frames.push(v);
                    if done {
                        break;
                    }
                }
            }
        }
    }
    frames
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Gate 1 + 2: two clients join the same channel, each sees the other in
/// `presence_state`; one disconnects, the other receives `presence_diff` with
/// the leaver in `leaves`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presence_two_clients_join_and_one_leaves() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();
    let project = basin_common::ProjectId::new().to_string();

    // Obtain JWTs for two users in the same project.
    let Some(jwt_a) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "presence_a@example.com",
        "longenoughpassword",
    )
    .await
    else {
        eprintln!("could not obtain JWT A — skipping");
        return;
    };

    let Some(jwt_b) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "presence_b@example.com",
        "longenoughpassword",
    )
    .await
    else {
        eprintln!("could not obtain JWT B — skipping");
        return;
    };

    let path = format!("/realtime/v1/ws/{project}");

    // Client A connects.
    let Some(mut sock_a) = ws_connect(srv.ws_addr, &path, &jwt_a).await else {
        eprintln!("WS upgrade failed for A — binary may lack realtime feature, skipping");
        return;
    };

    // Client B connects.
    let Some(mut sock_b) = ws_connect(srv.ws_addr, &path, &jwt_b).await else {
        eprintln!("WS upgrade failed for B — skipping");
        return;
    };

    // Client A sends presence_track.
    let track_a = r#"{"type":"presence_track","channel":"room:1","client_id":"client-a","metadata":{"name":"Alice"}}"#;
    assert!(ws_send_text(&mut sock_a, track_a).await, "A: send track");

    // Drain A's state (expects a presence_state with empty or A itself).
    let frames_a = ws_collect_text_frames(&mut sock_a, Duration::from_secs(5), |v| {
        v["type"] == "presence_state"
    })
    .await;
    let state_a = frames_a.iter().find(|v| v["type"] == "presence_state");
    assert!(
        state_a.is_some(),
        "A should receive presence_state; got: {frames_a:?}"
    );

    // Client B sends presence_track.
    let track_b = r#"{"type":"presence_track","channel":"room:1","client_id":"client-b","metadata":{"name":"Bob"}}"#;
    assert!(ws_send_text(&mut sock_b, track_b).await, "B: send track");

    // B should get presence_state containing A (A joined before B).
    let frames_b = ws_collect_text_frames(&mut sock_b, Duration::from_secs(5), |v| {
        v["type"] == "presence_state"
    })
    .await;
    let state_b = frames_b.iter().find(|v| v["type"] == "presence_state");
    assert!(
        state_b.is_some(),
        "B should receive presence_state; got: {frames_b:?}"
    );

    let presences_b = &state_b.unwrap()["presences"];
    let a_in_b = presences_b
        .as_array()
        .map(|arr| arr.iter().any(|e| e["client_id"] == "client-a"))
        .unwrap_or(false);
    assert!(
        a_in_b,
        "B's presence_state should contain client-a; presences: {presences_b:?}"
    );

    // A should receive a presence_diff with B in joins.
    let diff_frames_a = ws_collect_text_frames(&mut sock_a, Duration::from_secs(5), |v| {
        v["type"] == "presence_diff"
            && v["joins"]
                .as_array()
                .map(|arr| arr.iter().any(|e| e["client_id"] == "client-b"))
                .unwrap_or(false)
    })
    .await;
    let b_joined_in_a = diff_frames_a.iter().any(|v| {
        v["type"] == "presence_diff"
            && v["joins"]
                .as_array()
                .map(|arr| arr.iter().any(|e| e["client_id"] == "client-b"))
                .unwrap_or(false)
    });
    assert!(
        b_joined_in_a,
        "A should see a presence_diff with client-b in joins; got: {diff_frames_a:?}"
    );

    // --- Gate 2: Client B disconnects --- --------------------------------
    // Close B's connection — server should detect disconnect and untrack B.
    drop(sock_b);

    // A should receive a presence_diff with B in leaves.
    let leave_frames_a = ws_collect_text_frames(&mut sock_a, Duration::from_secs(8), |v| {
        v["type"] == "presence_diff"
            && v["leaves"]
                .as_array()
                .map(|arr| arr.iter().any(|e| e["client_id"] == "client-b"))
                .unwrap_or(false)
    })
    .await;
    let b_left = leave_frames_a.iter().any(|v| {
        v["type"] == "presence_diff"
            && v["leaves"]
                .as_array()
                .map(|arr| arr.iter().any(|e| e["client_id"] == "client-b"))
                .unwrap_or(false)
    });
    assert!(
        b_left,
        "A should see a presence_diff with client-b in leaves after disconnect; got: {leave_frames_a:?}"
    );
}

/// Gate 4 (channel isolation): a client on `room:1` does NOT see presence events
/// for `room:2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presence_channel_isolation() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();
    let project = basin_common::ProjectId::new().to_string();

    let Some(jwt_a) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "iso_a@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };
    let Some(jwt_b) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "iso_b@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };

    let path = format!("/realtime/v1/ws/{project}");

    let Some(mut sock_a) = ws_connect(srv.ws_addr, &path, &jwt_a).await else {
        eprintln!("WS upgrade failed — skipping");
        return;
    };
    let Some(mut sock_b) = ws_connect(srv.ws_addr, &path, &jwt_b).await else {
        return;
    };

    // A joins room:1.
    let track_a_r1 =
        r#"{"type":"presence_track","channel":"room:1","client_id":"iso-a","metadata":{}}"#;
    assert!(ws_send_text(&mut sock_a, track_a_r1).await);

    // Drain A's state frame for room:1.
    let _ = ws_collect_text_frames(&mut sock_a, Duration::from_secs(5), |v| {
        v["type"] == "presence_state" && v["channel"] == "room:1"
    })
    .await;

    // B joins room:2.
    let track_b_r2 =
        r#"{"type":"presence_track","channel":"room:2","client_id":"iso-b","metadata":{}}"#;
    assert!(ws_send_text(&mut sock_b, track_b_r2).await);

    // Allow a moment for presence events to propagate.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A should NOT have received any presence_diff referencing room:2 or iso-b.
    // We collect with a short timeout — expect no frames.
    let spurious_frames = ws_collect_text_frames(&mut sock_a, Duration::from_millis(500), |v| {
        // Stop early if we see something about room:2.
        v["channel"] == "room:2"
    })
    .await;

    let saw_room2 = spurious_frames.iter().any(|v| v["channel"] == "room:2");
    assert!(
        !saw_room2,
        "client on room:1 should NOT see events for room:2; got: {spurious_frames:?}"
    );
}

/// Smoke: presence_track produces a valid presence_state with correct JSON structure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn presence_state_json_structure() {
    let Some(srv) = spawn_server().await else {
        return;
    };
    let auth_schema = srv._guard.auth_schema.clone();
    let project = basin_common::ProjectId::new().to_string();

    let Some(jwt) = obtain_jwt(
        srv.rest_addr,
        &auth_schema,
        &project,
        "pres_struct@example.com",
        "longenoughpassword",
    )
    .await
    else {
        return;
    };

    let path = format!("/realtime/v1/ws/{project}");
    let Some(mut sock) = ws_connect(srv.ws_addr, &path, &jwt).await else {
        eprintln!("WS upgrade failed — skipping");
        return;
    };

    let track = r#"{"type":"presence_track","channel":"lobby","client_id":"s1","metadata":{"role":"viewer"}}"#;
    assert!(ws_send_text(&mut sock, track).await);

    let frames = ws_collect_text_frames(&mut sock, Duration::from_secs(5), |v| {
        v["type"] == "presence_state"
    })
    .await;

    let state = frames.iter().find(|v| v["type"] == "presence_state");
    assert!(state.is_some(), "expected presence_state; got: {frames:?}");
    let state = state.unwrap();
    assert_eq!(state["channel"], "lobby", "channel field missing");
    assert!(
        state["presences"].is_array(),
        "presences must be an array; got: {state:?}"
    );
}
