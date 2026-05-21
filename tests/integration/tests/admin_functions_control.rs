//! T-078 / T-079 / T-080 / T-081 — basin-fn control API.
//!
//! Drives the `/admin/v1/functions/*` endpoints through a real in-process
//! `RestService` and verifies the deploy → list → cpu-ms → delete →
//! list-after-delete sequence. The Wasm component is a minimal
//! `(component)` blob compiled from WAT — small enough to ship inline,
//! valid enough that the deploy endpoint's base64-decode + size-check
//! succeed. basin-rest treats the bytes as opaque, so we don't need a
//! component that actually exports `handle`.
//!
//! ## Skip-cleanly
//!
//! Needs Postgres at `127.0.0.1:5432` so `AuthService::connect_with_mailer`
//! can run schema migrations. The route handlers themselves never consult
//! `AuthService`, but the type system requires an `Arc<AuthService>` in
//! `RestConfig`. The same pattern used by `inbound_webhook_auth.rs`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_rest::{RestConfig, RestService, RunningRest};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn try_make_auth(secret: &[u8]) -> Option<Arc<AuthService>> {
    let cfg = AuthConfig {
        jwt_secret: secret.to_vec(),
        token_ttl: Duration::from_secs(300),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
        catalog_schema: format!(
            "basin_admin_fn_test_{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ),
        smtp: SmtpConfig {
            host: "smtp.invalid".into(),
            port: 587,
            username: "u".into(),
            password: "p".into(),
            from_email: "noreply@example.com".into(),
            from_name: None,
            tls: SmtpTls::StartTls,
        },
        bcrypt_cost: 4,
        password_min_len: 10,
        rate_limit_per_ip_per_min: 10_000,
        email_enabled: false,
        pgwire_public_host: "127.0.0.1:5433".into(),
    };
    let mailer = Arc::new(StubMailer::new(cfg.smtp.from_email.clone()));
    match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, mailer),
    )
    .await
    {
        Ok(Ok(a)) => Some(Arc::new(a)),
        Ok(Err(e)) => {
            eprintln!("postgres unreachable, skipping admin_functions_control: {e}");
            None
        }
        Err(_) => {
            eprintln!("postgres timeout, skipping admin_functions_control");
            None
        }
    }
}

/// Issue an `is_admin: true` JWT tied to `project`.
fn admin_jwt_for(secret: &[u8], project: &ProjectId) -> String {
    let keys = basin_auth::jwt::JwtKeys::new(secret);
    let now = chrono::Utc::now();
    let (jwt, _) = keys
        .issue_with_admin(
            project,
            Uuid::new_v4(),
            "admin@example.com",
            &[],
            true,
            now,
            Duration::from_secs(300),
        )
        .expect("issue admin jwt");
    jwt
}

struct Fixture {
    running: RunningRest,
    project: ProjectId,
    token: String,
    _dir: TempDir,
}

async fn boot() -> Option<Fixture> {
    let secret = vec![5u8; 32];
    let auth = try_make_auth(&secret).await?;
    let dir = TempDir::new().expect("tempdir");
    let engine = engine_in(&dir);
    let svc = RestService::new(RestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        auth,
        max_body_bytes: 1 << 20,
        default_page_size: 100,
        max_page_size: 1000,
        cors_origins: Vec::new(),
        rate_limit_per_sec: 1000,
    });
    let running = svc.run_until_bound().await.expect("rest bind");
    let project = ProjectId::new();
    let token = admin_jwt_for(&secret, &project);
    Some(Fixture {
        running,
        project,
        token,
        _dir: dir,
    })
}

// ---------------------------------------------------------------------------
// Minimal raw HTTP/1.1 client
// ---------------------------------------------------------------------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

impl HttpResp {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
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
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("missing header terminator");
    let head = std::str::from_utf8(&buf[..split]).expect("header utf8");
    let body = buf[split + 4..].to_vec();
    let status_line = head.split("\r\n").next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("parse status");
    HttpResp { status, body }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a minimal valid Wasm **component** from WAT. The shape `(component)`
/// produces a zero-export component — enough to satisfy the deploy endpoint
/// (which treats the bytes as opaque) and to compile via `wat::parse_str`.
fn tiny_component_b64() -> String {
    let bytes = wat::parse_str("(component)").expect("compile tiny WAT component");
    B64.encode(&bytes)
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_functions_deploy_list_cpu_delete_round_trip() {
    let Some(fx) = boot().await else {
        return;
    };
    let bearer_hdr = bearer(&fx.token);
    let auth = ("Authorization", bearer_hdr.as_str());

    // 1. Empty list to start.
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json().as_array().cloned().unwrap_or_default();
    assert!(
        arr.is_empty(),
        "no deploys yet → list empty, got {:?}",
        arr
    );

    // 2. Deploy a tiny component.
    let body = serde_json::json!({
        "name": "hello",
        "wasm_b64": tiny_component_b64(),
    })
    .to_string();
    let r = http_request(
        fx.running.local_addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json"), auth],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status, 201,
        "deploy should 201, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    assert_eq!(v["name"], "hello");
    assert_eq!(v["project_id"], fx.project.to_string());
    assert_eq!(v["version"], 1);
    assert!(v["function_id"].as_str().unwrap().contains(":hello"));

    // 3. List now shows one entry.
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json().as_array().cloned().expect("array");
    assert_eq!(arr.len(), 1, "list should have 1 entry, got {arr:?}");
    assert_eq!(arr[0]["name"], "hello");
    assert_eq!(arr[0]["version"], 1);
    assert!(arr[0]["deployed_at"].is_string());
    assert!(arr[0]["size_bytes"].as_u64().unwrap() > 0);

    // 4. cpu-ms returns 0 initially (stub; no invocations metered).
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions/hello/cpu-ms",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    assert_eq!(v["name"], "hello");
    assert_eq!(v["cpu_ms"], 0);

    // 5. logs returns empty lines list (stub).
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions/hello/logs",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    assert_eq!(v["name"], "hello");
    let lines = v["lines"].as_array().expect("lines array");
    assert!(lines.is_empty(), "logs stub should return [] until basin-fn buffers");

    // 6. Redeploy bumps version.
    let body = serde_json::json!({
        "name": "hello",
        "wasm_b64": tiny_component_b64(),
    })
    .to_string();
    let r = http_request(
        fx.running.local_addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json"), auth],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 201);
    assert_eq!(r.json()["version"], 2, "redeploy must bump version");

    // 7. Delete.
    let r = http_request(
        fx.running.local_addr,
        "DELETE",
        "/admin/v1/functions/hello",
        &[auth],
        None,
    )
    .await;
    assert_eq!(
        r.status, 204,
        "delete should 204, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    // 8. List-after-delete is empty.
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json().as_array().cloned().unwrap_or_default();
    assert!(
        arr.is_empty(),
        "list after delete should be empty, got {arr:?}"
    );

    // 9. Delete-again is 404.
    let r = http_request(
        fx.running.local_addr,
        "DELETE",
        "/admin/v1/functions/hello",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 404);

    // 10. logs/cpu-ms on a non-existent function → 404.
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions/hello/logs",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 404);
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions/hello/cpu-ms",
        &[auth],
        None,
    )
    .await;
    assert_eq!(r.status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_functions_require_admin_token() {
    let Some(fx) = boot().await else {
        return;
    };

    // No Authorization header on deploy → 401.
    let body = serde_json::json!({
        "name": "nope",
        "wasm_b64": tiny_component_b64(),
    })
    .to_string();
    let r = http_request(
        fx.running.local_addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 401);

    // Non-admin JWT also rejected.
    let secret = vec![5u8; 32];
    let keys = basin_auth::jwt::JwtKeys::new(&secret);
    let (user_token, _) = keys
        .issue(
            &fx.project,
            Uuid::new_v4(),
            "u@example.com",
            &[],
            chrono::Utc::now(),
            Duration::from_secs(300),
        )
        .expect("issue user");
    let r = http_request(
        fx.running.local_addr,
        "GET",
        "/admin/v1/functions",
        &[("Authorization", &format!("Bearer {user_token}"))],
        None,
    )
    .await;
    assert_eq!(r.status, 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_functions_deploy_rejects_invalid_body() {
    let Some(fx) = boot().await else {
        return;
    };
    let bearer_hdr = bearer(&fx.token);
    let auth = ("Authorization", bearer_hdr.as_str());

    // Bad base64 → 400.
    let body = serde_json::json!({
        "name": "bad",
        "wasm_b64": "not!!base64!!",
    })
    .to_string();
    let r = http_request(
        fx.running.local_addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json"), auth],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 400);

    // Empty bytes (valid empty b64) → 400.
    let body = serde_json::json!({
        "name": "empty",
        "wasm_b64": "",
    })
    .to_string();
    let r = http_request(
        fx.running.local_addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json"), auth],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 400);
}
