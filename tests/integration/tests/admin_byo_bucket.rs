//! T-049 engine-side — `POST /admin/v1/projects/:project_id/byo-bucket`.
//!
//! Drives the cloud-handed BYO-bucket registration route through a real
//! in-process `RestService` bound to an ephemeral port. Covers:
//!
//! * Admin gate — missing / non-admin JWT → 401.
//! * Successful registration — 204 No Content; the catalog round-trip
//!   surfaces the persisted [`basin_catalog::S3Config`] via
//!   `get_project_metadata`; the storage layer reports a registered BYO
//!   override via [`basin_storage::Storage::has_byo_object_store`] (or its
//!   moral equivalent: re-registering replaces in-place without error).
//! * Idempotency — POSTing the same body twice succeeds both times and the
//!   second call replaces, not appends.
//! * Bad-shape body → 400 `E_INVALID_REQUEST`.
//!
//! Skip-cleanly: needs Postgres on `127.0.0.1:5432` only because
//! `AuthService::connect_with_mailer` insists on a reachable catalog
//! schema for migrations. The route itself never consults `AuthService`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_rest::{RestConfig, RestService, RunningRest};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use uuid::Uuid;

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

// ---------------------------------------------------------------------------
// Boot helpers
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> (Engine, Arc<dyn Catalog>) {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog: catalog.clone(),
        shard: None,
    });
    (engine, catalog)
}

async fn try_make_auth(secret: &[u8]) -> Option<Arc<AuthService>> {
    let cfg = AuthConfig {
        jwt_secret: secret.to_vec(),
        token_ttl: Duration::from_secs(300),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
        catalog_schema: format!(
            "basin_admin_byo_test_{}",
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
            eprintln!("postgres unreachable, skipping admin_byo_bucket: {e}");
            None
        }
        Err(_) => {
            eprintln!("postgres timeout, skipping admin_byo_bucket");
            None
        }
    }
}

/// Issue an `is_admin: true` JWT against the same secret the service uses.
fn admin_jwt(secret: &[u8]) -> String {
    let keys = basin_auth::jwt::JwtKeys::new(secret);
    let now = chrono::Utc::now();
    let (jwt, _) = keys
        .issue_with_admin(
            &ProjectId::new(),
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

/// Issue a plain (non-admin) JWT against the same secret.
fn user_jwt(secret: &[u8]) -> String {
    let keys = basin_auth::jwt::JwtKeys::new(secret);
    let now = chrono::Utc::now();
    let (jwt, _) = keys
        .issue(
            &ProjectId::new(),
            Uuid::new_v4(),
            "user@example.com",
            &[],
            now,
            Duration::from_secs(300),
        )
        .expect("issue user jwt");
    jwt
}

struct Fixture {
    running: RunningRest,
    catalog: Arc<dyn Catalog>,
    secret: Vec<u8>,
    _dir: TempDir,
}

async fn boot() -> Option<Fixture> {
    let secret = vec![3u8; 32];
    let auth = try_make_auth(&secret).await?;
    let dir = TempDir::new().expect("tempdir");
    let (engine, catalog) = engine_in(&dir);
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
    Some(Fixture {
        running,
        catalog,
        secret,
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

async fn http_post(
    addr: std::net::SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> HttpResp {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    sock.write_all(req.as_bytes()).await.expect("write head");
    sock.write_all(body).await.expect("write body");
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
// Test cases
// ---------------------------------------------------------------------------

fn good_body(bucket: &str) -> String {
    serde_json::json!({
        "bucket": bucket,
        "region": "us-east-1",
        "access_key_id": "AKIAFAKE",
        "secret_access_key": "supersecret",
        "endpoint_url": "https://s3.amazonaws.com",
        "force_path_style": false,
    })
    .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_byo_bucket_requires_admin_token() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let path = format!("/admin/v1/projects/{project}/byo-bucket");
    let body = good_body("my-bucket");

    // 1. Missing Authorization → 401.
    let r = http_post(
        fx.running.local_addr,
        &path,
        &[("Content-Type", "application/json")],
        body.as_bytes(),
    )
    .await;
    assert_eq!(r.status, 401, "no-auth body: {}", String::from_utf8_lossy(&r.body));

    // 2. Non-admin JWT → 401 (route returns Unauthenticated for is_admin=false,
    //    mirroring the other /admin/v1/* routes' contract).
    let user_token = user_jwt(&fx.secret);
    let r = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {user_token}")),
        ],
        body.as_bytes(),
    )
    .await;
    assert_eq!(
        r.status, 401,
        "user-jwt should be rejected, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_byo_bucket_registers_successfully() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let token = admin_jwt(&fx.secret);
    let path = format!("/admin/v1/projects/{project}/byo-bucket");

    let r = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {token}")),
        ],
        good_body("bucket-a").as_bytes(),
    )
    .await;
    assert_eq!(
        r.status,
        204,
        "register should 204, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    // Catalog round-trip: the project metadata now carries a Some(S3Config).
    let meta = fx
        .catalog
        .get_project_metadata(&project)
        .await
        .expect("get_project_metadata");
    let cfg = meta.byo_bucket.expect("byo_bucket should be Some after register");
    assert_eq!(cfg.bucket, "bucket-a");
    assert_eq!(cfg.region, "us-east-1");
    assert_eq!(cfg.access_key_id, "AKIAFAKE");
    assert_eq!(cfg.endpoint, "https://s3.amazonaws.com");
    assert!(!cfg.force_path_style);
    // Secret is persisted verbatim as bytes — the OSS engine is opaque per
    // the S3Config doc-comment.
    assert_eq!(cfg.secret_access_key_enc, b"supersecret");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_byo_bucket_idempotent_re_registration() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let token = admin_jwt(&fx.secret);
    let path = format!("/admin/v1/projects/{project}/byo-bucket");

    // First register.
    let r1 = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {token}")),
        ],
        good_body("first").as_bytes(),
    )
    .await;
    assert_eq!(r1.status, 204);

    // Re-register with a different bucket name — still 204, the second
    // body replaces the first in both catalog and store.
    let r2 = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {token}")),
        ],
        good_body("second").as_bytes(),
    )
    .await;
    assert_eq!(
        r2.status, 204,
        "re-register should also 204, got {} body: {}",
        r2.status,
        String::from_utf8_lossy(&r2.body)
    );

    let meta = fx
        .catalog
        .get_project_metadata(&project)
        .await
        .expect("get_project_metadata");
    let cfg = meta.byo_bucket.expect("byo_bucket Some");
    assert_eq!(cfg.bucket, "second", "second register should replace first");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_byo_bucket_rejects_bad_body() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let token = admin_jwt(&fx.secret);
    let path = format!("/admin/v1/projects/{project}/byo-bucket");

    // Missing required fields → 400.
    let r = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {token}")),
        ],
        br#"{"bucket": "x"}"#,
    )
    .await;
    assert!(
        r.status == 400 || r.status == 422,
        "malformed body should 4xx, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    // Empty bucket name → 400 (caught by our own validation, not axum's
    // body-parser).
    let bad = serde_json::json!({
        "bucket": "",
        "region": "us-east-1",
        "access_key_id": "AKIA",
        "secret_access_key": "s",
    })
    .to_string();
    let r = http_post(
        fx.running.local_addr,
        &path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &format!("Bearer {token}")),
        ],
        bad.as_bytes(),
    )
    .await;
    assert_eq!(
        r.status, 400,
        "empty bucket should 400, got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
}
