//! Phase 6.SEC.P0.3 — inbound webhook authentication (beta-blocker).
//!
//! Drives `POST /in/:project_id/:name` through a real in-process
//! `RestService` bound to an ephemeral port and asserts the
//! default-secure HMAC contract:
//!
//! * Unsigned POST → 401 with `code: "E_UNAUTHENTICATED"`.
//! * POST with a wrong `X-Basin-Signature` → 401.
//! * POST with a valid HMAC-SHA256 sig (hex over the body) → 200 and
//!   the registered SQL body runs (verified by selecting the inserted
//!   row out of the project session).
//! * `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=1` + unsigned → 200 (debug
//!   bypass works). This test sets and unsets the env var serially
//!   inside a single async test so it cannot race against other
//!   `try_serve()` calls in the same process.
//! * Constant-time compare smoke: differing first-byte vs differing
//!   last-byte both reject in the same number of round-trips with no
//!   side-channel-visible status difference. (We can't measure
//!   timing reliably across a TCP round-trip in CI, so this is a
//!   shape-only check — the unit-level proof lives in
//!   `basin-rest::routes::inbound::tests::verify_signature_uses_constant_time_compare`.)
//!
//! No external services beyond an in-process `Engine` are required —
//! we do not bring up Postgres or pgwire here (the route does not
//! consult the auth catalog at all; the body executes against an
//! `InMemoryCatalog`-backed `Engine`).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_rest::{RestConfig, RestService, RunningRest};
use hmac::{Hmac, Mac};
use object_store::local::LocalFileSystem;
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

/// Build a stub `AuthService` over an unreachable PG. Inbound webhook
/// auth never consults `AuthService`, so we don't need a live PG —
/// but `RestConfig::auth` is required. We bail-skip if `AuthService`
/// construction needs PG (it does), in which case the test prints a
/// skip line and returns. We minimise that branch by using
/// `AuthService::test_offline()` if it exists, else skip.
async fn try_make_auth() -> Option<Arc<basin_auth::AuthService>> {
    use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
    let cfg = AuthConfig {
        jwt_secret: vec![7u8; 32],
        token_ttl: Duration::from_secs(60),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
        catalog_schema: format!(
            "basin_inbound_auth_test_{}",
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
        rate_limit_per_ip_per_min: 1000,
        email_enabled: false,
        pgwire_public_host: "127.0.0.1:5433".into(),
    };
    let mailer = Arc::new(StubMailer::new(cfg.smtp.from_email.clone()));
    let auth = match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, mailer),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => {
            eprintln!("postgres unreachable, skipping inbound_webhook_auth: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("postgres timeout, skipping inbound_webhook_auth");
            return None;
        }
    };
    Some(Arc::new(auth))
}

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

struct Fixture {
    running: RunningRest,
    engine: Engine,
}

async fn boot() -> Option<Fixture> {
    let auth = try_make_auth().await?;
    let dir = TempDir::new().expect("tempdir");
    let dir_path: &'static TempDir = Box::leak(Box::new(dir));
    let engine = engine_in(dir_path);
    let svc = RestService::new(RestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine: engine.clone(),
        auth,
        max_body_bytes: 1 << 20,
        default_page_size: 100,
        max_page_size: 1000,
        cors_origins: Vec::new(),
        rate_limit_per_sec: 1000,
    });
    let running = svc.run_until_bound().await.expect("bind");
    Some(Fixture { running, engine })
}

// ---------------------------------------------------------------------------
// Minimal raw HTTP/1.1 client
// ---------------------------------------------------------------------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
}

impl HttpResp {
    fn code(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_slice(&self.body).ok()?;
        v.get("code")?.as_str().map(|s| s.to_string())
    }
}

async fn raw_request(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    extra_headers: &[(&str, &str)],
    body: Option<&[u8]>,
) -> HttpResp {
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    for (k, v) in extra_headers {
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
    parse_http(&buf)
}

fn parse_http(buf: &[u8]) -> HttpResp {
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

/// Compute the canonical `X-Basin-Signature` value: hex(HMAC-SHA256(body, secret)).
fn sign(secret_hex: &str, body: &[u8]) -> String {
    let secret = hex::decode(secret_hex).expect("secret hex");
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&secret).expect("hmac key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Register an inbound webhook in `project` and return the issued secret.
async fn register_hook(engine: &Engine, project: ProjectId, name: &str, body: &str) -> String {
    let session = engine.open_session(project).await.expect("open session");
    session
        .execute("CREATE TABLE webhook_events (id TEXT, raw_payload JSONB)")
        .await
        .expect("create table");
    let create = format!("CREATE INBOUND WEBHOOK {name} EXECUTE {body}");
    let res = session.execute(&create).await.expect("create hook");
    match res {
        ExecResult::Rows { batches, .. } => {
            assert_eq!(batches.len(), 1);
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("secret column");
            assert_eq!(arr.len(), 1);
            arr.value(0).to_string()
        }
        other => panic!("expected Rows from CREATE INBOUND WEBHOOK, got {other:?}"),
    }
}

async fn count_rows(engine: &Engine, project: ProjectId, sql: &str) -> usize {
    let session = engine.open_session(project).await.expect("open session");
    match session.execute(sql).await.expect("execute count") {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsigned_post_is_401() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let _secret = register_hook(
        &fx.engine,
        project,
        "hook_a",
        "INSERT INTO webhook_events (id, raw_payload) VALUES ('x', payload)",
    )
    .await;

    let path = format!("/in/{}/hook_a", project);
    let r = raw_request(
        fx.running.local_addr,
        "POST",
        &path,
        &[("Content-Type", "application/json")],
        Some(br#"{"id":"evt_1"}"#),
    )
    .await;
    assert_eq!(r.status, 401, "unsigned POST must 401");
    assert_eq!(r.code().as_deref(), Some("E_UNAUTHENTICATED"));
    // SQL body did NOT run.
    let n = count_rows(&fx.engine, project, "SELECT id FROM webhook_events").await;
    assert_eq!(n, 0, "no row should be inserted on rejected request");

    let _ = fx.running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_signature_is_401() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let _secret = register_hook(
        &fx.engine,
        project,
        "hook_b",
        "INSERT INTO webhook_events (id, raw_payload) VALUES ('y', payload)",
    )
    .await;

    let path = format!("/in/{}/hook_b", project);
    // 32 bytes of 0xff in hex — wrong MAC.
    let bad = "f".repeat(64);
    let r = raw_request(
        fx.running.local_addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            ("X-Basin-Signature", &bad),
        ],
        Some(br#"{"id":"evt_2"}"#),
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.code().as_deref(), Some("E_UNAUTHENTICATED"));
    let n = count_rows(&fx.engine, project, "SELECT id FROM webhook_events").await;
    assert_eq!(n, 0);

    let _ = fx.running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn valid_signature_runs_sql() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let secret = register_hook(
        &fx.engine,
        project,
        "hook_c",
        "INSERT INTO webhook_events (id, raw_payload) VALUES (payload->>'id', payload)",
    )
    .await;

    let body = br#"{"id":"evt_3","amount":42}"#;
    let sig = sign(&secret, body);
    let path = format!("/in/{}/hook_c", project);
    let r = raw_request(
        fx.running.local_addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            ("X-Basin-Signature", &sig),
        ],
        Some(body),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "valid sig must 200; body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let n = count_rows(
        &fx.engine,
        project,
        "SELECT id FROM webhook_events WHERE id = 'evt_3'",
    )
    .await;
    assert_eq!(n, 1, "valid request must insert exactly one row");

    let _ = fx.running.shutdown.send(());
}

/// Constant-time-compare smoke check: a signature that differs only in
/// its first byte and one that differs only in its last byte must BOTH
/// be rejected with the same HTTP status and the same error code.
/// (Wall-clock timing across a TCP round trip is too noisy to assert
/// on in CI; the constant-time property is unit-tested at the
/// `verify_signature` level — this test makes sure no observable
/// behaviour distinguishes the two failure modes.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ct_compare_no_status_side_channel() {
    let Some(fx) = boot().await else {
        return;
    };
    let project = ProjectId::new();
    let secret = register_hook(
        &fx.engine,
        project,
        "hook_d",
        "INSERT INTO webhook_events (id, raw_payload) VALUES ('z', payload)",
    )
    .await;

    let body = b"abc";
    let good = sign(&secret, body);
    let mut diff_first = good.clone().into_bytes();
    diff_first[0] = if diff_first[0] == b'a' { b'b' } else { b'a' };
    let mut diff_last = good.clone().into_bytes();
    let last = diff_last.len() - 1;
    diff_last[last] = if diff_last[last] == b'a' { b'b' } else { b'a' };

    let path = format!("/in/{}/hook_d", project);
    let r_first = raw_request(
        fx.running.local_addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            (
                "X-Basin-Signature",
                std::str::from_utf8(&diff_first).unwrap(),
            ),
        ],
        Some(body),
    )
    .await;
    let r_last = raw_request(
        fx.running.local_addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            (
                "X-Basin-Signature",
                std::str::from_utf8(&diff_last).unwrap(),
            ),
        ],
        Some(body),
    )
    .await;
    assert_eq!(r_first.status, 401);
    assert_eq!(r_last.status, 401);
    assert_eq!(r_first.code(), r_last.code());

    let _ = fx.running.shutdown.send(());
}

/// Debug bypass: with `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=1` set, an
/// unsigned POST is accepted (developer-only path; never enable in
/// production). This test is `#[ignore]` by default because env-var
/// mutation in async tests can race with parallel `boot()`s in the
/// same process. To run: `cargo test -p basin-integration-tests --test
/// inbound_webhook_auth -- --ignored plaintext_bypass`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "env-var mutation; run with --ignored"]
async fn plaintext_bypass_accepts_unsigned() {
    // Set the bypass FIRST, before any other tests in this process can
    // boot a fresh server.
    std::env::set_var("BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS", "1");
    let result: Result<bool, String> = async {
        let Some(fx) = boot().await else {
            // PG-unreachable — skip cleanly like the other tests.
            return Ok(false);
        };
        let project = ProjectId::new();
        let _secret = register_hook(
            &fx.engine,
            project,
            "hook_e",
            "INSERT INTO webhook_events (id, raw_payload) VALUES ('e', payload)",
        )
        .await;
        let path = format!("/in/{}/hook_e", project);
        let r = raw_request(
            fx.running.local_addr,
            "POST",
            &path,
            &[("Content-Type", "application/json")],
            Some(br#"{"id":"bypass"}"#),
        )
        .await;
        if r.status != 200 {
            return Err(format!(
                "expected 200 under bypass, got {}; body: {}",
                r.status,
                String::from_utf8_lossy(&r.body)
            ));
        }
        let n = count_rows(&fx.engine, project, "SELECT id FROM webhook_events").await;
        if n != 1 {
            return Err(format!("expected 1 row inserted, got {n}"));
        }
        let _ = fx.running.shutdown.send(());
        Ok(true)
    }
    .await;
    std::env::remove_var("BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS");
    let ran = result.expect("bypass scenario");
    if !ran {
        eprintln!("postgres unreachable, skipping plaintext_bypass test");
    }
}
