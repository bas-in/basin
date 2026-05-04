//! End-to-end integration tests for `basin-rest`.
//!
//! Each test spins up a real `RestService` via `run_until_bound`, opens a TCP
//! connection, speaks raw HTTP/1.1 against it, and asserts on the parsed
//! response. We deliberately avoid pulling in a full HTTP client crate —
//! every dependency we don't take is a maintenance shadow we don't pay for,
//! and HTTP/1.1 is small enough to handle directly.
//!
//! Tests that need a live Postgres (anything that involves `basin-auth`
//! signup/signin) follow the same skip-cleanly pattern `basin-auth` uses:
//! if Postgres is unreachable we print a one-line note and return.

use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;

use crate::{RestConfig, RestService};

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

// --- helpers -----------------------------------------------------------------

fn unique_schema() -> String {
    format!(
        "basin_rest_test_{}",
        Ulid::new().to_string().to_lowercase()
    )
}

struct SchemaGuard {
    schema: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("basin-rest schema cleanup runtime: {e}");
                    return;
                }
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(pair)) => pair,
                    _ => return,
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

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

fn auth_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![9u8; 32],
        token_ttl: Duration::from_secs(60),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: PG_URL.to_owned(),
        catalog_schema: schema.to_owned(),
        smtp: SmtpConfig {
            host: "smtp.invalid".into(),
            port: 587,
            username: "u".into(),
            password: "p".into(),
            from_email: "noreply@example.com".into(),
            from_name: Some("Basin".into()),
            tls: SmtpTls::StartTls,
        },
        bcrypt_cost: 4,
        password_min_len: 10,
        rate_limit_per_ip_per_min: 1000,
    }
}

/// Try to spin up a server backed by live PG. Returns None if PG is
/// unreachable — every test then prints a skip line and exits Ok.
async fn try_serve() -> Option<(crate::RunningRest, RestService, AuthService, StubMailer, SchemaGuard)>
{
    let schema = unique_schema();
    let cfg = auth_cfg(&schema);
    let mailer = StubMailer::new(cfg.smtp.from_email.clone());
    let auth = match tokio::time::timeout(
        Duration::from_secs(2),
        AuthService::connect_with_mailer(cfg, Arc::new(mailer.clone())),
    )
    .await
    {
        Ok(Ok(a)) => a,
        Ok(Err(e)) => {
            eprintln!("postgres unreachable, skipping basin-rest test: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("postgres connect timed out, skipping basin-rest test");
            return None;
        }
    };
    let dir = TempDir::new().expect("tempdir");
    // Leak the tempdir for the test's lifetime — when the engine drops at the
    // end of the test, the directory goes too via Drop on the Storage's inner
    // Arc, but we don't want the TempDir to delete files mid-test.
    let dir_path: &'static TempDir = Box::leak(Box::new(dir));
    let engine = engine_in(dir_path);
    let svc = RestService::new(RestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        auth: Arc::new(auth.clone()),
        max_body_bytes: 256, // small so body_size_limit_enforced trips easily
        default_page_size: 100,
        max_page_size: 50, // small so select_cap_enforced is observable
        cors_origins: vec!["https://app.example.com".into()],
        rate_limit_per_sec: 1000, // generous so other tests don't hit it
    });
    let running = svc.clone().run_until_bound().await.expect("bind");
    Some((running, svc, auth, mailer, SchemaGuard { schema }))
}

fn last_token(mailer: &StubMailer) -> String {
    let log = mailer.sent();
    let body = &log.last().expect("at least one email sent").body;
    let needle = "token=";
    let start = body.find(needle).expect("body has token=") + needle.len();
    let tail = &body[start..];
    let end = tail
        .find(|c: char| !c.is_ascii_hexdigit())
        .unwrap_or(tail.len());
    tail[..end].to_owned()
}

/// Verified email, signed-in tokens for `tenant`, ready to use in
/// `Authorization: Bearer <jwt>` headers.
async fn make_user(auth: &AuthService, mailer: &StubMailer, tenant: &TenantId, email: &str) -> basin_auth::Tokens {
    let user = auth
        .signup(tenant, email, "longenoughpassword")
        .await
        .expect("signup");
    auth.request_email_verification(tenant, user)
        .await
        .expect("request verify");
    let tok = last_token(mailer);
    auth.verify_email(tenant, &tok).await.expect("verify");
    auth.signin(tenant, email, "longenoughpassword")
        .await
        .expect("signin")
}

// --- minimal HTTP/1.1 client over TCP ---------------------------------------

struct HttpResp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResp {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("body is not JSON ({e}): {}", String::from_utf8_lossy(&self.body)))
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
    parse_response(&buf)
}

fn parse_response(buf: &[u8]) -> HttpResp {
    // Find header/body split.
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response missing header terminator");
    let head = &buf[..split];
    let body = &buf[split + 4..];

    let head_str = std::str::from_utf8(head).expect("headers utf8");
    let mut lines = head_str.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status u16");

    let mut headers = Vec::new();
    let mut chunked = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_owned();
            let v = v.trim().to_owned();
            if k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked") {
                chunked = true;
            }
            headers.push((k, v));
        }
    }

    let body = if chunked {
        decode_chunked(body)
    } else {
        body.to_vec()
    };
    HttpResp {
        status,
        headers,
        body,
    }
}

/// Minimal chunked-decoder. Each chunk: `<hex-size>\r\n<bytes>\r\n`,
/// terminated by a zero-size chunk.
fn decode_chunked(mut buf: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let nl = match buf.windows(2).position(|w| w == b"\r\n") {
            Some(p) => p,
            None => return out,
        };
        let size_str = std::str::from_utf8(&buf[..nl]).expect("chunk size utf8");
        let size_str = size_str.split(';').next().unwrap().trim();
        let size = usize::from_str_radix(size_str, 16).expect("chunk size hex");
        buf = &buf[nl + 2..];
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&buf[..size]);
        buf = &buf[size + 2..]; // skip CRLF after data
    }
}

// --- tests -------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signup_signin_returns_jwt() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new().to_string();
    let tenant_parsed: TenantId = tenant.parse().unwrap();

    // signup via REST
    let body = serde_json::json!({
        "tenant_id": tenant,
        "email": "ss@example.com",
        "password": "longenoughpassword",
    })
    .to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signup",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        201,
        "signup body: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["ok"], true);

    // Verify email out of band (the email gets sent through the stub mailer).
    auth.request_email_verification(
        &tenant_parsed,
        r.json()["user_id"].as_str().unwrap().parse().unwrap(),
    )
    .await
    .unwrap();
    let tok = last_token(&mailer);
    let body = serde_json::json!({"tenant_id": tenant, "token": tok}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/verify-email",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 200, "verify-email body: {}", String::from_utf8_lossy(&r.body));

    // Now signin and assert tokens come back.
    let body = serde_json::json!({
        "tenant_id": tenant,
        "email": "ss@example.com",
        "password": "longenoughpassword",
    })
    .to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signin",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 200, "signin body: {}", String::from_utf8_lossy(&r.body));
    let v = r.json();
    assert!(v["access_token"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(v["refresh_token"].as_str().is_some_and(|s| !s.is_empty()));

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_required() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(addr, "GET", "/rest/v1/foo", &[], None).await;
    assert_eq!(r.status, 401);
    let body = r.json();
    assert_eq!(body["code"], "E_UNAUTHENTICATED");
    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_jwt_returns_401() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/foo",
        &[("Authorization", "Bearer not.a.jwt")],
        None,
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.json()["code"], "E_UNAUTHENTICATED");
    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crud_round_trip() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();

    // CREATE TABLE out-of-band via the engine session.
    let session = svc.inner.cfg.engine.open_session(tenant).await.unwrap();
    session
        .execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &tenant, "ct@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // POST a single object.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/items",
        &[("Authorization", &bearer), ("Content-Type", "application/json")],
        Some(br#"{"id": 1, "name": "alpha"}"#),
    )
    .await;
    assert_eq!(r.status, 201, "POST body: {}", String::from_utf8_lossy(&r.body));

    // POST an array.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/items",
        &[("Authorization", &bearer), ("Content-Type", "application/json")],
        Some(br#"[{"id": 2, "name": "beta"}, {"id": 3, "name": "gamma"}]"#),
    )
    .await;
    assert_eq!(r.status, 201);

    // GET back via REST.
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/items?order=id.asc",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().expect("array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], 1);
    assert_eq!(arr[0]["name"], "alpha");
    assert_eq!(arr[2]["name"], "gamma");

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn select_cap_enforced() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();

    let session = svc.inner.cfg.engine.open_session(tenant).await.unwrap();
    session
        .execute("CREATE TABLE big (id BIGINT NOT NULL)")
        .await
        .unwrap();
    // Insert 60 rows; max_page_size in the test fixture is 50.
    let values: Vec<String> = (0..60).map(|i| format!("({i})")).collect();
    let sql = format!("INSERT INTO big VALUES {}", values.join(", "));
    session.execute(&sql).await.unwrap();

    let toks = make_user(&auth, &mailer, &tenant, "cap@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = http_request(
        addr,
        "GET",
        "/rest/v1/big?select=*&limit=10000",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    assert_eq!(arr.as_array().unwrap().len(), 50, "limit must be capped to max_page_size");

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_size_limit_enforced() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    // Build a body well over max_body_bytes (256 in the test fixture).
    let big = "x".repeat(2000);
    let body = format!(r#"{{"id": 1, "name": "{big}"}}"#);
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/anything",
        &[
            ("Authorization", "Bearer doesntmatter"),
            ("Content-Type", "application/json"),
        ],
        Some(body.as_bytes()),
    )
    .await;
    // axum's DefaultBodyLimit returns 413 with no JSON body. Either 413 or
    // a connection-close shaped status counts as a "you got cut off" — assert
    // 413 explicitly because that's the documented behaviour.
    assert_eq!(r.status, 413, "body: {}", String::from_utf8_lossy(&r.body));

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eq_filter() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();

    let session = svc.inner.cfg.engine.open_session(tenant).await.unwrap();
    session
        .execute("CREATE TABLE rows (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO rows VALUES (40, 'a'), (41, 'b'), (42, 'c'), (43, 'd')")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &tenant, "eq@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = http_request(
        addr,
        "GET",
        "/rest/v1/rows?id=eq.42",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], 42);
    assert_eq!(arr[0]["name"], "c");

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn order_and_pagination() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();

    let session = svc.inner.cfg.engine.open_session(tenant).await.unwrap();
    session
        .execute("CREATE TABLE seq (id BIGINT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO seq VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &tenant, "ord@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // order=id.desc, limit=2, offset=1 → expect [4, 3].
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/seq?order=id.desc&limit=2&offset=1",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], 4);
    assert_eq!(arr[1]["id"], 3);

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_round_trip() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();

    // CREATE TABLE out-of-band via the engine session.
    let session = svc.inner.cfg.engine.open_session(tenant).await.unwrap();
    session
        .execute("CREATE TABLE patches (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO patches VALUES (1, 'before'), (2, 'other')")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &tenant, "patch@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // PATCH the row with id=1.
    let r = http_request(
        addr,
        "PATCH",
        "/rest/v1/patches?id=eq.1",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{"name": "after"}"#),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "PATCH body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    assert_eq!(v["ok"], true);
    assert_eq!(v["tag"], "UPDATE 1");

    // GET id=1 — must reflect the PATCH.
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/patches?id=eq.1",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "after");

    // Other row untouched.
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/patches?id=eq.2",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["name"], "other");

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_requires_filter() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let tenant = TenantId::new();
    let toks = make_user(&auth, &mailer, &tenant, "patchnf@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = http_request(
        addr,
        "PATCH",
        "/rest/v1/anything",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{"name": "x"}"#),
    )
    .await;
    assert_eq!(r.status, 400);
    assert_eq!(r.json()["code"], "E_INVALID_REQUEST");

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cors_preflight() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(
        addr,
        "OPTIONS",
        "/rest/v1/foo",
        &[
            ("Origin", "https://app.example.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
        None,
    )
    .await;
    assert!(
        r.header("access-control-allow-origin") == Some("https://app.example.com"),
        "headers: {:?}",
        r.headers
    );
    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cors_disallowed_origin() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(
        addr,
        "OPTIONS",
        "/rest/v1/foo",
        &[
            ("Origin", "https://evil.example.com"),
            ("Access-Control-Request-Method", "GET"),
        ],
        None,
    )
    .await;
    // Disallowed origin: no `Access-Control-Allow-Origin` header should be
    // emitted (the browser then refuses the request).
    assert!(
        r.header("access-control-allow-origin").is_none(),
        "disallowed origin must not be reflected back: {:?}",
        r.headers
    );
    let _ = running.shutdown.send(());
}
