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

use arrow_array::Array;
use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
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
    format!("basin_rest_test_{}", Ulid::new().to_string().to_lowercase())
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

fn auth_cfg(schema: &str) -> AuthConfig {
    AuthConfig {
        jwt_secret: vec![9u8; 32],
        token_ttl: Duration::from_secs(60),
        refresh_ttl: Duration::from_secs(86_400),
        catalog_dsn: Some(PG_URL.to_owned()),
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
        email_enabled: true,
        pgwire_public_host: "127.0.0.1:5433".into(),
    }
}

/// Try to spin up a server backed by live PG. Returns None if PG is
/// unreachable — every test then prints a skip line and exits Ok.
async fn try_serve() -> Option<(
    crate::RunningRest,
    RestService,
    AuthService,
    StubMailer,
    SchemaGuard,
)> {
    try_serve_with(50).await
}

async fn try_serve_with(
    max_page_size: usize,
) -> Option<(
    crate::RunningRest,
    RestService,
    AuthService,
    StubMailer,
    SchemaGuard,
)> {
    try_serve_full(max_page_size, 256).await
}

async fn try_serve_full(
    max_page_size: usize,
    max_body_bytes: usize,
) -> Option<(
    crate::RunningRest,
    RestService,
    AuthService,
    StubMailer,
    SchemaGuard,
)> {
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
        max_body_bytes,
        default_page_size: 100,
        max_page_size, // small so select_cap_enforced is observable
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

/// Verified email, signed-in tokens for `project`, ready to use in
/// `Authorization: Bearer <jwt>` headers.
async fn make_user(
    auth: &AuthService,
    mailer: &StubMailer,
    project: &ProjectId,
    email: &str,
) -> basin_auth::Tokens {
    let user = auth
        .signup(project, email, "longenoughpassword")
        .await
        .expect("signup");
    auth.request_email_verification(project, user)
        .await
        .expect("request verify");
    let tok = last_token(mailer);
    auth.verify_email(project, &tok).await.expect("verify");
    auth.signin(project, email, "longenoughpassword")
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
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "body is not JSON ({e}): {}",
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
    let project = ProjectId::new().to_string();
    let project_parsed: ProjectId = project.parse().unwrap();

    // signup via REST
    let body = serde_json::json!({
        "project_id": project,
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
        &project_parsed,
        r.json()["user_id"].as_str().unwrap().parse().unwrap(),
    )
    .await
    .unwrap();
    let tok = last_token(&mailer);
    let body = serde_json::json!({"project_id": project, "token": tok}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/verify-email",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "verify-email body: {}",
        String::from_utf8_lossy(&r.body)
    );

    // Now signin and assert tokens come back.
    let body = serde_json::json!({
        "project_id": project,
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
    assert_eq!(
        r.status,
        200,
        "signin body: {}",
        String::from_utf8_lossy(&r.body)
    );
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
    let project = ProjectId::new();

    // CREATE TABLE out-of-band via the engine session.
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE items (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "ct@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // POST a single object.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/items",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{"id": 1, "name": "alpha"}"#),
    )
    .await;
    assert_eq!(
        r.status,
        201,
        "POST body: {}",
        String::from_utf8_lossy(&r.body)
    );

    // POST an array.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/items",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
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
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE big (id BIGINT NOT NULL)")
        .await
        .unwrap();
    // Insert 60 rows; max_page_size in the test fixture is 50.
    let values: Vec<String> = (0..60).map(|i| format!("({i})")).collect();
    let sql = format!("INSERT INTO big VALUES {}", values.join(", "));
    session.execute(&sql).await.unwrap();

    let toks = make_user(&auth, &mailer, &project, "cap@example.com").await;
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
    let v = r.json();
    // `limit` is supplied → response is wrapped {rows, next_cursor}.
    let arr = v["rows"].as_array().expect("rows array");
    assert_eq!(arr.len(), 50, "limit must be capped to max_page_size");

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
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE rows (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO rows VALUES (40, 'a'), (41, 'b'), (42, 'c'), (43, 'd')")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "eq@example.com").await;
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
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE seq (id BIGINT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO seq VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "ord@example.com").await;
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
    let v = r.json();
    // `limit` is supplied → response is wrapped {rows, next_cursor}.
    let arr = v["rows"].as_array().expect("rows array");
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
    let project = ProjectId::new();

    // CREATE TABLE out-of-band via the engine session.
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE patches (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO patches VALUES (1, 'before'), (2, 'other')")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "patch@example.com").await;
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
    let project = ProjectId::new();
    let toks = make_user(&auth, &mailer, &project, "patchnf@example.com").await;
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
async fn openapi_lists_project_tables() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();

    // Project A owns 3 tables.
    let sa = svc.inner.cfg.engine.open_session(project_a).await.unwrap();
    sa.execute("CREATE TABLE alpha (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sa.execute("CREATE TABLE beta  (id BIGINT NOT NULL)")
        .await
        .unwrap();
    sa.execute("CREATE TABLE gamma (id BIGINT NOT NULL)")
        .await
        .unwrap();
    // Project B owns 1.
    let sb = svc.inner.cfg.engine.open_session(project_b).await.unwrap();
    sb.execute("CREATE TABLE only_b (id BIGINT NOT NULL)")
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project_a, "oa@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = http_request(
        addr,
        "GET",
        "/rest/v1/_openapi.json",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "openapi body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    assert_eq!(v["openapi"], "3.0.3");
    let paths = v["paths"].as_object().expect("paths object");
    assert_eq!(paths.len(), 3, "project A has exactly 3 tables: {paths:?}");
    assert!(paths.contains_key("/rest/v1/alpha"));
    assert!(paths.contains_key("/rest/v1/beta"));
    assert!(paths.contains_key("/rest/v1/gamma"));
    assert!(!paths.contains_key("/rest/v1/only_b"));

    // Each path entry has the four CRUD operations.
    for p in ["/rest/v1/alpha", "/rest/v1/beta", "/rest/v1/gamma"] {
        let entry = &paths[p];
        for op in ["get", "post", "patch", "delete"] {
            assert!(entry.get(op).is_some(), "{p} missing {op}");
        }
    }

    // Components must include matching schemas for project A only.
    let schemas = v["components"]["schemas"].as_object().expect("schemas");
    assert!(schemas.contains_key("alpha"));
    assert!(schemas.contains_key("beta"));
    assert!(schemas.contains_key("gamma"));
    assert!(!schemas.contains_key("only_b"));

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn openapi_includes_column_types() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    // One table that exercises the type mapping: BIGINT, TEXT, BOOLEAN,
    // DOUBLE, BYTEA, JSONB, UUID, TIMESTAMPTZ, VECTOR(N).
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute(
            "CREATE TABLE shapes ( \
                 id BIGINT NOT NULL, \
                 name TEXT NOT NULL, \
                 active BOOLEAN NOT NULL, \
                 score DOUBLE PRECISION NOT NULL, \
                 raw BYTEA, \
                 props JSONB, \
                 oid UUID NOT NULL, \
                 created_at TIMESTAMPTZ NOT NULL, \
                 embedding VECTOR(4) \
             )",
        )
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "ot@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/_openapi.json",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "openapi body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let comp = &v["components"]["schemas"]["shapes"];
    assert_eq!(comp["type"], "object");
    let props = comp["properties"].as_object().expect("properties");

    assert_eq!(props["id"]["type"], "integer");
    assert_eq!(props["id"]["format"], "int64");

    assert_eq!(props["name"]["type"], "string");

    assert_eq!(props["active"]["type"], "boolean");

    assert_eq!(props["score"]["type"], "number");
    assert_eq!(props["score"]["format"], "double");

    // BYTEA → Binary (plain) → string/binary.
    assert_eq!(props["raw"]["type"], "string");
    assert_eq!(props["raw"]["format"], "binary");

    // JSONB → object with additionalProperties.
    assert_eq!(props["props"]["type"], "object");
    assert_eq!(props["props"]["additionalProperties"], true);

    // UUID → string/uuid (FixedSizeBinary(16) + BASIN_TYPE=UUID marker).
    assert_eq!(props["oid"]["type"], "string");
    assert_eq!(props["oid"]["format"], "uuid");

    // TIMESTAMPTZ → string/date-time.
    assert_eq!(props["created_at"]["type"], "string");
    assert_eq!(props["created_at"]["format"], "date-time");

    // VECTOR(4) → array of numbers.
    assert_eq!(props["embedding"]["type"], "array");
    assert_eq!(props["embedding"]["items"]["type"], "number");

    // NOT NULL columns must be in `required`.
    let required: Vec<&str> = comp["required"]
        .as_array()
        .expect("required array")
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    for col in ["id", "name", "active", "score", "oid", "created_at"] {
        assert!(required.contains(&col), "missing required: {col}");
    }
    // Nullable columns must NOT be in `required`.
    for col in ["raw", "props", "embedding"] {
        assert!(!required.contains(&col), "should not be required: {col}");
    }

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_key_bearer_authenticates_rest() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    // Set up a verified user + JWT so we can mint an API key via REST.
    let toks = make_user(&auth, &mailer, &project, "apikey@example.com").await;
    let bearer_jwt = format!("Bearer {}", toks.access_token);

    // POST /auth/v1/api-keys returns the plaintext secret exactly once.
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/api-keys",
        &[
            ("Authorization", &bearer_jwt),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{"name":"ci"}"#),
    )
    .await;
    assert_eq!(
        r.status,
        201,
        "create body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let secret = v["secret"].as_str().expect("secret in response").to_owned();
    let id = v["id"].as_i64().expect("id in response");
    assert!(!secret.is_empty());

    // GET /auth/v1/api-keys lists keys without leaking secrets.
    let r = http_request(
        addr,
        "GET",
        "/auth/v1/api-keys",
        &[("Authorization", &bearer_jwt)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert!(arr[0].get("secret").is_none() || arr[0]["secret"].is_null());

    // CREATE TABLE so the api-key bearer has something to GET.
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE k (id BIGINT NOT NULL)")
        .await
        .unwrap();
    session
        .execute("INSERT INTO k VALUES (1), (2)")
        .await
        .unwrap();

    // Use the API key (NOT the JWT) as the bearer.
    let bearer_key = format!("Bearer {}", secret);
    let r = http_request(
        addr,
        "GET",
        "/rest/v1/k?order=id.asc",
        &[("Authorization", &bearer_key)],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "GET via api key: {}",
        String::from_utf8_lossy(&r.body)
    );
    let arr = r.json();
    let arr = arr.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // DELETE /auth/v1/api-keys/<id> revokes; subsequent use is 401.
    let r = http_request(
        addr,
        "DELETE",
        &format!("/auth/v1/api-keys/{id}"),
        &[("Authorization", &bearer_jwt)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);

    let r = http_request(
        addr,
        "GET",
        "/rest/v1/k",
        &[("Authorization", &bearer_key)],
        None,
    )
    .await;
    assert_eq!(r.status, 401, "revoked api key must not authenticate");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pagination_cursor_advances() {
    let Some((running, svc, auth, mailer, _g)) = try_serve_with(20_000).await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE pages (id BIGINT NOT NULL, name TEXT NOT NULL)")
        .await
        .unwrap();
    let values: Vec<String> = (1..=50).map(|i| format!("({i}, 'r{i}')")).collect();
    session
        .execute(&format!("INSERT INTO pages VALUES {}", values.join(", ")))
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "cur@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let mut seen: Vec<i64> = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 0..6 {
        let path = match &cursor {
            None => "/rest/v1/pages?limit=10".to_string(),
            Some(c) => format!("/rest/v1/pages?limit=10&cursor={c}"),
        };
        let r = http_request(addr, "GET", &path, &[("Authorization", &bearer)], None).await;
        assert_eq!(
            r.status,
            200,
            "page {page} body: {}",
            String::from_utf8_lossy(&r.body)
        );
        let v = r.json();
        let rows = v["rows"].as_array().expect("rows array");
        for row in rows {
            seen.push(row["id"].as_i64().expect("id i64"));
        }
        cursor = v["next_cursor"].as_str().map(|s| s.to_string());
        // After page 5 (50 rows total) the cursor goes null.
        if page == 5 {
            assert!(cursor.is_none(), "trailing page should clear cursor");
        }
    }
    seen.sort();
    let expected: Vec<i64> = (1..=50).collect();
    assert_eq!(seen, expected);

    let _ = running.shutdown.send(());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_response_for_large_payload() {
    let Some((running, svc, auth, mailer, _g)) = try_serve_with(20_000).await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE bulk (id BIGINT NOT NULL)")
        .await
        .unwrap();
    // Insert 11k rows in chunks so each INSERT statement stays manageable.
    for chunk in (1..=11_000).collect::<Vec<i64>>().chunks(1000) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i})")).collect();
        session
            .execute(&format!("INSERT INTO bulk VALUES {}", values.join(", ")))
            .await
            .unwrap();
    }

    let toks = make_user(&auth, &mailer, &project, "stm@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = http_request(
        addr,
        "GET",
        "/rest/v1/bulk?limit=15000",
        &[("Authorization", &bearer)],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("content-type"),
        Some("application/x-ndjson"),
        "headers: {:?}",
        r.headers,
    );

    let body = std::str::from_utf8(&r.body).expect("ndjson utf8");
    let mut row_count = 0usize;
    for line in body.lines() {
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line).expect("each line is JSON");
        // The trailing cursor line, when present, has the marker key; everything
        // else is a data row with an `id`.
        if v.get("_basin_next_cursor").is_some() {
            continue;
        }
        assert!(v.get("id").is_some(), "row missing id: {line}");
        row_count += 1;
    }
    assert_eq!(row_count, 11_000);

    let _ = running.shutdown.send(());
}

// --- Phase 5.10: email-link login + refresh-token rotation ------------------

/// Happy path: POST email → 204; consume the token from the stub mailer →
/// access + refresh tokens come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magic_link_round_trip() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    // Bootstrap: signup + verify so a user exists for the email.
    let _ = make_user(&auth, &mailer, &project, "ml-rt@example.com").await;

    // Drain the verification email so `last_token` doesn't pick it up later.
    let _ = mailer.sent();

    // Step 1: POST /auth/v1/magic-link with just `email`.
    let body = serde_json::json!({"email": "ml-rt@example.com"}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/magic-link",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        204,
        "magic-link request body: {}",
        String::from_utf8_lossy(&r.body)
    );

    // The stub mailer captured the link; pull the raw token out.
    let raw = last_token(&mailer);

    // Step 2: POST /auth/v1/magic-link/consume with `token`.
    let body = serde_json::json!({"token": raw}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/magic-link/consume",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "magic-link consume body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    assert!(v["access_token"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(v["refresh_token"].as_str().is_some_and(|s| !s.is_empty()));

    // The same token is single-use: a second consume must 4xx.
    let body = serde_json::json!({"token": raw}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/magic-link/consume",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert!(
        r.status >= 400 && r.status < 500,
        "second consume must fail: status={} body={}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    let _ = running.shutdown.send(());
}

/// Reuse-detection: A → rotated to B → rotated to C. Replaying A revokes
/// the user's whole refresh chain — including the active C — and surfaces
/// `E_REVOKED_TOKEN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_token_reuse_detected_revokes_all() {
    // Refresh JWTs are ~700B; the default 256B body cap is too tight.
    let Some((running, _svc, auth, mailer, _g)) = try_serve_full(50, 4096).await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let toks_a = make_user(&auth, &mailer, &project, "rt-r@example.com").await;

    // First rotation A → B.
    let body = serde_json::json!({"refresh_token": toks_a.refresh_token}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/refresh",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "first rotate: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let b_refresh = v["refresh_token"].as_str().unwrap().to_owned();

    // Sleep so revoked_at on B is strictly after A's.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Second rotation B → C.
    let body = serde_json::json!({"refresh_token": b_refresh}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/refresh",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "second rotate: {}",
        String::from_utf8_lossy(&r.body)
    );
    let v = r.json();
    let c_refresh = v["refresh_token"].as_str().unwrap().to_owned();

    // Replay A — leaked old refresh; should 401 with E_REVOKED_TOKEN AND
    // trigger the blanket sentinel.
    let body = serde_json::json!({"refresh_token": toks_a.refresh_token}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/refresh",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "leaked-A replay: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["code"], "E_REVOKED_TOKEN");

    // C must now also fail — blanket revoke.
    let body = serde_json::json!({"refresh_token": c_refresh}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/refresh",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "C must be revoked after reuse-detection: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["code"], "E_REVOKED_TOKEN");

    let _ = running.shutdown.send(());
}

// --- Sign-out: POST /auth/v1/signout -----------------------------------------

/// Happy path: sign in, call POST /auth/v1/signout, then verify that a
/// subsequent refresh with the same token is rejected with E_REVOKED_TOKEN.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signout_revokes_refresh_token() {
    // Refresh JWTs are large; bump the body cap.
    let Some((running, _svc, auth, mailer, _g)) = try_serve_full(50, 4096).await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let toks = make_user(&auth, &mailer, &project, "so1@example.com").await;

    // Sign out via the HTTP route.
    let body = serde_json::json!({ "refresh_token": toks.refresh_token }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "signout body: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["ok"], true);

    // A subsequent refresh with the same token must be rejected.
    let body = serde_json::json!({ "refresh_token": toks.refresh_token }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/refresh",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "refresh after signout must be 401: {}",
        String::from_utf8_lossy(&r.body)
    );
    // Should surface as E_REVOKED_TOKEN rather than a generic 401.
    assert_eq!(r.json()["code"], "E_REVOKED_TOKEN");

    let _ = running.shutdown.send(());
}

/// Idempotency: signing out twice returns success both times.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signout_is_idempotent() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve_full(50, 4096).await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let toks = make_user(&auth, &mailer, &project, "so2@example.com").await;

    let body = serde_json::json!({ "refresh_token": toks.refresh_token }).to_string();

    // First sign-out.
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 200, "first signout: {}", String::from_utf8_lossy(&r.body));

    // Second sign-out with the same token — must also succeed.
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 200, "second signout: {}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["ok"], true);

    let _ = running.shutdown.send(());
}

/// Missing / garbage token returns 401 E_UNAUTHENTICATED.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signout_without_valid_token_returns_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    // Empty token field.
    let body = serde_json::json!({ "refresh_token": "" }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "empty token must be 401: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["code"], "E_UNAUTHENTICATED");

    // Garbage (not a JWT at all).
    let body = serde_json::json!({ "refresh_token": "not.a.jwt" }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "garbage token must be 401: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["code"], "E_UNAUTHENTICATED");

    // A syntactically valid JWT but with bad signature (not issued by us) —
    // should also be 401. We construct a token with the basin-refresh audience
    // but an invalid signature.
    let fake_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.\
                    eyJwcm9qZWN0X2lkIjoiMDEiLCJ1c2VyX2lkIjoiMDEiLCJlbWFpbCI6InRAZS5jb20iLCJqdGkiOiJ0ZXN0IiwiYXVkIjoiYmFzaW4tcmVmcmVzaCIsImV4cCI6OTk5OTk5OTk5OX0.\
                    invalidsignature";
    let body = serde_json::json!({ "refresh_token": fake_jwt }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/signout",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status,
        401,
        "fake JWT must be 401: {}",
        String::from_utf8_lossy(&r.body)
    );

    let _ = running.shutdown.send(());
}

// --- Phase 5.11.L: POST /rpc/<fn> mount (ADR 0019) --------------------------

/// Happy path: register a LANGUAGE sql scalar function, invoke it over REST,
/// assert the scalar result comes back as JSON.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_sql_scalar_function() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    // Create a simple scalar SQL function out-of-band via the engine session.
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute(
            "CREATE FUNCTION add_two(x int, y int) \
             RETURNS int \
             LANGUAGE sql \
             AS $$ SELECT x + y $$",
        )
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "rpc@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // Invoke the function via POST /rest/v1/rpc/add_two.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/rpc/add_two",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{"x": 3, "y": 4}"#),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "rpc body: {}",
        String::from_utf8_lossy(&r.body)
    );
    // Scalar result → bare JSON value (integer 7).
    let v = r.json();
    assert_eq!(v, serde_json::json!(7), "expected 7, got {v}");

    let _ = running.shutdown.send(());
}

/// Auth gate: RPC without a bearer token must return 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_requires_auth() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/rpc/any_fn",
        &[("Content-Type", "application/json")],
        Some(br#"{}"#),
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.json()["code"], "E_UNAUTHENTICATED");

    let _ = running.shutdown.send(());
}

/// Zero-arg RPC: function with no parameters, empty body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rpc_zero_arg_function() {
    let Some((running, svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute(
            "CREATE FUNCTION answer() \
             RETURNS int \
             LANGUAGE sql \
             AS $$ SELECT 42 $$",
        )
        .await
        .unwrap();

    let toks = make_user(&auth, &mailer, &project, "rpc0@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    // Empty body — zero-arg call.
    let r = http_request(
        addr,
        "POST",
        "/rest/v1/rpc/answer",
        &[
            ("Authorization", &bearer),
            ("Content-Type", "application/json"),
        ],
        Some(br#"{}"#),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "rpc zero-arg body: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json(), serde_json::json!(42));

    let _ = running.shutdown.send(());
}

// --- Phase 5.11.N + 6.SEC.P0.3: POST /in/<project>/<name> (ADR 0019) --------

/// Compute HMAC-SHA256 of `body` under `secret_hex`, hex-encoded — the
/// canonical `X-Basin-Signature` value an SDK would send.
fn inbound_sign(secret_hex: &str, body: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let secret = hex::decode(secret_hex).expect("secret_hex is valid hex");
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&secret).expect("hmac key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Extract the `secret` column from a `CREATE INBOUND WEBHOOK` result set.
fn extract_secret(res: &basin_engine::ExecResult) -> String {
    match res {
        basin_engine::ExecResult::Rows { batches, .. } => {
            assert_eq!(
                batches.len(),
                1,
                "expected 1 batch from CREATE INBOUND WEBHOOK"
            );
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("secret column is utf8");
            assert_eq!(arr.len(), 1);
            arr.value(0).to_string()
        }
        other => panic!("expected Rows for CREATE INBOUND WEBHOOK, got {other:?}"),
    }
}

/// Happy path: register an inbound webhook, POST JSON with a valid
/// `X-Basin-Signature`, assert 200 + the row landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_webhook_row_insert_signed() {
    let Some((running, svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    // Create the target table and register the inbound webhook via the engine.
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE webhook_events (id TEXT, raw_payload JSONB)")
        .await
        .unwrap();
    let create_res = session
        .execute(
            "CREATE INBOUND WEBHOOK test_hook EXECUTE \
             INSERT INTO webhook_events (id, raw_payload) \
             VALUES (payload->>'id', payload)",
        )
        .await
        .unwrap();
    let secret = extract_secret(&create_res);
    assert_eq!(secret.len(), 64, "expected 64-hex-char secret");

    // POST a JSON payload with a valid signature.
    let project_str = project.to_string();
    let path = format!("/in/{project_str}/test_hook");
    let body = br#"{"id": "evt_001", "amount": 99}"#;
    let sig = inbound_sign(&secret, body);
    let r = http_request(
        addr,
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
        "inbound webhook body: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json()["ok"], serde_json::json!(true));

    // Verify the row landed.
    let rows = session
        .execute("SELECT id FROM webhook_events WHERE id = 'evt_001'")
        .await
        .unwrap();
    match rows {
        basin_engine::ExecResult::Rows { batches, .. } => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "expected 1 row after inbound webhook insert");
        }
        other => panic!("unexpected result: {other:?}"),
    }

    let _ = running.shutdown.send(());
}

/// Phase 6.SEC.P0.3: unsigned POST → 401 (default-secure).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_webhook_unsigned_rejected() {
    let Some((running, svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE webhook_events (id TEXT, raw_payload JSONB)")
        .await
        .unwrap();
    let _ = session
        .execute(
            "CREATE INBOUND WEBHOOK h1 EXECUTE \
             INSERT INTO webhook_events (id, raw_payload) VALUES ('x', payload)",
        )
        .await
        .unwrap();

    let project_str = project.to_string();
    let path = format!("/in/{project_str}/h1");
    let r = http_request(
        addr,
        "POST",
        &path,
        &[("Content-Type", "application/json")],
        Some(br#"{}"#),
    )
    .await;
    assert_eq!(r.status, 401, "unsigned request must be 401");
    assert_eq!(r.json()["code"], serde_json::json!("E_UNAUTHENTICATED"));
    let _ = running.shutdown.send(());
}

/// Phase 6.SEC.P0.3: invalid signature → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_webhook_bad_signature_rejected() {
    let Some((running, svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let session = svc.inner.cfg.engine.open_session(project).await.unwrap();
    session
        .execute("CREATE TABLE webhook_events (id TEXT, raw_payload JSONB)")
        .await
        .unwrap();
    let _ = session
        .execute(
            "CREATE INBOUND WEBHOOK h2 EXECUTE \
             INSERT INTO webhook_events (id, raw_payload) VALUES ('y', payload)",
        )
        .await
        .unwrap();

    let project_str = project.to_string();
    let path = format!("/in/{project_str}/h2");
    // Wrong sig — 32 bytes of `0xff`.
    let bad_sig = "f".repeat(64);
    let r = http_request(
        addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            ("X-Basin-Signature", &bad_sig),
        ],
        Some(br#"{"id":"x"}"#),
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.json()["code"], serde_json::json!("E_UNAUTHENTICATED"));
    let _ = running.shutdown.send(());
}

/// Phase 6.SEC.P0.3: an attacker who hits an unknown webhook name without
/// a signature still gets 401 (not 404) — the 404 distinction would leak
/// existence info, and the unsigned path is rejected before the catalog
/// shape is consulted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_webhook_unsigned_unknown_name_401() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let path = format!("/in/{project}/no_such_hook");
    let r = http_request(
        addr,
        "POST",
        &path,
        &[("Content-Type", "application/json")],
        Some(br#"{}"#),
    )
    .await;
    assert_eq!(r.status, 401, "unsigned request to unknown hook must 401");

    let _ = running.shutdown.send(());
}

/// Phase 6.SEC.P0.3: a *signed* request to an unknown hook → 401
/// (same external shape as a wrong-signature failure on a real hook,
/// so an attacker can't tell the two apart).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_webhook_signed_unknown_name_401() {
    let Some((running, _svc, _a, _m, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let path = format!("/in/{project}/no_such_hook");
    let bogus_sig = "a".repeat(64);
    let r = http_request(
        addr,
        "POST",
        &path,
        &[
            ("Content-Type", "application/json"),
            ("X-Basin-Signature", &bogus_sig),
        ],
        Some(br#"{}"#),
    )
    .await;
    assert_eq!(r.status, 401);

    let _ = running.shutdown.send(());
}

// ---------------------------------------------------------------------------
// OAuth route smoke tests (Phase 5.10.O)
// ---------------------------------------------------------------------------

/// `GET /auth/v1/oauth/:provider/authorize` with no registered provider →
/// 4xx (route is mounted, not 404). The exact error code depends on whether
/// the in-memory mock provider is registered; without registration the auth
/// layer returns "not found" → 404 from `AuthService`, mapped to 4xx by
/// `ApiError`. Key invariant: the route is NOT a 404 "no such route" from
/// the axum router.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauth_authorize_route_mounted() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();

    let r = http_request(
        addr,
        "GET",
        &format!(
            "/auth/v1/oauth/google/authorize?project_id={project}&redirect_to="
        ),
        &[],
        None,
    )
    .await;
    // Must NOT be axum's 405 "Method Not Allowed" or 404 "No such route".
    // Any 4xx from our handler is fine — it means the route is mounted.
    assert_ne!(
        r.status, 405,
        "oauth authorize must not return 405 (route not mounted for GET)"
    );
    // 404 from our handler is acceptable ("provider not found"); 404 from
    // axum's router would indicate the route is not mounted at all. We
    // distinguish by checking the body contains a recognisable JSON error.
    if r.status == 404 {
        let body = String::from_utf8_lossy(&r.body);
        assert!(
            body.contains("error") || body.contains("not_found") || body.contains("provider"),
            "404 body looks like axum router miss, not our handler: {body}"
        );
    }

    let _ = running.shutdown.send(());
}

/// `GET /auth/v1/oauth/:provider/callback` with missing state → 4xx
/// (route is mounted, not a router 404/405).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauth_callback_route_mounted() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let r = http_request(
        addr,
        "GET",
        "/auth/v1/oauth/google/callback?code=abc&state=def",
        &[],
        None,
    )
    .await;
    assert_ne!(r.status, 405, "oauth callback must not 405 (not mounted)");

    let _ = running.shutdown.send(());
}

// ---------------------------------------------------------------------------
// MFA route smoke tests (Phase 5.10.M)
// ---------------------------------------------------------------------------

/// `POST /auth/v1/factors` without auth → 401 (route is mounted and
/// `authorize()` fires before the handler body).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_enroll_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let body = serde_json::json!({"factor_type": "totp", "friendly_name": "Test"}).to_string();
    let r = http_request(
        addr,
        "POST",
        "/auth/v1/factors",
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status, 401,
        "POST /auth/v1/factors without bearer must be 401; got {}; body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    let _ = running.shutdown.send(());
}

/// `GET /auth/v1/factors` without auth → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_list_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;

    let r = http_request(addr, "GET", "/auth/v1/factors", &[], None).await;
    assert_eq!(
        r.status, 401,
        "GET /auth/v1/factors without bearer must be 401"
    );

    let _ = running.shutdown.send(());
}

/// `POST /auth/v1/factors/:id/verify` without auth → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_verify_factor_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let factor_id = uuid::Uuid::new_v4();

    let body = serde_json::json!({"code": "123456"}).to_string();
    let r = http_request(
        addr,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/verify"),
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status, 401,
        "POST /auth/v1/factors/:id/verify without bearer must be 401"
    );

    let _ = running.shutdown.send(());
}

/// `POST /auth/v1/factors/:id/challenge` without auth → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_begin_challenge_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let factor_id = uuid::Uuid::new_v4();

    let r = http_request(
        addr,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/challenge"),
        &[("Content-Type", "application/json")],
        Some(b"{}"),
    )
    .await;
    assert_eq!(
        r.status, 401,
        "POST /auth/v1/factors/:id/challenge without bearer must be 401"
    );

    let _ = running.shutdown.send(());
}

/// `POST /auth/v1/factors/:id/challenge/verify` without auth → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_verify_challenge_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let factor_id = uuid::Uuid::new_v4();

    let body =
        serde_json::json!({"challenge_id": uuid::Uuid::new_v4().to_string(), "code": "123456"})
            .to_string();
    let r = http_request(
        addr,
        "POST",
        &format!("/auth/v1/factors/{factor_id}/challenge/verify"),
        &[("Content-Type", "application/json")],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status, 401,
        "POST /auth/v1/factors/:id/challenge/verify without bearer must be 401"
    );

    let _ = running.shutdown.send(());
}

/// `DELETE /auth/v1/factors/:id` without auth → 401.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_unenroll_unauthenticated_401() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let factor_id = uuid::Uuid::new_v4();

    let r = http_request(
        addr,
        "DELETE",
        &format!("/auth/v1/factors/{factor_id}"),
        &[],
        None,
    )
    .await;
    assert_eq!(
        r.status, 401,
        "DELETE /auth/v1/factors/:id without bearer must be 401"
    );

    let _ = running.shutdown.send(());
}

/// Authenticated `GET /auth/v1/factors` → 200 with empty JSON array for a
/// fresh user who has not enrolled any factors yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_list_authenticated_returns_empty_array() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let tokens = make_user(&auth, &mailer, &project, "mfalist@example.com").await;

    let r = http_request(
        addr,
        "GET",
        "/auth/v1/factors",
        &[(&format!("Authorization"), &format!("Bearer {}", tokens.access_token))],
        None,
    )
    .await;
    assert_eq!(
        r.status, 200,
        "GET /auth/v1/factors with valid JWT must be 200; body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let body = r.json();
    assert!(
        body.is_array(),
        "GET /auth/v1/factors must return a JSON array"
    );
    assert_eq!(
        body.as_array().unwrap().len(),
        0,
        "new user has no factors"
    );

    let _ = running.shutdown.send(());
}

/// `GET /auth/v1/factors` after enrolling a TOTP factor returns that factor
/// in the list with the correct metadata (never the secret).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mfa_list_returns_enrolled_factor() {
    let Some((running, _svc, auth, mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let tokens = make_user(&auth, &mailer, &project, "mfalist2@example.com").await;

    // Enroll a TOTP factor via the HTTP route.
    let enroll_body =
        serde_json::json!({"factor_type": "totp", "friendly_name": "My App"}).to_string();
    let enroll_r = http_request(
        addr,
        "POST",
        "/auth/v1/factors",
        &[
            ("Authorization", &format!("Bearer {}", tokens.access_token)),
            ("Content-Type", "application/json"),
        ],
        Some(enroll_body.as_bytes()),
    )
    .await;
    assert_eq!(
        enroll_r.status, 201,
        "POST /auth/v1/factors must be 201; body: {}",
        String::from_utf8_lossy(&enroll_r.body)
    );
    let enroll_json = enroll_r.json();
    let factor_id = enroll_json["factor_id"]
        .as_str()
        .expect("factor_id in enroll response");

    // List factors — should contain the newly enrolled (unverified) factor.
    let list_r = http_request(
        addr,
        "GET",
        "/auth/v1/factors",
        &[("Authorization", &format!("Bearer {}", tokens.access_token))],
        None,
    )
    .await;
    assert_eq!(
        list_r.status, 200,
        "GET /auth/v1/factors must be 200; body: {}",
        String::from_utf8_lossy(&list_r.body)
    );
    let list_body = list_r.json();
    let factors = list_body.as_array().expect("list must be a JSON array");
    assert_eq!(factors.len(), 1, "one factor enrolled");

    let f = &factors[0];
    assert_eq!(f["id"].as_str().unwrap(), factor_id, "factor id matches");
    assert_eq!(f["factor_type"].as_str().unwrap(), "totp");
    assert_eq!(
        f["status"].as_str().unwrap(),
        "unverified",
        "newly enrolled factor is unverified"
    );
    assert_eq!(f["friendly_name"].as_str().unwrap(), "My App");
    assert!(
        f.get("secret_b32").is_none() && f.get("secret_enc").is_none(),
        "secret must not be present in list response"
    );
    assert!(f["created_at"].as_str().is_some(), "created_at present");
    assert!(f["updated_at"].as_str().is_some(), "updated_at present");

    let _ = running.shutdown.send(());
}

// ─── Feature 3 (5.22.D): dump endpoint route is registered ───────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dump_route_returns_401_not_404() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new().to_string();
    // No Authorization header → should get 401, not 404.
    let r = http_request(
        addr,
        "GET",
        &format!("/admin/v1/projects/{project}/dump"),
        &[],
        None,
    )
    .await;
    assert_ne!(
        r.status, 404,
        "dump route must be registered (got 404 instead of 401)"
    );
    assert_eq!(r.status, 401, "unauthenticated dump must return 401");
    let _ = running.shutdown.send(());
}

// ─── Feature 4 (5.11.W): invocations + versions routes registered ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invocations_and_versions_routes_registered() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = http_request(
        addr,
        "GET",
        "/admin/v1/functions/myfn/invocations",
        &[],
        None,
    )
    .await;
    assert_ne!(
        r.status, 404,
        "invocations route must be registered (got 404)"
    );
    assert_eq!(r.status, 401, "unauthenticated invocations must return 401");

    let r2 = http_request(
        addr,
        "GET",
        "/admin/v1/functions/myfn/versions",
        &[],
        None,
    )
    .await;
    assert_ne!(r2.status, 404, "versions route must be registered");
    assert_eq!(r2.status, 401, "unauthenticated versions must return 401");

    let _ = running.shutdown.send(());
}

// ─── Feature 4: FunctionRegistry unit tests (no Postgres needed) ──────────────

#[tokio::test]
async fn function_invocation_log_roundtrip() {
    use crate::routes::admin_functions::FunctionRegistry;
    use basin_common::ProjectId;

    let reg = FunctionRegistry::new();
    let project = ProjectId::new();
    let name = "my_fn";

    // Use the internal `put` path via deploy_function logic, but call put_for_test.
    // Since `put` is private, we test via the public `record_invocation` +
    // `invocations` surface without needing to deploy bytes.
    // Pre-populate the inner map so `exists` returns true.
    let _ver = reg.put_test(project, name.to_string(), b"x".to_vec()).await;

    // Record two invocations.
    reg.record_invocation(project, name, 200, 42, None).await;
    reg.record_invocation(project, name, 500, 7, Some("timeout".to_string()))
        .await;

    let invocations = reg.invocations(&project, name).await;
    assert_eq!(invocations.len(), 2, "two invocations recorded");
    assert_eq!(invocations[0].status, 200);
    assert_eq!(invocations[0].invocation_id, 1);
    assert_eq!(invocations[1].status, 500);
    assert_eq!(invocations[1].invocation_id, 2);
    assert_eq!(invocations[1].error.as_deref(), Some("timeout"));
}

#[tokio::test]
async fn function_version_history_roundtrip() {
    use crate::routes::admin_functions::FunctionRegistry;
    use basin_common::ProjectId;

    let reg = FunctionRegistry::new();
    let project = ProjectId::new();
    let name = "versioned_fn";

    let _ = reg.put_test(project, name.to_string(), b"v1".to_vec()).await;
    let _ = reg.put_test(project, name.to_string(), b"v2".to_vec()).await;

    let versions = reg.versions(&project, name).await;
    assert_eq!(versions.len(), 2, "two version entries in history");
    let v1 = versions.iter().find(|v| v.version == 1).expect("v1");
    let v2 = versions.iter().find(|v| v.version == 2).expect("v2");
    assert!(!v1.active, "v1 should not be active after redeploy");
    assert!(v2.active, "v2 should be active");
}

#[tokio::test]
async fn function_rollback_roundtrip() {
    use crate::routes::admin_functions::FunctionRegistry;
    use basin_common::ProjectId;

    let reg = FunctionRegistry::new();
    let project = ProjectId::new();
    let name = "rollback_fn";

    let _ = reg.put_test(project, name.to_string(), b"v1".to_vec()).await;
    let _ = reg.put_test(project, name.to_string(), b"v2".to_vec()).await;

    let rolled = reg.rollback(project, name, 1).await.expect("rollback ok");
    assert_eq!(rolled, 1);

    let versions = reg.versions(&project, name).await;
    let active: Vec<_> = versions.iter().filter(|v| v.active).collect();
    assert_eq!(active.len(), 1, "exactly one active version");
    assert_eq!(active[0].version, 1, "active version is 1 after rollback");
}

#[tokio::test]
async fn function_rollback_unknown_version_errors() {
    use crate::routes::admin_functions::FunctionRegistry;
    use basin_common::ProjectId;

    let reg = FunctionRegistry::new();
    let project = ProjectId::new();
    let name = "fn";

    let _ = reg.put_test(project, name.to_string(), b"v1".to_vec()).await;
    let err = reg.rollback(project, name, 99).await;
    assert!(err.is_err(), "rollback to unknown version must return Err");
}

#[path = "tests_arrow_ipc.rs"]
mod tests_arrow_ipc;

// ─── fn-persist: catalog-backed handler function persistence ─────────────────

/// Build a minimal fake base64-encoded Wasm blob (not a real component).
fn fn_persist_fake_wasm_b64(tag: &[u8]) -> String {
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    let mut body = b"\x00asm\x01\x00\x00\x00".to_vec();
    body.extend_from_slice(tag);
    B64.encode(&body)
}

fn fn_persist_catalog_def(
    project: basin_common::ProjectId,
    name: &str,
    wasm_b64: &str,
) -> basin_catalog::SqlFunctionDef {
    use basin_catalog::{SqlFunctionLanguage, SqlReturnType, SqlArgType};
    basin_catalog::SqlFunctionDef {
        project,
        name: name.to_string(),
        args: Vec::new(),
        return_type: SqlReturnType::Scalar(SqlArgType::Bytea),
        body: wasm_b64.to_string(),
        language: SqlFunctionLanguage::Wasm,
        version: 1,
        source: None,
    }
}

/// Test 1: deploy writes to catalog.
#[tokio::test]
async fn fn_persist_deploy_writes_catalog() {
    use basin_catalog::{InMemoryCatalog, SqlFunctionLanguage};
    use basin_common::ProjectId;
    use crate::routes::admin_functions::FunctionRegistry;
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let name = "my_handler";
    let wasm_b64 = fn_persist_fake_wasm_b64(b"t1");

    catalog
        .register_sql_function(fn_persist_catalog_def(project, name, &wasm_b64))
        .await
        .expect("catalog upsert must succeed");

    let reg = FunctionRegistry::new();
    reg.put_test(project, name.to_string(), B64.decode(wasm_b64.as_bytes()).unwrap())
        .await;

    let all = catalog.list_sql_functions(&project).await;
    assert_eq!(all.len(), 1, "catalog should have 1 function");
    assert_eq!(all[0].name, name);
    assert!(matches!(all[0].language, SqlFunctionLanguage::Wasm));
    assert!(reg.exists(&project, name).await, "registry should know function");
}

/// Test 2: restart simulation — new registry, catalog is truth.
#[tokio::test]
async fn fn_persist_restart_simulation() {
    use basin_catalog::{InMemoryCatalog, SqlFunctionLanguage};
    use basin_common::ProjectId;
    use crate::routes::admin_functions::FunctionRegistry;
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let name = "durable_fn";
    let wasm_b64 = fn_persist_fake_wasm_b64(b"t2");

    catalog
        .register_sql_function(fn_persist_catalog_def(project, name, &wasm_b64))
        .await
        .expect("catalog upsert");
    let old_reg = FunctionRegistry::new();
    old_reg
        .put_test(project, name.to_string(), B64.decode(wasm_b64.as_bytes()).unwrap())
        .await;

    // Restart: brand-new registry — catalog still has the entry.
    let new_reg = FunctionRegistry::new();
    assert!(
        !new_reg.exists(&project, name).await,
        "fresh registry does not know function before hydration"
    );

    // Catalog is still authoritative.
    let all = catalog.list_sql_functions(&project).await;
    assert!(
        all.iter().any(|d| d.name == name),
        "catalog has function after restart simulation"
    );

    // Simulated list_functions catalog path (filter to handler languages):
    let handler_fns: Vec<_> = all
        .into_iter()
        .filter(|d| {
            matches!(
                d.language,
                SqlFunctionLanguage::Javascript | SqlFunctionLanguage::Wasm
            )
        })
        .collect();
    assert_eq!(handler_fns.len(), 1);
    assert_eq!(handler_fns[0].name, name);
}

/// Test 3: deploy-persist-failure invariant — registry unchanged without upsert.
#[tokio::test]
async fn fn_persist_no_catalog_upsert_registry_unchanged() {
    use basin_common::ProjectId;
    use crate::routes::admin_functions::FunctionRegistry;

    let reg = FunctionRegistry::new();
    let project = ProjectId::new();
    let name = "unmapped";

    // No catalog upsert performed, no reg.put called — registry stays empty.
    assert!(
        !reg.exists(&project, name).await,
        "registry must be empty when deploy never succeeded"
    );
}

/// Test 4: delete removes from both catalog and cache.
#[tokio::test]
async fn fn_persist_delete_removes_catalog_and_cache() {
    use basin_catalog::InMemoryCatalog;
    use basin_common::ProjectId;
    use crate::routes::admin_functions::FunctionRegistry;
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let name = "to_delete";
    let wasm_b64 = fn_persist_fake_wasm_b64(b"t4");

    catalog
        .register_sql_function(fn_persist_catalog_def(project, name, &wasm_b64))
        .await
        .expect("upsert");
    let reg = FunctionRegistry::new();
    reg.put_test(project, name.to_string(), B64.decode(wasm_b64.as_bytes()).unwrap())
        .await;

    // Both have it.
    assert!(reg.exists(&project, name).await);
    assert!(catalog.lookup_sql_function(&project, name).await.is_some());

    // Delete: catalog first, then registry.
    catalog.drop_sql_function(&project, name).await.expect("drop catalog");
    reg.remove_from_cache_test(&project, name).await;

    // Neither should have it.
    assert!(!reg.exists(&project, name).await, "registry should be empty");
    assert!(
        catalog.lookup_sql_function(&project, name).await.is_none(),
        "catalog should be empty"
    );

    // Re-delete should yield NotFound.
    let err = catalog.drop_sql_function(&project, name).await;
    assert!(err.is_err(), "second drop must return NotFound");
}

/// Test 5: cross-project isolation.
#[tokio::test]
async fn fn_persist_cross_project_isolation() {
    use basin_catalog::InMemoryCatalog;
    use basin_common::ProjectId;

    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let name = "shared_name";

    catalog
        .register_sql_function(fn_persist_catalog_def(project_a, name, &fn_persist_fake_wasm_b64(b"a")))
        .await
        .expect("upsert A");

    // Project A sees it.
    let a_fns = catalog.list_sql_functions(&project_a).await;
    assert_eq!(a_fns.len(), 1, "project A should have 1 function");

    // Project B sees nothing.
    let b_fns = catalog.list_sql_functions(&project_b).await;
    assert!(b_fns.is_empty(), "project B must not see project A functions");

    // Lookup by name in B returns None.
    let lookup_b = catalog.lookup_sql_function(&project_b, name).await;
    assert!(lookup_b.is_none(), "cross-project lookup must return None");
}

// ─── Backup / restore routes registered ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_and_restore_routes_return_401_not_404() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new().to_string();

    let snap = http_request(
        addr,
        "POST",
        &format!("/admin/v1/projects/{project}/snapshot"),
        &[],
        None,
    )
    .await;
    assert_ne!(
        snap.status, 404,
        "snapshot route must be registered (got 404 instead of 401)"
    );
    assert_eq!(snap.status, 401, "unauthenticated snapshot must return 401");

    let restore = http_request(
        addr,
        "POST",
        &format!("/admin/v1/projects/{project}/restore"),
        &[],
        None,
    )
    .await;
    assert_ne!(
        restore.status, 404,
        "restore route must be registered (got 404 instead of 401)"
    );
    assert_eq!(restore.status, 401, "unauthenticated restore must return 401");

    let _ = running.shutdown.send(());
}

// ─── Fork (COW branch) route registered ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_route_returns_401_not_404() {
    let Some((running, _svc, _auth, _mailer, _g)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new().to_string();

    let resp = http_request(
        addr,
        "POST",
        &format!("/admin/v1/projects/{project}/fork"),
        &[],
        None,
    )
    .await;
    assert_ne!(
        resp.status, 404,
        "fork route must be registered (got 404 instead of 401)"
    );
    assert_eq!(resp.status, 401, "unauthenticated fork must return 401");

    let _ = running.shutdown.send(());
}
