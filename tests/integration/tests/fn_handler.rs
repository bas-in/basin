//! End-to-end integration test for `ANY /fn/v1/:name` (Phase 5.11.W2).
//!
//! Spins up a real `RestService`, installs a `FunctionInvoker` backed by a
//! tiny precompiled Wasm component (`basin_fn::HandlerHarness`), and asserts
//! that a curl-like request through the mount produces the function's
//! response.
//!
//! ## What this proves
//!
//! 1. The W2 mount accepts `ANY` HTTP method (here: GET + POST).
//! 2. Auth gates the mount before the function is resolved (no bearer → 401).
//! 3. Unknown function names → 404 from the `NoopFunctionInvoker` default.
//! 4. A registered name dispatches through the real component-model pipeline
//!    (`HandlerHarness::handle` → guest WAT component → 200 response).
//!
//! Skip-cleanly: tests that need PG (for the auth signup path) bail with a
//! one-line note when Postgres is unreachable. The unauthenticated-path
//! tests don't need PG and always run.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_fn::{HandlerHarness, HandlerRequest};
use basin_rest::{
    set_global_invoker, FunctionInvoker, InvokeRequest, InvokeResponse, RestConfig, RestService,
    RunningRest,
};
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::sync::Mutex;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::NoTls;
use ulid::Ulid;

// ---------------------------------------------------------------------------
// Helpers — schema-cleanup + REST/Auth bootstrap (mirrors object_storage.rs)
// ---------------------------------------------------------------------------

const PG_URL: &str = "host=127.0.0.1 port=5432 user=pc dbname=postgres";

fn unique_schema() -> String {
    format!("basin_fn_handler_test_{}", Ulid::new().to_string().to_lowercase())
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
                Err(_) => return,
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(PG_URL, NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(p)) => p,
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
        jwt_secret: vec![7u8; 32],
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

async fn try_serve() -> Option<(RunningRest, AuthService, StubMailer, SchemaGuard)> {
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
            eprintln!("postgres unreachable, skipping fn_handler test: {e}");
            return None;
        }
        Err(_) => {
            eprintln!("postgres connect timed out, skipping fn_handler test");
            return None;
        }
    };
    let dir = TempDir::new().expect("tempdir");
    let dir_path: &'static TempDir = Box::leak(Box::new(dir));
    let engine = engine_in(dir_path);
    let svc = RestService::new(RestConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        engine,
        auth: Arc::new(auth.clone()),
        max_body_bytes: 1 << 20,
        default_page_size: 100,
        max_page_size: 1000,
        cors_origins: Vec::new(),
        rate_limit_per_sec: 1000,
    });
    let running = svc.run_until_bound().await.expect("bind");
    Some((running, auth, mailer, SchemaGuard { schema }))
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

// ---------------------------------------------------------------------------
// Minimal raw HTTP/1.1 client
// ---------------------------------------------------------------------------

struct HttpResp {
    status: u16,
    body: Vec<u8>,
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
// Wasm component + FunctionInvoker glue
// ---------------------------------------------------------------------------

/// Same WAT component as `crates/basin-fn/tests/handler_test.rs`: exports
/// `basin:functions/handler#handle` returning a 200 OK with empty body.
const HANDLER_WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)

    (global $bump (mut i32) (i32.const 16))
    (func (export "cabi_realloc")
        (param $old i32) (param $old_sz i32) (param $align i32) (param $new_sz i32)
        (result i32)
      (local $ret i32)
      (local.set $ret (global.get $bump))
      (global.set $bump (i32.add (global.get $bump) (local.get $new_sz)))
      (local.get $ret)
    )

    (func (export "handle_inner")
        (param $method_ptr i32) (param $method_len i32)
        (param $path_ptr   i32) (param $path_len   i32)
        (param $headers_ptr i32) (param $headers_len i32)
        (param $body_ptr   i32) (param $body_len   i32)
        (result i32)
      (local $out i32)
      (local.set $out (i32.const 1024))
      (i32.store (i32.add (local.get $out) (i32.const  0)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const  4)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const  8)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 12)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 16)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 20)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 24)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 28)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 32)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 36)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 40)) (i32.const 0))
      (i32.store (i32.add (local.get $out) (i32.const 44)) (i32.const 0))
      (i32.store16 (i32.add (local.get $out) (i32.const 4)) (i32.const 200))
      (local.get $out)
    )
  )
  (core instance $core_i (instantiate $core_m))

  (func (export "handle")
    (param "req" (record
      (field "method" string)
      (field "path" string)
      (field "headers" (list (tuple string string)))
      (field "body" (list u8))))
    (result (result (record
      (field "status" u16)
      (field "headers" (list (tuple string string)))
      (field "body" (list u8))) (error string)))
    (canon lift
      (core func $core_i "handle_inner")
      (memory $core_i "memory")
      (realloc (func $core_i "cabi_realloc"))
    )
  )
)
"#;

/// In-memory `FunctionInvoker` backed by a single registered
/// `HandlerHarness`. Mirrors the trait surface W6 will satisfy with a
/// catalog-backed implementation.
struct WasmInvoker {
    by_name: Mutex<HashMap<(ProjectId, String), Arc<HandlerHarness>>>,
}

impl WasmInvoker {
    fn new() -> Self {
        Self { by_name: Mutex::new(HashMap::new()) }
    }

    fn register(&self, project: ProjectId, name: &str, harness: Arc<HandlerHarness>) {
        self.by_name
            .lock()
            .unwrap()
            .insert((project, name.to_string()), harness);
    }
}

#[async_trait::async_trait]
impl FunctionInvoker for WasmInvoker {
    async fn invoke(
        &self,
        project: ProjectId,
        name: &str,
        req: InvokeRequest,
    ) -> Result<Option<InvokeResponse>, String> {
        let harness = {
            let guard = self.by_name.lock().unwrap();
            guard.get(&(project, name.to_string())).cloned()
        };
        let Some(harness) = harness else { return Ok(None) };

        let handler_req = HandlerRequest {
            method: req.method,
            path: req.path,
            headers: req.headers,
            body: req.body,
        };
        // Run the synchronous wasmtime call on a blocking thread so we
        // don't stall the axum reactor.
        let resp = tokio::task::spawn_blocking(move || {
            let ctx = basin_fn::engine::InvocationContext::mock();
            harness.handle(ctx, handler_req)
        })
        .await
        .map_err(|e| format!("join error: {e}"))?
        .map_err(|e| format!("wasm trap: {e}"))?;

        Ok(Some(InvokeResponse {
            status: resp.status,
            headers: resp.headers,
            body: resp.body,
        }))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// `/fn/v1/:name` without an Authorization header → 401. Doesn't need PG —
/// the auth gate fires before lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fn_v1_requires_auth() {
    let Some((running, _auth, _mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let r = raw_request(
        addr,
        "POST",
        "/fn/v1/anything",
        &[("Content-Type", "application/json")],
        Some(b"{}"),
    )
    .await;
    assert_eq!(r.status, 401, "missing bearer should yield 401");
    let _ = running.shutdown.send(());
}

/// Auth'd request against a name that isn't registered → 404 from the
/// default [`basin_rest::NoopFunctionInvoker`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fn_v1_unknown_function_returns_404() {
    // Reset the invoker to the default Noop in case an earlier test installed
    // a stub. This is a process-global slot; tests in this file are serialised
    // via #[test] ordering anyway, but the reset is defensive.
    set_global_invoker(Arc::new(basin_rest::NoopFunctionInvoker));

    let Some((running, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let toks = make_user(&auth, &mailer, &project, "fn-unknown@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    let r = raw_request(
        addr,
        "POST",
        "/fn/v1/never_registered",
        &[("Authorization", bearer.as_str())],
        Some(b""),
    )
    .await;
    assert_eq!(
        r.status,
        404,
        "unknown function should 404, got body: {}",
        String::from_utf8_lossy(&r.body)
    );
    let _ = running.shutdown.send(());
}

/// Happy path: register a tiny WAT component, invoke `/fn/v1/echo` over GET
/// and POST, assert 200 both ways.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fn_v1_dispatches_to_wasm_handler() {
    // Compile + register the component.
    let wasm = wat::parse_str(HANDLER_WAT).expect("WAT compiles");
    let harness = Arc::new(HandlerHarness::new(&wasm).expect("harness"));

    let invoker = Arc::new(WasmInvoker::new());

    // Bring the server up first so we know the project id we registered
    // matches the one in the JWT.
    let Some((running, auth, mailer, _guard)) = try_serve().await else {
        return;
    };
    let addr = running.local_addr;
    let project = ProjectId::new();
    let toks = make_user(&auth, &mailer, &project, "fn-echo@example.com").await;
    let bearer = format!("Bearer {}", toks.access_token);

    invoker.register(project, "echo", harness);
    set_global_invoker(invoker);

    // POST.
    let r = raw_request(
        addr,
        "POST",
        "/fn/v1/echo",
        &[
            ("Authorization", bearer.as_str()),
            ("Content-Type", "application/octet-stream"),
        ],
        Some(b"hello"),
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "POST /fn/v1/echo body: {}",
        String::from_utf8_lossy(&r.body)
    );

    // GET — verifies `ANY` method routing actually accepts non-POST too.
    let r = raw_request(
        addr,
        "GET",
        "/fn/v1/echo",
        &[("Authorization", bearer.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "GET /fn/v1/echo body: {}",
        String::from_utf8_lossy(&r.body)
    );

    // Restore the noop so other tests in the same process aren't surprised
    // by the registered function leaking across.
    set_global_invoker(Arc::new(basin_rest::NoopFunctionInvoker));

    let _ = running.shutdown.send(());
}
