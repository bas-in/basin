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
//! `admin_functions_invoke_meters_cpu_and_logs` (#55): installs a real
//! `HandlerHarness`-backed `FunctionInvoker` that records CPU time and
//! synthetic log entries into the `FunctionRegistry`; then asserts the
//! `/admin/v1/functions/:name/cpu-ms` and `/logs` endpoints return real
//! (non-zero / non-empty) data after three invocations.
//!
//! ## Skip-cleanly
//!
//! Needs Postgres at `127.0.0.1:5432` so `AuthService::connect_with_mailer`
//! can run schema migrations. The route handlers themselves never consult
//! `AuthService`, but the type system requires an `Arc<AuthService>` in
//! `RestConfig`. The same pattern used by `inbound_webhook_auth.rs`.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use basin_auth::{AuthConfig, AuthService, SmtpConfig, SmtpTls, StubMailer};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig};
use basin_fn::{HandlerHarness, HandlerRequest};
use basin_rest::{
    set_global_invoker, FunctionInvoker, FunctionRegistry, InvokeRequest, InvokeResponse,
    RestConfig, RestService, RunningRest,
};
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
// Handler WAT + FunctionInvoker for #55 instrumentation test
// ---------------------------------------------------------------------------

/// Minimal WAT component that exports `basin:functions/handler#handle` and
/// returns a 200 OK with empty body. Identical to the component used by
/// `fn_handler.rs`. Reused here so we can run real Wasm invocations and
/// measure CPU time through the registry.
const HANDLER_WAT: &str = r#"
(component
  (core module $core_m
    (memory (export "memory") 1)

    (global $bump (mut i32) (i32.const 16))
    (func (export "cabi_realloc")
        (param $old i32) (param $old_sz i32) (param $align i32) (param $new_sz i32)
        (result i32)
      (local $ret i32)
      (local.set $ret
        (i32.and
          (i32.add (global.get $bump) (i32.sub (local.get $align) (i32.const 1)))
          (i32.xor (i32.sub (local.get $align) (i32.const 1)) (i32.const -1))))
      (global.set $bump (i32.add (local.get $ret) (local.get $new_sz)))
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

  (type $request (record
    (field "method" string)
    (field "path" string)
    (field "headers" (list (tuple string string)))
    (field "body" (list u8))))
  (export $request-name "request" (type $request))
  (type $response (record
    (field "status" u16)
    (field "headers" (list (tuple string string)))
    (field "body" (list u8))))
  (export $response-name "response" (type $response))

  (func (export "handle")
    (param "req" $request-name)
    (result (result $response-name (error string)))
    (canon lift
      (core func $core_i "handle_inner")
      (memory $core_i "memory")
      (realloc (func $core_i "cabi_realloc"))
    )
  )
)
"#;

/// `FunctionInvoker` implementation that:
///
/// 1. Runs the Wasm component via `HandlerHarness::handle`.
/// 2. Measures wall-CPU time and calls `FunctionRegistry::add_cpu_ms`.
/// 3. Appends a synthetic `"info"` log entry via `FunctionRegistry::append_log`
///    (simulating what the `basin:fn/log` host import would do once per call).
///
/// This is the minimal bridge needed by the integration test: the
/// `FunctionRegistry` belongs to basin-rest; the harness lives in basin-fn.
/// The invoker stitches them together without requiring basin-rest to depend
/// on basin-fn directly.
struct InstrumentedInvoker {
    by_name: Mutex<HashMap<(ProjectId, String), Arc<HandlerHarness>>>,
    registry: FunctionRegistry,
}

impl InstrumentedInvoker {
    fn new(registry: FunctionRegistry) -> Self {
        Self {
            by_name: Mutex::new(HashMap::new()),
            registry,
        }
    }

    fn register(&self, project: ProjectId, name: &str, harness: Arc<HandlerHarness>) {
        self.by_name
            .lock()
            .unwrap()
            .insert((project, name.to_string()), harness);
    }
}

#[async_trait]
impl FunctionInvoker for InstrumentedInvoker {
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
        let Some(harness) = harness else {
            return Ok(None);
        };

        let name_owned = name.to_string();
        let handler_req = HandlerRequest {
            method: req.method,
            path: req.path,
            headers: req.headers,
            body: req.body,
        };

        // Run the synchronous wasmtime call on a blocking thread, measuring
        // wall time as a CPU proxy (the WAT component does no I/O).
        let t0 = Instant::now();
        let resp = tokio::task::spawn_blocking(move || {
            let ctx = basin_fn::engine::InvocationContext::mock();
            harness.handle(ctx, handler_req)
        })
        .await
        .map_err(|e| format!("join error: {e}"))?
        .map_err(|e| format!("wasm trap: {e}"))?;
        let elapsed_ms = t0.elapsed().as_millis().max(1) as u64;

        // Record instrumentation into the shared registry. These writes are
        // visible to the `/admin/v1/functions/:name/cpu-ms` and `/logs`
        // endpoints served by the same process.
        self.registry
            .add_cpu_ms(project, &name_owned, elapsed_ms)
            .await;
        self.registry
            .append_log(
                project,
                &name_owned,
                "info",
                format!("invoked {name_owned} — status {}", resp.status),
            )
            .await;

        Ok(Some(InvokeResponse {
            status: resp.status,
            headers: resp.headers,
            body: resp.body,
        }))
    }
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

// ---------------------------------------------------------------------------
// #55: per-function log buffer + CPU counter
// ---------------------------------------------------------------------------

/// Deploy a real handler-shape Wasm component, invoke it 3 times through an
/// `InstrumentedInvoker` that records CPU time and log entries into the
/// `FunctionRegistry`, then assert that `/admin/v1/functions/:name/cpu-ms`
/// returns a value > 0 and `/logs` returns a non-empty array.
///
/// This is the end-to-end fix for issue #55:
///   - `cpu-ms` endpoint previously always returned `0`.
///   - `logs` endpoint previously always returned `[]`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_functions_invoke_meters_cpu_and_logs() {
    // Build the instrumented invoker backed by the real handler harness.
    let handler_bytes = wat::parse_str(HANDLER_WAT).expect("compile HANDLER_WAT");

    // Spin up a fresh REST service. We need the `RestService` handle (before
    // `run_until_bound` consumes it) to call `function_registry()` and obtain
    // the shared `Arc`-backed registry that the admin endpoints read from.
    let secret = vec![5u8; 32];
    let Some(auth) = try_make_auth(&secret).await else {
        return;
    };
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
    // Clone the registry before consuming `svc` with `run_until_bound`.
    let fn_registry = svc.function_registry();
    let running = svc.run_until_bound().await.expect("rest bind");
    let addr = running.local_addr;

    // Build a fresh project + admin JWT tied to this service's secret.
    let project = ProjectId::new();
    let token = admin_jwt_for(&secret, &project);
    let bearer_str = bearer(&token);
    let auth2 = ("Authorization", bearer_str.as_str());

    // Install the instrumented invoker globally so `/fn/v1/:name` routes here.
    let harness = Arc::new(
        HandlerHarness::new(&handler_bytes).expect("compile handler harness"),
    );
    let invoker = Arc::new(InstrumentedInvoker::new(fn_registry));
    invoker.register(project, "meter_fn", harness);
    set_global_invoker(invoker);

    // 1. Deploy the function (opaque bytes; admin endpoint).
    let wasm_b64 = B64.encode(&handler_bytes);
    let deploy_body = serde_json::json!({
        "name": "meter_fn",
        "wasm_b64": wasm_b64,
    })
    .to_string();
    let r = http_request(
        addr,
        "POST",
        "/admin/v1/functions/deploy",
        &[("Content-Type", "application/json"), auth2],
        Some(deploy_body.as_bytes()),
    )
    .await;
    assert_eq!(
        r.status, 201,
        "deploy should 201; got {} body: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );

    // 2. Invoke the function 3 times via `/fn/v1/meter_fn`.
    for i in 0..3 {
        let r = http_request(
            addr,
            "POST",
            "/fn/v1/meter_fn",
            &[auth2],
            Some(b"{}"),
        )
        .await;
        assert_eq!(
            r.status, 200,
            "invocation {} should 200; got {}",
            i, r.status
        );
    }

    // 3. cpu-ms must now be > 0.
    let r = http_request(
        addr,
        "GET",
        "/admin/v1/functions/meter_fn/cpu-ms",
        &[auth2],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    let cpu = v["cpu_ms"].as_u64().expect("cpu_ms field");
    assert!(
        cpu > 0,
        "cpu_ms must be > 0 after 3 invocations; got {cpu}"
    );

    // 4. logs must be non-empty (one entry per invocation).
    let r = http_request(
        addr,
        "GET",
        "/admin/v1/functions/meter_fn/logs",
        &[auth2],
        None,
    )
    .await;
    assert_eq!(r.status, 200);
    let v = r.json();
    let lines = v["lines"].as_array().expect("lines array");
    assert!(
        !lines.is_empty(),
        "logs must be non-empty after 3 invocations; got []"
    );
    assert_eq!(
        lines.len(),
        3,
        "expected 3 log entries (one per invocation); got {}",
        lines.len()
    );

    // Restore noop invoker so subsequent tests aren't affected.
    set_global_invoker(Arc::new(basin_rest::NoopFunctionInvoker));
    let _ = running.shutdown.send(());
}

/// `GET /admin/v1/projects/:id/usage` returns the project's billing-dimension
/// counters as JSON (an authenticated admin scoped to the project), and rejects
/// an unauthenticated request. Fresh project → zeros, but all fields present.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_project_usage_returns_counters() {
    let Some(fx) = boot().await else {
        return; // Postgres unavailable — skip (same gate as the other admin tests).
    };
    let bearer_hdr = bearer(&fx.token);
    let path = format!("/admin/v1/projects/{}/usage", fx.project);

    let r = http_request(
        fx.running.local_addr,
        "GET",
        &path,
        &[("Authorization", bearer_hdr.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "admin usage GET must succeed; body={:?}",
        String::from_utf8_lossy(&r.body)
    );
    let j = r.json();
    assert_eq!(j["project_id"].as_str(), Some(fx.project.to_string().as_str()));
    for k in [
        "ops_total",
        "bytes_read_total",
        "bytes_written_total",
        "class_a_ops_total",
        "class_b_ops_total",
        "cpu_micros_total",
        "errors_total",
        "latency_p99_ms_estimate",
    ] {
        assert!(
            j.get(k).map(|v| v.is_number()).unwrap_or(false),
            "usage must report numeric {k}, got {j}"
        );
    }

    // No admin token → must not return usage.
    let r2 = http_request(fx.running.local_addr, "GET", &path, &[], None).await;
    assert_ne!(r2.status, 200, "usage without an admin token must be rejected");
}

/// `GET /admin/v1/projects/:id/tables` lists the project's tables as JSON. A
/// fresh project has none → 200 with an empty array (route mechanics + auth).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_project_tables_lists_tables() {
    let Some(fx) = boot().await else {
        return;
    };
    let bearer_hdr = bearer(&fx.token);
    let path = format!("/admin/v1/projects/{}/tables", fx.project);

    let r = http_request(
        fx.running.local_addr,
        "GET",
        &path,
        &[("Authorization", bearer_hdr.as_str())],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        200,
        "admin tables GET must succeed; body={:?}",
        String::from_utf8_lossy(&r.body)
    );
    let j = r.json();
    assert_eq!(j["project_id"].as_str(), Some(fx.project.to_string().as_str()));
    assert!(
        j["tables"].as_array().map(|a| a.is_empty()).unwrap_or(false),
        "fresh project must report an empty tables array, got {j}"
    );

    let r2 = http_request(fx.running.local_addr, "GET", &path, &[], None).await;
    assert_ne!(r2.status, 200, "tables without an admin token must be rejected");
}

/// `DELETE /admin/v1/projects/:id` deprovisions: after provisioning credentials
/// for a project, DELETE returns 204 and the project's credentials are gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_delete_project_deprovisions() {
    let Some(fx) = boot().await else {
        return;
    };
    let bearer_hdr = bearer(&fx.token);
    let auth = ("Authorization", bearer_hdr.as_str());
    let addr = fx.running.local_addr;
    let creds_path = format!("/admin/v1/projects/{}/credentials", fx.project);

    // Provision a credential for fx.project (the admin token is scoped to it).
    let body = serde_json::json!({ "project_id": fx.project.to_string() }).to_string();
    let r = http_request(
        addr,
        "POST",
        "/admin/v1/projects",
        &[("Content-Type", "application/json"), auth],
        Some(body.as_bytes()),
    )
    .await;
    assert_eq!(r.status, 201, "provision must succeed; body={:?}", String::from_utf8_lossy(&r.body));

    // Credentials present.
    let r = http_request(addr, "GET", &creds_path, &[auth], None).await;
    assert_eq!(r.status, 200);
    assert!(
        !r.json().as_array().map(|a| a.is_empty()).unwrap_or(true),
        "credentials must exist after provision, got {}",
        r.json()
    );

    // Deprovision.
    let r = http_request(
        addr,
        "DELETE",
        &format!("/admin/v1/projects/{}", fx.project),
        &[auth],
        None,
    )
    .await;
    assert_eq!(
        r.status,
        204,
        "DELETE project must return 204; body={:?}",
        String::from_utf8_lossy(&r.body)
    );

    // Credentials gone.
    let r = http_request(addr, "GET", &creds_path, &[auth], None).await;
    assert_eq!(r.status, 200);
    assert!(
        r.json().as_array().map(|a| a.is_empty()).unwrap_or(false),
        "credentials must be empty after deprovision, got {}",
        r.json()
    );

    // No admin token → rejected.
    let r2 = http_request(addr, "DELETE", &format!("/admin/v1/projects/{}", fx.project), &[], None).await;
    assert_ne!(r2.status, 204, "delete without an admin token must be rejected");
}

/// `GET /admin/v1/projects` enumerates all provisioned projects. Provision two,
/// then list and assert both appear.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_list_projects_enumerates() {
    let Some(fx) = boot().await else {
        return;
    };
    let bearer_hdr = bearer(&fx.token);
    let auth = ("Authorization", bearer_hdr.as_str());
    let ct = ("Content-Type", "application/json");
    let addr = fx.running.local_addr;

    let p2 = ProjectId::new();
    for pid in [fx.project.to_string(), p2.to_string()] {
        let body = serde_json::json!({ "project_id": pid }).to_string();
        let r = http_request(
            addr,
            "POST",
            "/admin/v1/projects",
            &[ct, auth],
            Some(body.as_bytes()),
        )
        .await;
        assert_eq!(
            r.status,
            201,
            "provision {pid} must succeed; body={:?}",
            String::from_utf8_lossy(&r.body)
        );
    }

    let r = http_request(addr, "GET", "/admin/v1/projects", &[auth], None).await;
    assert_eq!(
        r.status,
        200,
        "list projects must succeed; body={:?}",
        String::from_utf8_lossy(&r.body)
    );
    let strs: Vec<String> = r.json()["projects"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(
        strs.contains(&fx.project.to_string()),
        "fx.project must be listed, got {strs:?}"
    );
    assert!(
        strs.contains(&p2.to_string()),
        "second project must be listed, got {strs:?}"
    );

    // No admin token → rejected.
    let r2 = http_request(addr, "GET", "/admin/v1/projects", &[], None).await;
    assert_ne!(r2.status, 200, "list projects without an admin token must be rejected");
}
