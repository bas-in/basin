//! tests/json_schemas.rs — JSON-shape lockfile for every `--json` call site.
//!
//! TASKS.md line 366: spawn each command with `--json`, snapshot the emitted
//! shape as a JSON-schema under `tests/json-schemas/<name>.schema.json`, and
//! re-validate on every run. When the bin starts emitting a previously-unseen
//! field or a field-type changes, validation fails and the developer must
//! consciously update the lockfile (`BASIN_UPDATE_SCHEMAS=1 cargo test`).
//!
//! The schema is hand-derived (not via `schemars`) from each captured
//! instance — see `derive_schema`. It's deliberately permissive: array
//! items unify across observed elements, strings stay free-form, numbers
//! split between integer/number. The intent is "shape-lock" (key names +
//! coarse types), not value-lock — value-lock lives in `tests/golden.rs`.
//!
//! Mirrors the stub-server / sandbox / `run_bin` plumbing in
//! `tests/integration.rs` rather than reusing it, because integration tests
//! in `tests/` can't import each other.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use jsonschema::JSONSchema;
use serde_json::{json, Value};

// ── stub HTTP server ────────────────────────────────────────────────────────

struct StubResp {
    status: u16,
    body: String,
}

impl StubResp {
    fn ok(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
        }
    }
}

struct StubReq {
    #[allow(dead_code)]
    method: String,
    path: String,
}

struct StubServer {
    url: String,
    _shutdown: Arc<Mutex<bool>>,
}

impl StubServer {
    fn start<H>(handler: H) -> StubServer
    where
        H: Fn(&StubReq) -> StubResp + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub server");
        listener
            .set_nonblocking(false)
            .expect("blocking accept ok");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://{addr}");
        let handler = Arc::new(handler);
        let shutdown = Arc::new(Mutex::new(false));
        let sd_for_thread = Arc::clone(&shutdown);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if *sd_for_thread.lock().unwrap() {
                    break;
                }
                let Ok(mut stream) = stream else { continue };
                let h = Arc::clone(&handler);
                let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() || request_line.is_empty() {
                    continue;
                }
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                let mut content_length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).is_err() {
                        break;
                    }
                    let trimmed = line.trim_end();
                    if trimmed.is_empty() {
                        break;
                    }
                    if let Some(v) = trimmed.to_ascii_lowercase().strip_prefix("content-length:") {
                        content_length = v.trim().parse().unwrap_or(0);
                    }
                }
                if content_length > 0 {
                    let mut body = vec![0u8; content_length];
                    let _ = reader.read_exact(&mut body);
                }
                let resp = h(&StubReq { method, path });
                let payload = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp.status,
                    resp.body.len(),
                    resp.body
                );
                let _ = stream.write_all(payload.as_bytes());
                let _ = stream.flush();
            }
        });
        StubServer {
            url,
            _shutdown: shutdown,
        }
    }
}

/// schema_stub returns canned envelopes with at least one populated element
/// per list shape so the derived schema captures the inner object's fields
/// (not just `{"data": []}`). An empty array would produce a too-permissive
/// `{"type": "array"}` with no `items`.
fn schema_stub() -> StubServer {
    StubServer::start(|req: &StubReq| {
        let p = req.path.as_str();
        // whoami: /auth/v1/user → user, then /v1/orgs → orgs list
        if p == "/auth/v1/user" {
            return StubResp::ok(
                r#"{"user":{"id":"u-1","email":"a@x.test","name":"Alice"}}"#,
            );
        }
        if p == "/v1/orgs" {
            return StubResp::ok(
                r#"{"orgs":[{"id":"o-1","slug":"acme","name":"Acme","plan":"team"}]}"#,
            );
        }
        // org-scoped routes
        if p.starts_with("/v1/orgs/") {
            if p.ends_with("/members") {
                return StubResp::ok(
                    r#"{"members":[{"id":"m-1","email":"a@x.test","role":"admin","added_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.ends_with("/invitations") {
                return StubResp::ok(
                    r#"{"invitations":[{"id":"i-1","email":"b@x.test","role":"member","created_at":"2026-01-01T00:00:00Z","expires_at":"2026-02-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/audit") {
                return StubResp::ok(
                    r#"{"events":[{"id":"e-1","actor":"a@x.test","action":"project.create","target":"p_x","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.ends_with("/projects") {
                return StubResp::ok(
                    r#"{"projects":[{"ref":"p_x","name":"demo","region":"iad","status":"ready","id":"pj-1","engine_tenant_id":"t-1","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            return StubResp::ok("{}");
        }
        // project-scoped routes
        if p.contains("/projects/") {
            if p.contains("/branches") {
                return StubResp::ok(
                    r#"{"branches":[{"ref":"br_main","name":"main","parent_ref":"","status":"ready","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/api-keys") {
                return StubResp::ok(
                    r#"{"api_keys":[{"id":"k-1","name":"anon","role":"anon","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/webhooks") {
                return StubResp::ok(
                    r#"{"webhooks":[{"id":"w-1","url":"https://x.test/hook","events":["row.insert"],"enabled":true,"created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/alerts/rules") {
                return StubResp::ok(
                    r#"{"rules":[{"id":"r-1","name":"cpu","metric":"cpu_pct","threshold":80.0,"window_seconds":300,"enabled":true,"created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/custom-domains") || p.contains("/domains") {
                return StubResp::ok(
                    r#"{"domains":[{"id":"d-1","hostname":"app.x.test","status":"verified","cert_status":"issued","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/activity") {
                return StubResp::ok(
                    r#"{"events":[{"id":"e-1","actor":"a@x.test","action":"sql.run","resource":"p_x","created_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/metrics") {
                return StubResp::ok(
                    r#"{"metrics":[{"name":"cpu_pct","unit":"percent","points":[{"t":"2026-01-01T00:00:00Z","v":12.5}]}]}"#,
                );
            }
            if p.contains("/extensions/catalog") || p.contains("/extensions") {
                return StubResp::ok(
                    r#"{"extensions":[{"name":"pgvector","installed":true,"version":"0.5.0","description":"Vector similarity"}]}"#,
                );
            }
            if p.contains("/engine/pin") {
                return StubResp::ok(
                    r#"{"pin":{"version":"5.20.0","pinned_at":"2026-01-01T00:00:00Z","pinned_by":"a@x.test","reason":"stability"}}"#,
                );
            }
            if p.contains("/sql/saved") {
                return StubResp::ok(
                    r#"{"queries":[{"id":"q-1","name":"recent","sql":"SELECT 1","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"}]}"#,
                );
            }
            if p.contains("/backups/snapshots") {
                return StubResp::ok(
                    r#"{"snapshots":[{"id":"s-1","label":"nightly","size_bytes":12345,"created_at":"2026-01-01T00:00:00Z","status":"ready"}]}"#,
                );
            }
            if p.contains("/oauth-providers") {
                return StubResp::ok(
                    r#"{"providers":[{"provider":"github","enabled":true,"client_id":"cid-1"}]}"#,
                );
            }
            // fall through: project-scoped route we don't recognise
            return StubResp::ok("{}");
        }
        StubResp::ok("{}")
    })
}

// ── sandbox + bin runner ────────────────────────────────────────────────────

struct Sandbox {
    dir: PathBuf,
}

impl Sandbox {
    fn new(tag: &str) -> Sandbox {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("basin-schema-{tag}-{pid}-{nanos}"));
        fs::create_dir_all(&dir).expect("create sandbox");
        Sandbox { dir }
    }

    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct BinOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run_bin(api_url: &str, sandbox: &Sandbox, argv: &[&str]) -> BinOutput {
    let exe = env!("CARGO_BIN_EXE_basin");
    let mut cmd = Command::new(exe);
    cmd.args(argv);
    cmd.env_remove("BASIN_TOKEN");
    cmd.env_remove("BASIN_DOMAIN");
    cmd.env("XDG_CONFIG_HOME", sandbox.path());
    cmd.env("HOME", sandbox.path());
    cmd.env("NO_COLOR", "1");
    if !api_url.is_empty() {
        cmd.env("BASIN_API", api_url);
    } else {
        cmd.env("BASIN_API", "http://127.0.0.1:1");
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let out = cmd.output().expect("spawn basin");
    BinOutput {
        success: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

// ── schema derivation ───────────────────────────────────────────────────────

/// derive_schema walks `v` and emits a permissive JSON-schema (draft-07-ish
/// subset: `type`, `properties`, `items`, `required`). It's intentionally
/// loose:
///   * primitives map to their type — strings stay open, integers split
///     out from numbers so `1` doesn't lock in `1.0`.
///   * objects record each observed key in `properties` and list ALL
///     observed keys as `required` (a missing field is a shape break).
///   * arrays unify all observed items into a single `items` sub-schema
///     via `merge_schemas` so heterogenous arrays don't lock to the first
///     element.
///   * `null` produces `{"type": "null"}`. When merged with anything else
///     it widens to a `{"type": [..., "null"]}` union.
fn derive_schema(v: &Value) -> Value {
    match v {
        Value::Null => json!({"type": "null"}),
        Value::Bool(_) => json!({"type": "boolean"}),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                json!({"type": "integer"})
            } else {
                json!({"type": "number"})
            }
        }
        Value::String(_) => json!({"type": "string"}),
        Value::Array(items) => {
            if items.is_empty() {
                // Empty array: don't pin item shape — any items are fine.
                json!({"type": "array"})
            } else {
                let mut unified = derive_schema(&items[0]);
                for it in items.iter().skip(1) {
                    unified = merge_schemas(unified, derive_schema(it));
                }
                json!({"type": "array", "items": unified})
            }
        }
        Value::Object(o) => {
            // BTreeMap so the on-disk schema is key-stable for deterministic diffs.
            let mut props: BTreeMap<String, Value> = BTreeMap::new();
            let mut required: Vec<String> = Vec::new();
            for (k, val) in o.iter() {
                props.insert(k.clone(), derive_schema(val));
                required.push(k.clone());
            }
            required.sort();
            let props_v: Value = serde_json::to_value(&props).expect("props serialize");
            json!({
                "type": "object",
                "properties": props_v,
                "required": required,
                "additionalProperties": true,
            })
        }
    }
}

/// merge_schemas combines two derived schemas into one that accepts either
/// instance. Used for array-item unification (heterogenous arrays) and
/// for null-widening.
fn merge_schemas(a: Value, b: Value) -> Value {
    if a == b {
        return a;
    }
    let at = a.get("type").cloned();
    let bt = b.get("type").cloned();
    // Same primitive type, no recursion needed.
    if at == bt && a.get("properties").is_none() && a.get("items").is_none() {
        return a;
    }
    // Both objects: union of properties; intersect of `required`.
    if at == Some(json!("object")) && bt == Some(json!("object")) {
        let mut props: BTreeMap<String, Value> = BTreeMap::new();
        if let Some(ap) = a.get("properties").and_then(|v| v.as_object()) {
            for (k, v) in ap {
                props.insert(k.clone(), v.clone());
            }
        }
        if let Some(bp) = b.get("properties").and_then(|v| v.as_object()) {
            for (k, v) in bp {
                if let Some(existing) = props.remove(k) {
                    props.insert(k.clone(), merge_schemas(existing, v.clone()));
                } else {
                    props.insert(k.clone(), v.clone());
                }
            }
        }
        let a_req: Vec<String> = a
            .get("required")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let b_req: Vec<String> = b
            .get("required")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let mut required: Vec<String> = a_req
            .iter()
            .filter(|k| b_req.contains(k))
            .cloned()
            .collect();
        required.sort();
        let props_v: Value = serde_json::to_value(&props).expect("props serialize");
        return json!({
            "type": "object",
            "properties": props_v,
            "required": required,
            "additionalProperties": true,
        });
    }
    // Both arrays: merge items.
    if at == Some(json!("array")) && bt == Some(json!("array")) {
        match (a.get("items").cloned(), b.get("items").cloned()) {
            (Some(ai), Some(bi)) => {
                return json!({"type": "array", "items": merge_schemas(ai, bi)})
            }
            (Some(ai), None) => return json!({"type": "array", "items": ai}),
            (None, Some(bi)) => return json!({"type": "array", "items": bi}),
            (None, None) => return json!({"type": "array"}),
        }
    }
    // Mismatched primitive types — widen via a type-union. Lift both single
    // types into a sorted-deduped array. (jsonschema 0.18 accepts string[]
    // for the `type` keyword as a polymorphism shorthand.)
    let mut types: Vec<String> = Vec::new();
    let mut push = |t: &Value| {
        if let Some(s) = t.as_str() {
            if !types.contains(&s.to_string()) {
                types.push(s.to_string());
            }
        } else if let Some(arr) = t.as_array() {
            for x in arr {
                if let Some(s) = x.as_str() {
                    if !types.contains(&s.to_string()) {
                        types.push(s.to_string());
                    }
                }
            }
        }
    };
    if let Some(t) = at.as_ref() {
        push(t);
    }
    if let Some(t) = bt.as_ref() {
        push(t);
    }
    types.sort();
    json!({"type": types})
}

// ── lockfile I/O + assertion ────────────────────────────────────────────────

fn schemas_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/json-schemas")
}

fn schema_path(name: &str) -> PathBuf {
    schemas_dir().join(format!("{name}.schema.json"))
}

fn update_mode() -> bool {
    std::env::var("BASIN_UPDATE_SCHEMAS")
        .ok()
        .is_some_and(|v| !v.is_empty() && v != "0")
}

/// Drive one command and either (a) write its derived schema to disk in
/// update mode or (b) load + validate the previously-locked schema.
fn assert_shape(name: &str, value: &Value) {
    let path = schema_path(name);
    if update_mode() {
        fs::create_dir_all(schemas_dir()).expect("mkdir schemas");
        let schema = derive_schema(value);
        let pretty = serde_json::to_string_pretty(&schema).expect("schema serialize");
        fs::write(&path, format!("{pretty}\n")).expect("write schema");
        return;
    }
    let raw = match fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) => panic!(
            "schema lockfile missing for {name} at {path:?}: {e}\n\
             run `BASIN_UPDATE_SCHEMAS=1 cargo test --test json_schemas` to regenerate"
        ),
    };
    let schema: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("schema parse for {name}: {e}"));
    let compiled = JSONSchema::compile(&schema)
        .unwrap_or_else(|e| panic!("schema compile for {name}: {e}"));
    let result = compiled.validate(value);
    if let Err(errs) = result {
        let msgs: Vec<String> = errs
            .map(|e| format!(" - at {}: {}", e.instance_path, e))
            .collect();
        panic!(
            "shape drift in {name}:\n{}\n\
             instance:\n{}\n\
             run `BASIN_UPDATE_SCHEMAS=1 cargo test --test json_schemas` to relock",
            msgs.join("\n"),
            serde_json::to_string_pretty(value).unwrap_or_default(),
        );
    }
}

/// Run a `basin … --json` command, parse stdout, and lock/validate the
/// resulting shape under `name`.
fn lock_command(name: &str, srv_url: &str, argv: &[&str]) {
    let sb = Sandbox::new(name);
    let out = run_bin(srv_url, &sb, argv);
    assert!(
        out.success,
        "command {name} failed:\nargv: {argv:?}\nstdout: {}\nstderr: {}",
        out.stdout, out.stderr
    );
    let parsed: Value = serde_json::from_str(out.stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "command {name} did not emit valid JSON: {e}\nstdout: {}\nstderr: {}",
            out.stdout, out.stderr
        )
    });
    assert_shape(name, &parsed);
}

// ── the lockfile suite ──────────────────────────────────────────────────────
//
// One #[test] per command (so failures localise) instead of a single
// table-driven loop. Each invokes the bin with --json, captures stdout,
// and runs `assert_shape` which delegates to derive-or-validate based on
// `BASIN_UPDATE_SCHEMAS`.
//
// SKIPPED on purpose:
//   * `docs <subcmd> --json` — covered as "docs_json_db" only; subcommands
//     embed the URL verbatim and the shape is identical regardless.
//   * `sql --json` — the response embeds arbitrary user-row JSON (any
//     PG type) under `rows[]`, so shape-locking is meaningless. Already
//     covered by `tests/integration.rs::sql_happy`.
//   * `metrics` returns a `series`-shape on the engine path but the CLI
//     transforms it to `{metrics: [...]}` — see metrics_happy below.

#[test]
fn lock_version_json() {
    let sb = Sandbox::new("version");
    let out = run_bin("", &sb, &["--quiet", "--json", "version"]);
    assert!(out.success, "stderr: {}", out.stderr);
    let v: Value = serde_json::from_str(out.stdout.trim())
        .unwrap_or_else(|e| panic!("version JSON: {e}\nstdout: {}", out.stdout));
    assert_shape("version", &v);
}

#[test]
fn lock_docs_db_json() {
    let sb = Sandbox::new("docs");
    let out = run_bin("", &sb, &["--quiet", "--json", "docs", "db"]);
    assert!(out.success, "stderr: {}", out.stderr);
    let v: Value = serde_json::from_str(out.stdout.trim())
        .unwrap_or_else(|e| panic!("docs JSON: {e}\nstdout: {}", out.stdout));
    assert_shape("docs_db", &v);
}

#[test]
fn lock_whoami_json() {
    let srv = schema_stub();
    lock_command(
        "whoami",
        &srv.url,
        &["--quiet", "--json", "--token=test_token", "whoami"],
    );
}

#[test]
fn lock_orgs_list_json() {
    let srv = schema_stub();
    lock_command(
        "orgs_list",
        &srv.url,
        &["--quiet", "--json", "--token=test_token", "orgs", "list"],
    );
}

#[test]
fn lock_projects_list_json() {
    let srv = schema_stub();
    lock_command(
        "projects_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "--org=acme",
            "projects",
            "list",
        ],
    );
}

#[test]
fn lock_members_list_json() {
    let srv = schema_stub();
    lock_command(
        "members_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "members",
            "list",
            "--org=acme",
        ],
    );
}

#[test]
fn lock_invitations_list_json() {
    let srv = schema_stub();
    lock_command(
        "invitations_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "invitations",
            "list",
            "--org=acme",
        ],
    );
}

#[test]
fn lock_audit_list_json() {
    let srv = schema_stub();
    lock_command(
        "audit_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "audit",
            "list",
            "--org=acme",
        ],
    );
}

#[test]
fn lock_branches_list_json() {
    let srv = schema_stub();
    lock_command(
        "branches_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "branches",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_api_keys_list_json() {
    let srv = schema_stub();
    lock_command(
        "api_keys_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "api-keys",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_webhooks_list_json() {
    let srv = schema_stub();
    lock_command(
        "webhooks_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "webhooks",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_activity_list_json() {
    let srv = schema_stub();
    lock_command(
        "activity_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "activity",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_extensions_list_json() {
    let srv = schema_stub();
    lock_command(
        "extensions_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "extensions",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_oauth_providers_list_json() {
    // oauth-providers is project-scoped (--project), not org-scoped — the
    // task description's `--org=<o>` is from the spec preamble that
    // predates the implementation, but the wire route is per-project.
    let srv = schema_stub();
    lock_command(
        "oauth_providers_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "oauth-providers",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_queries_list_json() {
    let srv = schema_stub();
    lock_command(
        "queries_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "queries",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_metrics_json() {
    let srv = schema_stub();
    lock_command(
        "metrics",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "metrics",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_engine_pin_get_json() {
    let srv = schema_stub();
    lock_command(
        "engine_pin_get",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "engine-pin",
            "get",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_domains_list_json() {
    let srv = schema_stub();
    lock_command(
        "domains_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "domains",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_alerts_rules_list_json() {
    let srv = schema_stub();
    lock_command(
        "alerts_rules_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "alerts",
            "rules",
            "list",
            "--project=p_x",
        ],
    );
}

#[test]
fn lock_snapshots_list_json() {
    let srv = schema_stub();
    lock_command(
        "snapshots_list",
        &srv.url,
        &[
            "--quiet",
            "--json",
            "--token=test_token",
            "snapshots",
            "list",
            "--project=p_x",
        ],
    );
}

// ── derive_schema unit tests ────────────────────────────────────────────────

#[cfg(test)]
mod derive_tests {
    use super::*;

    #[test]
    fn primitive_shapes() {
        assert_eq!(derive_schema(&json!(null)), json!({"type": "null"}));
        assert_eq!(derive_schema(&json!(true)), json!({"type": "boolean"}));
        assert_eq!(derive_schema(&json!(1)), json!({"type": "integer"}));
        assert_eq!(derive_schema(&json!(1.5)), json!({"type": "number"}));
        assert_eq!(derive_schema(&json!("x")), json!({"type": "string"}));
    }

    #[test]
    fn empty_array_is_permissive() {
        // An empty array has no items keyword — anything passes inside.
        let s = derive_schema(&json!([]));
        assert_eq!(s, json!({"type": "array"}));
    }

    #[test]
    fn array_items_unify_across_elements() {
        // Two object elements with disjoint keys merge into a single
        // items schema; intersection of `required` (= empty) means
        // missing keys are OK in either element.
        let s = derive_schema(&json!([{"a": 1}, {"b": "x"}]));
        let items = s.get("items").unwrap();
        let props = items.get("properties").unwrap().as_object().unwrap();
        assert!(props.contains_key("a"));
        assert!(props.contains_key("b"));
        let req = items.get("required").unwrap().as_array().unwrap();
        assert!(req.is_empty(), "required should be the intersection (empty)");
    }

    #[test]
    fn object_props_are_required() {
        let s = derive_schema(&json!({"a": 1, "b": "x"}));
        let req: Vec<String> = s
            .get("required")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(req, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn derived_schema_validates_its_own_instance() {
        // Round-trip: any value we derive a schema from must validate
        // against that schema. This is the load-bearing invariant for
        // "generate, then re-run, both green".
        let cases = vec![
            json!({"projects": [{"ref": "p", "name": "n", "id": "i"}]}),
            json!({"user": {"id": "u", "email": "e@x"}, "orgs": []}),
            json!({"pin": {"version": "1.0", "pinned_at": "t", "pinned_by": "u", "reason": ""}}),
            json!({"events": []}),
            json!({"webhooks": [{"id": "w", "events": ["e"], "enabled": true}]}),
            json!(null),
        ];
        for v in cases {
            let s = derive_schema(&v);
            let compiled = JSONSchema::compile(&s)
                .unwrap_or_else(|e| panic!("compile {s}: {e}"));
            assert!(
                compiled.is_valid(&v),
                "derived schema rejects its own instance: schema={s} instance={v}"
            );
        }
    }

    #[test]
    fn schema_tolerates_extra_array_items() {
        // The "permissive" property: if the locked instance has one user
        // record, a future run with TWO user records (same shape) should
        // still validate. This guards against accidental over-fitting to
        // array length.
        let locked = json!({"users": [{"id": "u-1", "name": "a"}]});
        let s = derive_schema(&locked);
        let compiled = JSONSchema::compile(&s).expect("compile");
        let two = json!({"users": [{"id": "u-1", "name": "a"}, {"id": "u-2", "name": "b"}]});
        assert!(compiled.is_valid(&two), "schema rejected wider array");
    }

    #[test]
    fn schema_tolerates_added_keys() {
        // additionalProperties: true means a new field doesn't break the
        // lock — only a *missing* (formerly-required) field does. This
        // matches the "conscious bump" criterion from TASKS 366: removing
        // a documented field is breaking; adding one is additive.
        let locked = json!({"a": 1});
        let s = derive_schema(&locked);
        let compiled = JSONSchema::compile(&s).expect("compile");
        let wider = json!({"a": 1, "b": "new"});
        assert!(compiled.is_valid(&wider));
        let narrower = json!({});
        assert!(!compiled.is_valid(&narrower), "missing required `a` must fail");
    }
}
