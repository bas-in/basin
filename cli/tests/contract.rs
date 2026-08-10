//! Contract-replay tests for basin-cli.
//!
//! Each fixture under tests/contract-fixtures/*.json describes a sequence of
//! HTTP request/response pairs the CLI is expected to issue. Replay-mode
//! validates the CLI still talks the expected protocol; record-mode (future
//! work, gated on a live control plane) would generate fresh fixtures.
//!
//! Run: `cargo test --test contract`

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

// ── fixture schema ──────────────────────────────────────────────────────────

/// FixtureStep is one request/response pair the CLI is expected to issue.
struct FixtureStep {
    method: String,
    path: String,
    status: u16,
    body: String,
}

/// Fixture is a parsed *.json file — the runner walks `steps` in order,
/// asserts each incoming request's method+path matches, and serves back
/// the canned response.
struct Fixture {
    name: String,
    steps: Vec<FixtureStep>,
    command: Vec<String>,
    expect_stdout_contains: Vec<String>,
    expect_stderr_contains: Vec<String>,
    expect_exit_code: i32,
}

fn parse_fixture(path: &Path) -> Fixture {
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read fixture {}: {e}", path.display()));
    let v: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("parse fixture {}: {e}", path.display()));

    let name = v
        .get("name")
        .and_then(|x| x.as_str())
        .unwrap_or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unnamed")
        })
        .to_string();

    let steps = v
        .get("steps")
        .and_then(|x| x.as_array())
        .unwrap_or_else(|| panic!("fixture {name}: missing `steps` array"))
        .iter()
        .map(|step| {
            let req = step
                .get("request")
                .unwrap_or_else(|| panic!("fixture {name}: step missing `request`"));
            let resp = step
                .get("response")
                .unwrap_or_else(|| panic!("fixture {name}: step missing `response`"));
            let method = req
                .get("method")
                .and_then(|x| x.as_str())
                .unwrap_or_else(|| panic!("fixture {name}: step.request.method required"))
                .to_string();
            let path = req
                .get("path")
                .and_then(|x| x.as_str())
                .unwrap_or_else(|| panic!("fixture {name}: step.request.path required"))
                .to_string();
            let status = resp
                .get("status")
                .and_then(|x| x.as_u64())
                .unwrap_or(200) as u16;
            // Body is required (an object) and serialised verbatim to the wire.
            let body_value = resp
                .get("body")
                .cloned()
                .unwrap_or(Value::Object(serde_json::Map::new()));
            let body = serde_json::to_string(&body_value).expect("re-serialise body");
            FixtureStep {
                method,
                path,
                status,
                body,
            }
        })
        .collect();

    let command = v
        .get("command")
        .and_then(|x| x.as_array())
        .unwrap_or_else(|| panic!("fixture {name}: missing `command` array"))
        .iter()
        .map(|a| {
            a.as_str()
                .unwrap_or_else(|| panic!("fixture {name}: command entries must be strings"))
                .to_string()
        })
        .collect();

    let expect_stdout_contains = string_array(&v, "expect_stdout_contains");
    let expect_stderr_contains = string_array(&v, "expect_stderr_contains");
    let expect_exit_code = v
        .get("expect_exit_code")
        .and_then(|x| x.as_i64())
        .map(|n| n as i32)
        .unwrap_or(0);

    Fixture {
        name,
        steps,
        command,
        expect_stdout_contains,
        expect_stderr_contains,
        expect_exit_code,
    }
}

fn string_array(v: &Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(|x| x.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

// ── stub HTTP server (sequenced, drift-detecting) ──────────────────────────

/// StubServer binds to 127.0.0.1:0 and serves a fixed sequence of
/// FixtureStep responses. The Nth incoming request gets the Nth response;
/// if the request's method or path doesn't match steps[N], the assert fires
/// inside the accept thread and is surfaced via `recorded_failure`.
struct StubServer {
    url: String,
    shutdown: Arc<Mutex<bool>>,
    cursor: Arc<Mutex<usize>>,
    recorded_failure: Arc<Mutex<Option<String>>>,
}

impl StubServer {
    fn start(name: String, steps: Vec<FixtureStep>) -> StubServer {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub server");
        listener
            .set_nonblocking(false)
            .expect("blocking accept ok");
        let addr = listener.local_addr().expect("local addr");
        let url = format!("http://{addr}");
        let shutdown = Arc::new(Mutex::new(false));
        let cursor = Arc::new(Mutex::new(0usize));
        let recorded_failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let sd_for_thread = Arc::clone(&shutdown);
        let cur_for_thread = Arc::clone(&cursor);
        let fail_for_thread = Arc::clone(&recorded_failure);
        let steps = Arc::new(steps);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if *sd_for_thread.lock().unwrap() {
                    break;
                }
                let Ok(mut stream) = stream else { continue };
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

                let mut idx = cur_for_thread.lock().unwrap();
                let i = *idx;
                if i >= steps.len() {
                    let msg = format!(
                        "fixture {name}: extra request beyond recorded steps — got {method} {path}"
                    );
                    *fail_for_thread.lock().unwrap() = Some(msg);
                    // Serve a 500 so the CLI fails fast rather than blocking.
                    let payload = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = stream.write_all(payload);
                    let _ = stream.flush();
                    continue;
                }
                let step = &steps[i];
                if step.method != method || step.path != path {
                    let msg = format!(
                        "fixture {name}: drift at step {i} — expected {} {}, got {method} {path}",
                        step.method, step.path
                    );
                    *fail_for_thread.lock().unwrap() = Some(msg);
                }
                *idx += 1;
                drop(idx);

                let payload = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    step.status,
                    step.body.len(),
                    step.body
                );
                let _ = stream.write_all(payload.as_bytes());
                let _ = stream.flush();
            }
        });

        StubServer {
            url,
            shutdown,
            cursor,
            recorded_failure,
        }
    }

    fn served_count(&self) -> usize {
        *self.cursor.lock().unwrap()
    }

    fn drift_failure(&self) -> Option<String> {
        self.recorded_failure.lock().unwrap().clone()
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        *self.shutdown.lock().unwrap() = true;
    }
}

// ── per-test sandbox + bin invocation ──────────────────────────────────────

/// Sandbox owns a tempdir used as $HOME / $XDG_CONFIG_HOME so the CLI's
/// per-user config doesn't pick up the developer's real credentials.
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
        let dir = std::env::temp_dir().join(format!("basin-contract-{tag}-{pid}-{nanos}"));
        std::fs::create_dir_all(&dir).expect("create sandbox");
        Sandbox { dir }
    }

    fn path(&self) -> &Path {
        &self.dir
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// run_bin spawns the basin binary with `BASIN_API=<stub_url>` +
/// `BASIN_TOKEN=test-token` and returns (exit_code, stdout, stderr).
fn run_bin(api_url: &str, sandbox: &Sandbox, argv: &[String]) -> (i32, String, String) {
    let exe = env!("CARGO_BIN_EXE_basin");
    let mut cmd = Command::new(exe);
    cmd.args(argv);
    cmd.env_remove("BASIN_DOMAIN");
    cmd.env("BASIN_API", api_url);
    cmd.env("BASIN_TOKEN", "test-token");
    cmd.env("XDG_CONFIG_HOME", sandbox.path());
    cmd.env("HOME", sandbox.path());
    cmd.env("NO_COLOR", "1");
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let child = cmd.spawn().expect("spawn basin");
    let out = child.wait_with_output().expect("wait basin");
    let exit_code = out.status.code().unwrap_or(-1);
    (
        exit_code,
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

// ── runner ─────────────────────────────────────────────────────────────────

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/contract-fixtures")
}

fn load_fixtures() -> Vec<(PathBuf, Fixture)> {
    let dir = fixtures_dir();
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixtures dir {}: {e}", dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("json"))
        .collect();
    entries.sort();
    entries
        .into_iter()
        .map(|p| {
            let f = parse_fixture(&p);
            (p, f)
        })
        .collect()
}

/// Drive one fixture end-to-end. Factored out of the iteration so each
/// failure points at the offending file via a clear panic message.
fn run_fixture(path: &Path, f: &Fixture) {
    let sandbox = Sandbox::new(&f.name);
    let stub = StubServer::start(f.name.clone(), clone_steps(&f.steps));
    // Prepend `--quiet` so the support-window + self-update side-effects
    // (which hit /v1/version + GitHub releases) are skipped — those would
    // otherwise show up as extra GETs and trip the drift detector.
    let mut argv: Vec<String> = Vec::with_capacity(f.command.len() + 1);
    argv.push("--quiet".to_string());
    argv.extend(f.command.iter().cloned());
    let (exit_code, stdout, stderr) = run_bin(&stub.url, &sandbox, &argv);

    if let Some(drift) = stub.drift_failure() {
        panic!(
            "[{}] request drift\n  fixture: {}\n  detail: {drift}\n  stdout: {stdout}\n  stderr: {stderr}",
            f.name,
            path.display()
        );
    }
    assert_eq!(
        exit_code,
        f.expect_exit_code,
        "[{}] exit code mismatch (expected {}, got {})\n  stdout: {stdout}\n  stderr: {stderr}",
        f.name,
        f.expect_exit_code,
        exit_code,
    );
    assert_eq!(
        stub.served_count(),
        f.steps.len(),
        "[{}] served {} of {} steps\n  stdout: {stdout}\n  stderr: {stderr}",
        f.name,
        stub.served_count(),
        f.steps.len(),
    );
    for needle in &f.expect_stdout_contains {
        assert!(
            stdout.contains(needle),
            "[{}] stdout missing {needle:?}\n  stdout: {stdout}\n  stderr: {stderr}",
            f.name,
        );
    }
    for needle in &f.expect_stderr_contains {
        assert!(
            stderr.contains(needle),
            "[{}] stderr missing {needle:?}\n  stdout: {stdout}\n  stderr: {stderr}",
            f.name,
        );
    }
}

fn clone_steps(steps: &[FixtureStep]) -> Vec<FixtureStep> {
    steps
        .iter()
        .map(|s| FixtureStep {
            method: s.method.clone(),
            path: s.path.clone(),
            status: s.status,
            body: s.body.clone(),
        })
        .collect()
}

#[test]
fn contract_replay_all_fixtures() {
    let fixtures = load_fixtures();
    assert!(
        !fixtures.is_empty(),
        "no contract fixtures discovered under {}",
        fixtures_dir().display()
    );
    for (path, f) in &fixtures {
        run_fixture(path, f);
    }
}

#[test]
fn contract_fixtures_dir_is_present() {
    // Sanity check: the dir exists and has the expected count. The README
    // documents fixture authoring; tests would silently skip if the dir
    // disappeared, so we fence that explicitly.
    let count = load_fixtures().len();
    assert!(
        count >= 8,
        "expected at least 8 contract fixtures, found {count}"
    );
}
