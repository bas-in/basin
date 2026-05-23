//! tests/integration.rs — per-subcommand happy-path drive-through coverage.
//!
//! Each top-level entry in `commands::all()` gets at least one invocation
//! here. Tests spawn the built `basin` binary via `env!("CARGO_BIN_EXE_basin")`
//! and target a fresh in-test HTTP stub (or no server for shape-only / help
//! routes). This is the Rust analogue of the Go test that walked the Go
//! dispatch table — TASKS.md line 362.
//!
//! Each test uses an isolated temp HOME / XDG_CONFIG_HOME / cwd so the
//! `~/.config/basin` config and `./basin/` working-project files never
//! collide across parallel runs.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ── stub HTTP server ────────────────────────────────────────────────────────

/// StubResp is a canned response: HTTP status + body string (treated as JSON).
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

/// StubReq captures the parsed request line for handler inspection.
struct StubReq {
    #[allow(dead_code)]
    method: String,
    path: String,
}

/// StubServer is the bin's-eye view of api.basin.run: bind to 127.0.0.1:0,
/// return its URL, serve until dropped. The handler closure is `Send + Sync`
/// because the accept loop lives on its own thread.
///
/// We re-implement (rather than reuse `crate::testutil`) because integration
/// tests in `tests/` cannot import the bin crate's private modules.
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

// ── per-test sandbox ────────────────────────────────────────────────────────

/// Sandbox owns a tempdir used as XDG_CONFIG_HOME + (when needed) cwd.
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
        let dir = std::env::temp_dir().join(format!("basin-it-{tag}-{pid}-{nanos}"));
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

// ── bin invocation helper ───────────────────────────────────────────────────

/// run_bin spawns the basin binary with the supplied argv, returning
/// (exit-code-success, stdout, stderr). HOME/XDG_CONFIG_HOME default to a
/// fresh sandbox; api_url defaults to a stub URL (or empty for no-network
/// commands). Pass `--token=test_token` in argv (or via env) to satisfy
/// `require_client` for HTTP-touching commands.
struct BinOutput {
    success: bool,
    stdout: String,
    #[allow(dead_code)]
    stderr: String,
}

fn run_bin(api_url: &str, sandbox: &Sandbox, cwd: Option<&Path>, argv: &[&str]) -> BinOutput {
    run_bin_with_stdin(api_url, sandbox, cwd, argv, None)
}

/// run_bin_with_stdin is the full helper — when `stdin_input` is `Some`, the
/// child reads that string before any prompt (used by `basin login` which
/// reads the token from stdin when --token isn't honoured per-subcommand).
fn run_bin_with_stdin(
    api_url: &str,
    sandbox: &Sandbox,
    cwd: Option<&Path>,
    argv: &[&str],
    stdin_input: Option<&str>,
) -> BinOutput {
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
        // Point at a clearly-unreachable URL so any accidental HTTP call
        // fails fast rather than blocking on real DNS.
        cmd.env("BASIN_API", "http://127.0.0.1:1");
    }
    if let Some(c) = cwd {
        cmd.current_dir(c);
    }
    if stdin_input.is_some() {
        cmd.stdin(Stdio::piped());
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn basin");
    if let Some(s) = stdin_input {
        if let Some(mut sin) = child.stdin.take() {
            let _ = sin.write_all(s.as_bytes());
        }
    }
    let out = child.wait_with_output().expect("wait basin");
    BinOutput {
        success: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).to_string(),
        stderr: String::from_utf8_lossy(&out.stderr).to_string(),
    }
}

/// happy_stub returns a default-200 server that maps known list-shaped
/// routes to empty-array bodies and everything else to `{}`. This is
/// enough for every "list" happy-path the tests below need.
fn happy_stub() -> StubServer {
    StubServer::start(|req: &StubReq| {
        let p = req.path.as_str();
        // Endpoint-specific shapes the CLI parses out:
        if p == "/auth/v1/user" {
            return StubResp::ok(r#"{"user":{"id":"u-1","email":"a@x.test"}}"#);
        }
        if p == "/v1/orgs" {
            return StubResp::ok(r#"{"orgs":[]}"#);
        }
        if p.starts_with("/v1/orgs/") {
            // Org-scoped: members, invitations, audit, oauth-apps, saml, scim,
            // ownership-transfers, metrics — all serve "list"-shaped empty.
            if p.ends_with("/members") {
                return StubResp::ok(r#"{"members":[]}"#);
            }
            if p.ends_with("/invitations") {
                return StubResp::ok(r#"{"invitations":[]}"#);
            }
            if p.starts_with("/v1/orgs/") && p.contains("/audit") {
                return StubResp::ok(r#"{"events":[]}"#);
            }
            if p.contains("/oauth-apps") {
                return StubResp::ok(r#"{"apps":[]}"#);
            }
            if p.contains("/metrics") {
                return StubResp::ok(r#"{"series":[]}"#);
            }
            return StubResp::ok("{}");
        }
        // Project-scoped routes.
        if p.contains("/projects/") {
            if p.ends_with("/projects") || p.contains("/orgs/") && p.ends_with("/projects") {
                return StubResp::ok(r#"{"projects":[]}"#);
            }
            if p.contains("/branches") {
                return StubResp::ok(r#"{"branches":[]}"#);
            }
            if p.contains("/api-keys") {
                return StubResp::ok(r#"{"api_keys":[]}"#);
            }
            if p.contains("/webhooks") {
                return StubResp::ok(r#"{"webhooks":[]}"#);
            }
            if p.contains("/alerts/rules") {
                return StubResp::ok(r#"{"rules":[]}"#);
            }
            if p.contains("/alerts/events") {
                return StubResp::ok(r#"{"events":[]}"#);
            }
            if p.contains("/domains") {
                return StubResp::ok(r#"{"domains":[]}"#);
            }
            if p.contains("/activity") {
                return StubResp::ok(r#"{"events":[]}"#);
            }
            if p.contains("/metrics") {
                return StubResp::ok(r#"{"series":[]}"#);
            }
            if p.contains("/extensions/catalog") || p.contains("/extensions") {
                return StubResp::ok(r#"{"extensions":[]}"#);
            }
            if p.contains("/engine/pin") {
                return StubResp::ok(
                    r#"{"pin":{"version":"5.20.0","pinned_at":"2026-01-01T00:00:00Z","reason":""}}"#,
                );
            }
            if p.contains("/sql/saved") {
                return StubResp::ok(r#"{"queries":[]}"#);
            }
            if p.contains("/sql/query") {
                return StubResp::ok(r#"{"columns":[],"rows":[],"elapsed_ms":1}"#);
            }
            if p.contains("/backups/snapshots") {
                return StubResp::ok(r#"{"snapshots":[]}"#);
            }
            if p.contains("/backups/policy") {
                return StubResp::ok(r#"{"policy":{}}"#);
            }
            if p.contains("/oauth-providers") {
                return StubResp::ok(r#"{"providers":[]}"#);
            }
            if p.contains("/email-config") {
                return StubResp::ok(r#"{"templates":{}}"#);
            }
            if p.contains("/storage") {
                return StubResp::ok(r#"{"storage":null}"#);
            }
            if p.contains("/erd") {
                // erd uses a raw GET to the URL; harmless empty object.
                return StubResp::ok("{}");
            }
            if p.contains("/tables") {
                return StubResp::ok(r#"{"tables":[]}"#);
            }
            if p.contains("/functions") {
                return StubResp::ok(r#"[]"#);
            }
            if p.contains("/transfers") {
                return StubResp::ok(r#"{"transfers":[]}"#);
            }
            // Fallback for unmatched project-scoped routes.
            return StubResp::ok("{}");
        }
        StubResp::ok("{}")
    })
}

// ── tests ───────────────────────────────────────────────────────────────────

#[test]
fn version_runs_without_network() {
    let sb = Sandbox::new("version");
    let out = run_bin("", &sb, None, &["--quiet", "version"]);
    assert!(out.success, "stderr: {}", out.stderr);
    // "basin-cli" is in the build_info preamble; "basin" is in the binary name printed.
    assert!(
        out.stdout.to_lowercase().contains("basin"),
        "stdout: {}",
        out.stdout
    );
}

#[test]
fn help_top_level_runs() {
    let sb = Sandbox::new("help");
    let out = run_bin("", &sb, None, &["--quiet", "help"]);
    assert!(out.success, "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("USAGE") || out.stdout.contains("COMMANDS"),
        "stdout: {}",
        out.stdout
    );
}

#[test]
fn help_for_subcommand_runs() {
    let sb = Sandbox::new("help-sub");
    let out = run_bin("", &sb, None, &["--quiet", "help", "projects"]);
    assert!(out.success, "stderr: {}", out.stderr);
    assert!(
        out.stdout.to_lowercase().contains("usage")
            || out.stdout.to_lowercase().contains("list")
            || out.stdout.to_lowercase().contains("project"),
        "stdout: {}",
        out.stdout
    );
}

#[test]
fn completion_bash_runs() {
    let sb = Sandbox::new("completion");
    let out = run_bin("", &sb, None, &["--quiet", "completion", "bash"]);
    assert!(out.success, "stderr: {}", out.stderr);
    assert!(!out.stdout.trim().is_empty(), "stdout: {}", out.stdout);
}

#[test]
fn whoami_happy_path() {
    let sb = Sandbox::new("whoami");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &["--quiet", "--token=test_token", "whoami"],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn login_with_token_runs() {
    let sb = Sandbox::new("login");
    let srv = happy_stub();
    // parse_global_flags consumes --token=X before login.rs's per-command
    // parser sees it, so login falls back to its stdin prompt. Pipe the
    // PAT in via stdin to drive the happy path through the bin.
    let out = run_bin_with_stdin(
        &srv.url,
        &sb,
        None,
        &["--quiet", "login"],
        Some("bso_org_abc123\n"),
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn logout_runs_clean() {
    let sb = Sandbox::new("logout");
    let out = run_bin("", &sb, None, &["--quiet", "logout"]);
    // No config to remove; logout still exits 0 (no-op).
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn projects_list_happy() {
    let sb = Sandbox::new("projects-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "--org=acme",
            "projects",
            "list",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn orgs_list_happy() {
    let sb = Sandbox::new("orgs-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &["--quiet", "--token=test_token", "orgs", "list"],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn members_list_happy() {
    let sb = Sandbox::new("members-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "members",
            "list",
            "--org=acme",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn invitations_list_happy() {
    let sb = Sandbox::new("invitations-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "invitations",
            "list",
            "--org=acme",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn init_runs_in_temp_dir() {
    let sb = Sandbox::new("init");
    // init is cwd-scoped; spawn with cwd = sandbox.
    let out = run_bin("", &sb, Some(sb.path()), &["--quiet", "init"]);
    assert!(out.success, "stderr: {}", out.stderr);
    assert!(
        sb.path().join("basin/config.toml").exists(),
        "expected ./basin/config.toml in sandbox"
    );
}

#[test]
fn link_writes_project_ref() {
    let sb = Sandbox::new("link");
    let srv = happy_stub();
    // link requires ./basin/ to already exist (init runs first).
    run_bin("", &sb, Some(sb.path()), &["--quiet", "init"]);
    let out = run_bin(
        &srv.url,
        &sb,
        Some(sb.path()),
        &[
            "--quiet",
            "--token=test_token",
            "link",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn unlink_runs_after_link() {
    let sb = Sandbox::new("unlink");
    let srv = happy_stub();
    run_bin("", &sb, Some(sb.path()), &["--quiet", "init"]);
    run_bin(
        &srv.url,
        &sb,
        Some(sb.path()),
        &[
            "--quiet",
            "--token=test_token",
            "link",
            "--project=p_x",
        ],
    );
    let out = run_bin("", &sb, Some(sb.path()), &["--quiet", "unlink"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn status_after_link_runs() {
    // status fetches the linked project; happy stub returns empty/200.
    let sb = Sandbox::new("status");
    let srv = happy_stub();
    run_bin("", &sb, Some(sb.path()), &["--quiet", "init"]);
    run_bin(
        &srv.url,
        &sb,
        Some(sb.path()),
        &[
            "--quiet",
            "--token=test_token",
            "link",
            "--project=p_x",
        ],
    );
    let out = run_bin(
        &srv.url,
        &sb,
        Some(sb.path()),
        &["--quiet", "--token=test_token", "status"],
    );
    assert!(out.success, "stderr: {}\nstdout: {}", out.stderr, out.stdout);
}

#[test]
fn sql_happy() {
    let sb = Sandbox::new("sql");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "sql",
            "--project=p_x",
            "-e",
            "SELECT 1",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn dump_help_runs() {
    // Full dump shells out to engine SQL routes + writes a file; --help is
    // the safe happy-path that exercises the dispatch entry.
    let sb = Sandbox::new("dump");
    let out = run_bin("", &sb, None, &["--quiet", "dump", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn restore_help_runs() {
    let sb = Sandbox::new("restore");
    let out = run_bin("", &sb, None, &["--quiet", "restore", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn migrate_from_pg_help_runs() {
    let sb = Sandbox::new("migrate-from-pg");
    let out = run_bin("", &sb, None, &["--quiet", "migrate-from-pg", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn rpc_help_runs() {
    let sb = Sandbox::new("rpc");
    let out = run_bin("", &sb, None, &["--quiet", "rpc", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn db_help_runs() {
    let sb = Sandbox::new("db");
    let out = run_bin("", &sb, None, &["--quiet", "db", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn tables_list_happy() {
    let sb = Sandbox::new("tables-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "tables",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn rows_help_runs() {
    // rows insert/update/delete need real keys; help exercises dispatch.
    let sb = Sandbox::new("rows");
    let out = run_bin("", &sb, None, &["--quiet", "rows", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn storage_help_runs() {
    // storage hits /storage/v1/bucket; help is the safe drive-through.
    let sb = Sandbox::new("storage");
    let out = run_bin("", &sb, None, &["--quiet", "storage", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn functions_list_happy() {
    let sb = Sandbox::new("functions-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "functions",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn rls_help_runs() {
    let sb = Sandbox::new("rls");
    let out = run_bin("", &sb, None, &["--quiet", "rls", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn migrations_help_runs() {
    // migrations list hits /admin/v1/projects/.../migrations; help is the
    // safe drive-through (admin path isn't stubbed by happy_stub).
    let sb = Sandbox::new("migrations");
    let out = run_bin("", &sb, None, &["--quiet", "migrations", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn branches_list_happy() {
    let sb = Sandbox::new("branches-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "branches",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn gen_help_runs() {
    // gen types typescript needs an information_schema rowshape; --help
    // is the safe shape-only drive-through.
    let sb = Sandbox::new("gen");
    let out = run_bin("", &sb, None, &["--quiet", "gen", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn snapshots_list_happy() {
    let sb = Sandbox::new("snapshots-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "snapshots",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn backups_snapshots_list_happy() {
    let sb = Sandbox::new("backups-snapshots-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "backups",
            "snapshots",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn logs_help_runs() {
    // history-once needs a stub but lines:[] is fine; --help still drives dispatch.
    let sb = Sandbox::new("logs");
    let out = run_bin("", &sb, None, &["--quiet", "logs", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn realtime_help_runs() {
    // realtime subscribe is WebSocket — happy-path the help.
    let sb = Sandbox::new("realtime");
    let out = run_bin("", &sb, None, &["--quiet", "realtime", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn api_keys_list_happy() {
    let sb = Sandbox::new("api-keys-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "api-keys",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn pgwire_help_runs() {
    // pgwire show hits multiple credential routes; happy-path the help.
    let sb = Sandbox::new("pgwire");
    let out = run_bin("", &sb, None, &["--quiet", "pgwire", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn domains_list_happy() {
    let sb = Sandbox::new("domains-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "domains",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn webhooks_list_happy() {
    let sb = Sandbox::new("webhooks-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "webhooks",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn alerts_rules_list_happy() {
    let sb = Sandbox::new("alerts-rules-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "alerts",
            "rules",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn audit_list_happy() {
    let sb = Sandbox::new("audit-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "audit",
            "list",
            "--org=acme",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn activity_list_happy() {
    let sb = Sandbox::new("activity-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "activity",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn metrics_happy() {
    let sb = Sandbox::new("metrics");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "metrics",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn erd_help_runs() {
    // erd uses an out-of-band raw GET; --help drives dispatch safely.
    let sb = Sandbox::new("erd");
    let out = run_bin("", &sb, None, &["--quiet", "erd", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn queries_list_happy() {
    let sb = Sandbox::new("queries-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "queries",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn engine_pin_get_happy() {
    let sb = Sandbox::new("engine-pin-get");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "engine-pin",
            "get",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn extensions_list_happy() {
    let sb = Sandbox::new("extensions-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "extensions",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn oauth_providers_list_happy() {
    let sb = Sandbox::new("oauth-providers-list");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "oauth-providers",
            "list",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn email_help_runs() {
    // email templates get is project-scoped (not org as the spec implied);
    // --help drives dispatch without picking a verb.
    let sb = Sandbox::new("email");
    let out = run_bin("", &sb, None, &["--quiet", "email", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn byo_bucket_get_happy() {
    let sb = Sandbox::new("byo-bucket-get");
    let srv = happy_stub();
    let out = run_bin(
        &srv.url,
        &sb,
        None,
        &[
            "--quiet",
            "--token=test_token",
            "byo",
            "bucket",
            "get",
            "--project=p_x",
        ],
    );
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn saml_help_runs() {
    let sb = Sandbox::new("saml");
    let out = run_bin("", &sb, None, &["--quiet", "saml", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn scim_help_runs() {
    let sb = Sandbox::new("scim");
    let out = run_bin("", &sb, None, &["--quiet", "scim", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn oauth_apps_help_runs() {
    let sb = Sandbox::new("oauth-apps");
    let out = run_bin("", &sb, None, &["--quiet", "oauth-apps", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn docs_json_no_browser() {
    let sb = Sandbox::new("docs");
    let out = run_bin("", &sb, None, &["--quiet", "--json", "docs", "db"]);
    assert!(out.success, "stderr: {}", out.stderr);
    assert!(
        out.stdout.contains("\"url\""),
        "expected url in JSON: {}",
        out.stdout
    );
}

#[test]
fn secrets_help_runs() {
    let sb = Sandbox::new("secrets");
    let out = run_bin("", &sb, None, &["--quiet", "secrets", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn tokens_help_runs() {
    let sb = Sandbox::new("tokens");
    let out = run_bin("", &sb, None, &["--quiet", "tokens", "--help"]);
    assert!(out.success, "stderr: {}", out.stderr);
}

#[test]
fn config_show_runs() {
    let sb = Sandbox::new("config");
    let out = run_bin("", &sb, None, &["--quiet", "config", "show"]);
    assert!(out.success, "stderr: {}", out.stderr);
}
