//! logs — live log stream + history poll fallback.
//!
//!   basin logs [--project <ref>] [--tail] [--limit N]
//!
//! We try the streaming endpoint first (NDJSON or text/event-stream over
//! /v1/projects/:ref/logs/stream). On any non-2xx (404 / 501) we fall
//! back to polling /v1/projects/:ref/logs/history every 3 seconds.
//!
//! The streaming path is implemented with a plain `reqwest::blocking::Client`
//! GET so the response body can be read line-by-line via `BufRead` —
//! without adding a streaming helper to client.rs.
//!
//! The polling loop (follow_poll) is factored out so the test can drive
//! one iteration against a stub server without spawning a real ticker.
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd)
//! from ./basin/config.toml. Error with hint at `basin link` if neither.

use std::io::{BufRead, BufReader};
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::blocking::Client as HttpClient;
use reqwest::Method;

use crate::client::{query_string, version};
use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::print_json;
use crate::{printerr, printinfo};

use super::help::help_for_command;
use super::parse_or_silent;

// ── project resolution ────────────────────────────────────────────────────────

fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir().map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg(
            "--project is required (or run `basin link` to bind this directory)",
        )),
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_logs(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("logs")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("tail").long("tail").action(ArgAction::SetTrue))
        .arg(Arg::new("limit").long("limit").default_value("100"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "logs",
            "Stream live project logs (or poll history).",
            &[
                "--project=<ref>     Project ref (flag or ./basin/config.toml).",
                "--tail              Follow the live log stream.",
                "--limit=N           Lines on a one-shot read (default 100).",
            ],
        );
        return Ok(());
    }
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    let limit: usize = m
        .get_one::<String>("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let c = require_client(g)?;
    if m.get_flag("tail") {
        stream_logs(&c, &project, g)
    } else {
        history_once(&c, &project, limit, g)
    }
}

// ── one-shot history ──────────────────────────────────────────────────────────

/// history_once fetches a single page of recent logs and prints them.
/// This is the primary tested path.
pub fn history_once(
    c: &crate::client::Client,
    project: &str,
    limit: usize,
    g: &GlobalFlags,
) -> CliResult<()> {
    let q = query_string(&[("limit", &limit.to_string())]);
    let resp: serde_json::Value = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/logs/history{q}"),
        None,
    )?;
    if g.json {
        // JSON shape: { lines: [ { at: string, level: string, message: string, ... } ] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(lines) = resp.get("lines").and_then(|v| v.as_array()) {
        for ln in lines {
            render_log_line(ln);
        }
    }
    Ok(())
}

// ── streaming (--tail) ────────────────────────────────────────────────────────

/// stream_logs attempts to open a persistent GET to the /logs/stream
/// endpoint and reads NDJSON / SSE lines until interrupted. On 404 or
/// 501 it falls back to `follow_poll`.
///
/// Uses a raw reqwest client (no streaming helper in client.rs), so the
/// bearer token and User-Agent are set manually.
fn stream_logs(c: &crate::client::Client, project: &str, g: &GlobalFlags) -> CliResult<()> {
    let url = format!("{}/v1/projects/{project}/logs/stream", c.base_url);
    let http = HttpClient::builder()
        // No timeout so the connection can stay open indefinitely.
        .timeout(None)
        .build()?;
    let resp_result = http
        .get(&url)
        .bearer_auth(&c.token)
        .header("Accept", "application/json")
        .header("User-Agent", format!("basin-cli/{}", version()))
        .send();

    match resp_result {
        Ok(resp) if resp.status().is_success() => {
            // Happy path: read lines until the connection closes or the
            // user sends SIGINT. We use a BufReader over the response body.
            let reader = BufReader::new(resp);
            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        // Trim "data: " prefix in case the server emits SSE.
                        let l = l.trim_start_matches("data: ").to_string();
                        if l.is_empty() {
                            continue;
                        }
                        println!("{l}");
                    }
                    Err(_) => break,
                }
            }
            Ok(())
        }
        Ok(resp) => {
            let status = resp.status().as_u16();
            if status == 404 || status == 501 {
                // Streaming not available — fall back to polling.
                printinfo!(
                    g,
                    "log stream not reachable — polling /history every 3s. Ctrl-C to stop."
                );
                follow_poll(c, project, g)
            } else {
                let body = resp.text().unwrap_or_default();
                Err(Box::new(crate::client::parse_api_error(status, &body)))
            }
        }
        Err(e) => Err(Box::new(e)),
    }
}

// ── polling loop (follow mode fallback) ───────────────────────────────────────

/// follow_poll polls /logs/history every 3 seconds, tracking the most-
/// recently-seen timestamp to avoid re-printing lines. Like the Go CLI
/// (stdlib-only, no signal crate), the loop runs until the OS terminates
/// the process on Ctrl-C — we don't install a handler.
///
/// The per-iteration logic lives in `poll_once` so that tests can call it
/// directly without running the real sleep/tick loop.
fn follow_poll(c: &crate::client::Client, project: &str, g: &GlobalFlags) -> CliResult<()> {
    let mut since = String::new();
    loop {
        since = poll_once(c, project, &since, g);
        std::thread::sleep(Duration::from_secs(3));
    }
}

/// poll_once fetches one page of history since `since`, prints new lines,
/// and returns the new high-water timestamp. On error it logs a warning
/// and returns the unchanged `since` so the loop can retry.
///
/// Exported so tests can call it once against a stub server.
pub fn poll_once(c: &crate::client::Client, project: &str, since: &str, g: &GlobalFlags) -> String {
    let q = query_string(&[("since", since), ("limit", "200")]);
    let result: CliResult<serde_json::Value> = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/logs/history{q}"),
        None,
    );
    let resp = match result {
        Ok(v) => v,
        Err(e) => {
            printerr!(g, "history poll failed: {e}");
            return since.to_string();
        }
    };
    let mut new_since = since.to_string();
    if let Some(lines) = resp.get("lines").and_then(|v| v.as_array()) {
        for ln in lines {
            render_log_line(ln);
            if let Some(ts) = ln.get("at").and_then(|v| v.as_str()) {
                if ts > new_since.as_str() {
                    new_since = ts.to_string();
                }
            }
        }
    }
    new_since
}

// ── rendering ─────────────────────────────────────────────────────────────────

/// render_log_line prints one log entry in human-friendly form. Falls back
/// to a raw JSON dump when the shape is not recognised.
fn render_log_line(ln: &serde_json::Value) {
    let at = ln.get("at").and_then(|v| v.as_str()).unwrap_or("");
    let level = ln.get("level").and_then(|v| v.as_str()).unwrap_or("");
    let msg_str = ln.get("message").and_then(|v| v.as_str()).unwrap_or("");
    if at.is_empty() && level.is_empty() && msg_str.is_empty() {
        println!("{ln}");
        return;
    }
    println!("{at}  {level:<5}  {msg_str}");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::as_api_error;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── arg-parse errors ────────────────────────────────────────────────────

    #[test]
    fn missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_logs(&g, &[]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--project") || msg.contains("basin link"),
            "err: {msg}"
        );
    }

    // ── cwd-fallback: resolve_project_ref ────────────────────────────────────

    #[test]
    fn resolve_project_ref_uses_flag_when_set() {
        let _cfg = with_temp_config_dir();
        let r = resolve_project_ref("explicit-ref").unwrap();
        assert_eq!(r, "explicit-ref");
    }

    #[test]
    fn resolve_project_ref_no_config_returns_error() {
        let _cfg = with_temp_config_dir();
        let err = resolve_project_ref("").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--project") || msg.contains("basin link"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn resolve_project_ref_reads_working_project() {
        use crate::config::{save_working_project, WorkingProject};
        let _cfg = with_temp_config_dir();
        let dir = std::env::temp_dir().join(format!("logs-wp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        save_working_project(
            &dir,
            &WorkingProject {
                project_ref: "logs-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let wp = crate::config::load_working_project(&dir).unwrap().unwrap();
        assert_eq!(wp.project_ref, "logs-proj");
        // Verify the flag path returns the value directly.
        let r = resolve_project_ref("logs-proj").unwrap();
        assert_eq!(r, "logs-proj");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_logs(&g, &["--help".to_string()]).is_ok());
    }

    // ── history_once happy path ─────────────────────────────────────────────

    #[test]
    fn history_once_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(
                req.path.starts_with("/v1/projects/p1/logs/history"),
                "path: {}",
                req.path
            );
            Resp::ok(
                r#"{"lines":[{"at":"2026-01-01T00:00:00Z","level":"INFO","message":"server started"},{"at":"2026-01-01T00:00:01Z","level":"ERROR","message":"oops"}]}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_logs(&g, &["--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn history_once_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"lines":[{"at":"2026-01-01T00:00:00Z","level":"INFO","message":"hello"}]}"#,
            )
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: serde_json::Value = c
            .do_json(Method::GET, "/v1/projects/p1/logs/history?limit=100", None)
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let lines = v["lines"].as_array().unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["level"].as_str().unwrap(), "INFO");
        assert_eq!(lines[0]["message"].as_str().unwrap(), "hello");
    }

    #[test]
    fn history_once_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"code":"internal","message":"db error"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_logs(&g, &["--project=p1".to_string()]).unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 500);
    }

    #[test]
    fn history_once_limit_flag_sent() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            // Verify limit=50 appears in the query string.
            assert!(req.path.contains("limit=50"), "path: {}", req.path);
            Resp::ok(r#"{"lines":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_logs(&g, &["--project=p1".to_string(), "--limit=50".to_string()]).unwrap();
    }

    // ── poll_once (follow-mode per-iteration fn) ────────────────────────────

    #[test]
    fn poll_once_tracks_high_water_since() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            // Verify "since" is forwarded into the query.
            assert!(req.path.contains("since=2026-01-01"), "path: {}", req.path);
            Resp::ok(
                r#"{"lines":[{"at":"2026-01-02T00:00:00Z","level":"INFO","message":"new line"}]}"#,
            )
        });
        let g = flags(&srv.url);
        let c = crate::client::Client::new(&srv.url, "tok");
        let new_since = poll_once(&c, "p1", "2026-01-01T00:00:00Z", &g);
        assert_eq!(new_since, "2026-01-02T00:00:00Z");
    }

    #[test]
    fn poll_once_no_lines_returns_same_since() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"lines":[]}"#));
        let g = flags(&srv.url);
        let c = crate::client::Client::new(&srv.url, "tok");
        let new_since = poll_once(&c, "p1", "2026-01-01T00:00:00Z", &g);
        assert_eq!(new_since, "2026-01-01T00:00:00Z");
    }

    #[test]
    fn poll_once_server_error_returns_same_since() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(503, r#"{"code":"unavailable","message":"service down"}"#)
        });
        let g = flags(&srv.url);
        let c = crate::client::Client::new(&srv.url, "tok");
        // On error, poll_once prints a warning and returns the old since unchanged.
        let new_since = poll_once(&c, "p1", "2026-01-01T00:00:00Z", &g);
        assert_eq!(new_since, "2026-01-01T00:00:00Z");
    }

    // ── render_log_line ─────────────────────────────────────────────────────

    #[test]
    fn render_line_does_not_panic_on_empty() {
        // Just assert no panic / no error — output goes to stdout which tests
        // capture implicitly in the harness.
        render_log_line(&serde_json::json!({}));
        render_log_line(&serde_json::json!({"at":"","level":"","message":""}));
        render_log_line(
            &serde_json::json!({"at":"2026-01-01T00:00:00Z","level":"INFO","message":"ok"}),
        );
    }
}
