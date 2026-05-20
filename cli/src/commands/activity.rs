//! activity — project-scoped activity feed.
//!
//!   basin activity list [--project=<ref>] [--limit=N] [--since=<rfc3339>]
//!
//! Backed by GET /v1/projects/{ref}/activity.
//! Project ref resolution: --project= flag, else loadWorkingProject(cwd).
//! Error with hint at `basin link` if neither is set.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// ActivityEvent is the read shape for a single project activity event.
/// JSON shape: { id, kind, actor_id, occurred_at, summary }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ActivityEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub occurred_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
}

#[derive(Deserialize, Serialize)]
struct ActivityResp {
    #[serde(default)]
    events: Vec<ActivityEvent>,
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors with a helpful hint
/// when neither is available.
fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg("--project is required (or run `basin link` to bind this directory)")),
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_activity(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "activity",
                "List project activity events.",
                &[
                    "list [--project=<ref>] [--limit=N] [--since=<rfc3339>]   List recent project activity.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for activity", other);
            Err(silent())
        }
    }
}

// ── activity list ─────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("activity list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit").default_value("50"))
        .arg(Arg::new("since").long("since"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "activity list",
            "List recent project activity events.",
            &[
                "--project <ref>        Project ref (required, or use basin link).",
                "--limit <N>            Maximum number of events to return (default 50).",
                "--since <rfc3339>      Return events at or after this RFC3339 timestamp.",
            ],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let limit = m.get_one::<String>("limit").cloned().unwrap_or_else(|| "50".into());
    let since = m.get_one::<String>("since").cloned().unwrap_or_default();

    let c = require_client(g)?;

    let q = query_string(&[("limit", &limit), ("since", &since)]);
    let resp: ActivityResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/activity{q}"),
        None,
    ).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 403 {
                return msg(format!(
                    "access denied: insufficient permissions to view activity for project {:?}",
                    r#ref
                ));
            }
        }
        e
    })?;

    if g.json {
        // JSON shape: { events: [ ActivityEvent ] }
        return print_json(&mut std::io::stdout(), &json!({ "events": resp.events }));
    }
    if resp.events.is_empty() {
        println!("(no activity)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "KIND", "ACTOR", "OCCURRED_AT", "SUMMARY"]);
    for ev in &resp.events {
        t.row(&[&ev.id, &ev.kind, &ev.actor_id, &ev.occurred_at, &ev.summary]);
    }
    t.flush()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
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

    fn sample_event() -> &'static str {
        r#"{"id":"act1","kind":"migration.apply","actor_id":"user-1","occurred_at":"2026-05-01T10:00:00Z","summary":"Applied migration v1"}"#
    }

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_args_defaults_to_list_which_errors_without_project() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        // no --project and no working project → error
        assert!(cmd_activity(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_activity(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_activity(&g, &["help".to_string()]).is_ok());
    }

    // ── activity list happy path ────────────────────────────────────────────

    #[test]
    fn list_happy_path_table() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path.split('?').next().unwrap(), "/v1/projects/proj1/activity");
            Resp::ok(format!(r#"{{"events":[{}]}}"#, sample_event()))
        });
        let g = flags(&srv.url);
        cmd_activity(&g, &["list".to_string(), "--project=proj1".to_string()]).unwrap();
    }

    #[test]
    fn list_empty_prints_no_activity() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        cmd_activity(&g, &["list".to_string(), "--project=proj1".to_string()]).unwrap();
    }

    // ── query-string passthrough ────────────────────────────────────────────

    #[test]
    fn list_limit_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("limit=20"), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_activity(
            &g,
            &["list".to_string(), "--project=proj1".to_string(), "--limit=20".to_string()],
        ).unwrap();
    }

    #[test]
    fn list_since_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("since="), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_activity(
            &g,
            &[
                "list".to_string(),
                "--project=proj1".to_string(),
                "--since=2026-01-01T00:00:00Z".to_string(),
            ],
        ).unwrap();
    }

    // ── JSON shape ──────────────────────────────────────────────────────────

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"events":[{}]}}"#, sample_event()))
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ActivityResp =
            c.do_json(reqwest::Method::GET, "/v1/projects/proj1/activity", None).unwrap();
        print_json(&mut buf, &json!({ "events": resp.events })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let events = v["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"].as_str().unwrap(), "act1");
        assert_eq!(events[0]["kind"].as_str().unwrap(), "migration.apply");
        assert_eq!(events[0]["summary"].as_str().unwrap(), "Applied migration v1");
    }

    // ── error mapping ───────────────────────────────────────────────────────

    #[test]
    fn list_403_gives_access_denied_message() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"code":"forbidden","message":"insufficient permissions"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_activity(&g, &["list".to_string(), "--project=proj1".to_string()]).unwrap_err();
        assert!(
            err.to_string().contains("access denied") || err.to_string().contains("forbidden"),
            "err={err}"
        );
    }

    #[test]
    fn list_404_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_activity(&g, &["list".to_string(), "--project=proj1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn list_422_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(422, r#"{"code":"validation_error","message":"invalid since value"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_activity(&g, &["list".to_string(), "--project=proj1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── project ref resolution ──────────────────────────────────────────────

    #[test]
    fn list_no_project_flag_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_activity(&g, &["list".to_string()]).unwrap_err();
        assert!(!crate::error::is_silent(err.as_ref()) || err.to_string().contains("project"));
    }
}
