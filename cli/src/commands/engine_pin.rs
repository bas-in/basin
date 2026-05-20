//! engine-pin — engine version pin lifecycle for a project.
//!
//!   basin engine-pin get    [--project=<ref>]
//!   basin engine-pin put    --version=<v> [--project=<ref>] [--yes]
//!   basin engine-pin delete [--project=<ref>] [--yes]
//!   basin engine-pin events [--project=<ref>] [--limit=N]
//!
//! Backed by /v1/projects/{ref}/engine/pin (GET/PUT/DELETE)
//! and /v1/projects/{ref}/engine/events (GET).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// EnginePin is the "pin" object returned by GET/PUT /engine/pin.
/// JSON shape: { version, pinned_at, pinned_by }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct EnginePin {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pinned_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pinned_by: String,
}

/// EnginePinEvent is one entry in GET /engine/events.
/// JSON shape: { id, action, version, at, actor_id }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct EnginePinEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_id: String,
}

#[derive(Deserialize, Serialize)]
struct PinResp {
    #[serde(default)]
    pin: Option<EnginePin>,
}

#[derive(Deserialize)]
struct EventsResp {
    #[serde(default)]
    events: Vec<EnginePinEvent>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

fn help_text() -> (&'static str, &'static str, &'static [&'static str]) {
    (
        "engine-pin",
        "Get / set / delete the engine version pin for a project.",
        &[
            "get    [--project=<ref>]                      Show the current engine version pin.",
            "put    --version=<v> [--project=<ref>] [--yes] Pin the engine to a specific version.",
            "delete [--project=<ref>] [--yes]               Unpin the engine version.",
            "events [--project=<ref>] [--limit=N]           List pin audit events.",
        ],
    )
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_engine_pin(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            let (name, summary, lines) = help_text();
            help_for_command(name, summary, lines);
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => get(g, rest),
        "put" => put(g, rest),
        "delete" => delete(g, rest),
        "events" => events(g, rest),
        "--help" | "-h" | "help" => {
            let (name, summary, lines) = help_text();
            help_for_command(name, summary, lines);
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for engine-pin", other);
            Err(silent())
        }
    }
}

// ── engine-pin get ───────────────────────────────────────────────────────────

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("engine-pin get").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: PinResp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/engine/pin"), None)?;
    if g.json {
        // JSON shape: { pin: EnginePin | null }
        return print_json(&mut std::io::stdout(), &resp);
    }
    match resp.pin {
        None => println!("(no engine version pin set)"),
        Some(p) => {
            println!("version:   {}", p.version);
            println!("pinned_at: {}", p.pinned_at);
            println!("pinned_by: {}", p.pinned_by);
        }
    }
    Ok(())
}

// ── engine-pin put ───────────────────────────────────────────────────────────

fn put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("engine-pin put")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("version").long("version"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    let ver = m.get_one::<String>("version").cloned().unwrap_or_default();
    if ver.is_empty() {
        return Err(msg("engine-pin put requires --version=<version>"));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to pin engine version {:?} non-interactively under --json",
                ver
            )));
        }
        eprint!(
            "Pin engine to version {:?} on project {:?}? [y/N] ",
            ver, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let body = json!({ "version": ver });
    let resp: PinResp = c.do_json(
        Method::PUT,
        &format!("/v1/projects/{ref}/engine/pin"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { pin: EnginePin }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(p) = resp.pin {
        println!("Pinned engine to version {}.", p.version);
    } else {
        println!("Pinned engine to version {ver}.");
    }
    Ok(())
}

// ── engine-pin delete ────────────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("engine-pin delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to unpin engine version non-interactively under --json"
            ));
        }
        eprint!(
            "Remove the engine version pin from project {:?}? [y/N] ",
            r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct UnpinResp {
        #[serde(default)]
        unpinned: bool,
    }
    let resp: UnpinResp = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/engine/pin"),
        None,
    )?;
    if g.json {
        // JSON shape: { unpinned: true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Engine version pin removed.");
    Ok(())
}

// ── engine-pin events ────────────────────────────────────────────────────────

fn events(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("engine-pin events")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let limit_str = m
        .get_one::<String>("limit")
        .cloned()
        .unwrap_or_else(|| "50".into());
    let c = require_client(g)?;
    let q = query_string(&[("limit", &limit_str)]);
    let resp: EventsResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/engine/events{q}"),
        None,
    )?;
    if g.json {
        // JSON shape: { events: [ EnginePinEvent ] }
        return print_json(&mut std::io::stdout(), &json!({ "events": resp.events }));
    }
    if resp.events.is_empty() {
        println!("(no engine pin events)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "ACTION", "VERSION", "AT", "ACTOR_ID"]);
    for ev in &resp.events {
        t.row(&[&ev.id, &ev.action, &ev.version, &ev.at, &ev.actor_id]);
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
            token: "bso_org_test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn flags_json(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "bso_org_test".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    fn sample_pin(version: &str) -> String {
        format!(
            r#"{{"version":"{version}","pinned_at":"2026-05-01T00:00:00Z","pinned_by":"user-1"}}"#
        )
    }

    // ── engine-pin get ────────────────────────────────────────────────────────

    #[test]
    fn get_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/engine/pin");
            Resp::ok(format!(r#"{{"pin":{}}}"#, sample_pin("1.2.3")))
        });
        let g = flags(&srv.url);
        cmd_engine_pin(&g, &["get".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn get_null_pin() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"pin":null}"#));
        let g = flags(&srv.url);
        cmd_engine_pin(&g, &["get".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn get_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"pin":{}}}"#, sample_pin("2.0.0")))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PinResp = c
            .do_json(reqwest::Method::GET, "/v1/projects/proj1/engine/pin", None)
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["pin"]["version"].as_str().unwrap(), "2.0.0");
        let _ = &g;
    }

    #[test]
    fn get_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"project not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_engine_pin(&g, &["get".into(), "--project=proj1".into()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn get_403() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_engine_pin(&g, &["get".into(), "--project=proj1".into()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    #[test]
    fn get_no_project_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_engine_pin(&g, &["get".into()]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("link"));
    }

    // ── engine-pin put ────────────────────────────────────────────────────────

    #[test]
    fn put_missing_version_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_engine_pin(&g, &["put".into(), "--project=proj1".into()]).unwrap_err();
        assert!(err.to_string().contains("--version"));
    }

    #[test]
    fn put_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PUT");
            assert_eq!(req.path, "/v1/projects/proj1/engine/pin");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["version"], "1.5.0");
            Resp::ok(format!(r#"{{"pin":{}}}"#, sample_pin("1.5.0")))
        });
        let g = flags(&srv.url);
        cmd_engine_pin(
            &g,
            &[
                "put".into(),
                "--project=proj1".into(),
                "--version=1.5.0".into(),
                "--yes".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn put_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"pin":{}}}"#, sample_pin("3.0.0")))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PinResp = c
            .do_json(
                reqwest::Method::PUT,
                "/v1/projects/proj1/engine/pin",
                Some(json!({"version":"3.0.0"})),
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["pin"]["version"].as_str().unwrap(), "3.0.0");
        let _ = &g;
    }

    #[test]
    fn put_422() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"validation_error","message":"invalid version format"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_engine_pin(
            &g,
            &[
                "put".into(),
                "--project=proj1".into(),
                "--version=bad-ver".into(),
                "--yes".into(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── engine-pin delete ─────────────────────────────────────────────────────

    #[test]
    fn delete_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/engine/pin");
            Resp::ok(r#"{"unpinned":true}"#)
        });
        let g = flags(&srv.url);
        cmd_engine_pin(
            &g,
            &["delete".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap();
    }

    #[test]
    fn delete_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"unpinned":true}"#));
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct UR {
            unpinned: bool,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: UR = c
            .do_json(
                reqwest::Method::DELETE,
                "/v1/projects/proj1/engine/pin",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["unpinned"].as_bool().unwrap());
        let _ = &g;
    }

    // ── engine-pin events ─────────────────────────────────────────────────────

    #[test]
    fn events_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req.path.starts_with("/v1/projects/proj1/engine/events"));
            Resp::ok(
                r#"{"events":[{"id":"ev1","action":"pin","version":"1.5.0","at":"2026-05-01T00:00:00Z","actor_id":"user-1"},{"id":"ev2","action":"unpin","version":"","at":"2026-05-02T00:00:00Z","actor_id":"user-2"}]}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_engine_pin(&g, &["events".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn events_limit_passthrough() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(
                req.path.contains("limit=10"),
                "limit not in path: {}",
                req.path
            );
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_engine_pin(
            &g,
            &[
                "events".into(),
                "--project=proj1".into(),
                "--limit=10".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn events_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        cmd_engine_pin(&g, &["events".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn events_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"events":[{"id":"ev1","action":"pin","version":"2.0.0","at":"2026-05-01T00:00:00Z","actor_id":"user-1"}]}"#,
            )
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: EventsResp = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/proj1/engine/events",
                None,
            )
            .unwrap();
        print_json(&mut out, &json!({ "events": resp.events })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["events"][0]["id"].as_str().unwrap(), "ev1");
        let _ = &g;
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_args_shows_help() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        // no args → shows help and returns Ok
        assert!(cmd_engine_pin(&g, &[]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_engine_pin(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_engine_pin(&g, &["help".into()]).is_ok());
    }
}
