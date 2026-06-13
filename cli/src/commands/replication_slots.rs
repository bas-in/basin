//! replication_slots — logical replication slot management (T-121).
//!
//!   basin replication-slots list   [--project=<ref>]
//!   basin replication-slots create <name> [--plugin=<plugin>] [--project=<ref>]
//!   basin replication-slots drop   <name> [--project=<ref>] [--yes]
//!
//! Backed by:
//!   GET    /v1/projects/{ref}/replication-slots
//!   POST   /v1/projects/{ref}/replication-slots
//!   DELETE /v1/projects/{ref}/replication-slots/:name
//!
//! Slot names must match [a-z_][a-z0-9_]* and be ≤ 63 bytes (PostgreSQL
//! convention; the cloud enforces this server-side).
//!
//! Useful for CDC pipelines (Debezium, River) that drain from the engine's
//! logical replication stream. Neon surfaces an equivalent `neonctl
//! replication-slot` command.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

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

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// ReplicationSlot mirrors the cloud's list / create response shape.
/// JSON shape: { slot_name, plugin, active, restart_lsn, confirmed_flush_lsn }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ReplicationSlot {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub slot_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub plugin: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub restart_lsn: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub confirmed_flush_lsn: String,
}

#[derive(Default, Deserialize, Serialize)]
struct SlotsListResp {
    #[serde(default)]
    slots: Vec<ReplicationSlot>,
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_replication_slots(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            print_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "drop" => drop_slot(g, rest),
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for replication-slots", other);
            Err(silent())
        }
    }
}

fn print_help() {
    help_for_command(
        "replication-slots",
        "Manage logical replication slots for CDC pipelines (Debezium, River, etc.).",
        &[
            "list   [--project=<ref>]                           List all slots.",
            "create <name> [--plugin=<plugin>] [--project=<ref>]  Create a new slot.",
            "drop   <name> [--project=<ref>] [--yes]            Drop a slot (with confirmation).",
        ],
    );
}

// ── list ──────────────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("replication-slots list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "replication-slots list",
            "List all logical replication slots for the project.",
            &["--project=<ref>   Project ref (or use basin link)."],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/replication-slots", project_ref);
    // The cloud may return a wrapped array or a plain array; try both shapes.
    let raw: serde_json::Value = c.do_json(Method::GET, &path, None)?;

    let slots: Vec<ReplicationSlot> = if raw.is_array() {
        serde_json::from_value(raw).unwrap_or_default()
    } else {
        let resp: SlotsListResp = serde_json::from_value(raw).unwrap_or_default();
        resp.slots
    };

    if g.json {
        // JSON shape: [ ReplicationSlot ]
        return print_json(&mut std::io::stdout(), &slots);
    }

    if slots.is_empty() {
        if !g.quiet {
            println!("No replication slots.");
        }
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "PLUGIN", "ACTIVE", "RESTART_LSN", "CONFIRMED_FLUSH"]);
    for s in &slots {
        let active = if s.active { "yes" } else { "no" };
        t.row(&[
            &s.slot_name,
            &s.plugin,
            active,
            &s.restart_lsn,
            &s.confirmed_flush_lsn,
        ]);
    }
    t.flush()
}

// ── create ────────────────────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("replication-slots create")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("plugin").long("plugin").default_value("pgoutput"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "replication-slots create",
            "Create a new logical replication slot.",
            &[
                "<name>              Slot name: [a-z_][a-z0-9_]*, max 63 chars (required).",
                "--plugin=<plugin>   Output plugin (default: pgoutput). Also: wal2json.",
                "--project=<ref>     Project ref (or use basin link).",
            ],
        );
        return Ok(());
    }

    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin replication-slots create <name> [--plugin=<plugin>]"))?;
    let plugin = m
        .get_one::<String>("plugin")
        .cloned()
        .unwrap_or_else(|| "pgoutput".into());
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/replication-slots", project_ref);
    let body = json!({ "slot_name": name, "plugin": plugin });
    let slot: ReplicationSlot = c.do_json(Method::POST, &path, Some(body))?;

    if g.json {
        // JSON shape: ReplicationSlot
        return print_json(&mut std::io::stdout(), &slot);
    }
    println!("Created replication slot {:?} (plugin: {}).", slot.slot_name, slot.plugin);
    Ok(())
}

// ── drop ──────────────────────────────────────────────────────────────────────

fn drop_slot(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("replication-slots drop")
        .arg(Arg::new("name").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").short('y').action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "replication-slots drop",
            "Drop (delete) a logical replication slot. Requires confirmation.",
            &[
                "<name>           Slot name (required).",
                "--project=<ref>  Project ref (or use basin link).",
                "--yes            Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let name = m
        .get_one::<String>("name")
        .cloned()
        .ok_or_else(|| msg("usage: basin replication-slots drop <name>"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;
    let yes = m.get_flag("yes");

    if !yes && !g.json {
        eprint!("Drop slot {:?}? This cannot be undone. [y/N] ", name);
        let answer = read_line()?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Cancelled.");
            return Ok(());
        }
    } else if g.json && !yes {
        return Err(msg(
            "replication-slots drop: --yes is required when using --json (no interactive prompt)",
        ));
    }

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/replication-slots/{}", project_ref, name);
    c.do_noout(Method::DELETE, &path, None)?;

    if g.json {
        // JSON shape: { slot_name, dropped: true }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "slot_name": name, "dropped": true }),
        );
    }
    println!("Dropped replication slot {:?}.", name);
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::{Arc, Mutex};

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

    fn sample_slot() -> &'static str {
        r#"{"slot_name":"my_slot","plugin":"pgoutput","active":false,"restart_lsn":"0/1A000","confirmed_flush_lsn":"0/1A000"}"#
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_args_prints_help() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_replication_slots(&g, &[]).is_ok());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_replication_slots(&g, &["--help".to_string()]).is_ok());
        assert!(cmd_replication_slots(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_replication_slots(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn subcommand_help_flags_work() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        for sub in &["list", "create", "drop"] {
            let res = cmd_replication_slots(&g, &[sub.to_string(), "--help".to_string()]);
            assert!(res.is_ok(), "{sub} --help: {res:?}");
        }
    }

    // ── list ──────────────────────────────────────────────────────────────────

    #[test]
    fn list_sends_get_to_correct_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!("[{}]", sample_slot()))
        });
        let g = flags(&srv.url);
        cmd_replication_slots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
        assert_eq!(*cap.lock().unwrap(), "/v1/projects/p1/replication-slots");
    }

    #[test]
    fn list_empty_array_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("[]"));
        let g = flags(&srv.url);
        cmd_replication_slots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(format!("[{}]", sample_slot())));
        let g = flags_json(&srv.url);
        cmd_replication_slots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_wrapped_shape_ok() {
        // Cloud may return { "slots": [ ... ] } — verify we handle both shapes.
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::ok(format!(r#"{{"slots":[{}]}}"#, sample_slot()))
        });
        let g = flags(&srv.url);
        cmd_replication_slots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    // ── create ────────────────────────────────────────────────────────────────

    #[test]
    fn create_sends_post_with_body() {
        let _g = with_temp_config_dir();
        let cap_path = Arc::new(Mutex::new(String::new()));
        let cap_body = Arc::new(Mutex::new(String::new()));
        let p2 = Arc::clone(&cap_path);
        let b2 = Arc::clone(&cap_body);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            *p2.lock().unwrap() = req.path.clone();
            *b2.lock().unwrap() = req.body.clone();
            Resp::ok(sample_slot())
        });
        let g = flags(&srv.url);
        cmd_replication_slots(
            &g,
            &[
                "create".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(*cap_path.lock().unwrap(), "/v1/projects/p1/replication-slots");
        let body: serde_json::Value =
            serde_json::from_str(&cap_body.lock().unwrap()).unwrap();
        assert_eq!(body["slot_name"].as_str().unwrap(), "my_slot");
        assert_eq!(body["plugin"].as_str().unwrap(), "pgoutput");
    }

    #[test]
    fn create_custom_plugin() {
        let _g = with_temp_config_dir();
        let cap_body = Arc::new(Mutex::new(String::new()));
        let b2 = Arc::clone(&cap_body);
        let srv = TestServer::start(move |req: &Req| {
            *b2.lock().unwrap() = req.body.clone();
            Resp::ok(sample_slot())
        });
        let g = flags(&srv.url);
        cmd_replication_slots(
            &g,
            &[
                "create".to_string(),
                "my_slot".to_string(),
                "--plugin=wal2json".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let body: serde_json::Value =
            serde_json::from_str(&cap_body.lock().unwrap()).unwrap();
        assert_eq!(body["plugin"].as_str().unwrap(), "wal2json");
    }

    #[test]
    fn create_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_replication_slots(
            &g,
            &["create".to_string(), "--project=p1".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"), "err: {err}");
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(sample_slot()));
        let g = flags_json(&srv.url);
        cmd_replication_slots(
            &g,
            &[
                "create".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
    }

    // ── drop ──────────────────────────────────────────────────────────────────

    #[test]
    fn drop_with_yes_sends_delete() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok("{}")
        });
        let g = flags(&srv.url);
        cmd_replication_slots(
            &g,
            &[
                "drop".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(
            *cap.lock().unwrap(),
            "/v1/projects/p1/replication-slots/my_slot"
        );
    }

    #[test]
    fn drop_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = flags_json("http://127.0.0.1:1");
        let err = cmd_replication_slots(
            &g,
            &[
                "drop".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--yes"),
            "err: {err}"
        );
    }

    #[test]
    fn drop_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_replication_slots(
            &g,
            &["drop".to_string(), "--project=p1".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"), "err: {err}");
    }

    #[test]
    fn drop_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("{}"));
        let g = flags_json(&srv.url);
        cmd_replication_slots(
            &g,
            &[
                "drop".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
    }

    // ── error mapping ─────────────────────────────────────────────────────────

    #[test]
    fn not_found_maps_to_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_replication_slots(
            &g,
            &["list".to_string(), "--project=gone".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn conflict_on_create_maps_to_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(409, r#"{"code":"conflict","message":"slot already exists"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_replication_slots(
            &g,
            &[
                "create".to_string(),
                "my_slot".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 409);
    }
}
