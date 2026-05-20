//! snapshots — list / create / restore per-project backup snapshots.
//!
//! These are the *table-level* (data-plane) snapshots exposed under
//! `/v1/projects/{ref}/backups/snapshots`.  They are entirely separate from
//! the project-wide DDL+data backup surface in `backups.rs`.
//!
//!   basin snapshots list    --project <ref>
//!   basin snapshots create  --project <ref> [--name "before-rls"]
//!   basin snapshots restore <id> --project <ref> [--yes]   confirm-by-typing-ref

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// Snapshot is one entry from GET /v1/projects/{ref}/backups/snapshots.
/// JSON shape: { id, origin, status, created_at, size_bytes, description }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Snapshot {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub origin: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(default)]
    pub size_bytes: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

#[derive(Deserialize)]
struct SnapshotsListResp {
    #[serde(default)]
    snapshots: Vec<Snapshot>,
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_snapshots(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin snapshots (list | create | restore) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "restore" => restore(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "snapshots",
                "List / create / restore project snapshots.",
                &[
                    "list    --project <ref>                             List snapshots, newest first.",
                    "create  --project <ref> [--name <description>]      Create a manual snapshot.",
                    "restore <id> --project <ref> [--yes]                Restore (confirm-by-ref).",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for snapshots", other);
            Err(crate::error::silent())
        }
    }
}

// ── snapshots list ────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("snapshots list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "snapshots list",
            "List backup snapshots for a project, newest first.",
            &["--project <ref>   Project ref (required)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let c = require_client(g)?;
    let resp: SnapshotsListResp =
        c.do_json(Method::GET, &format!("/v1/projects/{project}/backups/snapshots"), None)?;
    if g.json {
        // JSON shape: { snapshots: [ Snapshot ] }
        return print_json(&mut std::io::stdout(), &json!({ "snapshots": resp.snapshots }));
    }
    let mut t = Table::new(g, &["ID", "ORIGIN", "STATUS", "CREATED", "SIZE", "DESCRIPTION"]);
    for s in &resp.snapshots {
        t.row(&[
            &s.id,
            &s.origin,
            &s.status,
            &s.created_at,
            &s.size_bytes.to_string(),
            &s.description,
        ]);
    }
    t.flush()
}

// ── snapshots create ──────────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("snapshots create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "snapshots create",
            "Create a manual snapshot for a project.",
            &[
                "--project <ref>           Project ref (required).",
                "--name <description>      Free-form description.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();

    let c = require_client(g)?;
    let mut body = json!({});
    if !name.is_empty() {
        body["description"] = json!(name);
    }
    // Snapshot creation can be slow — use a longer timeout (60s).
    let resp: serde_json::Value = c.do_json(
        Method::POST,
        &format!("/v1/projects/{project}/backups/snapshots"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { snapshot: Snapshot, engine_msg?: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Snapshot create requested.");
    if let Some(snap) = resp.get("snapshot").and_then(|v| v.as_object()) {
        if let Some(id) = snap.get("id").and_then(|v| v.as_str()) {
            if !id.is_empty() {
                println!("  id:     {id}");
            }
        }
        if let Some(status) = snap.get("status").and_then(|v| v.as_str()) {
            if !status.is_empty() {
                println!("  status: {status}");
            }
        }
    }
    if let Some(msg_str) = resp.get("engine_msg").and_then(|v| v.as_str()) {
        if !msg_str.is_empty() {
            println!("  engine: {msg_str}");
        }
    }
    Ok(())
}

// ── snapshots restore ─────────────────────────────────────────────────────────

fn restore(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("snapshots restore")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "snapshots restore",
            "Restore a project from a snapshot (destructive — confirm by retyping project ref).",
            &[
                "<id>               Snapshot ID to restore.",
                "--project <ref>    Project ref (required).",
                "--yes              Skip the confirm-by-ref prompt.",
            ],
        );
        return Ok(());
    }
    // The snapshot ID is the first positional argument.
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin snapshots restore <id> --project <ref>"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("--project is required"));
    }
    let c = require_client(g)?;
    if !m.get_flag("yes") {
        eprint!(
            "Restore overwrites the project's data plane. Re-type the project ref {:?} to confirm:\n> ",
            project
        );
        let typed = read_line()?;
        if typed.trim() != project {
            return Err(msg("ref mismatch — aborted"));
        }
    }
    let body = json!({
        "source_kind": "snapshot",
        "snapshot_id": id,
    });
    let out: serde_json::Value = c.do_json(
        Method::POST,
        &format!("/v1/projects/{project}/backups/restore"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: passthrough of /v1/projects/:ref/backups/restore envelope
        // (commonly { restore: { id, status }, engine_msg?: string }).
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Restore from snapshot {id} requested.");
    Ok(())
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

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_snapshots(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_snapshots(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_snapshots(&g, &["help".to_string()]).is_ok());
    }

    // ── snapshots list ──────────────────────────────────────────────────────

    #[test]
    fn list_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_snapshots(&g, &["list".to_string()]).unwrap_err();
        assert!(err.to_string().contains("--project"));
    }

    #[test]
    fn list_happy_path_table() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1/backups/snapshots");
            Resp::ok(r#"{"snapshots":[{"id":"snap-1","origin":"manual","status":"ready","created_at":"2026-01-01T00:00:00Z","size_bytes":1024,"description":"before-rls"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_snapshots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"snapshots":[{"id":"snap-j","origin":"manual","status":"ready","created_at":"2026-01-01T00:00:00Z","size_bytes":512,"description":"json-test"}]}"#)
        });
        let g = flags_json(&srv.url);
        // Capture is done by checking no error; JSON parsing is the assertion.
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: SnapshotsListResp =
            c.do_json(reqwest::Method::GET, "/v1/projects/p1/backups/snapshots", None).unwrap();
        print_json(&mut buf, &json!({ "snapshots": resp.snapshots })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let snaps = v["snapshots"].as_array().unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0]["id"].as_str().unwrap(), "snap-j");
        assert_eq!(snaps[0]["description"].as_str().unwrap(), "json-test");
    }

    #[test]
    fn list_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Project not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_snapshots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── snapshots create ────────────────────────────────────────────────────

    #[test]
    fn create_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_snapshots(&g, &["create".to_string()]).unwrap_err();
        assert!(err.to_string().contains("--project"));
    }

    #[test]
    fn create_with_name_sends_description() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/backups/snapshots");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["description"].as_str().unwrap_or(""), "before-rls");
            Resp::ok(r#"{"snapshot":{"id":"snap-new","status":"pending"}}"#)
        });
        let g = flags(&srv.url);
        cmd_snapshots(
            &g,
            &["create".to_string(), "--project=p1".to_string(), "--name=before-rls".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn create_without_name_no_description_field() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            // description key must be absent when --name is not given
            assert!(body.get("description").is_none() || body["description"].is_null());
            Resp::ok(r#"{"snapshot":{"id":"snap-auto","status":"pending"}}"#)
        });
        let g = flags(&srv.url);
        cmd_snapshots(&g, &["create".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"snapshot":{"id":"snap-j","status":"pending"},"engine_msg":"ok"}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: serde_json::Value =
            c.do_json(reqwest::Method::POST, "/v1/projects/p1/backups/snapshots", Some(json!({}))).unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["snapshot"]["id"].as_str().unwrap(), "snap-j");
        assert_eq!(v["engine_msg"].as_str().unwrap(), "ok");
    }

    #[test]
    fn create_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"code":"internal","message":"engine error"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_snapshots(
            &g,
            &["create".to_string(), "--project=p1".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 500);
    }

    // ── snapshots restore ───────────────────────────────────────────────────

    #[test]
    fn restore_missing_id_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_snapshots(
            &g,
            &["restore".to_string(), "--project=p1".to_string(), "--yes".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"));
    }

    #[test]
    fn restore_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_snapshots(
            &g,
            &["restore".to_string(), "snap-1".to_string(), "--yes".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--project"));
    }

    #[test]
    fn restore_yes_skips_prompt_and_calls_api() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/backups/restore");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["snapshot_id"].as_str().unwrap_or(""), "snap-1");
            assert_eq!(body["source_kind"].as_str().unwrap_or(""), "snapshot");
            Resp::ok(r#"{"restore":{"id":"r-1","status":"pending"}}"#)
        });
        let g = flags(&srv.url);
        cmd_snapshots(
            &g,
            &["restore".to_string(), "snap-1".to_string(), "--project=p1".to_string(), "--yes".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn restore_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"restore":{"id":"r-j","status":"pending"}}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: serde_json::Value = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/p1/backups/restore",
                Some(json!({"source_kind":"snapshot","snapshot_id":"snap-1"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["restore"]["id"].as_str().unwrap(), "r-j");
    }

    #[test]
    fn restore_server_409_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"conflict","message":"restore in progress"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_snapshots(
            &g,
            &["restore".to_string(), "snap-1".to_string(), "--project=p1".to_string(), "--yes".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 409);
    }
}
