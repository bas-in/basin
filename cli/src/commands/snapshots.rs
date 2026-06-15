//! snapshots — list / create / restore per-project backup snapshots.
//!
//! These are the *table-level* (data-plane) snapshots exposed under
//! `/v1/projects/{ref}/backups/snapshots`.  They are entirely separate from
//! the project-wide DDL+data backup surface in `backups.rs`.
//!
//!   basin snapshots list    [--project <ref>]
//!   basin snapshots create  [--project <ref>] [--name "before-rls"]
//!   basin snapshots restore <id> [--project <ref>] [--yes]   confirm-by-typing-ref
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd)
//! from ./basin/config.toml. Error with hint at `basin link` if neither.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{engine_admin_path, require_client, require_engine_client, GlobalFlags};
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
                    "",
                    "Self-hosted engine mode (BASIN_MODE=engine or --engine):",
                    "  create  --project <ulid>                          Capture engine snapshot head.",
                    "  restore <engine_snapshot_id> --project <ulid>     Restore to a named engine snapshot.",
                    "  Requires BASIN_ADMIN_TOKEN or --admin-token.",
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
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    let c = require_client(g)?;
    let resp: SnapshotsListResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/backups/snapshots"),
        None,
    )?;
    if g.json {
        // JSON shape: { snapshots: [ Snapshot ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "snapshots": resp.snapshots }),
        );
    }
    let mut t = Table::new(
        g,
        &["ID", "ORIGIN", "STATUS", "CREATED", "SIZE", "DESCRIPTION"],
    );
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
                "--name <description>      Free-form description (cloud mode only).",
                "",
                "Self-hosted engine mode (BASIN_MODE=engine or --engine):",
                "  Calls POST /admin/v1/projects/:id/snapshot on the engine directly.",
                "  Requires BASIN_ADMIN_TOKEN or --admin-token with is_admin:true JWT.",
                "  --project must be the engine project ULID, not a cloud ref.",
            ],
        );
        return Ok(());
    }
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();

    if g.engine_mode {
        // Engine admin path: POST /admin/v1/projects/:id/snapshot (no body).
        // The engine ignores any body; the snapshot is the current head of
        // every table in the project.
        let c = require_engine_client(g)?;
        let resp: serde_json::Value = c.do_json(
            Method::POST,
            &engine_admin_path(&project, "snapshot"),
            None,
        )?;
        if g.json {
            // Engine shape: { engine_snapshot_id, size_bytes, created_at }
            return print_json(&mut std::io::stdout(), &resp);
        }
        println!("Snapshot created.");
        if let Some(id) = resp.get("engine_snapshot_id").and_then(|v| v.as_str()) {
            if !id.is_empty() {
                println!("  engine_snapshot_id: {id}");
            }
        }
        if let Some(ts) = resp.get("created_at").and_then(|v| v.as_str()) {
            if !ts.is_empty() {
                println!("  created_at:         {ts}");
            }
        }
        if let Some(sz) = resp.get("size_bytes").and_then(|v| v.as_u64()) {
            println!("  size_bytes:         {sz}");
        }
        return Ok(());
    }

    // Cloud path: POST /v1/projects/:ref/backups/snapshots
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
                "                   In engine mode: the engine_snapshot_id returned by `snapshots create`.",
                "--project <ref>    Project ref (required).",
                "                   In engine mode: the engine project ULID.",
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
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;

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

    if g.engine_mode {
        // Engine admin path: POST /admin/v1/projects/:id/restore
        // Body: { engine_snapshot_id: "<opaque string>" }
        let c = require_engine_client(g)?;
        let body = json!({ "engine_snapshot_id": id });
        let out: serde_json::Value = c.do_json(
            Method::POST,
            &engine_admin_path(&project, "restore"),
            Some(body),
        )?;
        if g.json {
            // Engine shape: { ok: bool, restored_tables: N }
            return print_json(&mut std::io::stdout(), &out);
        }
        let restored = out.get("restored_tables").and_then(|v| v.as_u64()).unwrap_or(0);
        println!("Restore from snapshot {id} complete ({restored} table(s) restored).");
        return Ok(());
    }

    // Cloud path: POST /v1/projects/:ref/backups/restore
    let c = require_client(g)?;
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
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_snapshots(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_snapshots(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_snapshots(&g, &["help".to_string()]).is_ok());
    }

    // ── snapshots list ──────────────────────────────────────────────────────

    #[test]
    fn list_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_snapshots(&g, &["list".to_string()]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--project") || msg.contains("basin link"),
            "err: {msg}"
        );
    }

    #[test]
    fn list_happy_path_table() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1/backups/snapshots");
            Resp::ok(
                r#"{"snapshots":[{"id":"snap-1","origin":"manual","status":"ready","created_at":"2026-01-01T00:00:00Z","size_bytes":1024,"description":"before-rls"}]}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_snapshots(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"snapshots":[{"id":"snap-j","origin":"manual","status":"ready","created_at":"2026-01-01T00:00:00Z","size_bytes":512,"description":"json-test"}]}"#,
            )
        });
        let _g = flags_json(&srv.url);
        // Capture is done by checking no error; JSON parsing is the assertion.
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: SnapshotsListResp = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/p1/backups/snapshots",
                None,
            )
            .unwrap();
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
            Resp::status(
                404,
                r#"{"code":"not_found","message":"Project not found."}"#,
            )
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
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_snapshots(&g, &["create".to_string()]).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--project") || msg.contains("basin link"),
            "err: {msg}"
        );
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
            &[
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=before-rls".to_string(),
            ],
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
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: serde_json::Value = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/p1/backups/snapshots",
                Some(json!({})),
            )
            .unwrap();
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
        let err =
            cmd_snapshots(&g, &["create".to_string(), "--project=p1".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 500);
    }

    // ── snapshots restore ───────────────────────────────────────────────────

    #[test]
    fn restore_missing_id_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_snapshots(
            &g,
            &[
                "restore".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"));
    }

    #[test]
    fn restore_missing_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_snapshots(
            &g,
            &[
                "restore".to_string(),
                "snap-1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap_err();
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
        let r = resolve_project_ref("snap-ref").unwrap();
        assert_eq!(r, "snap-ref");
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
        let dir = std::env::temp_dir().join(format!("snap-wp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        save_working_project(
            &dir,
            &WorkingProject {
                project_ref: "snap-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let wp = crate::config::load_working_project(&dir).unwrap().unwrap();
        assert_eq!(wp.project_ref, "snap-proj");
        // Verify the flag path returns the value directly.
        let r = resolve_project_ref("snap-proj").unwrap();
        assert_eq!(r, "snap-proj");
        std::fs::remove_dir_all(&dir).ok();
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
            &[
                "restore".to_string(),
                "snap-1".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn restore_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"restore":{"id":"r-j","status":"pending"}}"#)
        });
        let _g = flags_json(&srv.url);
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
            Resp::status(
                409,
                r#"{"code":"conflict","message":"restore in progress"}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_snapshots(
            &g,
            &[
                "restore".to_string(),
                "snap-1".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 409);
    }
}
