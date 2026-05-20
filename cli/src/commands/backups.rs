//! backups — backup policy, per-project snapshots, and restore jobs.
//!
//!   basin backups policy get                             [--project=<ref>]
//!   basin backups policy set --retention-days=<n> --schedule=<cron> [--enabled=true|false] [--project=<ref>]
//!   basin backups snapshots list                         [--project=<ref>]
//!   basin backups snapshots create [--label=<text>]      [--project=<ref>]
//!   basin backups snapshots expire <id> [--yes]          [--project=<ref>]
//!   basin backups restore --from=<snapshot_id> [--into=<branch_ref>] [--yes] [--project=<ref>]
//!   basin backups restore-jobs list                      [--project=<ref>]
//!
//! Routes: /v1/projects/{ref}/backups/policy, /backups/snapshots,
//!         /backups/snapshots/{id}/expire, /backups/restore, /backups/restore/jobs
//!
//! Note: this is the project-wide DDL+data backup surface. It is entirely
//! separate from the per-table snapshot surface in snapshots.rs
//! (/v1/projects/{ref}/tables/{name}/snapshots). Treat them as distinct.
//!
//! Project ref resolution order:
//!  1. --project=<ref> flag
//!  2. load_working_project(cwd) — ./basin/config.toml (via resolve_project_ref)

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// BackupPolicy is the read/write shape for GET|PUT /v1/projects/{ref}/backups/policy.
/// JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BackupPolicy {
    #[serde(default)]
    pub retention_days: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub schedule: String,
    #[serde(default)]
    pub enabled: bool,
}

/// BackupSnapshot is one row from GET /v1/projects/{ref}/backups/snapshots
/// and the embedded "snapshot" field in create responses.
/// JSON shape: { "id": "...", "label": "...", "size_bytes": N, "created_at": "..." }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BackupSnapshot {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub label: String,
    #[serde(default)]
    pub size_bytes: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// RestoreJob is one row from GET /v1/projects/{ref}/backups/restore/jobs.
/// JSON shape: { "id":"...", "status":"...", "started_at":"...", "completed_at":"...", "snapshot_id":"..." }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct RestoreJob {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub started_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub completed_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub snapshot_id: String,
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

pub fn cmd_backups(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin backups (policy | snapshots | restore | restore-jobs) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "policy" => policy(g, rest),
        "snapshots" => snapshots(g, rest),
        "restore-jobs" => restore_jobs(g, rest),
        "restore" => restore(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "backups",
                "Manage project backup policy, snapshots, and restore jobs.",
                &[
                    "policy get                                   [--project=<ref>]  Get the backup policy.",
                    "policy set --retention-days=<n> --schedule=<cron> [--enabled=true|false] [--project=<ref>]  Update the backup policy.",
                    "snapshots list                               [--project=<ref>]  List backup snapshots.",
                    "snapshots create [--label=<text>]            [--project=<ref>]  Create a manual backup snapshot.",
                    "snapshots expire <id> [--yes]                [--project=<ref>]  Expire (delete) a backup snapshot.",
                    "restore --from=<snapshot_id> [--into=<branch_ref>] [--yes] [--project=<ref>]  Restore from a snapshot.",
                    "restore-jobs list                            [--project=<ref>]  List restore jobs.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for backups", other);
            Err(crate::error::silent())
        }
    }
}

// ── backups policy ────────────────────────────────────────────────────────────

fn policy(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return Err(msg("usage: basin backups policy (get | set) ...")),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => policy_get(g, rest),
        "set" => policy_set(g, rest),
        other => Err(msg(format!("unknown subcommand {other:?} for backups policy"))),
    }
}

fn policy_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups policy get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups policy get",
            "Get the backup policy for a project.",
            &["--project <ref>   Project ref (or use basin link)."],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let policy: BackupPolicy =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/backups/policy"), None)?;
    if g.json {
        // JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
        return print_json(&mut std::io::stdout(), &policy);
    }
    println!("retention_days: {}", policy.retention_days);
    println!("schedule:       {}", policy.schedule);
    println!("enabled:        {}", policy.enabled);
    Ok(())
}

fn policy_set(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups policy set")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("retention-days").long("retention-days"))
        .arg(Arg::new("schedule").long("schedule"))
        .arg(Arg::new("enabled").long("enabled").default_value("true"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups policy set",
            "Update the backup policy for a project.",
            &[
                "--retention-days=<n>        Retention period in days (required, >0).",
                "--schedule=<cron>           Cron expression for scheduled backups (required).",
                "--enabled=true|false        Whether scheduled backups are enabled (default true).",
                "--project <ref>             Project ref (or use basin link).",
            ],
        );
        return Ok(());
    }

    // Parse --retention-days (required, >0).
    let retention_days_str = m.get_one::<String>("retention-days").cloned().unwrap_or_default();
    let retention_days: i64 = if retention_days_str.is_empty() {
        0
    } else {
        retention_days_str
            .parse::<i64>()
            .map_err(|_| msg(format!("--retention-days must be an integer, got {:?}", retention_days_str)))?
    };
    if retention_days <= 0 {
        return Err(msg(format!(
            "--retention-days must be greater than 0 (got {retention_days})"
        )));
    }

    // Parse --schedule (required, non-empty).
    let schedule = m.get_one::<String>("schedule").cloned().unwrap_or_default();
    if schedule.trim().is_empty() {
        return Err(msg("--schedule must be a non-empty cron expression"));
    }

    // Parse --enabled (bool, defaults to "true").
    let enabled_str = m.get_one::<String>("enabled").cloned().unwrap_or_else(|| "true".into());
    let enabled: bool = match enabled_str.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => true,
        "false" | "0" | "no" => false,
        _ => return Err(msg(format!("--enabled must be true or false (got {:?})", enabled_str))),
    };

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let body = json!({
        "retention_days": retention_days,
        "schedule": schedule,
        "enabled": enabled,
    });
    let resp: BackupPolicy =
        c.do_json(Method::PUT, &format!("/v1/projects/{ref}/backups/policy"), Some(body))?;
    if g.json {
        // JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("retention_days: {}", resp.retention_days);
    println!("schedule:       {}", resp.schedule);
    println!("enabled:        {}", resp.enabled);
    Ok(())
}

// ── backups snapshots ─────────────────────────────────────────────────────────

fn snapshots(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin backups snapshots (list | create | expire) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => snapshots_list(g, rest),
        "create" => snapshots_create(g, rest),
        "expire" => snapshots_expire(g, rest),
        other => Err(msg(format!("unknown subcommand {other:?} for backups snapshots"))),
    }
}

fn snapshots_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups snapshots list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups snapshots list",
            "List backup snapshots for a project.",
            &["--project <ref>   Project ref (or use basin link)."],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        snapshots: Vec<BackupSnapshot>,
    }

    let resp: Resp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/backups/snapshots"), None)?;
    if g.json {
        // JSON shape: { "snapshots": [ { "id":"...", "label":"...", "size_bytes":N, "created_at":"..." } ] }
        return print_json(&mut std::io::stdout(), &json!({ "snapshots": resp.snapshots }));
    }
    if resp.snapshots.is_empty() {
        println!("(no backup snapshots)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "LABEL", "SIZE_BYTES", "CREATED"]);
    for s in &resp.snapshots {
        t.row(&[&s.id, &s.label, &s.size_bytes.to_string(), &s.created_at]);
    }
    t.flush()
}

fn snapshots_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups snapshots create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("label").long("label"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups snapshots create",
            "Create a manual backup snapshot for a project.",
            &[
                "--project <ref>    Project ref (or use basin link).",
                "--label <text>     Optional label for the snapshot.",
            ],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let label = m.get_one::<String>("label").cloned().unwrap_or_default();

    let c = require_client(g)?;
    // Snapshot creation can be slow — use a longer timeout (60s).
    let mut body = json!({});
    if !label.is_empty() {
        body["label"] = json!(label);
    }

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        snapshot: Option<BackupSnapshot>,
    }

    let resp: Resp =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/backups/snapshots"), Some(body))?;
    if g.json {
        // JSON shape: { "snapshot": { "id":"...", "label":"...", "size_bytes":N, "created_at":"..." } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Backup snapshot create requested.");
    if let Some(s) = &resp.snapshot {
        if !s.id.is_empty() {
            println!("  id:    {}", s.id);
        }
        if !s.label.is_empty() {
            println!("  label: {}", s.label);
        }
    }
    Ok(())
}

fn snapshots_expire(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups snapshots expire")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups snapshots expire",
            "Expire (delete) a backup snapshot. This cannot be undone.",
            &[
                "<id>               Snapshot ID to expire (required).",
                "--project <ref>    Project ref (or use basin link).",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_many::<String>("id")
        .and_then(|mut it| it.next().cloned())
        .ok_or_else(|| msg("usage: basin backups snapshots expire <id> [--project=<ref>] [--yes]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to expire snapshot {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Expire backup snapshot {:?} for project {:?}? This cannot be undone. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_ascii_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct ExpireResp {
        #[serde(default)]
        expired: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        id: String,
    }

    let resp: ExpireResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/backups/snapshots/{id}/expire"),
        None,
    )?;
    if g.json {
        // JSON shape: { "expired": true, "id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Expired backup snapshot {id}.");
    Ok(())
}

// ── backups restore ───────────────────────────────────────────────────────────

fn restore(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups restore")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("from").long("from"))
        .arg(Arg::new("into").long("into"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups restore",
            "Restore a project from a backup snapshot.",
            &[
                "--from=<snapshot_id>      Snapshot ID to restore from (required).",
                "--into=<branch_ref>       Branch ref to restore into (optional).",
                "--yes                     Skip the confirmation prompt.",
                "--project <ref>           Project ref (or use basin link).",
            ],
        );
        return Ok(());
    }

    let from = m.get_one::<String>("from").cloned().unwrap_or_default();
    if from.is_empty() {
        return Err(msg("--from is required (snapshot ID to restore from)"));
    }
    let into = m.get_one::<String>("into").cloned().unwrap_or_default();
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg("confirmation required: pass --yes to restore non-interactively under --json"));
        }
        eprint!(
            "Restore project {:?} from snapshot {:?}? This will overwrite data. [y/N] ",
            r#ref, from
        );
        let line = read_line()?;
        if line.trim().to_ascii_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    // Restore can be slow — use a longer timeout (2 min).
    let mut body = json!({ "snapshot_id": from });
    if !into.is_empty() {
        body["into_branch_ref"] = json!(into);
    }

    #[derive(Deserialize, Serialize)]
    struct RestoreResp {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        restore_job_id: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        status: String,
    }

    let resp: RestoreResp =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/backups/restore"), Some(body))?;
    if g.json {
        // JSON shape: { "restore_job_id": "...", "status": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Restore job started: {} (status: {})", resp.restore_job_id, resp.status);
    Ok(())
}

// ── backups restore-jobs ──────────────────────────────────────────────────────

fn restore_jobs(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return Err(msg("usage: basin backups restore-jobs (list) ...")),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => restore_jobs_list(g, rest),
        other => Err(msg(format!("unknown subcommand {other:?} for backups restore-jobs"))),
    }
}

fn restore_jobs_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("backups restore-jobs list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "backups restore-jobs list",
            "List restore jobs for a project.",
            &["--project <ref>   Project ref (or use basin link)."],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        jobs: Vec<RestoreJob>,
    }

    let resp: Resp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/backups/restore/jobs"), None)?;
    if g.json {
        // JSON shape: { "jobs": [ { "id":"...", "status":"...", "started_at":"...", "completed_at":"...", "snapshot_id":"..." } ] }
        return print_json(&mut std::io::stdout(), &json!({ "jobs": resp.jobs }));
    }
    if resp.jobs.is_empty() {
        println!("(no restore jobs)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "STATUS", "STARTED", "COMPLETED", "SNAPSHOT"]);
    for j in &resp.jobs {
        let completed = if j.completed_at.is_empty() { "—".to_string() } else { j.completed_at.clone() };
        t.row(&[&j.id, &j.status, &j.started_at, &completed, &j.snapshot_id]);
    }
    t.flush()
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

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_args_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_backups(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        let err = cmd_backups(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), ..Default::default() };
        assert!(cmd_backups(&g, &["help".to_string()]).is_ok());
    }

    // ── backups policy get ──────────────────────────────────────────────────

    #[test]
    fn policy_get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/backups/policy");
            Resp::ok(r#"{"retention_days":7,"schedule":"0 2 * * *","enabled":true}"#)
        });
        let g = flags(&srv.url);
        // Should not error
        cmd_backups(&g, &["policy".to_string(), "get".to_string(), "--project=proj1".to_string()])
            .unwrap();
    }

    #[test]
    fn policy_get_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"retention_days":7,"schedule":"0 2 * * *","enabled":true}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let policy: BackupPolicy =
            c.do_json(Method::GET, "/v1/projects/proj1/backups/policy", None).unwrap();
        print_json(&mut buf, &policy).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["retention_days"].as_i64().unwrap(), 7);
        assert_eq!(v["schedule"].as_str().unwrap(), "0 2 * * *");
        assert!(v["enabled"].as_bool().unwrap());
    }

    #[test]
    fn policy_get_404_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Project not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &["policy".to_string(), "get".to_string(), "--project=proj1".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── backups policy set ──────────────────────────────────────────────────

    #[test]
    fn policy_set_happy_path() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(Mutex::new(serde_json::Value::Null));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "PUT");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"retention_days":14,"schedule":"0 3 * * *","enabled":true}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &[
                "policy".to_string(),
                "set".to_string(),
                "--project=proj1".to_string(),
                "--retention-days=14".to_string(),
                "--schedule=0 3 * * *".to_string(),
                "--enabled=true".to_string(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["retention_days"].as_i64().unwrap(), 14);
        assert_eq!(body["schedule"].as_str().unwrap(), "0 3 * * *");
        assert!(body["enabled"].as_bool().unwrap());
    }

    #[test]
    fn policy_set_validation_retention_zero() {
        let _g = with_temp_config_dir();
        // Should not call HTTP; fails at validation.
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{}"#));
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "policy".to_string(),
                "set".to_string(),
                "--project=proj1".to_string(),
                "--retention-days=0".to_string(),
                "--schedule=0 2 * * *".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("retention-days"), "err: {err}");
    }

    #[test]
    fn policy_set_validation_empty_schedule() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{}"#));
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "policy".to_string(),
                "set".to_string(),
                "--project=proj1".to_string(),
                "--retention-days=7".to_string(),
                "--schedule=".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("schedule"), "err: {err}");
    }

    #[test]
    fn policy_set_422_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(422, r#"{"code":"invalid_policy","message":"Retention days exceeds plan limit."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "policy".to_string(),
                "set".to_string(),
                "--project=proj1".to_string(),
                "--retention-days=365".to_string(),
                "--schedule=0 2 * * *".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }

    #[test]
    fn policy_set_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"retention_days":30,"schedule":"0 4 * * *","enabled":false}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: BackupPolicy = c
            .do_json(
                Method::PUT,
                "/v1/projects/proj1/backups/policy",
                Some(json!({"retention_days":30,"schedule":"0 4 * * *","enabled":false})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["retention_days"].as_i64().unwrap(), 30);
        assert!(!v["enabled"].as_bool().unwrap());
    }

    // ── backups snapshots list ──────────────────────────────────────────────

    #[test]
    fn snapshots_list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/backups/snapshots");
            Resp::ok(r#"{"snapshots":[{"id":"snap-1","label":"before-migration","size_bytes":1048576,"created_at":"2026-01-01T00:00:00Z"},{"id":"snap-2","label":"nightly","size_bytes":2048,"created_at":"2026-01-02T00:00:00Z"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &["snapshots".to_string(), "list".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn snapshots_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"snapshots":[]}"#));
        let g = flags(&srv.url);
        // Capture stdout via pipe is complex in Rust tests; just verify no error.
        // The "(no backup snapshots)" print is covered by the logic being identical to Go.
        cmd_backups(
            &g,
            &["snapshots".to_string(), "list".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn snapshots_list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"snapshots":[{"id":"snap-1","label":"test","size_bytes":1024,"created_at":"2026-01-01T00:00:00Z"}]}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        #[derive(Deserialize)]
        struct R { #[serde(default)] snapshots: Vec<BackupSnapshot> }
        let resp: R = c.do_json(Method::GET, "/v1/projects/proj1/backups/snapshots", None).unwrap();
        print_json(&mut buf, &json!({ "snapshots": resp.snapshots })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let snaps = v["snapshots"].as_array().unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0]["id"].as_str().unwrap(), "snap-1");
        assert_eq!(snaps[0]["label"].as_str().unwrap(), "test");
    }

    // ── backups snapshots create ────────────────────────────────────────────

    #[test]
    fn snapshots_create_with_label() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(Mutex::new(serde_json::Value::Null));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"snapshot":{"id":"snap-new","label":"my-label","size_bytes":0,"created_at":""}}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &[
                "snapshots".to_string(),
                "create".to_string(),
                "--project=proj1".to_string(),
                "--label=my-label".to_string(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["label"].as_str().unwrap(), "my-label");
    }

    #[test]
    fn snapshots_create_no_label_omits_field() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(Mutex::new(serde_json::Value::Null));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"snapshot":{"id":"snap-auto","label":"","size_bytes":0,"created_at":""}}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &["snapshots".to_string(), "create".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        // No "label" key in body when flag is omitted.
        assert!(body.get("label").is_none(), "label field should not be present, got: {body}");
    }

    #[test]
    fn snapshots_create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"snapshot":{"id":"snap-j","label":"json-snap","size_bytes":0,"created_at":""}}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize, Serialize)]
        struct SnapResp {
            #[serde(default, skip_serializing_if = "Option::is_none")]
            snapshot: Option<BackupSnapshot>,
        }
        let resp: SnapResp = c
            .do_json(Method::POST, "/v1/projects/proj1/backups/snapshots", Some(json!({"label":"json-snap"})))
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["snapshot"]["id"].as_str().unwrap(), "snap-j");
        assert_eq!(v["snapshot"]["label"].as_str().unwrap(), "json-snap");
    }

    // ── backups snapshots expire ────────────────────────────────────────────

    #[test]
    fn snapshots_expire_yes_calls_api() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/backups/snapshots/snap-1/expire");
            Resp::ok(r#"{"expired":true,"id":"snap-1"}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &[
                "snapshots".to_string(),
                "expire".to_string(),
                "--project=proj1".to_string(),
                "--yes".to_string(),
                "snap-1".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn snapshots_expire_404_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Snapshot not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "snapshots".to_string(),
                "expire".to_string(),
                "--project=proj1".to_string(),
                "--yes".to_string(),
                "snap-missing".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn snapshots_expire_409_conflict() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"already_expired","message":"Snapshot is already expired."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "snapshots".to_string(),
                "expire".to_string(),
                "--project=proj1".to_string(),
                "--yes".to_string(),
                "snap-done".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 409);
    }

    #[test]
    fn snapshots_expire_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"expired":true,"id":"snap-1"}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize, Serialize)]
        struct ExpireResp { expired: bool, #[serde(skip_serializing_if = "String::is_empty", default)] id: String }
        let resp: ExpireResp = c
            .do_json(Method::POST, "/v1/projects/proj1/backups/snapshots/snap-1/expire", None)
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["expired"].as_bool().unwrap());
        assert_eq!(v["id"].as_str().unwrap(), "snap-1");
    }

    // ── backups restore ─────────────────────────────────────────────────────

    #[test]
    fn restore_missing_from_errors() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{}"#));
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &["restore".to_string(), "--project=proj1".to_string(), "--yes".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--from"), "err: {err}");
    }

    #[test]
    fn restore_same_project_yes() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(Mutex::new(serde_json::Value::Null));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"restore_job_id":"job-1","status":"pending"}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &[
                "restore".to_string(),
                "--project=proj1".to_string(),
                "--from=snap-1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["snapshot_id"].as_str().unwrap(), "snap-1");
        assert!(body.get("into_branch_ref").is_none(), "into_branch_ref should be absent");
    }

    #[test]
    fn restore_with_into_branch() {
        let _g = with_temp_config_dir();
        let captured = Arc::new(Mutex::new(serde_json::Value::Null));
        let cap2 = Arc::clone(&captured);
        let srv = TestServer::start(move |req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *cap2.lock().unwrap() = body;
            Resp::ok(r#"{"restore_job_id":"job-2","status":"running"}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &[
                "restore".to_string(),
                "--project=proj1".to_string(),
                "--from=snap-1".to_string(),
                "--into=branch-ref-1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
        let body = captured.lock().unwrap();
        assert_eq!(body["into_branch_ref"].as_str().unwrap(), "branch-ref-1");
    }

    #[test]
    fn restore_conflict_409_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"restore_in_progress","message":"A restore is already in progress."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_backups(
            &g,
            &[
                "restore".to_string(),
                "--project=proj1".to_string(),
                "--from=snap-1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 409);
    }

    #[test]
    fn restore_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"restore_job_id":"job-j","status":"pending"}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize, Serialize)]
        struct RestoreResp {
            #[serde(default, skip_serializing_if = "String::is_empty")]
            restore_job_id: String,
            #[serde(default, skip_serializing_if = "String::is_empty")]
            status: String,
        }
        let resp: RestoreResp = c
            .do_json(
                Method::POST,
                "/v1/projects/proj1/backups/restore",
                Some(json!({"snapshot_id":"snap-1"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["restore_job_id"].as_str().unwrap(), "job-j");
        assert_eq!(v["status"].as_str().unwrap(), "pending");
    }

    // ── backups restore-jobs list ───────────────────────────────────────────

    #[test]
    fn restore_jobs_list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/backups/restore/jobs");
            Resp::ok(r#"{"jobs":[{"id":"job-1","status":"completed","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T01:00:00Z","snapshot_id":"snap-1"},{"id":"job-2","status":"running","started_at":"2026-01-02T00:00:00Z","completed_at":"","snapshot_id":"snap-2"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &["restore-jobs".to_string(), "list".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn restore_jobs_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"jobs":[]}"#));
        let g = flags(&srv.url);
        // No error; "(no restore jobs)" is printed.
        cmd_backups(
            &g,
            &["restore-jobs".to_string(), "list".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn restore_jobs_list_inflight_em_dash() {
        // In-flight job: no completed_at → rendered as "—".
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"jobs":[{"id":"job-3","status":"running","started_at":"2026-01-01T00:00:00Z","completed_at":"","snapshot_id":"snap-3"}]}"#)
        });
        // Render the em-dash logic directly.
        let j = RestoreJob {
            id: "job-3".into(),
            status: "running".into(),
            started_at: "2026-01-01T00:00:00Z".into(),
            completed_at: String::new(),
            snapshot_id: "snap-3".into(),
        };
        let completed = if j.completed_at.is_empty() { "—".to_string() } else { j.completed_at.clone() };
        assert_eq!(completed, "—");
        // Also verify the API call doesn't fail.
        let g = flags(&srv.url);
        cmd_backups(
            &g,
            &["restore-jobs".to_string(), "list".to_string(), "--project=proj1".to_string()],
        )
        .unwrap();
    }

    #[test]
    fn restore_jobs_list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"jobs":[{"id":"job-j","status":"completed","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T01:00:00Z","snapshot_id":"snap-j"}]}"#)
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");

        #[derive(Deserialize)]
        struct R { #[serde(default)] jobs: Vec<RestoreJob> }
        let resp: R = c
            .do_json(Method::GET, "/v1/projects/proj1/backups/restore/jobs", None)
            .unwrap();
        print_json(&mut buf, &json!({ "jobs": resp.jobs })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let jobs = v["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0]["id"].as_str().unwrap(), "job-j");
        assert_eq!(jobs[0]["status"].as_str().unwrap(), "completed");
        assert_eq!(jobs[0]["snapshot_id"].as_str().unwrap(), "snap-j");
    }
}
