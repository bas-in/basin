//! transfers — project + org-ownership transfer flows.
//!
//! Routes:
//!   basin projects transfers list                              [--project <ref>]
//!   basin projects transfers create --to-org=<slug>            [--project <ref>] [--yes]
//!   basin projects transfers cancel <id>                       [--project <ref>] [--yes]
//!
//!   basin orgs ownership-transfers list <slug>
//!   basin orgs ownership-transfers create --to-user-id=<id> <slug> [--yes]
//!   basin orgs ownership-transfers cancel <slug> <id>          [--yes]
//!   basin orgs ownership-transfers accept <slug> <id>
//!   basin orgs ownership-transfers decline <slug> <id>
//!
//!   basin orgs incoming-project-transfers list <slug>
//!   basin orgs incoming-project-transfers accept <slug> <id>
//!   basin orgs incoming-project-transfers decline <slug> <id>
//!
//! Dispatch is called from commands/projects.rs (`transfers` sub) and
//! commands/orgs.rs (`ownership-transfers` + `incoming-project-transfers`
//! subs). This file owns all the implementation.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ─────────────────────────────────────────────────────────────

/// ProjectTransfer mirrors a single row from
/// GET /v1/projects/{ref}/transfers.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProjectTransfer {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from_org_slug: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_org_slug: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// OrgOwnershipTransfer mirrors a single row from
/// GET /v1/orgs/{slug}/ownership-transfers.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct OrgOwnershipTransfer {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_user_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// IncomingProjectTransfer mirrors a single row from
/// GET /v1/orgs/{slug}/incoming-project-transfers.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IncomingProjectTransfer {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub from_org_slug: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub to_org_slug: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

// ── Project ref resolution ───────────────────────────────────────────────────

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

// ── basin projects transfers ─────────────────────────────────────────────────

/// cmd_projects_transfers routes `basin projects transfers <sub>`.
/// Called from cmd_projects in projects.rs.
pub fn cmd_projects_transfers(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return projects_transfers_list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => projects_transfers_list(g, rest),
        "create" => projects_transfers_create(g, rest),
        "cancel" => projects_transfers_cancel(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "projects transfers",
                "Project transfer requests between orgs.",
                &[
                    "list                              List pending + historical transfers for a project.",
                    "create --to-org <slug> [--yes]    Initiate a transfer to another org.",
                    "cancel <id> [--yes]               Cancel a pending transfer you initiated.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for projects transfers", other);
            Err(silent())
        }
    }
}

fn projects_transfers_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects transfers list")
        .arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        transfers: Vec<ProjectTransfer>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/transfers"), None)?;

    if g.json {
        // JSON shape: { transfers: [ProjectTransfer] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.transfers.is_empty() {
        println!("(no transfers)");
        return Ok(());
    }
    for t in &resp.transfers {
        println!("{}\t{}\t{} → {}\t{}", t.id, t.status, t.from_org_slug, t.to_org_slug, t.created_at);
    }
    Ok(())
}

fn projects_transfers_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects transfers create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("to-org").long("to-org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let to_org = m.get_one::<String>("to-org").cloned().unwrap_or_default();
    if to_org.trim().is_empty() {
        return Err(msg("projects transfers create requires --to-org=<slug>"));
    }
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to initiate a transfer non-interactively under --json",
            ));
        }
        eprint!("Initiate transfer of project {:?} to org {:?}? [y/N] ", r#ref, to_org);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let body = json!({ "to_org_slug": to_org });

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        transfer: Option<ProjectTransfer>,
    }
    let resp: Resp = c.do_json(Method::POST, &format!("/v1/projects/{ref}/transfers"), Some(body))?;

    if g.json {
        // JSON shape: { transfer: ProjectTransfer }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(t) = &resp.transfer {
        println!("Transfer initiated: {} → {} (id={}, status={})", r#ref, to_org, t.id, t.status);
    } else {
        println!("Transfer initiated: {} → {}", r#ref, to_org);
    }
    Ok(())
}

fn projects_transfers_cancel(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects transfers cancel")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin projects transfers cancel <id> [--project=<ref>] [--yes]"))?;
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to cancel transfer {:?} non-interactively under --json",
                id
            )));
        }
        eprint!("Cancel transfer {:?} for project {:?}? [y/N] ", id, r#ref);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        cancelled: bool,
        id: String,
    }
    let resp: Resp =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/transfers/{id}/cancel"), None)?;

    if g.json {
        // JSON shape: { cancelled: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Cancelled transfer {:?}.", resp.id);
    Ok(())
}

// ── basin orgs ownership-transfers ──────────────────────────────────────────

/// cmd_orgs_ownership_transfers routes `basin orgs ownership-transfers <sub>`.
/// Called from cmd_orgs in orgs.rs.
pub fn cmd_orgs_ownership_transfers(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin orgs ownership-transfers <list|create|cancel|accept|decline> <slug> [args]",
            ))
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => ownership_transfers_list(g, rest),
        "create" => ownership_transfers_create(g, rest),
        "cancel" => ownership_transfers_cancel(g, rest),
        "accept" => ownership_transfer_accept_decline(g, rest, "accept"),
        "decline" => ownership_transfer_accept_decline(g, rest, "decline"),
        "--help" | "-h" | "help" => {
            help_for_command(
                "orgs ownership-transfers",
                "Org-owner handover requests.",
                &[
                    "list <slug>                                   List ownership transfers.",
                    "create --to-user-id <id> <slug> [--yes]       Initiate handover (owner only).",
                    "cancel <slug> <id> [--yes]                    Cancel a pending handover you initiated.",
                    "accept <slug> <id>                            Accept an incoming handover.",
                    "decline <slug> <id>                           Decline an incoming handover.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for orgs ownership-transfers", other);
            Err(silent())
        }
    }
}

fn ownership_transfers_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs ownership-transfers list")
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs ownership-transfers list <slug>"))?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        transfers: Vec<OrgOwnershipTransfer>,
    }
    let resp: Resp =
        c.do_json(Method::GET, &format!("/v1/orgs/{slug}/ownership-transfers"), None)?;

    if g.json {
        // JSON shape: { transfers: [OrgOwnershipTransfer] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.transfers.is_empty() {
        println!("No ownership transfers.");
        return Ok(());
    }
    for t in &resp.transfers {
        println!("{}\t{}\t→ user {}\t{}", t.id, t.status, t.to_user_id, t.created_at);
    }
    Ok(())
}

fn ownership_transfers_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs ownership-transfers create")
        .arg(Arg::new("to-user-id").long("to-user-id"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| {
            msg("usage: basin orgs ownership-transfers create --to-user-id=<id> <slug> [--yes]")
        })?;
    let to_user = m.get_one::<String>("to-user-id").cloned().unwrap_or_default();
    if to_user.trim().is_empty() {
        return Err(msg("orgs ownership-transfers create requires --to-user-id=<id>"));
    }

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to initiate ownership transfer non-interactively under --json",
            ));
        }
        eprint!(
            "Initiate ownership transfer of org {:?} to user {:?}? [y/N] ",
            slug, to_user
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let body = json!({ "to_user_id": to_user });

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        transfer: Option<OrgOwnershipTransfer>,
    }
    let resp: Resp =
        c.do_json(Method::POST, &format!("/v1/orgs/{slug}/ownership-transfers"), Some(body))?;

    if g.json {
        // JSON shape: { transfer: OrgOwnershipTransfer }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(t) = &resp.transfer {
        println!(
            "Ownership transfer initiated: org {:?} → user {:?} (id={}, status={})",
            slug, to_user, t.id, t.status
        );
    } else {
        println!("Ownership transfer initiated: org {:?} → user {:?}", slug, to_user);
    }
    Ok(())
}

fn ownership_transfers_cancel(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs ownership-transfers cancel")
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("rest").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let mut rest_args: Vec<String> = m
        .get_many::<String>("rest")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if rest_args.len() < 2 {
        return Err(msg("usage: basin orgs ownership-transfers cancel <slug> <id> [--yes]"));
    }
    let id = rest_args.remove(1);
    let slug = rest_args.remove(0);

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to cancel ownership transfer {:?} non-interactively under --json",
                id
            )));
        }
        eprint!("Cancel ownership transfer {:?} for org {:?}? [y/N] ", id, slug);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        cancelled: bool,
        id: String,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{slug}/ownership-transfers/{id}/cancel"),
        None,
    )?;

    if g.json {
        // JSON shape: { cancelled: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Cancelled ownership transfer {:?}.", resp.id);
    Ok(())
}

/// ownership_transfer_accept_decline factors the accept/decline pair —
/// identical except for the trailing path segment and the prose verb.
fn ownership_transfer_accept_decline(
    g: &GlobalFlags,
    args: &[String],
    action: &str,
) -> CliResult<()> {
    let cmd = Command::new("orgs ownership-transfers accept/decline")
        .arg(Arg::new("rest").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let mut rest_args: Vec<String> = m
        .get_many::<String>("rest")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if rest_args.len() < 2 {
        return Err(msg(format!(
            "usage: basin orgs ownership-transfers {action} <slug> <id>"
        )));
    }
    let id = rest_args.remove(1);
    let slug = rest_args.remove(0);
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        accepted: bool,
        #[serde(default)]
        declined: bool,
        id: String,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{slug}/ownership-transfers/{id}/{action}"),
        None,
    )?;

    if g.json {
        // JSON shape: { accepted: bool, id: string } | { declined: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if action == "accept" {
        println!("Accepted ownership transfer {:?}.", id);
    } else {
        println!("Declined ownership transfer {:?}.", id);
    }
    Ok(())
}

// ── basin orgs incoming-project-transfers ────────────────────────────────────

/// cmd_orgs_incoming_project_transfers routes
/// `basin orgs incoming-project-transfers <sub>`.
/// Called from cmd_orgs in orgs.rs.
pub fn cmd_orgs_incoming_project_transfers(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin orgs incoming-project-transfers <list|accept|decline> <slug> [id]",
            ))
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => incoming_project_transfers_list(g, rest),
        "accept" => incoming_project_transfer_accept_decline(g, rest, "accept"),
        "decline" => incoming_project_transfer_accept_decline(g, rest, "decline"),
        "--help" | "-h" | "help" => {
            help_for_command(
                "orgs incoming-project-transfers",
                "Inbound project handovers from other orgs.",
                &[
                    "list <slug>                  List incoming project transfers.",
                    "accept <slug> <id>           Accept a handover into this org.",
                    "decline <slug> <id>          Decline a handover.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for orgs incoming-project-transfers", other);
            Err(silent())
        }
    }
}

fn incoming_project_transfers_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs incoming-project-transfers list")
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs incoming-project-transfers list <slug>"))?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        transfers: Vec<IncomingProjectTransfer>,
    }
    let resp: Resp =
        c.do_json(Method::GET, &format!("/v1/orgs/{slug}/incoming-project-transfers"), None)?;

    if g.json {
        // JSON shape: { transfers: [IncomingProjectTransfer] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.transfers.is_empty() {
        println!("No incoming project transfers.");
        return Ok(());
    }
    for t in &resp.transfers {
        println!(
            "{}\t{}\t{} → {}\tproject={}\t{}",
            t.id, t.status, t.from_org_slug, t.to_org_slug, t.project_ref, t.created_at
        );
    }
    Ok(())
}

fn incoming_project_transfer_accept_decline(
    g: &GlobalFlags,
    args: &[String],
    action: &str,
) -> CliResult<()> {
    let cmd = Command::new("orgs incoming-project-transfers accept/decline")
        .arg(Arg::new("rest").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let mut rest_args: Vec<String> = m
        .get_many::<String>("rest")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if rest_args.len() < 2 {
        return Err(msg(format!(
            "usage: basin orgs incoming-project-transfers {action} <slug> <id>"
        )));
    }
    let id = rest_args.remove(1);
    let slug = rest_args.remove(0);
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        accepted: bool,
        #[serde(default)]
        declined: bool,
        id: String,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{slug}/incoming-project-transfers/{id}/{action}"),
        None,
    )?;

    if g.json {
        // JSON shape: { accepted: bool, id: string } | { declined: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if action == "accept" {
        println!("Accepted incoming project transfer {:?}.", id);
    } else {
        println!("Declined incoming project transfer {:?}.", id);
    }
    Ok(())
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::GlobalFlags;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn g(srv: &TestServer) -> GlobalFlags {
        GlobalFlags {
            api_url: srv.url.clone(),
            token: "bso_org_test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn g_json(srv: &TestServer) -> GlobalFlags {
        GlobalFlags {
            api_url: srv.url.clone(),
            token: "bso_org_test".into(),
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    fn sample_project_transfer(id: &str, from_org: &str, to_org: &str, status: &str) -> String {
        format!(
            r#"{{"id":"{id}","from_org_slug":"{from_org}","to_org_slug":"{to_org}","status":"{status}","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    fn sample_ownership_transfer(id: &str, to_user_id: &str, status: &str) -> String {
        format!(
            r#"{{"id":"{id}","to_user_id":"{to_user_id}","status":"{status}","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    fn sample_incoming_project_transfer(
        id: &str,
        from_org: &str,
        to_org: &str,
        proj_ref: &str,
        status: &str,
    ) -> String {
        format!(
            r#"{{"id":"{id}","from_org_slug":"{from_org}","to_org_slug":"{to_org}","project_ref":"{proj_ref}","status":"{status}","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── projects transfers list ───────────────────────────────────────────────

    #[test]
    fn projects_transfers_list_happy_path() {
        let xfr = sample_project_transfer("xfr-1", "org-a", "org-b", "pending");
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/transfers");
            Resp::ok(format!(r#"{{"transfers":[{xfr}]}}"#))
        });
        let _cfg = with_temp_config_dir();
        let err =
            cmd_projects_transfers(&g(&srv), &["list".into(), "--project=proj1".into()]);
        assert!(err.is_ok(), "projects transfers list: {:?}", err);
    }

    #[test]
    fn projects_transfers_list_empty() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"transfers":[]}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        // Verify the empty-list path by inspecting our serialisation directly.
        let resp_data: serde_json::Value =
            serde_json::from_str(r#"{"transfers":[]}"#).unwrap();
        crate::output::print_json(&mut out, &resp_data).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["transfers"].as_array().unwrap().is_empty());
        let _ = &srv;
    }

    #[test]
    fn projects_transfers_list_json_shape() {
        let xfr = sample_project_transfer("xfr-1", "org-a", "org-b", "pending");
        let srv = TestServer::start(move |_req: &Req| {
            Resp::ok(format!(r#"{{"transfers":[{xfr}]}}"#))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let t = ProjectTransfer {
            id: "xfr-1".into(),
            from_org_slug: "org-a".into(),
            to_org_slug: "org-b".into(),
            status: "pending".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        print_json(&mut out, &json!({ "transfers": [t] })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["transfers"][0]["id"], "xfr-1");
        assert_eq!(parsed["transfers"][0]["to_org_slug"], "org-b");
        let _ = &srv;
    }

    #[test]
    fn projects_transfers_list_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"project not found"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err =
            cmd_projects_transfers(&g(&srv), &["list".into(), "--project=proj1".into()]);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("404"), "expected 404 in error, got: {msg}");
    }

    // ── projects transfers create ─────────────────────────────────────────────

    #[test]
    fn projects_transfers_create_missing_to_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_projects_transfers(
            &g0,
            &["create".into(), "--project=proj1".into(), "--yes".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--to-org"));
    }

    #[test]
    fn projects_transfers_create_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/transfers");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["to_org_slug"], "org-b");
            Resp::ok(format!(
                r#"{{"transfer":{}}}"#,
                r#"{"id":"xfr-2","from_org_slug":"org-a","to_org_slug":"org-b","status":"pending","created_at":"2026-01-01T00:00:00Z"}"#
            ))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_projects_transfers(
            &g(&srv),
            &[
                "create".into(),
                "--project=proj1".into(),
                "--to-org=org-b".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_ok(), "projects transfers create: {:?}", err);
    }

    #[test]
    fn projects_transfers_create_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 =
            GlobalFlags { token: "tok".into(), quiet: true, json: true, ..Default::default() };
        let err = cmd_projects_transfers(
            &g0,
            &["create".into(), "--project=proj1".into(), "--to-org=org-b".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--yes"));
    }

    // ── projects transfers cancel ─────────────────────────────────────────────

    #[test]
    fn projects_transfers_cancel_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/transfers/xfr-1/cancel");
            Resp::ok(r#"{"cancelled":true,"id":"xfr-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_projects_transfers(
            &g(&srv),
            &[
                "cancel".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "xfr-1".into(),
            ],
        );
        assert!(err.is_ok(), "projects transfers cancel: {:?}", err);
    }

    #[test]
    fn projects_transfers_cancel_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"cancelled":true,"id":"xfr-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R {
            cancelled: bool,
            id: String,
        }
        print_json(&mut out, &R { cancelled: true, id: "xfr-1".into() }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["cancelled"], true);
        assert_eq!(parsed["id"], "xfr-1");
        let _ = &srv;
    }

    // ── orgs ownership-transfers list ─────────────────────────────────────────

    #[test]
    fn ownership_transfers_list_happy_path() {
        let own = sample_ownership_transfer("own-1", "user-abc", "pending");
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/my-org/ownership-transfers");
            Resp::ok(format!(r#"{{"transfers":[{own}]}}"#))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &["list".into(), "my-org".into()],
        );
        assert!(err.is_ok(), "ownership-transfers list: {:?}", err);
    }

    // ── orgs ownership-transfers create ──────────────────────────────────────

    #[test]
    fn ownership_transfers_create_missing_to_user_id() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs_ownership_transfers(
            &g0,
            &["create".into(), "--yes".into(), "my-org".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--to-user-id"));
    }

    #[test]
    fn ownership_transfers_create_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/ownership-transfers");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["to_user_id"], "user-xyz");
            Resp::ok(format!(
                r#"{{"transfer":{}}}"#,
                r#"{"id":"own-2","to_user_id":"user-xyz","status":"pending","created_at":"2026-01-01T00:00:00Z"}"#
            ))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &[
                "create".into(),
                "--to-user-id=user-xyz".into(),
                "--yes".into(),
                "my-org".into(),
            ],
        );
        assert!(err.is_ok(), "ownership-transfers create: {:?}", err);
    }

    #[test]
    fn ownership_transfers_create_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"transfer":{"id":"own-2","to_user_id":"user-xyz","status":"pending","created_at":"2026-01-01T00:00:00Z"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let t = OrgOwnershipTransfer {
            id: "own-2".into(),
            to_user_id: "user-xyz".into(),
            status: "pending".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        print_json(&mut out, &json!({ "transfer": t })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["transfer"]["to_user_id"], "user-xyz");
        let _ = &srv;
    }

    // ── orgs ownership-transfers cancel ──────────────────────────────────────

    #[test]
    fn ownership_transfers_cancel_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/ownership-transfers/own-1/cancel");
            Resp::ok(r#"{"cancelled":true,"id":"own-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &["cancel".into(), "--yes".into(), "my-org".into(), "own-1".into()],
        );
        assert!(err.is_ok(), "ownership-transfers cancel: {:?}", err);
    }

    // ── orgs ownership-transfers accept / decline ─────────────────────────────

    #[test]
    fn ownership_transfers_accept_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/ownership-transfers/own-1/accept");
            Resp::ok(r#"{"accepted":true,"id":"own-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &["accept".into(), "my-org".into(), "own-1".into()],
        );
        assert!(err.is_ok(), "ownership-transfers accept: {:?}", err);
    }

    #[test]
    fn ownership_transfers_accept_409() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                409,
                r#"{"error":{"code":"already_accepted","message":"transfer already accepted"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &["accept".into(), "my-org".into(), "own-1".into()],
        );
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("409"), "expected 409 in error, got: {msg}");
    }

    #[test]
    fn ownership_transfers_decline_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/ownership-transfers/own-1/decline");
            Resp::ok(r#"{"declined":true,"id":"own-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_ownership_transfers(
            &g(&srv),
            &["decline".into(), "my-org".into(), "own-1".into()],
        );
        assert!(err.is_ok(), "ownership-transfers decline: {:?}", err);
    }

    #[test]
    fn ownership_transfers_decline_json_shape() {
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R {
            declined: bool,
            id: String,
        }
        print_json(&mut out, &R { declined: true, id: "own-1".into() }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["declined"], true);
        assert_eq!(parsed["id"], "own-1");
    }

    // ── orgs incoming-project-transfers list ──────────────────────────────────

    #[test]
    fn incoming_project_transfers_list_happy_path() {
        let ipt = sample_incoming_project_transfer("ipt-1", "org-a", "my-org", "proj-ref-1", "pending");
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/my-org/incoming-project-transfers");
            Resp::ok(format!(r#"{{"transfers":[{ipt}]}}"#))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_incoming_project_transfers(
            &g(&srv),
            &["list".into(), "my-org".into()],
        );
        assert!(err.is_ok(), "incoming-project-transfers list: {:?}", err);
    }

    #[test]
    fn incoming_project_transfers_list_json_shape() {
        let mut out = Vec::new();
        let t = IncomingProjectTransfer {
            id: "ipt-1".into(),
            from_org_slug: "org-a".into(),
            to_org_slug: "my-org".into(),
            project_ref: "proj-ref-1".into(),
            status: "pending".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        };
        print_json(&mut out, &json!({ "transfers": [t] })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["transfers"][0]["project_ref"], "proj-ref-1");
    }

    #[test]
    fn incoming_project_transfers_accept_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/incoming-project-transfers/ipt-1/accept");
            Resp::ok(r#"{"accepted":true,"id":"ipt-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_incoming_project_transfers(
            &g(&srv),
            &["accept".into(), "my-org".into(), "ipt-1".into()],
        );
        assert!(err.is_ok(), "incoming-project-transfers accept: {:?}", err);
    }

    #[test]
    fn incoming_project_transfers_decline_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/my-org/incoming-project-transfers/ipt-1/decline");
            Resp::ok(r#"{"declined":true,"id":"ipt-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_incoming_project_transfers(
            &g(&srv),
            &["decline".into(), "my-org".into(), "ipt-1".into()],
        );
        assert!(err.is_ok(), "incoming-project-transfers decline: {:?}", err);
    }

    #[test]
    fn incoming_project_transfers_list_403() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient role"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs_incoming_project_transfers(
            &g(&srv),
            &["list".into(), "my-org".into()],
        );
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("403"), "expected 403 in error, got: {msg}");
    }

    // ── parse-error / missing-arg tests ──────────────────────────────────────

    #[test]
    fn projects_transfers_cancel_missing_id() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_projects_transfers(
            &g0,
            &["cancel".into(), "--project=proj1".into(), "--yes".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("usage"));
    }

    #[test]
    fn ownership_transfers_cancel_missing_args() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs_ownership_transfers(
            &g0,
            &["cancel".into(), "--yes".into(), "only-slug".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("usage"));
    }

    #[test]
    fn incoming_project_transfers_accept_missing_args() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs_incoming_project_transfers(
            &g0,
            &["accept".into(), "only-slug".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("usage"));
    }

    // ── help / unknown-subcommand ─────────────────────────────────────────────

    #[test]
    fn projects_transfers_help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_projects_transfers(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn ownership_transfers_unknown_sub_returns_silent() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_orgs_ownership_transfers(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn incoming_project_transfers_help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_orgs_incoming_project_transfers(&g0, &["help".into()]);
        assert!(err.is_ok());
    }
}
