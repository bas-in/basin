//! branches — branch (preview environment) lifecycle.
//!
//!   basin branches list   [--project=<ref>]
//!   basin branches create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]
//!   basin branches get    <branch_ref> [--project=<ref>]
//!   basin branches merge  <branch_ref> [--project=<ref>] [--yes]
//!   basin branches delete <branch_ref> [--project=<ref>] [--yes]
//!   basin branches events [--project=<ref>] [--follow]
//!
//! Branches are backed by the cloud's `/v1/projects/{ref}/branches/*`
//! surface. Each branch is itself a Project row with parent_project_id
//! set. Merge + delete are destructive: require confirmation unless
//! --yes is passed.
//!
//! Project ref resolution order:
//!  1. --project=<ref> flag
//!  2. load_working_project(cwd) — ./basin/config.toml

use std::collections::HashSet;
use std::time::Duration;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{engine_admin_path, require_client, require_engine_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;
use crate::printinfo;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// Branch is the read shape from GET /v1/projects/{ref}/branches (list)
/// and the embedded "branch" field in create/merge/delete responses.
/// It mirrors the models.Project row that the cloud serialises for a
/// branch project.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Branch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    org_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    slug: String,
    #[serde(default, rename = "ref", skip_serializing_if = "String::is_empty")]
    r#ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    region: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_project_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    branch_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    branch_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    merged_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retired_at: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    created_at: String,
}

/// BranchEvent mirrors handlers.branchEventResponse — the on-wire shape
/// returned by GET /v1/projects/{ref}/branches/events.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BranchEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_id: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    created_at: String,
}

/// Pgwire mirrors the create-branch response's "pgwire" envelope.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Pgwire {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    connection_url: String,
}

// ── Project ref resolution ────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the working-project config file. Returns an error when neither is set.
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

// ── Dispatcher ────────────────────────────────────────────────────────────────

pub fn cmd_branches(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return branches_list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => branches_list(g, rest),
        "create" => branches_create(g, rest),
        "get" => branches_get(g, rest),
        "merge" => branches_merge(g, rest),
        "delete" => branches_delete(g, rest),
        "events" => branches_events(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "branches",
                "List / create / get / merge / delete branch preview environments.",
                &[
                    "list   [--project=<ref>]                               List branches for a project.",
                    "create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]  Create a branch.",
                    "get    <branch_ref> [--project=<ref>]                  Show one branch.",
                    "merge  <branch_ref> [--project=<ref>] [--yes]          Merge a branch into its parent.",
                    "delete <branch_ref> [--project=<ref>] [--yes]          Delete (retire) a branch.",
                    "events [--project=<ref>] [--follow]                    List branch events; --follow polls every 5s.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for branches", other);
            Err(silent())
        }
    }
}

// ── branches list ─────────────────────────────────────────────────────────────

fn branches_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("branches list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches list",
            "List branches for a project.",
            &["--project=<ref>   Project ref (required if not linked)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        branches: Vec<Branch>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{ref}/branches"), None)?;

    if g.json {
        // JSON shape: { branches: [ Branch ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "branches": resp.branches }),
        );
    }
    if resp.branches.is_empty() {
        println!("(no branches)");
        return Ok(());
    }
    let mut t = Table::new(g, &["REF", "NAME", "KIND", "STATUS", "MERGED", "CREATED"]);
    for b in &resp.branches {
        let merged = b
            .merged_at
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("\u{2014}");
        t.row(&[
            &b.r#ref,
            &b.branch_name,
            &b.branch_kind,
            &b.status,
            merged,
            &b.created_at,
        ]);
    }
    t.flush()
}

// ── branches create ───────────────────────────────────────────────────────────

fn branches_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    // Note: positional <name> coexists with flags — use plain Arg (not
    // trailing_var_arg) to avoid swallowing the flags. We join leftover
    // positionals to support multi-word names.
    let cmd = Command::new("branches create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("from").long("from"))
        .arg(Arg::new("kind").long("kind").default_value("branch"))
        .arg(Arg::new("dst-project").long("dst-project"))
        .arg(Arg::new("name").num_args(1..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches create",
            "Create a new branch for a project.",
            &[
                "<name>                    Branch display name (required; ignored in engine mode).",
                "--project=<ref>           Project ref (required if not linked).",
                "                          In engine mode: the source project ULID.",
                "--from=<branch_ref>       Branch ref to create from (cloud mode only).",
                "--kind=<branch|preview>   Branch kind (default: branch; cloud mode only).",
                "",
                "Self-hosted engine mode (BASIN_MODE=engine or global --engine flag):",
                "  Calls POST /admin/v1/projects/:id/fork {dst_project_id} on the engine.",
                "  --dst-project=<ulid>    Destination project ULID (required in engine mode).",
                "  Requires BASIN_ADMIN_TOKEN or --admin-token with is_admin:true JWT.",
                "  Note: the fork route is in-progress in the engine; verify it is wired",
                "  in the engine's admin router before using.",
            ],
        );
        return Ok(());
    }
    let name: String = m
        .get_many::<String>("name")
        .map(|v| v.cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();

    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if g.engine_mode {
        // Engine admin path: POST /admin/v1/projects/:id/fork
        // Body: { dst_project_id: "<ulid>" }
        // The fork route copies all tables from src to dst at their current
        // snapshot head (a point-in-time data copy / branch).
        let dst_project = m
            .get_one::<String>("dst-project")
            .cloned()
            .unwrap_or_default();
        if dst_project.is_empty() {
            return Err(msg(
                "engine mode: --dst-project=<ulid> is required for `branches create` (fork).\n\
                 The destination project must already exist on the engine.",
            ));
        }
        let c = require_engine_client(g)?;
        let body = json!({ "dst_project_id": dst_project });
        let resp: serde_json::Value = c.do_json_timeout(
            Method::POST,
            &engine_admin_path(&r#ref, "fork"),
            Some(body),
            Duration::from_secs(120),
        )?;
        if g.json {
            // Engine shape: { ok: bool, forked_tables: N }
            return print_json(&mut std::io::stdout(), &resp);
        }
        let forked = resp
            .get("forked_tables")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        println!(
            "Fork from project {:?} to {:?} complete ({forked} table(s) copied).",
            r#ref, dst_project
        );
        return Ok(());
    }

    // Cloud path: POST /v1/projects/:ref/branches
    if name.is_empty() {
        return Err(msg(
            "usage: basin branches create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]",
        ));
    }
    let from_ref = m.get_one::<String>("from").cloned().unwrap_or_default();
    let kind = m
        .get_one::<String>("kind")
        .cloned()
        .unwrap_or_else(|| "branch".to_string());

    let c = require_client(g)?;

    // POST body: { branch_name, kind, [from_ref] }
    // 60s timeout — engine provisioning can be slow.
    let mut body = json!({ "branch_name": name, "kind": kind });
    if !from_ref.is_empty() {
        body["from_ref"] = json!(from_ref);
    }

    #[derive(Deserialize, Serialize)]
    struct CreateResp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<Branch>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        created_event: Option<BranchEvent>,
        #[serde(default)]
        engine_ok: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        engine_msg: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pgwire: Option<Pgwire>,
    }
    let resp: CreateResp = c
        .do_json_timeout(
            Method::POST,
            &format!("/v1/projects/{ref}/branches"),
            Some(body),
            Duration::from_secs(60),
        )
        .map_err(|err| {
            // Map 409 conflict to a user-friendly message.
            if let Some(ae) = as_api_error(err.as_ref()) {
                if ae.http_status == 409 {
                    return msg(format!(
                        "branch name conflict: a branch named {:?} already exists for this project",
                        name
                    ));
                }
            }
            err
        })?;

    if g.json {
        // JSON shape: { branch: Branch, created_event: BranchEvent, engine_ok: bool, engine_msg?: string, pgwire?: Pgwire }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(b) = &resp.branch {
        println!(
            "Created branch {:?} ({}) — kind={}",
            b.branch_name, b.r#ref, b.branch_kind
        );
    }
    if let Some(pg) = &resp.pgwire {
        if !pg.connection_url.is_empty() {
            println!();
            println!("Connection URL (shown ONCE — store it now):");
            println!("  {}", pg.connection_url);
        }
    }
    if !resp.engine_msg.is_empty() {
        println!("engine: {}", resp.engine_msg);
    }
    Ok(())
}

// ── branches get ──────────────────────────────────────────────────────────────

/// branches get resolves the branch via GET /v1/projects/{branch_ref}.
/// A branch is just a project with parent_project_id set; the cloud does
/// not have a dedicated GET /v1/projects/{ref}/branches/{branch_ref} endpoint.
fn branches_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("branches get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("branch_ref"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches get",
            "Show one branch.",
            &[
                "<branch_ref>      Branch ref (required).",
                "--project=<ref>   Parent project ref (for display context).",
            ],
        );
        return Ok(());
    }
    let branch_ref = m
        .get_one::<String>("branch_ref")
        .cloned()
        .ok_or_else(|| msg("usage: basin branches get <branch_ref> [--project=<ref>]"))?;
    // --project accepted for consistency; not needed for the GET
    let _project = m.get_one::<String>("project").cloned().unwrap_or_default();

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project: Option<Branch>,
    }
    let resp: Resp = c.do_json(Method::GET, &format!("/v1/projects/{branch_ref}"), None)?;
    let b = resp
        .project
        .as_ref()
        .ok_or_else(|| msg(format!("branch not found: {branch_ref}")))?;

    if g.json {
        // JSON shape: { project: Branch }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("ref:         {}", b.r#ref);
    println!("branch_name: {}", b.branch_name);
    println!("kind:        {}", b.branch_kind);
    println!("status:      {}", b.status);
    println!("region:      {}", b.region);
    if let Some(pid) = &b.parent_project_id {
        println!("parent_id:   {pid}");
    }
    if let Some(ma) = &b.merged_at {
        if !ma.is_empty() {
            println!("merged_at:   {ma}");
        }
    }
    println!("created_at:  {}", b.created_at);
    Ok(())
}

// ── branches merge ────────────────────────────────────────────────────────────

fn branches_merge(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("branches merge")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("branch_ref"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches merge",
            "Merge a branch into its parent.",
            &[
                "<branch_ref>      Branch ref (required).",
                "--project=<ref>   Parent project ref (required if not linked).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let branch_ref = m
        .get_one::<String>("branch_ref")
        .cloned()
        .ok_or_else(|| msg("usage: basin branches merge <branch_ref> [--project=<ref>] [--yes]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let yes = m.get_flag("yes");

    if !yes {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to merge branch {:?} non-interactively under --json",
                branch_ref
            )));
        }
        eprint!(
            "Merge branch {:?} into project {:?}? This marks the branch as merged. [y/N] ",
            branch_ref, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct MergeResp {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<Branch>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event: Option<BranchEvent>,
    }
    let resp: MergeResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/branches/{branch_ref}/merge"),
        None,
    )?;

    if g.json {
        // JSON shape: { branch: Branch, event: BranchEvent }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(b) = &resp.branch {
        println!("Merged branch {:?} ({}).", b.branch_name, b.r#ref);
    } else {
        println!("Merged branch {branch_ref}.");
    }
    Ok(())
}

// ── branches delete ───────────────────────────────────────────────────────────

fn branches_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("branches delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("branch_ref"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches delete",
            "Delete (retire) a branch.",
            &[
                "<branch_ref>      Branch ref (required).",
                "--project=<ref>   Parent project ref (required if not linked).",
                "--yes             Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let branch_ref = m.get_one::<String>("branch_ref").cloned().ok_or_else(|| {
        msg("usage: basin branches delete <branch_ref> [--project=<ref>] [--yes]")
    })?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let yes = m.get_flag("yes");

    if !yes {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete branch {:?} non-interactively under --json",
                branch_ref
            )));
        }
        eprint!(
            "Delete (retire) branch {:?} from project {:?}? This cannot be undone. [y/N] ",
            branch_ref, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct DeleteResp {
        #[serde(default)]
        retired: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event: Option<BranchEvent>,
    }
    let resp: DeleteResp = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/branches/{branch_ref}"),
        None,
    )?;

    if g.json {
        // JSON shape: { retired: bool, event: BranchEvent }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted branch {branch_ref}.");
    Ok(())
}

// ── branches events ───────────────────────────────────────────────────────────

fn branches_events(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("branches events")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("follow").long("follow").action(ArgAction::SetTrue))
        .arg(Arg::new("limit").long("limit").default_value("50"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "branches events",
            "List branch events.",
            &[
                "--project=<ref>   Project ref (required if not linked).",
                "--follow          Poll every 5s for new events.",
                "--limit=<n>       Events to fetch per page (default: 50).",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let follow = m.get_flag("follow");
    let limit: u32 = m
        .get_one::<String>("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);

    let c = require_client(g)?;
    if follow {
        follow_branch_events(&c, &r#ref, limit, g)
    } else {
        fetch_branch_events_once(&c, &r#ref, limit, g)
    }
}

/// fetch_branch_events_once fetches one page of events and prints them.
pub fn fetch_branch_events_once(
    c: &crate::client::Client,
    r#ref: &str,
    limit: u32,
    g: &GlobalFlags,
) -> CliResult<()> {
    let qs = query_string(&[("limit", &limit.to_string())]);

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        events: Vec<BranchEvent>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/branches/events{qs}"),
        None,
    )?;

    if g.json {
        // JSON shape: { events: [ BranchEvent ] }
        return print_json(&mut std::io::stdout(), &json!({ "events": resp.events }));
    }
    if resp.events.is_empty() {
        println!("(no events)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "KIND", "PROJECT", "CREATED", "MESSAGE"]);
    for ev in &resp.events {
        let msg_str = ev.message.as_deref().unwrap_or("");
        t.row(&[&ev.id, &ev.kind, &ev.project_id, &ev.created_at, msg_str]);
    }
    t.flush()
}

/// fetch_and_print_new_events fetches one page of events and prints only
/// those whose IDs haven't been seen before. Updates `seen` in-place.
pub fn fetch_and_print_new_events(
    c: &crate::client::Client,
    r#ref: &str,
    limit: u32,
    g: &GlobalFlags,
    seen: &mut HashSet<String>,
) -> CliResult<()> {
    let qs = query_string(&[("limit", &limit.to_string())]);

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        events: Vec<BranchEvent>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/branches/events{qs}"),
        None,
    )?;

    for ev in &resp.events {
        if seen.contains(&ev.id) {
            continue;
        }
        seen.insert(ev.id.clone());
        let msg_str = ev.message.as_deref().unwrap_or("");
        if g.json {
            let _ = print_json(&mut std::io::stdout(), ev);
        } else {
            println!(
                "{:20}  {:<20}  {}  {}",
                ev.created_at, ev.kind, ev.project_id, msg_str
            );
        }
    }
    Ok(())
}

/// follow_branch_events polls GET /v1/projects/{ref}/branches/events every
/// 5s and prints new events (de-duplicated by ID). Uses std::thread::sleep
/// and a running flag — no signal handler (stdlib-only approach mirroring Go).
fn follow_branch_events(
    c: &crate::client::Client,
    r#ref: &str,
    limit: u32,
    g: &GlobalFlags,
) -> CliResult<()> {
    printinfo!(
        g,
        "Following branch events for project {} — Ctrl-C to stop.",
        r#ref
    );
    let mut seen: HashSet<String> = HashSet::new();

    // Fetch immediately, then poll every 5s.
    if let Err(e) = fetch_and_print_new_events(c, r#ref, limit, g, &mut seen) {
        printerr!(g, "events poll failed: {e}");
    }

    loop {
        std::thread::sleep(Duration::from_secs(5));
        if let Err(e) = fetch_and_print_new_events(c, r#ref, limit, g, &mut seen) {
            printerr!(g, "events poll failed: {e}");
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

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

    fn sample_branch_json(r: &str, name: &str) -> String {
        format!(
            r#"{{"id":"b-{r}","org_id":"org1","slug":"proj-{name}","ref":"{r}","name":"Demo · {name}","region":"jnb","status":"active","parent_project_id":"parent-id","branch_name":"{name}","branch_kind":"branch","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    fn sample_event_json(id: &str, kind: &str, project_id: &str) -> String {
        format!(
            r#"{{"id":"{id}","project_id":"{project_id}","kind":"{kind}","message":"{kind} event","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── branches list ─────────────────────────────────────────────────────────

    #[test]
    fn list_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/branches");
            Resp::ok(r#"{"branches":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_branches(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_populated() {
        let _cfg = with_temp_config_dir();
        let br1 = sample_branch_json("br1", "feat-a");
        let br2 = sample_branch_json("br2", "feat-b");
        let body = format!(r#"{{"branches":[{br1},{br2}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let g = flags(&srv.url);
        cmd_branches(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json() {
        let _cfg = with_temp_config_dir();
        let br1 = sample_branch_json("br1", "feat-a");
        let body = format!(r#"{{"branches":[{br1}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    // ── branches create ───────────────────────────────────────────────────────

    #[test]
    fn create_defaults() {
        let _cfg = with_temp_config_dir();
        let body_captured = Arc::new(std::sync::Mutex::new(String::new()));
        let bc = Arc::clone(&body_captured);
        let br = sample_branch_json("br-new", "my-feature");
        let ev = sample_event_json("ev1", "created", "br-new");
        let resp_body = format!(r#"{{"branch":{br},"created_event":{ev},"engine_ok":true}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            *bc.lock().unwrap() = req.body.clone();
            Resp::status(201, resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "my-feature".into(),
            ],
        )
        .unwrap();
        let captured = body_captured.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&captured).unwrap();
        assert_eq!(v["branch_name"].as_str(), Some("my-feature"));
        assert_eq!(v["kind"].as_str(), Some("branch"));
    }

    #[test]
    fn create_with_from() {
        let _cfg = with_temp_config_dir();
        let body_captured = Arc::new(std::sync::Mutex::new(String::new()));
        let bc = Arc::clone(&body_captured);
        let br = sample_branch_json("br-new", "feat-from");
        let ev = sample_event_json("ev1", "created", "br-new");
        let resp_body = format!(r#"{{"branch":{br},"created_event":{ev},"engine_ok":true}}"#);
        let srv = TestServer::start(move |req: &Req| {
            *bc.lock().unwrap() = req.body.clone();
            Resp::status(201, resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--from=br-parent".into(),
                "--kind=preview".into(),
                "feat-from".into(),
            ],
        )
        .unwrap();
        let captured = body_captured.lock().unwrap();
        let v: serde_json::Value = serde_json::from_str(&captured).unwrap();
        assert_eq!(v["from_ref"].as_str(), Some("br-parent"));
        assert_eq!(v["kind"].as_str(), Some("preview"));
    }

    #[test]
    fn create_name_conflict_409() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                409,
                r#"{"error":{"code":"bad_request","message":"A branch with that name already exists for this project."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_branches(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "existing-branch".into(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("branch name conflict"),
            "err={err}"
        );
    }

    #[test]
    fn create_json_shape() {
        let _cfg = with_temp_config_dir();
        let br = sample_branch_json("br-json", "json-feature");
        let ev = sample_event_json("ev1", "created", "br-json");
        let resp_body = format!(
            r#"{{"branch":{br},"created_event":{ev},"engine_ok":false,"engine_msg":"engine_unconfigured"}}"#
        );
        let srv = TestServer::start(move |_req: &Req| Resp::status(201, resp_body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "json-feature".into(),
            ],
        )
        .unwrap();
    }

    // ── branches get ──────────────────────────────────────────────────────────

    #[test]
    fn get_200() {
        let _cfg = with_temp_config_dir();
        let br = sample_branch_json("br1", "feat-a");
        let resp_body = format!(r#"{{"project":{br}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/br1");
            Resp::ok(resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(&g, &["get".into(), "br1".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn get_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"Project not found."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_branches(&g, &["get".into(), "br-missing".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn get_json_shape() {
        let _cfg = with_temp_config_dir();
        let br = sample_branch_json("br1", "feat-a");
        let resp_body = format!(r#"{{"project":{br}}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(resp_body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(&g, &["get".into(), "br1".into()]).unwrap();
    }

    // ── branches merge ────────────────────────────────────────────────────────

    #[test]
    fn merge_confirmed_yes_flag() {
        let _cfg = with_temp_config_dir();
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called2 = Arc::clone(&called);
        let br = sample_branch_json("br1", "feat-a");
        let ev = sample_event_json("ev-m", "merged", "br1");
        let resp_body = format!(r#"{{"branch":{br},"event":{ev}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/branches/br1/merge");
            called2.store(true, Ordering::SeqCst);
            Resp::ok(resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(
            &g,
            &[
                "merge".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br1".into(),
            ],
        )
        .unwrap();
        assert!(
            called.load(Ordering::SeqCst),
            "merge endpoint was not called"
        );
    }

    #[test]
    fn merge_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags_json(&srv.url);
        let err = cmd_branches(
            &g,
            &["merge".into(), "--project=proj1".into(), "br1".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "err={err}"
        );
    }

    #[test]
    fn merge_400_api_error() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                400,
                r#"{"error":{"code":"bad_request","message":"Branch is already merged."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_branches(
            &g,
            &[
                "merge".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br1".into(),
            ],
        )
        .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 400);
    }

    #[test]
    fn merge_json_shape() {
        let _cfg = with_temp_config_dir();
        let br = sample_branch_json("br1", "feat-a");
        let ev = sample_event_json("ev-m", "merged", "br1");
        let resp_body = format!(r#"{{"branch":{br},"event":{ev}}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(resp_body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(
            &g,
            &[
                "merge".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br1".into(),
            ],
        )
        .unwrap();
    }

    // ── branches delete ───────────────────────────────────────────────────────

    #[test]
    fn delete_confirmed_yes_flag() {
        let _cfg = with_temp_config_dir();
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called2 = Arc::clone(&called);
        let ev = sample_event_json("ev-r", "retired", "br1");
        let resp_body = format!(r#"{{"retired":true,"event":{ev}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/branches/br1");
            called2.store(true, Ordering::SeqCst);
            Resp::ok(resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br1".into(),
            ],
        )
        .unwrap();
        assert!(
            called.load(Ordering::SeqCst),
            "delete endpoint was not called"
        );
    }

    #[test]
    fn delete_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"Branch not found for this project."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_branches(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br-missing".into(),
            ],
        )
        .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn delete_json_shape() {
        let _cfg = with_temp_config_dir();
        let ev = sample_event_json("ev-r", "retired", "br1");
        let resp_body = format!(r#"{{"retired":true,"event":{ev}}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(resp_body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "br1".into(),
            ],
        )
        .unwrap();
    }

    // ── branches events ───────────────────────────────────────────────────────

    #[test]
    fn events_snapshot() {
        let _cfg = with_temp_config_dir();
        let ev1 = sample_event_json("ev1", "created", "br1");
        let ev2 = sample_event_json("ev2", "ready", "br1");
        let resp_body = format!(r#"{{"events":[{ev1},{ev2}]}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(
                req.path.starts_with("/v1/projects/proj1/branches/events"),
                "path={}",
                req.path
            );
            Resp::ok(resp_body.clone())
        });
        let g = flags(&srv.url);
        cmd_branches(&g, &["events".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn events_snapshot_json() {
        let _cfg = with_temp_config_dir();
        let ev1 = sample_event_json("ev1", "created", "br1");
        let resp_body = format!(r#"{{"events":[{ev1}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(resp_body.clone()));
        let g = flags_json(&srv.url);
        cmd_branches(&g, &["events".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn events_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        cmd_branches(&g, &["events".into(), "--project=proj1".into()]).unwrap();
    }

    // ── follow dedup ──────────────────────────────────────────────────────────

    #[test]
    fn events_follow_dedup() {
        let _cfg = with_temp_config_dir();
        let ev1 = sample_event_json("ev1", "created", "br1");
        let ev2 = sample_event_json("ev2", "ready", "br1");
        let resp_body = format!(r#"{{"events":[{ev1},{ev2}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(resp_body.clone()));
        let c = Client::new(&srv.url, "tok");
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };

        let mut seen = HashSet::new();
        // First call — both events new.
        fetch_and_print_new_events(&c, "proj1", 50, &g, &mut seen).unwrap();
        assert_eq!(seen.len(), 2);

        // Second call — same events; seen set unchanged by new insertions but
        // nothing new should be printed (dedup).
        let len_before = seen.len();
        fetch_and_print_new_events(&c, "proj1", 50, &g, &mut seen).unwrap();
        assert_eq!(seen.len(), len_before, "seen grew — dedup broken");
    }

    // ── arg-parse error paths ─────────────────────────────────────────────────

    #[test]
    fn create_missing_name() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags(&srv.url);
        assert!(
            cmd_branches(&g, &["create".into(), "--project=proj1".into()]).is_err(),
            "expected error for missing branch name"
        );
    }

    #[test]
    fn get_missing_ref() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags(&srv.url);
        assert!(
            cmd_branches(&g, &["get".into()]).is_err(),
            "expected error for missing branch ref in get"
        );
    }

    #[test]
    fn merge_missing_ref() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags(&srv.url);
        assert!(
            cmd_branches(
                &g,
                &["merge".into(), "--project=proj1".into(), "--yes".into()]
            )
            .is_err(),
            "expected error for missing branch ref in merge"
        );
    }

    #[test]
    fn delete_missing_ref() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let g = flags(&srv.url);
        assert!(
            cmd_branches(
                &g,
                &["delete".into(), "--project=proj1".into(), "--yes".into()]
            )
            .is_err(),
            "expected error for missing branch ref in delete"
        );
    }

    #[test]
    fn no_project_flag_no_config() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"branches":[]}"#));
        let g = flags(&srv.url);
        // withTempConfigDir means no config.toml → resolveProjectRef fails.
        assert!(
            cmd_branches(&g, &["list".into()]).is_err(),
            "expected error when --project missing and no working project"
        );
    }

    #[test]
    fn unknown_subcommand() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_branches(&g, &["frobnicate".into()]).unwrap_err();
        assert!(
            crate::error::is_silent(err.as_ref()),
            "expected silent error, got: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        cmd_branches(&g, &["help".into()]).unwrap();
    }

    #[test]
    fn events_count_calls_quiet() {
        let _cfg = with_temp_config_dir();
        let call_count = Arc::new(AtomicU32::new(0));
        let cc = Arc::clone(&call_count);
        let ev1 = sample_event_json("ev1", "created", "br1");
        let resp_body = format!(r#"{{"events":[{ev1}]}}"#);
        let srv = TestServer::start(move |_req: &Req| {
            cc.fetch_add(1, Ordering::SeqCst);
            Resp::ok(resp_body.clone())
        });
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "bso_org_test".into(),
            quiet: true,
            ..Default::default()
        };
        cmd_branches(&g, &["events".into(), "--project=proj1".into()]).unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
