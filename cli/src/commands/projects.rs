//! projects — list / get / create / delete / pause / resume.
//!
//! Delete is destructive: we require the user to retype the project's
//! name exactly (mirroring the dashboard's confirm-by-name modal),
//! unless --yes is passed.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::client::query_string;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;
use crate::types::{CreateProjectResponse, Project};

use super::help::help_for_command;
use super::parse_or_silent;

#[derive(Deserialize)]
struct ProjectsResp {
    #[serde(default)]
    projects: Vec<Project>,
}

#[derive(Deserialize)]
struct ProjectResp {
    #[serde(default)]
    project: Option<Project>,
}

pub fn cmd_projects(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "get" => get(g, rest),
        "create" => create(g, rest),
        "delete" => delete(g, rest),
        "pause" => pause_resume(g, rest, "pause"),
        "resume" => pause_resume(g, rest, "resume"),
        "transfers" => super::transfers::cmd_projects_transfers(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "projects",
                "List / get / create / delete / pause / resume / transfer projects.",
                &[
                    "list      [--org <slug>]                               List projects.",
                    "get       <ref>                                        Show one project.",
                    "create    <name> --org <slug> [--region <code>]        Create a project.",
                    "delete    <ref> [--yes]                                Delete a project (confirm by name).",
                    "pause     <ref>                                        Pause a project's data plane.",
                    "resume    <ref>                                        Resume a paused project.",
                    "transfers <list|create|cancel>  [--project=<ref>]      Manage project org-transfers.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for projects");
            Err(silent())
        }
    }
}

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects list").arg(Arg::new("org").long("org"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    if org.is_empty() {
        return Err(msg("projects list requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: ProjectsResp = c.do_json(Method::GET, &format!("/v1/orgs/{org}/projects"), None)?;
    if g.json {
        // JSON shape: { projects: [ Project ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "projects": resp.projects }),
        );
    }
    let mut t = Table::new(g, &["REF", "NAME", "REGION", "STATUS", "CREATED"]);
    for p in &resp.projects {
        t.row(&[&p.r#ref, &p.name, &p.region, &p.status, &p.created_at]);
    }
    t.flush()
}

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let r#ref = args
        .first()
        .ok_or_else(|| msg("usage: basin projects get <ref>"))?;
    let c = require_client(g)?;
    let resp: ProjectResp = c.do_json(Method::GET, &format!("/v1/projects/{ref}"), None)?;
    if g.json {
        // JSON shape: { project: Project }
        return print_json(&mut std::io::stdout(), &json!({ "project": resp.project }));
    }
    let Some(p) = resp.project else {
        println!("(empty)");
        return Ok(());
    };
    println!("ref:             {}", p.r#ref);
    println!("name:            {}", p.name);
    println!("region:          {}", p.region);
    println!("status:          {}", p.status);
    println!("id:              {}", p.id);
    println!("engine_tenant:   {}", p.engine_tenant_id);
    println!("created_at:      {}", p.created_at);
    Ok(())
}

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects create")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("region").long("region"))
        .arg(Arg::new("name").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let name: String = m
        .get_many::<String>("name")
        .map(|v| v.cloned().collect::<Vec<_>>().join(" "))
        .unwrap_or_default();
    if name.is_empty() {
        return Err(msg(
            "usage: basin projects create <name> --org <slug> [--region <code>]",
        ));
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    if org.is_empty() {
        return Err(msg("projects create requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let mut body = json!({ "name": name });
    if let Some(region) = m.get_one::<String>("region") {
        body["region"] = json!(region);
    }
    let resp: CreateProjectResponse = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/projects"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { project: Project, keys: {…}, pgwire: Pgwire }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(p) = &resp.project {
        println!("Created project {} ({}) in {}.", p.name, p.r#ref, p.region);
    }
    if let Some(pg) = &resp.pgwire {
        if !pg.connection_url.is_empty() {
            println!();
            println!("Connection URL (shown ONCE — store it now):");
            println!("  {}", pg.connection_url);
        }
    }
    if let Some(keys) = &resp.keys {
        if let Some(anon) = keys.get("anon").and_then(|v| v.as_str()) {
            if !anon.is_empty() {
                println!();
                println!("API keys (also shown ONCE):");
                println!("  anon: {anon}");
            }
        }
    }
    Ok(())
}

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("projects delete")
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("ref").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let r#ref = m
        .get_many::<String>("ref")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin projects delete <ref>"))?;
    let c = require_client(g)?;
    let meta: ProjectResp = c.do_json(Method::GET, &format!("/v1/projects/{ref}"), None)?;
    let Some(p) = meta.project else {
        return Err(msg(format!("project not found: {ref}")));
    };
    if !m.get_flag("yes") {
        eprint!(
            "About to delete project {:?} ({}). Re-type the name to confirm:\n> ",
            p.name, p.r#ref
        );
        let typed = read_line()?;
        if typed.trim() != p.name {
            return Err(msg("name mismatch — aborted"));
        }
    }
    let q = query_string(&[("confirm_name", &p.name)]);
    c.do_noout(Method::DELETE, &format!("/v1/projects/{ref}{q}"), None)?;
    if g.json {
        // JSON shape: { deleted: bool, ref: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "deleted": true, "ref": r#ref }),
        );
    }
    println!("Deleted project {ref}.");
    Ok(())
}

fn pause_resume(g: &GlobalFlags, args: &[String], action: &str) -> CliResult<()> {
    let r#ref = args
        .first()
        .ok_or_else(|| msg(format!("usage: basin projects {action} <ref>")))?;
    let c = require_client(g)?;
    let out: serde_json::Value =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/{action}"), None)?;
    if g.json {
        // JSON shape: passthrough of the pause|resume envelope.
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("{ref}: {action} ok.");
    Ok(())
}
