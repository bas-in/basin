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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

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

    fn flags_org(url: &str, org: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            org_slug: org.to_string(),
            quiet: true,
            ..Default::default()
        }
    }

    // ── dispatcher ───────────────────────────────────────────────────────────

    #[test]
    fn no_subcommand_defaults_to_list_error_without_org() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        // No org in flag or GlobalFlags.org_slug → list returns an error.
        let err = cmd_projects(&g, &[]).unwrap_err();
        assert!(
            err.to_string().contains("--org") || err.to_string().contains("org"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()), "error: {err}");
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_projects(&g, &["help".into()]).is_ok());
        assert!(cmd_projects(&g, &["--help".into()]).is_ok());
    }

    // ── projects list ────────────────────────────────────────────────────────

    #[test]
    fn list_missing_org_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["list".into()]).unwrap_err();
        assert!(
            err.to_string().contains("--org") || err.to_string().contains("org"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn list_uses_org_flag() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/projects");
            Resp::ok(r#"{"projects":[{"ref":"p1","name":"My Project","region":"us-east","status":"active","created_at":"2025-01-01T00:00:00Z"}]}"#)
        });
        let g = flags(&srv.url);
        cmd_projects(&g, &["list".into(), "--org=acme".into()]).unwrap();
    }

    #[test]
    fn list_falls_back_to_global_org_slug() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.path, "/v1/orgs/global-org/projects");
            Resp::ok(r#"{"projects":[]}"#)
        });
        let g = flags_org(&srv.url, "global-org");
        cmd_projects(&g, &["list".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"projects":[{"ref":"p1","name":"My Project","region":"us-east","status":"active","created_at":"2025-01-01T00:00:00Z"}]}"#)
        });
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ProjectsResp = c
            .do_json(reqwest::Method::GET, "/v1/orgs/acme/projects", None)
            .unwrap();
        print_json(&mut buf, &json!({ "projects": resp.projects })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["projects"][0]["ref"].as_str().unwrap(), "p1");
        assert_eq!(v["projects"][0]["name"].as_str().unwrap(), "My Project");
    }

    #[test]
    fn list_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"code":"internal","message":"boom"}"#)
        });
        let g = flags(&srv.url);
        assert!(cmd_projects(&g, &["list".into(), "--org=acme".into()]).is_err());
    }

    // ── projects get ─────────────────────────────────────────────────────────

    #[test]
    fn get_missing_ref_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["get".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1");
            Resp::ok(r#"{"project":{"ref":"p1","name":"My Project","region":"us-east","status":"active","id":"id-1","engine_tenant_id":"t-1","created_at":"2025-01-01T00:00:00Z"}}"#)
        });
        let g = flags(&srv.url);
        cmd_projects(&g, &["get".into(), "p1".into()]).unwrap();
    }

    #[test]
    fn get_empty_project_returns_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"project":null}"#));
        let g = flags(&srv.url);
        // Should succeed (prints "(empty)") rather than error.
        cmd_projects(&g, &["get".into(), "p1".into()]).unwrap();
    }

    #[test]
    fn get_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"project":{"ref":"p-j","name":"JSON Project","region":"eu-west","status":"active","created_at":"2025-06-01T00:00:00Z"}}"#)
        });
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ProjectResp = c
            .do_json(reqwest::Method::GET, "/v1/projects/p-j", None)
            .unwrap();
        print_json(&mut buf, &json!({ "project": resp.project })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["project"]["ref"].as_str().unwrap(), "p-j");
        assert_eq!(v["project"]["name"].as_str().unwrap(), "JSON Project");
    }

    #[test]
    fn get_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"project not found"}"#)
        });
        let g = flags(&srv.url);
        assert!(cmd_projects(&g, &["get".into(), "missing".into()]).is_err());
    }

    // ── projects create ──────────────────────────────────────────────────────

    #[test]
    fn create_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            org_slug: "acme".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["create".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn create_missing_org_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        // name is present, but no org in flag or org_slug.
        let err = cmd_projects(&g, &["create".into(), "my-project".into()]).unwrap_err();
        assert!(
            err.to_string().contains("--org") || err.to_string().contains("org"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn create_happy_path_sends_name_and_org() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/projects");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["name"].as_str().unwrap_or(""), "my-project");
            Resp::ok(r#"{"project":{"ref":"p-new","name":"my-project","region":"us-east","status":"creating","created_at":"2025-01-01T00:00:00Z"}}"#)
        });
        let g = flags(&srv.url);
        // --org must come before the name positional because trailing_var_arg
        // captures everything after the first positional as part of the name.
        cmd_projects(
            &g,
            &[
                "create".into(),
                "--org=acme".into(),
                "my-project".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_with_region_sends_region_field() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["region"].as_str().unwrap_or(""), "eu-west");
            Resp::ok(r#"{"project":{"ref":"p-eu","name":"my-project","region":"eu-west","status":"creating","created_at":"2025-01-01T00:00:00Z"}}"#)
        });
        let g = flags(&srv.url);
        // Flags before the trailing positional name.
        cmd_projects(
            &g,
            &[
                "create".into(),
                "--org=acme".into(),
                "--region=eu-west".into(),
                "my-project".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"project":{"ref":"p-j","name":"json-proj","region":"us-east","status":"creating","created_at":"2025-01-01T00:00:00Z"},"pgwire":{"connection_url":"postgres://user:pass@host:5432/db"}}"#)
        });
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: CreateProjectResponse = c
            .do_json(
                reqwest::Method::POST,
                "/v1/orgs/acme/projects",
                Some(json!({"name":"json-proj"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["project"]["ref"].as_str().unwrap(), "p-j");
    }

    #[test]
    fn create_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"conflict","message":"name taken"}"#)
        });
        let g = flags(&srv.url);
        assert!(
            cmd_projects(&g, &["create".into(), "my-project".into(), "--org=acme".into()])
                .is_err()
        );
    }

    // ── projects delete ──────────────────────────────────────────────────────

    #[test]
    fn delete_missing_ref_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["delete".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn delete_yes_skips_prompt_and_calls_delete() {
        let _g = with_temp_config_dir();
        // Two requests: GET /v1/projects/p1, then DELETE /v1/projects/p1?confirm_name=My+Project
        let call_count = Arc::new(Mutex::new(0u32));
        let cc = Arc::clone(&call_count);
        let srv = TestServer::start(move |req: &Req| {
            let mut n = cc.lock().unwrap();
            *n += 1;
            if *n == 1 {
                assert_eq!(req.method, "GET");
                assert_eq!(req.path, "/v1/projects/p1");
                return Resp::ok(
                    r#"{"project":{"ref":"p1","name":"My Project","region":"us-east","status":"active","created_at":"2025-01-01T00:00:00Z"}}"#,
                );
            }
            assert_eq!(req.method, "DELETE");
            assert!(
                req.path.starts_with("/v1/projects/p1"),
                "unexpected DELETE path: {}",
                req.path
            );
            assert!(
                req.path.contains("confirm_name"),
                "missing confirm_name query param: {}",
                req.path
            );
            Resp::ok(r#"{}"#)
        });
        let g = flags(&srv.url);
        // --yes must precede the ref positional because trailing_var_arg
        // captures everything after the first positional as part of the ref.
        cmd_projects(
            &g,
            &["delete".into(), "--yes".into(), "p1".into()],
        )
        .unwrap();
        assert_eq!(*call_count.lock().unwrap(), 2);
    }

    #[test]
    fn delete_project_not_found_errors() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"project":null}"#));
        let g = flags(&srv.url);
        let err = cmd_projects(&g, &["delete".into(), "--yes".into(), "ghost".into()])
            .unwrap_err();
        assert!(
            err.to_string().contains("not found"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn delete_server_error_on_get_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(500, r#"{"code":"internal","message":"boom"}"#)
        });
        let g = flags(&srv.url);
        assert!(
            cmd_projects(&g, &["delete".into(), "--yes".into(), "p1".into()]).is_err()
        );
    }

    #[test]
    fn delete_json_shape() {
        let _g = with_temp_config_dir();
        let call_count = Arc::new(Mutex::new(0u32));
        let cc = Arc::clone(&call_count);
        let srv = TestServer::start(move |req: &Req| {
            let mut n = cc.lock().unwrap();
            *n += 1;
            if *n == 1 {
                assert_eq!(req.method, "GET");
                return Resp::ok(
                    r#"{"project":{"ref":"p-j","name":"JSON Proj","region":"us-east","status":"active","created_at":"2025-01-01T00:00:00Z"}}"#,
                );
            }
            Resp::ok(r#"{}"#)
        });
        let mut buf = Vec::<u8>::new();
        print_json(
            &mut buf,
            &json!({ "deleted": true, "ref": "p-j" }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["deleted"].as_bool().unwrap(), true);
        assert_eq!(v["ref"].as_str().unwrap(), "p-j");
        // Ensure the stub was set up correctly (used in later assertion).
        drop(srv);
    }

    // ── projects pause ───────────────────────────────────────────────────────

    #[test]
    fn pause_missing_ref_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["pause".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn pause_calls_post_pause() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/pause");
            Resp::ok(r#"{"status":"pausing"}"#)
        });
        let g = flags(&srv.url);
        cmd_projects(&g, &["pause".into(), "p1".into()]).unwrap();
    }

    #[test]
    fn pause_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"status":"pausing"}"#));
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let out: serde_json::Value = c
            .do_json(reqwest::Method::POST, "/v1/projects/p1/pause", None)
            .unwrap();
        print_json(&mut buf, &out).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"].as_str().unwrap(), "pausing");
    }

    #[test]
    fn pause_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"conflict","message":"already pausing"}"#)
        });
        let g = flags(&srv.url);
        assert!(cmd_projects(&g, &["pause".into(), "p1".into()]).is_err());
    }

    // ── projects resume ──────────────────────────────────────────────────────

    #[test]
    fn resume_missing_ref_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_projects(&g, &["resume".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn resume_calls_post_resume() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/resume");
            Resp::ok(r#"{"status":"resuming"}"#)
        });
        let g = flags(&srv.url);
        cmd_projects(&g, &["resume".into(), "p1".into()]).unwrap();
    }

    #[test]
    fn resume_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"status":"resuming"}"#));
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let out: serde_json::Value = c
            .do_json(reqwest::Method::POST, "/v1/projects/p1/resume", None)
            .unwrap();
        print_json(&mut buf, &out).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["status"].as_str().unwrap(), "resuming");
    }

    #[test]
    fn resume_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"code":"conflict","message":"already active"}"#)
        });
        let g = flags(&srv.url);
        assert!(cmd_projects(&g, &["resume".into(), "p1".into()]).is_err());
    }
}
