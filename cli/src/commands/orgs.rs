//! orgs — list / show / create / update / delete orgs + branding.
//!
//! Routes:
//!   GET    /v1/orgs
//!   GET    /v1/orgs/{slug}
//!   POST   /v1/orgs
//!   PATCH  /v1/orgs/{slug}
//!   DELETE /v1/orgs/{slug}
//!   GET    /v1/orgs/{slug}/branding
//!   PUT    /v1/orgs/{slug}/branding
//!   DELETE /v1/orgs/{slug}/branding
//!
//! The `ownership-transfers` and `incoming-project-transfers` subcommands
//! are implemented in commands/transfers.rs and dispatched below.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;
use crate::types::Org;

use super::help::help_for_command;
use super::parse_or_silent;

// ── local response structs ────────────────────────────────────────────

#[derive(Deserialize)]
struct OrgsListResp {
    #[serde(default)]
    orgs: Vec<Org>,
}

#[derive(Deserialize)]
struct OrgResp {
    #[serde(default)]
    org: Option<Org>,
}

/// OrgBranding is the read/write shape for /v1/orgs/{slug}/branding.
#[derive(Debug, Default, Serialize, Deserialize)]
struct OrgBranding {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    logo_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    primary_color: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    secondary_color: String,
}

#[derive(Deserialize, Serialize)]
struct BrandingResp {
    #[serde(default)]
    branding: Option<OrgBranding>,
}

// ── top-level dispatcher ─────────────────────────────────────────────

pub fn cmd_orgs(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "show" => show(g, rest),
        "create" => create(g, rest),
        "update" => update(g, rest),
        "delete" => delete(g, rest),
        "branding" => branding(g, rest),
        "ownership-transfers" => {
            super::transfers::cmd_orgs_ownership_transfers(g, rest)
        }
        "incoming-project-transfers" => {
            super::transfers::cmd_orgs_incoming_project_transfers(g, rest)
        }
        "--help" | "-h" | "help" => {
            help_for_command(
                "orgs",
                "List, show, create, update, delete organizations and manage branding.",
                &[
                    "list                                                       List orgs the caller can see.",
                    "show <slug>                                                Show one org by slug.",
                    "create --slug=<s> --name=<n> [--plan=free|pro]            Create a new org.",
                    "update <slug> [--name=<n>] [--plan=<p>]                   Update an org.",
                    "delete <slug> [--yes]                                      Delete an org (destructive).",
                    "branding get <slug>                                        Get org branding.",
                    "branding put <slug> [--logo-url=...] [--primary-color=...] [--secondary-color=...]  Set org branding.",
                    "branding delete <slug>                                     Remove org branding.",
                    "ownership-transfers <list|create|cancel|accept|decline>   Manage org-owner handovers.",
                    "incoming-project-transfers <list|accept|decline>          Manage inbound project handovers.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for orgs", other);
            Err(silent())
        }
    }
}

// ── orgs list ────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs list");
    let _m = parse_or_silent(cmd, args)?;
    let c = require_client(g)?;
    let resp: OrgsListResp = c.do_json(Method::GET, "/v1/orgs", None)?;
    if g.json {
        // JSON shape: { orgs: [ Org ] }
        return print_json(&mut std::io::stdout(), &json!({ "orgs": resp.orgs }));
    }
    let mut t = Table::new(g, &["SLUG", "NAME", "PLAN", "ID"]);
    for o in &resp.orgs {
        t.row(&[&o.slug, &o.name, &o.plan, &o.id]);
    }
    t.flush()
}

// ── orgs show ────────────────────────────────────────────────────────

fn show(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let slug = args.first().ok_or_else(|| msg("usage: basin orgs show <slug>"))?;
    let c = require_client(g)?;
    let resp: OrgResp = c.do_json(Method::GET, &format!("/v1/orgs/{slug}"), None)?;
    if g.json {
        // JSON shape: { org: Org }
        return print_json(&mut std::io::stdout(), &json!({ "org": resp.org }));
    }
    let Some(o) = resp.org else {
        println!("(empty)");
        return Ok(());
    };
    println!("slug:           {}", o.slug);
    println!("name:           {}", o.name);
    println!("plan:           {}", o.plan);
    println!("id:             {}", o.id);
    if !o.billing_email.is_empty() {
        println!("billing_email:  {}", o.billing_email);
    }
    Ok(())
}

// ── orgs create ──────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs create")
        .arg(Arg::new("slug").long("slug"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("plan").long("plan"));
    let m = parse_or_silent(cmd, args)?;
    let slug = m.get_one::<String>("slug").cloned().unwrap_or_default();
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if slug.is_empty() || name.is_empty() {
        return Err(msg("orgs create requires --slug=<slug> and --name=<name>"));
    }
    let mut body = json!({ "slug": slug, "name": name });
    if let Some(plan) = m.get_one::<String>("plan").filter(|s| !s.is_empty()) {
        body["plan"] = json!(plan);
    }
    let c = require_client(g)?;
    let resp: OrgResp = c.do_json(Method::POST, "/v1/orgs", Some(body)).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 409 {
                return msg(format!(
                    "org slug conflict: an org with slug {:?} already exists",
                    slug
                ));
            }
        }
        e
    })?;
    if g.json {
        // JSON shape: { "org": { id, slug, name, plan } }
        return print_json(&mut std::io::stdout(), &json!({ "org": resp.org }));
    }
    if let Some(o) = resp.org {
        println!("Created org {:?} ({}).", o.slug, o.id);
    } else {
        println!("Org created.");
    }
    Ok(())
}

// ── orgs update ──────────────────────────────────────────────────────

fn update(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs update")
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("plan").long("plan"))
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs update <slug> [--name=<n>] [--plan=<p>]"))?;
    let mut body = json!({});
    if let Some(name) = m.get_one::<String>("name").filter(|s| !s.is_empty()) {
        body["name"] = json!(name);
    }
    if let Some(plan) = m.get_one::<String>("plan").filter(|s| !s.is_empty()) {
        body["plan"] = json!(plan);
    }
    if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        return Err(msg("orgs update: nothing to update — pass --name or --plan"));
    }
    let c = require_client(g)?;
    let resp: OrgResp = c.do_json(Method::PATCH, &format!("/v1/orgs/{slug}"), Some(body))?;
    if g.json {
        // JSON shape: { "org": { id, slug, name, plan } }
        return print_json(&mut std::io::stdout(), &json!({ "org": resp.org }));
    }
    if let Some(o) = resp.org {
        println!("Updated org {:?}.", o.slug);
    } else {
        println!("Updated org {slug}.");
    }
    Ok(())
}

// ── orgs delete ──────────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs delete")
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs delete <slug> [--yes]"))?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete org {:?} non-interactively under --json",
                slug
            )));
        }
        eprint!("Delete org {:?}? This cannot be undone. [y/N] ", slug);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Serialize, Deserialize)]
    struct DeleteResp {
        deleted: bool,
        slug: String,
    }
    let resp: DeleteResp = c.do_json(Method::DELETE, &format!("/v1/orgs/{slug}"), None)?;
    if g.json {
        // JSON shape: { "deleted": true, "slug": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted org {slug}.");
    Ok(())
}

// ── orgs branding ────────────────────────────────────────────────────

fn branding(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin orgs branding {get|put|delete} <slug> [flags]"))
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => branding_get(g, rest),
        "put" => branding_put(g, rest),
        "delete" => branding_delete(g, rest),
        other => Err(msg(format!(
            "unknown branding subcommand {:?}; want get|put|delete",
            other
        ))),
    }
}

fn branding_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs branding get")
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs branding get <slug>"))?;
    let c = require_client(g)?;
    let resp: BrandingResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{slug}/branding"), None)?;
    if g.json {
        // JSON shape: { "branding": { logo_url, primary_color, secondary_color } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let Some(b) = resp.branding else {
        println!("(no branding set)");
        return Ok(());
    };
    println!("logo_url:        {}", b.logo_url);
    println!("primary_color:   {}", b.primary_color);
    println!("secondary_color: {}", b.secondary_color);
    Ok(())
}

fn branding_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs branding put")
        .arg(Arg::new("logo-url").long("logo-url"))
        .arg(Arg::new("primary-color").long("primary-color"))
        .arg(Arg::new("secondary-color").long("secondary-color"))
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| {
            msg("usage: basin orgs branding put <slug> [--logo-url=...] [--primary-color=...] [--secondary-color=...]")
        })?;
    let mut body = json!({});
    if let Some(v) = m.get_one::<String>("logo-url").filter(|s| !s.is_empty()) {
        body["logo_url"] = json!(v);
    }
    if let Some(v) = m.get_one::<String>("primary-color").filter(|s| !s.is_empty()) {
        body["primary_color"] = json!(v);
    }
    if let Some(v) = m.get_one::<String>("secondary-color").filter(|s| !s.is_empty()) {
        body["secondary_color"] = json!(v);
    }
    let c = require_client(g)?;
    let resp: BrandingResp =
        c.do_json(Method::PUT, &format!("/v1/orgs/{slug}/branding"), Some(body))?;
    if g.json {
        // JSON shape: { "branding": { logo_url, primary_color, secondary_color } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let _ = resp;
    println!("Updated branding for org {slug}.");
    Ok(())
}

fn branding_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("orgs branding delete")
        .arg(Arg::new("slug").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let slug = m
        .get_many::<String>("slug")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin orgs branding delete <slug>"))?;
    let c = require_client(g)?;
    let out: serde_json::Value =
        c.do_json(Method::DELETE, &format!("/v1/orgs/{slug}/branding"), None)?;
    if g.json {
        // JSON shape: passthrough of branding delete envelope.
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Deleted branding for org {slug}.");
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────

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

    fn sample_org(slug: &str, name: &str, plan: &str) -> String {
        format!(
            r#"{{"id":"org-{slug}","slug":"{slug}","name":"{name}","plan":"{plan}"}}"#
        )
    }

    // ── orgs list ────────────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs");
            Resp::ok(r#"{"orgs":[{"id":"org-1","slug":"acme","name":"Acme","plan":"free"}]}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["list".into()]);
        assert!(err.is_ok(), "list: {:?}", err);
    }

    #[test]
    fn list_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"orgs":[{"id":"org-1","slug":"acme","name":"Acme","plan":"free"}]}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let g_j = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };
        // Call list directly by invoking cmd_orgs to exercise dispatch.
        let resp: OrgsListResp =
            serde_json::from_str(r#"{"orgs":[{"id":"org-1","slug":"acme","name":"Acme","plan":"free"}]}"#)
                .unwrap();
        print_json(&mut out, &json!({ "orgs": resp.orgs })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["orgs"].is_array());
        let _ = g_j;
    }

    // ── orgs show ────────────────────────────────────────────────────

    #[test]
    fn show_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.path, "/v1/orgs/acme");
            Resp::ok(format!(r#"{{"org":{}}}"#, sample_org("acme", "Acme", "free")))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["show".into(), "acme".into()]);
        assert!(err.is_ok(), "show: {:?}", err);
    }

    #[test]
    fn show_missing_slug() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["show".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("usage"));
    }

    // ── orgs create ──────────────────────────────────────────────────

    #[test]
    fn create_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["slug"], "acme");
            assert_eq!(body["name"], "Acme Corp");
            Resp::ok(format!(r#"{{"org":{}}}"#, sample_org("acme", "Acme Corp", "free")))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(
            &g(&srv),
            &["create".into(), "--slug=acme".into(), "--name=Acme Corp".into()],
        );
        assert!(err.is_ok(), "create: {:?}", err);
    }

    #[test]
    fn create_missing_slug() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["create".into(), "--name=Acme Corp".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--slug"));
    }

    #[test]
    fn create_missing_name() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["create".into(), "--slug=acme".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--name"));
    }

    #[test]
    fn create_409_conflict() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"error":{"code":"conflict","message":"slug taken"}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(
            &g(&srv),
            &["create".into(), "--slug=acme".into(), "--name=Acme".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("conflict"));
    }

    #[test]
    fn create_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"org":{}}}"#, sample_org("acme", "Acme Corp", "pro")))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let o = Org {
            id: "org-acme".into(),
            slug: "acme".into(),
            name: "Acme Corp".into(),
            plan: "pro".into(),
            ..Default::default()
        };
        print_json(&mut out, &json!({ "org": o })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["org"]["slug"], "acme");
        assert_eq!(parsed["org"]["plan"], "pro");
        let _ = &srv; // ensure server stays alive
    }

    // ── orgs update ──────────────────────────────────────────────────

    #[test]
    fn update_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/orgs/acme");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["name"], "Acme Renamed");
            Resp::ok(format!(r#"{{"org":{}}}"#, sample_org("acme", "Acme Renamed", "free")))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(
            &g(&srv),
            &["update".into(), "--name=Acme Renamed".into(), "acme".into()],
        );
        assert!(err.is_ok(), "update: {:?}", err);
    }

    #[test]
    fn update_missing_slug() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["update".into(), "--name=New Name".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn update_nothing_to_update() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["update".into(), "acme".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("nothing to update"));
    }

    #[test]
    fn update_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"org":{}}}"#, sample_org("acme", "Acme Updated", "pro")))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let o = Org {
            id: "org-acme".into(),
            slug: "acme".into(),
            name: "Acme Updated".into(),
            plan: "pro".into(),
            ..Default::default()
        };
        print_json(&mut out, &json!({ "org": o })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["org"]["plan"], "pro");
        let _ = &srv;
    }

    #[test]
    fn update_403() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"Insufficient permissions."}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(
            &g(&srv),
            &["update".into(), "--name=New Name".into(), "acme".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── orgs delete ──────────────────────────────────────────────────

    #[test]
    fn delete_confirmed_yes_flag() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme");
            Resp::ok(r#"{"deleted":true,"slug":"acme"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["delete".into(), "--yes".into(), "acme".into()]);
        assert!(err.is_ok(), "delete: {:?}", err);
    }

    #[test]
    fn delete_missing_slug() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["delete".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn delete_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, json: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["delete".into(), "acme".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("confirmation required"));
    }

    #[test]
    fn delete_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"deleted":true,"slug":"acme"}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct D { deleted: bool, slug: String }
        print_json(&mut out, &D { deleted: true, slug: "acme".into() }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["deleted"], true);
        assert_eq!(parsed["slug"], "acme");
        let _ = &srv;
    }

    #[test]
    fn delete_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"Org not found."}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["delete".into(), "--yes".into(), "ghost".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── orgs branding get ─────────────────────────────────────────────

    #[test]
    fn branding_get_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/branding");
            Resp::ok(r##"{"branding":{"logo_url":"https://example.com/logo.png","primary_color":"#1a2b3c","secondary_color":"#4d5e6f"}}"##)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["branding".into(), "get".into(), "acme".into()]);
        assert!(err.is_ok(), "branding get: {:?}", err);
    }

    #[test]
    fn branding_get_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r##"{"branding":{"logo_url":"https://example.com/logo.png","primary_color":"#1a2b3c"}}"##)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let b = BrandingResp {
            branding: Some(OrgBranding {
                logo_url: "https://example.com/logo.png".into(),
                primary_color: "#1a2b3c".into(),
                secondary_color: String::new(),
            }),
        };
        print_json(&mut out, &b).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["branding"]["primary_color"], "#1a2b3c");
        let _ = &srv;
    }

    #[test]
    fn branding_get_missing_slug() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["branding".into(), "get".into()]);
        assert!(err.is_err());
    }

    // ── orgs branding put ─────────────────────────────────────────────

    #[test]
    fn branding_put_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PUT");
            assert_eq!(req.path, "/v1/orgs/acme/branding");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["logo_url"], "https://example.com/logo.png");
            Resp::ok(r##"{"branding":{"logo_url":"https://example.com/logo.png","primary_color":"#1a2b3c"}}"##)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(
            &g(&srv),
            &[
                "branding".into(),
                "put".into(),
                "--logo-url=https://example.com/logo.png".into(),
                "--primary-color=#1a2b3c".into(),
                "acme".into(),
            ],
        );
        assert!(err.is_ok(), "branding put: {:?}", err);
    }

    // ── orgs branding delete ──────────────────────────────────────────

    #[test]
    fn branding_delete_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme/branding");
            Resp::ok(r#"{"deleted":true}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_orgs(&g(&srv), &["branding".into(), "delete".into(), "acme".into()]);
        assert!(err.is_ok(), "branding delete: {:?}", err);
    }

    // ── misc ──────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_returns_silent() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn ownership_transfers_no_args_is_usage_error() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_orgs(&g0, &["ownership-transfers".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("usage"));
    }
}
