//! rls — row-level security management for a project table.
//!
//!   basin rls enable   <table> [--project=<ref>]
//!   basin rls disable  <table> [--project=<ref>]
//!   basin rls policies list   <table> [--project=<ref>]
//!   basin rls policies create <table> --name=<n> --command=<cmd> --using=<sql>
//!                             [--with-check=<sql>] [--roles=<csv>] [--project=<ref>]
//!   basin rls policies drop   <table> --name=<n> [--project=<ref>] [--yes]
//!
//! Routes:
//!   POST   /v1/projects/{ref}/tables/{name}/rls/enable
//!   POST   /v1/projects/{ref}/tables/{name}/rls/disable
//!   GET    /v1/projects/{ref}/tables/{name}/policies
//!   POST   /v1/projects/{ref}/tables/{name}/policies      body { name, command, using, with_check, roles }
//!   DELETE /v1/projects/{ref}/tables/{name}/policies/{policy_name}

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// RLSStatus is the JSON shape returned by rls/enable and rls/disable.
/// JSON shape: { "enabled": bool, "table": string }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RLSStatus {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub table: String,
}

/// Policy is one row returned by GET …/policies.
/// JSON shape: { name, command, using, with_check, roles }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Policy {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub command: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub using: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub with_check: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
}

/// policyCreateResponse: JSON shape { "policy": Policy }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PolicyCreateResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<Policy>,
}

/// policyDropResponse: JSON shape { "dropped": bool, "name": string }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PolicyDropResponse {
    #[serde(default)]
    pub dropped: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

pub fn cmd_rls(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin rls (enable | disable | policies) <table> ...",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "enable" => rls_enable(g, rest),
        "disable" => rls_disable(g, rest),
        "policies" => rls_policies(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "rls",
                "Enable / disable / manage RLS policies on a project table.",
                &[
                    "enable   <table> [--project=<ref>]                                         Enable RLS.",
                    "disable  <table> [--project=<ref>]                                         Disable RLS.",
                    "policies list   <table> [--project=<ref>]                                  List policies.",
                    "policies create <table> --name=<n> --command=<cmd> --using=<sql>           Create a policy.",
                    "                        [--with-check=<sql>] [--roles=<csv>]",
                    "policies drop   <table> --name=<n> [--project=<ref>] [--yes]               Drop a policy.",
                ],
            );
            Ok(())
        }
        other => Err(msg(format!("unknown subcommand {:?} for rls", other))),
    }
}

// ── rls enable ────────────────────────────────────────────────────────────────

fn rls_enable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rls enable")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rls enable",
            "Enable RLS on a table.",
            &[
                "<table>            Table name (required).",
                "--project <ref>    Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m
        .get_one::<String>("table")
        .cloned()
        .ok_or_else(|| msg("usage: basin rls enable <table> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let resp: RLSStatus = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables/{table}/rls/enable"),
        None,
    )?;
    if g.json {
        // JSON shape: { "enabled": true, "table": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("RLS enabled on table {table}.");
    Ok(())
}

// ── rls disable ───────────────────────────────────────────────────────────────

fn rls_disable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rls disable")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rls disable",
            "Disable RLS on a table.",
            &[
                "<table>            Table name (required).",
                "--project <ref>    Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m
        .get_one::<String>("table")
        .cloned()
        .ok_or_else(|| msg("usage: basin rls disable <table> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let resp: RLSStatus = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables/{table}/rls/disable"),
        None,
    )?;
    if g.json {
        // JSON shape: { "enabled": false, "table": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("RLS disabled on table {table}.");
    Ok(())
}

// ── rls policies dispatcher ───────────────────────────────────────────────────

fn rls_policies(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin rls policies (list | create | drop) <table> ...",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => policies_list(g, rest),
        "create" => policies_create(g, rest),
        "drop" => policies_drop(g, rest),
        other => Err(msg(format!(
            "unknown subcommand {:?} for rls policies",
            other
        ))),
    }
}

// ── rls policies list ─────────────────────────────────────────────────────────

fn policies_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rls policies list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rls policies list",
            "List RLS policies on a table.",
            &[
                "<table>            Table name (required).",
                "--project <ref>    Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m
        .get_one::<String>("table")
        .cloned()
        .ok_or_else(|| msg("usage: basin rls policies list <table> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        policies: Vec<Policy>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/tables/{table}/policies"),
        None,
    )?;
    if g.json {
        // JSON shape: { "policies": [{ name, command, using, with_check, roles }] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "policies": resp.policies }),
        );
    }
    if resp.policies.is_empty() {
        println!("(no policies)");
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "COMMAND", "ROLES", "USING"]);
    for p in &resp.policies {
        let roles = if p.roles.is_empty() {
            "\u{2014}".to_string() // em dash
        } else {
            p.roles.join(",")
        };
        t.row(&[&p.name, &p.command, &roles, &p.using]);
    }
    t.flush()
}

// ── rls policies create ───────────────────────────────────────────────────────

fn policies_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rls policies create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("command").long("command"))
        .arg(Arg::new("using").long("using"))
        .arg(Arg::new("with-check").long("with-check"))
        .arg(Arg::new("roles").long("roles"))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rls policies create",
            "Create an RLS policy on a table.",
            &[
                "<table>                    Table name (required).",
                "--name=<n>                 Policy name (required).",
                "--command=<cmd>            Policy command: select|insert|update|delete|all (required).",
                "--using=<sql>              USING expression (required).",
                "--with-check=<sql>         WITH CHECK expression (optional).",
                "--roles=<csv>              Comma-separated list of roles (optional).",
                "--project <ref>            Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin rls policies create <table> --name=<n> --command=<cmd> --using=<sql> [--project=<ref>]")
    })?;

    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("rls policies create: --name is required"));
    }
    let command = m.get_one::<String>("command").cloned().unwrap_or_default();
    if command.is_empty() {
        return Err(msg(
            "rls policies create: --command is required (select|insert|update|delete|all)",
        ));
    }
    let valid_commands = ["select", "insert", "update", "delete", "all"];
    if !valid_commands.contains(&command.to_ascii_lowercase().as_str()) {
        return Err(msg(
            "rls policies create: --command must be one of: select, insert, update, delete, all",
        ));
    }
    let using = m.get_one::<String>("using").cloned().unwrap_or_default();
    if using.is_empty() {
        return Err(msg("rls policies create: --using is required"));
    }
    let with_check = m
        .get_one::<String>("with-check")
        .cloned()
        .unwrap_or_default();
    let roles_str = m.get_one::<String>("roles").cloned().unwrap_or_default();

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let mut body = json!({
        "name": name,
        "command": command.to_ascii_lowercase(),
        "using": using,
    });
    if !with_check.is_empty() {
        body["with_check"] = json!(with_check);
    }
    if !roles_str.is_empty() {
        let role_list: Vec<String> = roles_str
            .split(',')
            .map(|r| r.trim().to_string())
            .filter(|r| !r.is_empty())
            .collect();
        body["roles"] = json!(role_list);
    }

    let resp: PolicyCreateResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/tables/{table}/policies"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "policy": { name, command, using, with_check, roles } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let p_name = resp
        .policy
        .as_ref()
        .map(|p| p.name.as_str())
        .unwrap_or(&name);
    println!("Created policy {p_name:?} on table {table}.");
    Ok(())
}

// ── rls policies drop ─────────────────────────────────────────────────────────

fn policies_drop(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("rls policies drop")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("table"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "rls policies drop",
            "Drop an RLS policy from a table.",
            &[
                "<table>              Table name (required).",
                "--name=<n>           Policy name to drop (required).",
                "--yes                Skip the confirmation prompt.",
                "--project <ref>      Project ref.",
            ],
        );
        return Ok(());
    }
    let table = m.get_one::<String>("table").cloned().ok_or_else(|| {
        msg("usage: basin rls policies drop <table> --name=<n> [--project=<ref>] [--yes]")
    })?;
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("rls policies drop: --name is required"));
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to drop policy {:?} non-interactively under --json",
                name
            )));
        }
        eprint!(
            "Drop policy {:?} from table {table}? This cannot be undone. [y/N] ",
            name
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    let resp: PolicyDropResponse = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/tables/{table}/policies/{name}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "dropped": true, "name": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Dropped policy {:?} from table {table}.", name);
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

    fn sample_policy_json() -> &'static str {
        r#"{"name":"read_own","command":"select","using":"auth.uid() = user_id","with_check":"auth.uid() = user_id","roles":["authenticated"]}"#
    }

    // ── dispatcher ──────────────────────────────────────────────────────────

    #[test]
    fn no_args_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rls(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rls(&g, &["frobnicate".to_string()]).is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rls(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn policies_unknown_subcommand_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rls(&g, &["policies".to_string(), "frobnicate".to_string()]).is_err());
    }

    // ── rls enable ──────────────────────────────────────────────────────────

    #[test]
    fn enable_happy_path() {
        let _g = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let c2 = Arc::clone(&called);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/posts/rls/enable");
            *c2.lock().unwrap() = true;
            Resp::ok(r#"{"enabled":true,"table":"posts"}"#)
        });
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "enable".to_string(),
                "--project=p1".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn enable_missing_table_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        assert!(cmd_rls(&g, &["enable".to_string(), "--project=p1".to_string()]).is_err());
    }

    #[test]
    fn enable_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"enabled":true,"table":"posts"}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RLSStatus = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables/posts/rls/enable",
                None,
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["enabled"].as_bool().unwrap());
        assert_eq!(v["table"].as_str().unwrap(), "posts");
    }

    #[test]
    fn enable_403_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"code":"forbidden","message":"Insufficient permissions."}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_rls(
            &g,
            &[
                "enable".to_string(),
                "--project=p1".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── rls disable ─────────────────────────────────────────────────────────

    #[test]
    fn disable_happy_path() {
        let _g = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let c2 = Arc::clone(&called);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/posts/rls/disable");
            *c2.lock().unwrap() = true;
            Resp::ok(r#"{"enabled":false,"table":"posts"}"#)
        });
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "disable".to_string(),
                "--project=p1".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn disable_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"enabled":false,"table":"posts"}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RLSStatus = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables/posts/rls/disable",
                None,
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(!v["enabled"].as_bool().unwrap());
    }

    // ── rls policies list ────────────────────────────────────────────────────

    #[test]
    fn policies_list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1/tables/posts/policies");
            Resp::ok(format!(
                r#"{{"policies":[{},{}]}}"#,
                sample_policy_json(),
                r#"{"name":"write_own","command":"insert","using":"auth.uid() = user_id"}"#
            ))
        });
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn policies_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"policies":[]}"#));
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn policies_list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"policies":[{}]}}"#, sample_policy_json()))
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        #[derive(Deserialize)]
        struct R {
            #[serde(default)]
            policies: Vec<Policy>,
        }
        let resp: R = c
            .do_json(Method::GET, "/v1/projects/p1/tables/posts/policies", None)
            .unwrap();
        print_json(&mut buf, &json!({ "policies": resp.policies })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let pols = v["policies"].as_array().unwrap();
        assert_eq!(pols.len(), 1);
        assert_eq!(pols[0]["name"].as_str().unwrap(), "read_own");
        assert_eq!(pols[0]["command"].as_str().unwrap(), "select");
    }

    #[test]
    fn policies_list_404_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Table not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "list".to_string(),
                "--project=p1".to_string(),
                "ghost".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── rls policies create ──────────────────────────────────────────────────

    #[test]
    fn policies_create_happy_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(serde_json::Value::Null));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/p1/tables/posts/policies");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            *c2.lock().unwrap() = body;
            Resp::ok(format!(r#"{{"policy":{}}}"#, sample_policy_json()))
        });
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=read_own".to_string(),
                "--command=select".to_string(),
                "--using=auth.uid()=user_id".to_string(),
                "--roles=authenticated".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
        let body = cap.lock().unwrap();
        assert_eq!(body["name"].as_str().unwrap(), "read_own");
        assert_eq!(body["command"].as_str().unwrap(), "select");
    }

    #[test]
    fn policies_create_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--command=select".to_string(),
                "--using=true".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name is required"), "err: {err}");
    }

    #[test]
    fn policies_create_missing_command_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=p1".to_string(),
                "--using=true".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--command is required"),
            "err: {err}"
        );
    }

    #[test]
    fn policies_create_invalid_command_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=p1".to_string(),
                "--command=truncate".to_string(),
                "--using=true".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--command must be one of"),
            "err: {err}"
        );
    }

    #[test]
    fn policies_create_missing_using_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=p1".to_string(),
                "--command=select".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("--using is required"),
            "err: {err}"
        );
    }

    #[test]
    fn policies_create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"policy":{"name":"write_own","command":"insert","using":"true"}}"#)
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PolicyCreateResponse = c
            .do_json(
                Method::POST,
                "/v1/projects/p1/tables/posts/policies",
                Some(json!({"name":"write_own","command":"insert","using":"true"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["policy"]["name"].as_str().unwrap(), "write_own");
    }

    #[test]
    fn policies_create_422_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"code":"validation_error","message":"policy already exists"}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "create".to_string(),
                "--project=p1".to_string(),
                "--name=dup".to_string(),
                "--command=all".to_string(),
                "--using=true".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── rls policies drop ────────────────────────────────────────────────────

    #[test]
    fn policies_drop_with_yes_calls_delete() {
        let _g = with_temp_config_dir();
        let called = Arc::new(Mutex::new(false));
        let c2 = Arc::clone(&called);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/p1/tables/posts/policies/read_own");
            *c2.lock().unwrap() = true;
            Resp::ok(r#"{"dropped":true,"name":"read_own"}"#)
        });
        let g = flags(&srv.url);
        cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--name=read_own".to_string(),
                "--yes".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap();
        assert!(*called.lock().unwrap());
    }

    #[test]
    fn policies_drop_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name is required"), "err: {err}");
    }

    #[test]
    fn policies_drop_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            json: true,
            ..Default::default()
        };
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--name=read_own".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "err: {err}"
        );
    }

    #[test]
    fn policies_drop_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"dropped":true,"name":"read_own"}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: PolicyDropResponse = c
            .do_json(
                Method::DELETE,
                "/v1/projects/p1/tables/posts/policies/read_own",
                None,
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["dropped"].as_bool().unwrap());
        assert_eq!(v["name"].as_str().unwrap(), "read_own");
    }

    #[test]
    fn policies_drop_server_error_propagates() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"Policy not found."}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_rls(
            &g,
            &[
                "policies".to_string(),
                "drop".to_string(),
                "--project=p1".to_string(),
                "--name=ghost".to_string(),
                "--yes".to_string(),
                "posts".to_string(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── no project resolution ─────────────────────────────────────────────────

    #[test]
    fn no_project_resolution_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_rls(&g, &["enable".to_string(), "posts".to_string()]).unwrap_err();
        assert!(
            err.to_string().contains("--project") || err.to_string().contains("basin link"),
            "err: {err}"
        );
    }
}
