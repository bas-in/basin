//! email — project email template configuration and allowance.
//!
//!   basin email templates get    [--project=<ref>]
//!   basin email templates put    --kind=<k> --subject=<s> --body=<b> [--project=<ref>]
//!   basin email templates delete [--project=<ref>] [--yes]
//!   basin email templates test   --kind=<k> --to=<email>             [--project=<ref>]
//!   basin email allowance        [--project=<ref>]
//!
//! Cloud routes:
//!   GET    /v1/projects/{ref}/email-config        → templates get
//!   PATCH  /v1/projects/{ref}/email-config        → templates put (partial update)
//!   DELETE /v1/projects/{ref}/email-config        → templates delete (reset to defaults)
//!   POST   /v1/projects/{ref}/email-config/test   → templates test
//!   GET    /v1/projects/{ref}/email/allowance     → allowance
//!
//! Note: the cloud mounts the email template surface under /email-config
//! (not /email/templates). The CLI surfaces it as `email templates ...`
//! for user-friendliness; the underlying path is /email-config.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// EmailAllowance is the read shape from GET /email/allowance.
/// JSON shape: { limit, used, reset_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct EmailAllowance {
    #[serde(default)]
    pub limit: i64,
    #[serde(default)]
    pub used: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reset_at: String,
}

#[derive(Deserialize, Serialize)]
struct TemplatesResp {
    #[serde(default)]
    templates: HashMap<String, serde_json::Value>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

/// valid_email_address performs a minimal sanity check on an email address.
/// Requires exactly one "@" with non-empty local and domain parts.
fn valid_email_address(s: &str) -> bool {
    let parts: Vec<&str> = s.splitn(2, '@').collect();
    if parts.len() != 2 {
        return false;
    }
    !parts[0].is_empty() && parts[1].contains('.')
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_email(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let help_lines = &[
        "templates get    [--project=<ref>]                           Get all email templates.",
        "templates put    --kind=<k> --subject=<s> --body=<b>        Update a template kind.",
        "                 [--project=<ref>]",
        "templates delete [--project=<ref>] [--yes]                  Reset templates to defaults.",
        "templates test   --kind=<k> --to=<email> [--project=<ref>]  Send a test email.",
        "allowance        [--project=<ref>]                           Show email send allowance.",
    ];
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command("email", "Manage project email templates and view allowance.", help_lines);
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "templates" => templates(g, rest),
        "allowance" => allowance(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command("email", "Manage project email templates and view allowance.", help_lines);
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for email", other);
            Err(silent())
        }
    }
}

/// templates dispatches email templates sub-subcommands.
fn templates(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return templates_get(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => templates_get(g, rest),
        "put" => templates_put(g, rest),
        "delete" => templates_delete(g, rest),
        "test" => templates_test(g, rest),
        other => {
            printerr!(g, "unknown subcommand {:?} for email templates", other);
            Err(silent())
        }
    }
}

// ── email templates get ──────────────────────────────────────────────────────

fn templates_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("email templates get").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: TemplatesResp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/email-config"), None)?;
    if g.json {
        // JSON shape: { templates: { <kind>: { subject, body } } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.templates.is_empty() {
        println!("(no email templates configured)");
        return Ok(());
    }
    let mut t = Table::new(g, &["KIND", "SUBJECT"]);
    for (kind, v) in &resp.templates {
        let subject = v
            .get("subject")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        t.row(&[kind, &subject]);
    }
    t.flush()
}

// ── email templates put ──────────────────────────────────────────────────────

fn templates_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("email templates put")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("kind").long("kind"))
        .arg(Arg::new("subject").long("subject"))
        .arg(Arg::new("body").long("body"));
    let m = parse_or_silent(cmd, args)?;
    let kind = m.get_one::<String>("kind").cloned().unwrap_or_default();
    if kind.is_empty() {
        return Err(msg("email templates put requires --kind=<kind>"));
    }
    let subject = m.get_one::<String>("subject").cloned().unwrap_or_default();
    if subject.is_empty() {
        return Err(msg("email templates put requires --subject=<subject>"));
    }
    let body_text = m.get_one::<String>("body").cloned().unwrap_or_default();
    if body_text.is_empty() {
        return Err(msg("email templates put requires --body=<body>"));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let kind_key = kind.clone();
    let req_body = json!({
        "templates": {
            kind_key: {
                "subject": subject,
                "body": body_text,
            }
        }
    });
    let resp: TemplatesResp =
        c.do_json(Method::PATCH, &format!("/v1/projects/{ref}/email-config"), Some(req_body))?;
    if g.json {
        // JSON shape: { templates: { <kind>: { subject, body } } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Updated email template {:?}.", kind);
    Ok(())
}

// ── email templates delete ───────────────────────────────────────────────────

fn templates_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("email templates delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(
                "confirmation required: pass --yes to reset email templates non-interactively under --json"
            ));
        }
        eprint!("Reset email templates to defaults for project {:?}? [y/N] ", r#ref);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct ResetResp {
        #[serde(default)]
        reset_to_defaults: bool,
    }
    let resp: ResetResp =
        c.do_json(Method::DELETE, &format!("/v1/projects/{ref}/email-config"), None)?;
    if g.json {
        // JSON shape: { reset_to_defaults: true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Email templates reset to defaults.");
    Ok(())
}

// ── email templates test ─────────────────────────────────────────────────────

fn templates_test(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("email templates test")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("kind").long("kind"))
        .arg(Arg::new("to").long("to"));
    let m = parse_or_silent(cmd, args)?;
    let kind = m.get_one::<String>("kind").cloned().unwrap_or_default();
    if kind.is_empty() {
        return Err(msg("email templates test requires --kind=<kind>"));
    }
    let to = m.get_one::<String>("to").cloned().unwrap_or_default();
    if to.is_empty() {
        return Err(msg("email templates test requires --to=<email>"));
    }
    if !valid_email_address(&to) {
        return Err(msg(format!("email templates test: invalid email address {:?}", to)));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct TestResp {
        #[serde(default)]
        sent: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        message_id: String,
    }
    let body = json!({ "kind": kind, "to": to });
    let resp: TestResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/email-config/test"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { sent: true, message_id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.sent {
        print!("Test email sent to {to}");
        if !resp.message_id.is_empty() {
            print!(" (message_id: {})", resp.message_id);
        }
        println!();
    } else {
        println!("Test email send failed (cloud returned sent=false).");
    }
    Ok(())
}

// ── email allowance ──────────────────────────────────────────────────────────

fn allowance(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("email allowance").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct AllowanceResp {
        #[serde(default)]
        allowance: Option<EmailAllowance>,
    }
    let resp: AllowanceResp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/email/allowance"), None)?;
    if g.json {
        // JSON shape: { allowance: { limit, used, reset_at } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let Some(a) = resp.allowance else {
        println!("(email allowance data unavailable)");
        return Ok(());
    };
    println!("limit:    {}", a.limit);
    println!("used:     {}", a.used);
    println!("reset_at: {}", a.reset_at);
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

    // ── valid_email_address unit tests ────────────────────────────────────────

    #[test]
    fn valid_email_basic() {
        assert!(valid_email_address("user@example.com"));
        assert!(valid_email_address("admin@example.com"));
    }

    #[test]
    fn invalid_email_no_at() {
        assert!(!valid_email_address("not-an-email"));
    }

    #[test]
    fn invalid_email_no_dot_in_domain() {
        assert!(!valid_email_address("user@nodot"));
    }

    // ── validation: templates test ────────────────────────────────────────────

    #[test]
    fn templates_test_missing_kind_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_email(
            &g,
            &["templates".into(), "test".into(), "--project=proj1".into(), "--to=user@example.com".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--kind"));
    }

    #[test]
    fn templates_test_missing_to_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_email(
            &g,
            &["templates".into(), "test".into(), "--project=proj1".into(), "--kind=confirmation".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--to"));
    }

    #[test]
    fn templates_test_invalid_to_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_email(
            &g,
            &[
                "templates".into(),
                "test".into(),
                "--project=proj1".into(),
                "--kind=confirmation".into(),
                "--to=not-an-email".into(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("invalid email address") || err.to_string().contains("not-an-email"),
            "unexpected error: {}",
            err
        );
    }

    // ── email templates get ───────────────────────────────────────────────────

    #[test]
    fn templates_get_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/email-config");
            Resp::ok(r#"{"templates":{"confirmation":{"subject":"Confirm your email","body":"<p>Click here</p>"},"recovery":{"subject":"Reset your password","body":"<p>Reset</p>"}}}"#)
        });
        let g = flags(&srv.url);
        cmd_email(&g, &["templates".into(), "get".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn templates_get_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"templates":{"confirmation":{"subject":"Confirm","body":"body"}}}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TemplatesResp =
            c.do_json(reqwest::Method::GET, "/v1/projects/proj1/email-config", None).unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["templates"]["confirmation"].is_object());
        let _ = &g;
    }

    #[test]
    fn templates_get_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"project not found"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_email(&g, &["templates".into(), "get".into(), "--project=proj1".into()])
            .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── email templates put ───────────────────────────────────────────────────

    #[test]
    fn templates_put_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/projects/proj1/email-config");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            // Verify body structure: { templates: { confirmation: { subject, body } } }
            assert_eq!(body["templates"]["confirmation"]["subject"], "New subject");
            Resp::ok(r#"{"templates":{"confirmation":{"subject":"New subject","body":"New body"}}}"#)
        });
        let g = flags(&srv.url);
        cmd_email(
            &g,
            &[
                "templates".into(),
                "put".into(),
                "--project=proj1".into(),
                "--kind=confirmation".into(),
                "--subject=New subject".into(),
                "--body=New body".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn templates_put_missing_kind_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_email(
            &g,
            &[
                "templates".into(),
                "put".into(),
                "--project=proj1".into(),
                "--subject=Subject".into(),
                "--body=Body".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--kind"));
    }

    // ── email templates delete ────────────────────────────────────────────────

    #[test]
    fn templates_delete_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/email-config");
            Resp::ok(r#"{"reset_to_defaults":true}"#)
        });
        let g = flags(&srv.url);
        cmd_email(
            &g,
            &["templates".into(), "delete".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap();
    }

    #[test]
    fn templates_delete_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"reset_to_defaults":true}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct RR { reset_to_defaults: bool }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RR =
            c.do_json(reqwest::Method::DELETE, "/v1/projects/proj1/email-config", None).unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["reset_to_defaults"].as_bool().unwrap(), true);
        let _ = &g;
    }

    // ── email templates test ──────────────────────────────────────────────────

    #[test]
    fn templates_test_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/email-config/test");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["kind"], "confirmation");
            assert_eq!(body["to"], "user@example.com");
            Resp::ok(r#"{"sent":true,"message_id":"msg-123"}"#)
        });
        let g = flags(&srv.url);
        cmd_email(
            &g,
            &[
                "templates".into(),
                "test".into(),
                "--project=proj1".into(),
                "--kind=confirmation".into(),
                "--to=user@example.com".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn templates_test_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"sent":true,"message_id":"msg-456"}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct TR { sent: bool, message_id: String }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/email-config/test",
                Some(json!({"kind":"recovery","to":"admin@example.com"})),
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["sent"].as_bool().unwrap(), true);
        assert_eq!(v["message_id"].as_str().unwrap(), "msg-456");
        let _ = &g;
    }

    #[test]
    fn templates_test_403() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"not allowed"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_email(
            &g,
            &[
                "templates".into(),
                "test".into(),
                "--project=proj1".into(),
                "--kind=confirmation".into(),
                "--to=user@example.com".into(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── email allowance ───────────────────────────────────────────────────────

    #[test]
    fn allowance_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/email/allowance");
            Resp::ok(r#"{"allowance":{"limit":1000,"used":42,"reset_at":"2026-06-01T00:00:00Z"}}"#)
        });
        let g = flags(&srv.url);
        cmd_email(&g, &["allowance".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn allowance_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"allowance":{"limit":500,"used":100,"reset_at":"2026-07-01T00:00:00Z"}}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct AR { allowance: Option<EmailAllowance> }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: AR =
            c.do_json(reqwest::Method::GET, "/v1/projects/proj1/email/allowance", None).unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["allowance"]["limit"].as_i64().unwrap(), 500);
        assert_eq!(v["allowance"]["used"].as_i64().unwrap(), 100);
        let _ = &g;
    }

    #[test]
    fn allowance_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"project not found"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_email(&g, &["allowance".into(), "--project=proj1".into()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn allowance_no_project_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_email(&g, &["allowance".into()]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("link"));
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_is_silent() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_email(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_email(&g, &["help".into()]).is_ok());
    }

    #[test]
    fn no_args_shows_help() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        // no args → shows help and returns Ok
        assert!(cmd_email(&g, &[]).is_ok());
    }
}
