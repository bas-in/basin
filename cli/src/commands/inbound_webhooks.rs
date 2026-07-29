//! inbound_webhooks — inbound webhook receiver lifecycle (T-094).
//!
//! Inbound webhooks are HMAC-verified receive-and-invoke endpoints:
//! an external service (Stripe, GitHub, etc.) POSTs to a signed URL;
//! the cloud verifies the HMAC and calls a user-configured SQL function
//! with the payload as a JSONB argument.
//!
//! This is entirely separate from the outbound webhook dispatcher
//! (see `webhooks.rs`).
//!
//!   basin webhooks inbound list   [--project=<ref>]
//!   basin webhooks inbound create --fn=<function-name> [--project=<ref>]
//!   basin webhooks inbound get    <id> [--project=<ref>]
//!   basin webhooks inbound delete <id> [--project=<ref>] [--yes]
//!   basin webhooks inbound rotate <id> [--project=<ref>] [--yes]
//!
//! Backed by:
//!   GET    /v1/projects/{ref}/inbound-webhooks
//!   POST   /v1/projects/{ref}/inbound-webhooks
//!   GET    /v1/projects/{ref}/inbound-webhooks/:id
//!   DELETE /v1/projects/{ref}/inbound-webhooks/:id
//!   POST   /v1/projects/{ref}/inbound-webhooks/:id/rotate
//!
//! HMAC scheme: X-Basin-Signature (sha256=<hex>) + X-Basin-Timestamp header,
//! identical to GitHub / Stripe but with a replay-window guard. The receive
//! URL is printed once on create (POST /v1/inbound/:token — data-plane public
//! receiver; no authentication on the caller side, just HMAC).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
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

// ── Wire shapes ───────────────────────────────────────────────────────────────

/// InboundWebhook is the read shape for a receiver configuration.
/// JSON shape: { id, function_name, receive_url, created_at }
/// The HMAC secret is only revealed on create and rotate; it is
/// not returned by list or get.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct InboundWebhook {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub function_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub receive_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    /// Only populated on create and rotate (reveal-once secret).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hmac_secret: Option<String>,
}

#[derive(Default, Deserialize)]
struct ListResp {
    #[serde(default)]
    inbound_webhooks: Vec<InboundWebhook>,
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

/// cmd_webhooks_inbound is the top-level dispatcher for the `webhooks inbound`
/// sub-namespace. Called from `cmd_webhooks` when args[0] == "inbound".
pub fn cmd_webhooks_inbound(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            print_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "get" => get(g, rest),
        "delete" => delete(g, rest),
        "rotate" => rotate(g, rest),
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for webhooks inbound", other);
            Err(silent())
        }
    }
}

fn print_help() {
    help_for_command(
        "webhooks inbound",
        "Manage inbound webhook receivers (HMAC-verified, routes payload to a SQL function).",
        &[
            "list   [--project=<ref>]                           List receivers.",
            "create --fn=<function-name> [--project=<ref>]      Create a new receiver (HMAC secret shown ONCE).",
            "get    <id> [--project=<ref>]                      Get a receiver by ID.",
            "delete <id> [--project=<ref>] [--yes]              Delete a receiver.",
            "rotate <id> [--project=<ref>] [--yes]              Rotate the HMAC secret (new secret shown ONCE).",
        ],
    );
}

// ── list ──────────────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks inbound list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "webhooks inbound list",
            "List all inbound webhook receivers for the project.",
            &["--project=<ref>   Project ref (or use basin link)."],
        );
        return Ok(());
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/inbound-webhooks", project_ref);
    // Handle both array and wrapped response shapes.
    let raw: serde_json::Value = c.do_json(Method::GET, &path, None)?;
    let receivers: Vec<InboundWebhook> = if raw.is_array() {
        serde_json::from_value(raw).unwrap_or_default()
    } else {
        let r: ListResp = serde_json::from_value(raw).unwrap_or_default();
        r.inbound_webhooks
    };

    if g.json {
        // JSON shape: [ InboundWebhook ]
        return print_json(&mut std::io::stdout(), &receivers);
    }

    if receivers.is_empty() {
        if !g.quiet {
            println!("No inbound webhook receivers.");
        }
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "FUNCTION", "RECEIVE_URL", "CREATED"]);
    for r in &receivers {
        t.row(&[&r.id, &r.function_name, &r.receive_url, &r.created_at]);
    }
    t.flush()
}

// ── create ────────────────────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks inbound create")
        .arg(Arg::new("fn").long("fn"))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "webhooks inbound create",
            "Create an inbound webhook receiver. The HMAC secret is shown ONCE.",
            &[
                "--fn=<name>        SQL/Wasm function to invoke on receipt (required).",
                "--project=<ref>    Project ref (or use basin link).",
            ],
        );
        return Ok(());
    }

    let fn_name = m
        .get_one::<String>("fn")
        .cloned()
        .ok_or_else(|| msg("webhooks inbound create: --fn is required"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/inbound-webhooks", project_ref);
    let body = json!({ "function_name": fn_name });
    let receiver: InboundWebhook = c.do_json(Method::POST, &path, Some(body))?;

    if g.json {
        // JSON shape: InboundWebhook (with hmac_secret set)
        return print_json(&mut std::io::stdout(), &receiver);
    }

    println!("Created inbound webhook receiver {:?}.", receiver.id);
    println!("  Function : {}", receiver.function_name);
    println!("  Receive URL: {}", receiver.receive_url);
    if let Some(ref secret) = receiver.hmac_secret {
        println!("  HMAC secret (shown once): {secret}");
    }
    Ok(())
}

// ── get ───────────────────────────────────────────────────────────────────────

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks inbound get")
        .arg(Arg::new("id").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "webhooks inbound get",
            "Get a single inbound webhook receiver by ID.",
            &[
                "<id>               Receiver ID (required).",
                "--project=<ref>    Project ref (or use basin link).",
            ],
        );
        return Ok(());
    }

    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks inbound get <id>"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/inbound-webhooks/{}", project_ref, id);
    let receiver: InboundWebhook = c.do_json(Method::GET, &path, None)?;

    if g.json {
        // JSON shape: InboundWebhook
        return print_json(&mut std::io::stdout(), &receiver);
    }

    println!("ID       : {}", receiver.id);
    println!("Function : {}", receiver.function_name);
    println!("URL      : {}", receiver.receive_url);
    println!("Created  : {}", receiver.created_at);
    Ok(())
}

// ── delete ────────────────────────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks inbound delete")
        .arg(Arg::new("id").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").short('y').action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "webhooks inbound delete",
            "Delete an inbound webhook receiver.",
            &[
                "<id>               Receiver ID (required).",
                "--project=<ref>    Project ref (or use basin link).",
                "--yes              Skip confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks inbound delete <id>"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;
    let yes = m.get_flag("yes");

    if !yes && !g.json {
        eprint!("Delete inbound webhook {:?}? [y/N] ", id);
        let answer = read_line()?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Cancelled.");
            return Ok(());
        }
    } else if g.json && !yes {
        return Err(msg(
            "webhooks inbound delete: --yes is required when using --json",
        ));
    }

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/inbound-webhooks/{}", project_ref, id);
    c.do_noout(Method::DELETE, &path, None)?;

    if g.json {
        return print_json(
            &mut std::io::stdout(),
            &json!({ "id": id, "deleted": true }),
        );
    }
    println!("Deleted inbound webhook receiver {:?}.", id);
    Ok(())
}

// ── rotate ────────────────────────────────────────────────────────────────────

fn rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks inbound rotate")
        .arg(Arg::new("id").index(1))
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").short('y').action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "webhooks inbound rotate",
            "Rotate the HMAC secret for an inbound webhook receiver. New secret shown ONCE.",
            &[
                "<id>               Receiver ID (required).",
                "--project=<ref>    Project ref (or use basin link).",
                "--yes              Skip confirmation prompt.",
            ],
        );
        return Ok(());
    }

    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks inbound rotate <id>"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let project_ref = resolve_project_ref(&project_flag)?;
    let yes = m.get_flag("yes");

    if !yes && !g.json {
        eprint!(
            "Rotate HMAC secret for inbound webhook {:?}? External services must be updated. [y/N] ",
            id
        );
        let answer = read_line()?;
        if !matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
            println!("Cancelled.");
            return Ok(());
        }
    } else if g.json && !yes {
        return Err(msg(
            "webhooks inbound rotate: --yes is required when using --json",
        ));
    }

    let c = require_client(g)?;
    let path = format!("/v1/projects/{}/inbound-webhooks/{}/rotate", project_ref, id);
    let receiver: InboundWebhook = c.do_json(Method::POST, &path, None)?;

    if g.json {
        // JSON shape: InboundWebhook (with new hmac_secret)
        return print_json(&mut std::io::stdout(), &receiver);
    }

    println!("Rotated HMAC secret for inbound webhook {:?}.", receiver.id);
    if let Some(ref secret) = receiver.hmac_secret {
        println!("  New secret (shown once): {secret}");
    }
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

    fn sample_receiver() -> &'static str {
        r#"{"id":"iw1","function_name":"handle_stripe","receive_url":"https://api.basin.run/v1/inbound/tok123","created_at":"2026-06-01T00:00:00Z"}"#
    }

    fn sample_receiver_with_secret() -> &'static str {
        r#"{"id":"iw1","function_name":"handle_stripe","receive_url":"https://api.basin.run/v1/inbound/tok123","created_at":"2026-06-01T00:00:00Z","hmac_secret":"wh_sec_abcdef123"}"#
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn no_args_prints_help() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_webhooks_inbound(&g, &[]).is_ok());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        assert!(cmd_webhooks_inbound(&g, &["--help".to_string()]).is_ok());
        assert!(cmd_webhooks_inbound(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_webhooks_inbound(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn subcommand_help_flags_work() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        for sub in &["list", "create", "get", "delete", "rotate"] {
            let res = cmd_webhooks_inbound(&g, &[sub.to_string(), "--help".to_string()]);
            assert!(res.is_ok(), "{sub} --help: {res:?}");
        }
    }

    // ── list ──────────────────────────────────────────────────────────────────

    #[test]
    fn list_sends_get_to_correct_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(format!("[{}]", sample_receiver()))
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
        assert_eq!(*cap.lock().unwrap(), "/v1/projects/p1/inbound-webhooks");
    }

    #[test]
    fn list_empty_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("[]"));
        let g = flags(&srv.url);
        cmd_webhooks_inbound(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_wrapped_shape_ok() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::ok(format!(r#"{{"inbound_webhooks":[{}]}}"#, sample_receiver()))
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(format!("[{}]", sample_receiver())));
        let g = flags_json(&srv.url);
        cmd_webhooks_inbound(&g, &["list".to_string(), "--project=p1".to_string()]).unwrap();
    }

    // ── create ────────────────────────────────────────────────────────────────

    #[test]
    fn create_posts_fn_name_in_body() {
        let _g = with_temp_config_dir();
        let cap_body = Arc::new(Mutex::new(String::new()));
        let b2 = Arc::clone(&cap_body);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            *b2.lock().unwrap() = req.body.clone();
            Resp::ok(sample_receiver_with_secret())
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &[
                "create".to_string(),
                "--fn=handle_stripe".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_str(&cap_body.lock().unwrap()).unwrap();
        assert_eq!(body["function_name"].as_str().unwrap(), "handle_stripe");
    }

    #[test]
    fn create_missing_fn_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        // clap will surface an error for missing required --fn
        let err = cmd_webhooks_inbound(
            &g,
            &["create".to_string(), "--project=p1".to_string()],
        );
        // Clap will print an error and return silent, OR we return our own msg.
        assert!(err.is_err());
    }

    #[test]
    fn create_json_shows_secret() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok(sample_receiver_with_secret()));
        let g = flags_json(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &[
                "create".to_string(),
                "--fn=handle_stripe".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap();
    }

    // ── get ───────────────────────────────────────────────────────────────────

    #[test]
    fn get_sends_get_to_item_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(sample_receiver())
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &["get".to_string(), "iw1".to_string(), "--project=p1".to_string()],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "/v1/projects/p1/inbound-webhooks/iw1");
    }

    #[test]
    fn get_missing_id_errors() {
        let _g = with_temp_config_dir();
        let g = flags("http://127.0.0.1:1");
        let err = cmd_webhooks_inbound(
            &g,
            &["get".to_string(), "--project=p1".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"), "err: {err}");
    }

    // ── delete ────────────────────────────────────────────────────────────────

    #[test]
    fn delete_with_yes_sends_delete_to_item_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "DELETE");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok("{}")
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &[
                "delete".to_string(),
                "iw1".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(*cap.lock().unwrap(), "/v1/projects/p1/inbound-webhooks/iw1");
    }

    #[test]
    fn delete_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = flags_json("http://127.0.0.1:1");
        let err = cmd_webhooks_inbound(
            &g,
            &[
                "delete".to_string(),
                "iw1".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--yes"), "err: {err}");
    }

    #[test]
    fn delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| Resp::ok("{}"));
        let g = flags_json(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &[
                "delete".to_string(),
                "iw1".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
    }

    // ── rotate ────────────────────────────────────────────────────────────────

    #[test]
    fn rotate_with_yes_posts_to_rotate_path() {
        let _g = with_temp_config_dir();
        let cap = Arc::new(Mutex::new(String::new()));
        let c2 = Arc::clone(&cap);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            *c2.lock().unwrap() = req.path.clone();
            Resp::ok(sample_receiver_with_secret())
        });
        let g = flags(&srv.url);
        cmd_webhooks_inbound(
            &g,
            &[
                "rotate".to_string(),
                "iw1".to_string(),
                "--project=p1".to_string(),
                "--yes".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(
            *cap.lock().unwrap(),
            "/v1/projects/p1/inbound-webhooks/iw1/rotate"
        );
    }

    #[test]
    fn rotate_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = flags_json("http://127.0.0.1:1");
        let err = cmd_webhooks_inbound(
            &g,
            &[
                "rotate".to_string(),
                "iw1".to_string(),
                "--project=p1".to_string(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--yes"), "err: {err}");
    }

    // ── error mapping ─────────────────────────────────────────────────────────

    #[test]
    fn not_found_maps_to_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"receiver not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_webhooks_inbound(
            &g,
            &["get".to_string(), "gone".to_string(), "--project=p1".to_string()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("ApiError");
        assert_eq!(ae.http_status, 404);
    }
}
