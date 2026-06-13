//! webhooks — signed-webhook subscription lifecycle for a project.
//!
//!   basin webhooks list       [--project=<ref>]
//!   basin webhooks create     --url=<u> --events=<csv> [--project=<ref>]
//!   basin webhooks patch      <id> [--url=<u>] [--events=<csv>] [--enabled=true|false] [--project=<ref>]
//!   basin webhooks test       <id> [--project=<ref>]
//!   basin webhooks redeliver  <id> --delivery=<delivery-id> [--project=<ref>]
//!   basin webhooks delete     <id> [--project=<ref>] [--yes]
//!   basin webhooks deliveries <id> [--project=<ref>] [--limit=N]
//!
//! Backed by /v1/projects/{ref}/webhooks/* (migration 0024).
//!
//! Events CSV: split on comma, trim whitespace, drop empty tokens,
//! deduplicate (order-preserving, first-seen wins).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// Webhook is the read shape from GET /v1/projects/{ref}/webhooks.
/// JSON shape: { id, url, events, secret_prefix, enabled, created_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct Webhook {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret_prefix: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

/// WebhookDelivery is one delivery attempt returned by
/// GET /v1/projects/{ref}/webhooks/{id}/deliveries.
/// JSON shape: { id, status_code, delivered_at, event, url }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WebhookDelivery {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default)]
    pub status_code: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub delivered_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
}

#[derive(Deserialize)]
struct WebhooksListResp {
    #[serde(default)]
    webhooks: Vec<Webhook>,
}

#[derive(Deserialize)]
struct DeliveriesListResp {
    #[serde(default)]
    deliveries: Vec<WebhookDelivery>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or the
/// directory-scoped ./basin/config.toml.
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

/// parse_events_csv splits a comma-separated events string into a
/// deduplicated, whitespace-trimmed slice. Empty tokens are discarded.
/// Order is preserved (first occurrence wins on duplicates).
pub fn parse_events_csv(raw: &str) -> Vec<String> {
    let mut seen: HashMap<String, ()> = HashMap::new();
    let mut out = Vec::new();
    for part in raw.split(',') {
        let p = part.trim().to_string();
        if p.is_empty() {
            continue;
        }
        if seen.contains_key(&p) {
            continue;
        }
        seen.insert(p.clone(), ());
        out.push(p);
    }
    out
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_webhooks(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "patch" => patch(g, rest),
        "test" => test_webhook(g, rest),
        "redeliver" => redeliver(g, rest),
        "delete" => delete(g, rest),
        "deliveries" => deliveries(g, rest),
        "inbound" => super::inbound_webhooks::cmd_webhooks_inbound(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "webhooks",
                "Manage webhook subscriptions for a project.",
                &[
                    "list                         [--project=<ref>]                     List webhooks.",
                    "create --url=<u> --events=<csv> [--project=<ref>]                 Create a webhook (secret shown ONCE).",
                    "patch  <id> [--url=<u>] [--events=<csv>] [--enabled=bool] [--project=<ref>]  Update a webhook.",
                    "test   <id>                  [--project=<ref>]                     Send a test delivery.",
                    "redeliver <id> --delivery=<id> [--project=<ref>]                  Redeliver a past delivery.",
                    "delete <id>                  [--project=<ref>] [--yes]             Delete a webhook.",
                    "deliveries <id> [--limit=N]  [--project=<ref>]                    List deliveries for a webhook.",
                    "inbound list/create/get/delete/rotate [--project=<ref>]            Manage inbound webhook receivers.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for webhooks", other);
            Err(silent())
        }
    }
}

// ── webhooks list ─────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks list").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: WebhooksListResp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/webhooks"), None)?;
    if g.json {
        // JSON shape: { webhooks: [ Webhook ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "webhooks": resp.webhooks }),
        );
    }
    if resp.webhooks.is_empty() {
        println!("(no webhooks)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "URL", "EVENTS", "ENABLED", "CREATED"]);
    for wh in &resp.webhooks {
        let enabled = if wh.enabled { "true" } else { "false" };
        t.row(&[
            &wh.id,
            &wh.url,
            &wh.events.join(","),
            enabled,
            &wh.created_at,
        ]);
    }
    t.flush()
}

// ── webhooks create ───────────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("url").long("url"))
        .arg(Arg::new("events").long("events"));
    let m = parse_or_silent(cmd, args)?;
    let raw_url = m.get_one::<String>("url").cloned().unwrap_or_default();
    if raw_url.is_empty() {
        return Err(msg("webhooks create requires --url=<url>"));
    }
    let raw_events = m.get_one::<String>("events").cloned().unwrap_or_default();
    let events = parse_events_csv(&raw_events);
    if events.is_empty() {
        return Err(msg(
            "webhooks create requires --events=<csv> with at least one event type",
        ));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct CreateResp {
        #[serde(default)]
        webhook: Option<Webhook>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        secret: String,
    }
    let body = json!({ "url": raw_url, "events": events });
    let resp: CreateResp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/webhooks"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { webhook: Webhook, secret: string }
        // secret is reveal-once; subsequent list calls only show secret_prefix.
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(wh) = &resp.webhook {
        println!("Created webhook {} → {}", wh.id, wh.url);
    }
    if !resp.secret.is_empty() {
        if !g.quiet {
            eprintln!("Store this signing secret now — it will NOT be shown again.");
        }
        println!("secret: {}", resp.secret);
    }
    Ok(())
}

// ── webhooks patch ────────────────────────────────────────────────────────────

fn patch(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks patch")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("url").long("url"))
        .arg(Arg::new("events").long("events"))
        .arg(Arg::new("enabled").long("enabled"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks patch <id> [--url=<u>] [--events=<csv>] [--enabled=true|false] [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;

    let mut body = serde_json::Map::new();
    if let Some(u) = m.get_one::<String>("url").filter(|s| !s.is_empty()) {
        body.insert("url".into(), json!(u));
    }
    if let Some(ev) = m.get_one::<String>("events").filter(|s| !s.is_empty()) {
        let events = parse_events_csv(ev);
        if events.is_empty() {
            return Err(msg(
                "webhooks patch --events must contain at least one event type",
            ));
        }
        body.insert("events".into(), json!(events));
    }
    if let Some(enabled_str) = m.get_one::<String>("enabled").filter(|s| !s.is_empty()) {
        match enabled_str.to_lowercase().as_str() {
            "true" | "1" | "yes" => {
                body.insert("enabled".into(), json!(true));
            }
            "false" | "0" | "no" => {
                body.insert("enabled".into(), json!(false));
            }
            _ => {
                return Err(msg(format!(
                    "webhooks patch --enabled must be true or false, got {:?}",
                    enabled_str
                )));
            }
        }
    }
    if body.is_empty() {
        return Err(msg(
            "webhooks patch: at least one of --url, --events, --enabled must be provided",
        ));
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct PatchResp {
        #[serde(default)]
        webhook: Option<Webhook>,
    }
    let resp: PatchResp = c
        .do_json(
            Method::PATCH,
            &format!("/v1/projects/{ref}/webhooks/{id}"),
            Some(serde_json::Value::Object(body)),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("webhook {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { webhook: Webhook }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(wh) = &resp.webhook {
        let enabled = if wh.enabled { "true" } else { "false" };
        println!(
            "Updated webhook {} (url={}, enabled={})",
            wh.id, wh.url, enabled
        );
    } else {
        println!("Updated webhook {id}.");
    }
    Ok(())
}

// ── webhooks test ─────────────────────────────────────────────────────────────

fn test_webhook(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks test")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks test <id> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct TestResp {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        delivery_id: String,
        #[serde(default)]
        status_code: i64,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        delivered_at: String,
    }
    let resp: TestResp = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/webhooks/{id}/test"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("webhook {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { delivery_id: string, status_code: int, delivered_at: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!(
        "Test delivery sent (delivery_id={}, status={}, at={})",
        resp.delivery_id, resp.status_code, resp.delivered_at
    );
    Ok(())
}

// ── webhooks redeliver ────────────────────────────────────────────────────────

fn redeliver(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks redeliver")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("delivery").long("delivery"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m.get_one::<String>("id").cloned().ok_or_else(|| {
        msg("usage: basin webhooks redeliver <id> --delivery=<delivery-id> [--project=<ref>]")
    })?;
    let delivery_id = m.get_one::<String>("delivery").cloned().unwrap_or_default();
    if delivery_id.is_empty() {
        return Err(msg("webhooks redeliver requires --delivery=<delivery-id>"));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct RedeliverResp {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        delivery_id: String,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        status: String,
    }
    let path = format!("/v1/projects/{ref}/webhooks/{id}/redeliver/{delivery_id}");
    let resp: RedeliverResp = c.do_json(Method::POST, &path, None).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 404 {
                return msg(format!(
                    "webhook {:?} or delivery {:?} not found on project {:?}",
                    id, delivery_id, r#ref
                ));
            }
        }
        e
    })?;
    if g.json {
        // JSON shape: { delivery_id: string, status: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!(
        "Redelivered delivery {} (status={})",
        resp.delivery_id, resp.status
    );
    Ok(())
}

// ── webhooks delete ───────────────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin webhooks delete <id> [--project=<ref>] [--yes]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete webhook {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Delete webhook {:?} on project {:?}? This cannot be undone. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct DeleteResp {
        #[serde(default)]
        deleted: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        id: String,
    }
    let resp: DeleteResp = c
        .do_json(
            Method::DELETE,
            &format!("/v1/projects/{ref}/webhooks/{id}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("webhook {:?} not found on project {:?}", id, r#ref));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { deleted: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted webhook {id}.");
    Ok(())
}

// ── webhooks deliveries ───────────────────────────────────────────────────────

fn deliveries(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("webhooks deliveries")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m.get_one::<String>("id").cloned().ok_or_else(|| {
        msg("usage: basin webhooks deliveries <id> [--project=<ref>] [--limit=N]")
    })?;
    let limit_str = m
        .get_one::<String>("limit")
        .cloned()
        .unwrap_or_else(|| "50".into());
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let q = query_string(&[("limit", &limit_str)]);
    let path = format!("/v1/projects/{ref}/webhooks/{id}/deliveries{q}");
    let resp: DeliveriesListResp = c.do_json(Method::GET, &path, None).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 404 {
                return msg(format!("webhook {:?} not found on project {:?}", id, r#ref));
            }
        }
        e
    })?;
    if g.json {
        // JSON shape: { deliveries: [ WebhookDelivery ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "deliveries": resp.deliveries }),
        );
    }
    if resp.deliveries.is_empty() {
        println!("(no deliveries)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "EVENT", "STATUS", "DELIVERED_AT"]);
    for d in &resp.deliveries {
        let status = if d.status_code == 0 {
            "—".to_string()
        } else {
            d.status_code.to_string()
        };
        t.row(&[&d.id, &d.event, &status, &d.delivered_at]);
    }
    t.flush()
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

    // ── parse_events_csv unit tests ───────────────────────────────────────────

    #[test]
    fn parse_events_basic() {
        let got = parse_events_csv("a,b,c");
        assert_eq!(got, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_events_trims_whitespace() {
        let got = parse_events_csv("  foo , bar  , baz ");
        assert_eq!(got, vec!["foo", "bar", "baz"]);
    }

    #[test]
    fn parse_events_deduplication() {
        let got = parse_events_csv("a,b,a,c,b");
        assert_eq!(got.len(), 3);
        // order preserved (first occurrence wins)
        assert_eq!(got[0], "a");
        assert_eq!(got[1], "b");
        assert_eq!(got[2], "c");
    }

    #[test]
    fn parse_events_empty_tokens_dropped() {
        let got = parse_events_csv(",foo,,bar,");
        assert_eq!(got, vec!["foo", "bar"]);
    }

    #[test]
    fn parse_events_empty_string() {
        let got = parse_events_csv("");
        assert!(got.is_empty());
    }

    // ── webhooks list ─────────────────────────────────────────────────────────

    #[test]
    fn list_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/webhooks");
            Resp::ok(r#"{"webhooks":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_webhooks(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_populated() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"webhooks":[{"id":"wh1","url":"https://example.com/hook","events":["project.created"],"enabled":true,"created_at":"2026-01-01T00:00:00Z"},{"id":"wh2","url":"https://other.com/hook","events":["table.altered"],"enabled":false,"created_at":"2026-01-02T00:00:00Z"}]}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_webhooks(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"webhooks":[{"id":"wh1","url":"https://example.com/hook","events":["project.created"],"enabled":true}]}"#,
            )
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: WebhooksListResp = c
            .do_json(reqwest::Method::GET, "/v1/projects/p1/webhooks", None)
            .unwrap();
        print_json(&mut out, &json!({ "webhooks": resp.webhooks })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["webhooks"].is_array());
        assert_eq!(v["webhooks"][0]["id"].as_str().unwrap(), "wh1");
        let _ = &g;
    }

    #[test]
    fn list_no_project_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(&g, &["list".into()]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("link"));
    }

    // ── webhooks create ───────────────────────────────────────────────────────

    #[test]
    fn create_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/webhooks");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["url"], "https://example.com/hook");
            Resp::ok(
                r#"{"webhook":{"id":"wh-new","url":"https://example.com/hook","events":["project.created"],"enabled":true},"secret":"whsec_supersecret1234"}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--url=https://example.com/hook".into(),
                "--events=project.created,table.altered".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_missing_url_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--events=project.created".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--url"));
    }

    #[test]
    fn create_missing_events_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--url=https://example.com/hook".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--events"));
    }

    #[test]
    fn create_empty_events_csv_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--url=https://example.com/hook".into(),
                "--events=,,,".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--events"));
    }

    #[test]
    fn create_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"webhook":{"id":"wh-json","url":"https://example.com/hook","events":["project.created"],"enabled":true},"secret":"whsec_jsontest"}"#,
            )
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct CR {
            #[serde(default)]
            webhook: Option<Webhook>,
            #[serde(default)]
            secret: String,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: CR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/webhooks",
                Some(json!({"url":"https://example.com/hook","events":["project.created"]})),
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["secret"].as_str().unwrap(), "whsec_jsontest");
        assert_eq!(v["webhook"]["id"].as_str().unwrap(), "wh-json");
        let _ = &g;
    }

    // ── webhooks patch ────────────────────────────────────────────────────────

    #[test]
    fn patch_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/projects/proj1/webhooks/wh1");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["url"], "https://new.example.com/hook");
            assert!(!body["enabled"].as_bool().unwrap());
            Resp::ok(
                r#"{"webhook":{"id":"wh1","url":"https://new.example.com/hook","events":["project.created"],"enabled":false}}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &[
                "patch".into(),
                "--project=proj1".into(),
                "--url=https://new.example.com/hook".into(),
                "--enabled=false".into(),
                "wh1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn patch_no_fields_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &["patch".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("at least one of"));
    }

    #[test]
    fn patch_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"webhook not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_webhooks(
            &g,
            &[
                "patch".into(),
                "--project=proj1".into(),
                "--url=https://x.com/h".into(),
                "missing".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    // ── webhooks test ─────────────────────────────────────────────────────────

    #[test]
    fn test_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/webhooks/wh1/test");
            Resp::ok(
                r#"{"delivery_id":"del-001","status_code":200,"delivered_at":"2026-01-01T00:00:00Z"}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_webhooks(&g, &["test".into(), "--project=proj1".into(), "wh1".into()]).unwrap();
    }

    #[test]
    fn test_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"delivery_id":"del-json","status_code":200,"delivered_at":"2026-01-01T00:00:00Z"}"#,
            )
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct TR {
            delivery_id: String,
            status_code: i64,
            delivered_at: String,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: TR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/webhooks/wh1/test",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["delivery_id"].as_str().unwrap(), "del-json");
        assert_eq!(v["status_code"].as_i64().unwrap(), 200);
        let _ = &g;
    }

    // ── webhooks redeliver ────────────────────────────────────────────────────

    #[test]
    fn redeliver_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(
                req.path,
                "/v1/projects/proj1/webhooks/wh1/redeliver/del-001"
            );
            Resp::ok(r#"{"delivery_id":"del-001","status":"queued"}"#)
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &[
                "redeliver".into(),
                "--project=proj1".into(),
                "--delivery=del-001".into(),
                "wh1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn redeliver_missing_delivery_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &["redeliver".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--delivery"));
    }

    #[test]
    fn redeliver_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"delivery_id":"del-001","status":"queued"}"#)
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        #[derive(Serialize, Deserialize)]
        struct RR {
            delivery_id: String,
            status: String,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RR = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/webhooks/wh1/redeliver/del-001",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["status"].as_str().unwrap(), "queued");
        let _ = &g;
    }

    // ── webhooks delete ───────────────────────────────────────────────────────

    #[test]
    fn delete_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/webhooks/wh1");
            Resp::ok(r#"{"deleted":true,"id":"wh1"}"#)
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "wh1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn delete_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_webhooks(
            &g,
            &["delete".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("confirmation required"));
    }

    #[test]
    fn delete_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":true,"id":"wh1"}"#));
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct D {
            deleted: bool,
            id: String,
        }
        print_json(
            &mut out,
            &D {
                deleted: true,
                id: "wh1".into(),
            },
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["deleted"].as_bool().unwrap());
        assert_eq!(v["id"].as_str().unwrap(), "wh1");
        let _ = &g;
    }

    // ── webhooks deliveries ───────────────────────────────────────────────────

    #[test]
    fn deliveries_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req
                .path
                .starts_with("/v1/projects/proj1/webhooks/wh1/deliveries"));
            Resp::ok(
                r#"{"deliveries":[{"id":"del-001","status_code":200,"delivered_at":"2026-01-01T00:00:00Z","event":"project.created"},{"id":"del-002","status_code":500,"delivered_at":"2026-01-02T00:00:00Z","event":"table.altered"}]}"#,
            )
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &["deliveries".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap();
    }

    #[test]
    fn deliveries_limit_passthrough() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(
                req.path.contains("limit=25"),
                "limit not in path: {}",
                req.path
            );
            Resp::ok(r#"{"deliveries":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &[
                "deliveries".into(),
                "--project=proj1".into(),
                "--limit=25".into(),
                "wh1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn deliveries_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deliveries":[]}"#));
        let g = flags(&srv.url);
        cmd_webhooks(
            &g,
            &["deliveries".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap();
    }

    #[test]
    fn deliveries_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"deliveries":[{"id":"del-001","status_code":200,"delivered_at":"2026-01-01T00:00:00Z","event":"project.created"}]}"#,
            )
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: DeliveriesListResp = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/proj1/webhooks/wh1/deliveries",
                None,
            )
            .unwrap();
        print_json(&mut out, &json!({ "deliveries": resp.deliveries })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["deliveries"][0]["id"].as_str().unwrap(), "del-001");
        let _ = &g;
    }

    #[test]
    fn deliveries_422() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"validation_error","message":"invalid limit value"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_webhooks(
            &g,
            &["deliveries".into(), "--project=proj1".into(), "wh1".into()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── dispatcher ────────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_is_silent() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_webhooks(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_webhooks(&g, &["help".into()]).is_ok());
    }
}
