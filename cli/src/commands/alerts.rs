//! alerts — project-scoped alert rules and events.
//!
//!   basin alerts rules list        [--project=<ref>]
//!   basin alerts rules create      --name=<n> --metric=<m> --threshold=<n> --comparator=<op> [--enabled=true] [--project=<ref>]
//!   basin alerts rules get         <id> [--project=<ref>]
//!   basin alerts rules patch       <id> [--name=<n>] [--threshold=<n>] [--enabled=true|false] [--project=<ref>]
//!   basin alerts rules delete      <id> [--project=<ref>] [--yes]
//!   basin alerts rules silence     <id> --until=<rfc3339> [--project=<ref>]
//!   basin alerts rules unsilence   <id> [--project=<ref>]
//!   basin alerts events list       [--project=<ref>] [--limit=N] [--since=<rfc3339>]
//!
//! Backed by /v1/projects/{ref}/alerts/*.
//! Project ref resolution: --project= flag, else loadWorkingProject(cwd).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// AlertRule is the read shape for a single alert rule.
/// JSON shape: { id, name, metric, threshold, comparator, enabled, silenced_until }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct AlertRule {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub metric: String,
    #[serde(default)]
    pub threshold: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub comparator: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub silenced_until: Option<String>,
}

/// AlertEvent is the read shape for a fired/resolved alert event.
/// JSON shape: { id, rule_id, fired_at, resolved_at, value }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct AlertEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub rule_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub fired_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<String>,
    #[serde(default)]
    pub value: f64,
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

/// valid_comparator checks whether a comparator string is one of the
/// allowed values: gt, lt, gte, lte, eq.
fn valid_comparator(s: &str) -> bool {
    matches!(
        s.to_lowercase().as_str(),
        "gt" | "lt" | "gte" | "lte" | "eq"
    )
}

fn silenced_display(r: &AlertRule) -> String {
    match &r.silenced_until {
        Some(s) if !s.is_empty() => s.clone(),
        _ => "—".to_string(),
    }
}

fn resolved_display(ev: &AlertEvent) -> String {
    match &ev.resolved_at {
        Some(s) if !s.is_empty() => s.clone(),
        _ => "—".to_string(),
    }
}

// ── Top-level dispatcher ─────────────────────────────────────────────────────

const ALERTS_HELP_LINES: &[&str] = &[
    "rules list   [--project=<ref>]                                         List alert rules.",
    "rules create --name=<n> --metric=<m> --threshold=<n> --comparator=<op> Create an alert rule.",
    "rules get    <id> [--project=<ref>]                                    Get an alert rule.",
    "rules patch  <id> [...] [--project=<ref>]                              Update an alert rule.",
    "rules delete <id> [--project=<ref>] [--yes]                            Delete an alert rule.",
    "rules silence   <id> --until=<rfc3339> [--project=<ref>]               Silence a rule until a time.",
    "rules unsilence <id> [--project=<ref>]                                 Remove a silence.",
    "events list  [--project=<ref>] [--limit=N] [--since=<rfc3339>]         List fired alert events.",
];

pub fn cmd_alerts(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    if args.is_empty() {
        help_for_command(
            "alerts",
            "Manage project alert rules and events.",
            ALERTS_HELP_LINES,
        );
        return Ok(());
    }
    let (sub, rest) = (args[0].as_str(), &args[1..]);
    match sub {
        "rules" => cmd_alerts_rules(g, rest),
        "events" => cmd_alerts_events(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "alerts",
                "Manage project alert rules and events.",
                ALERTS_HELP_LINES,
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for alerts", other);
            Err(silent())
        }
    }
}

// ── alerts rules dispatcher ───────────────────────────────────────────────────

fn cmd_alerts_rules(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return rules_list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => rules_list(g, rest),
        "create" => rules_create(g, rest),
        "get" => rules_get(g, rest),
        "patch" => rules_patch(g, rest),
        "delete" => rules_delete(g, rest),
        "silence" => rules_silence(g, rest),
        "unsilence" => rules_unsilence(g, rest),
        other => {
            printerr!(g, "unknown subcommand {:?} for alerts rules", other);
            Err(silent())
        }
    }
}

// ── alerts events dispatcher ──────────────────────────────────────────────────

fn cmd_alerts_events(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return events_list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => events_list(g, rest),
        other => {
            printerr!(g, "unknown subcommand {:?} for alerts events", other);
            Err(silent())
        }
    }
}

// ── alerts rules list ─────────────────────────────────────────────────────────

fn rules_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules list").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        rules: Vec<AlertRule>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/alerts/rules"),
        None,
    )?;

    if g.json {
        // JSON shape: { rules: [ AlertRule ] }
        return print_json(&mut std::io::stdout(), &json!({ "rules": resp.rules }));
    }
    if resp.rules.is_empty() {
        println!("(no alert rules)");
        return Ok(());
    }
    let mut t = Table::new(
        g,
        &[
            "ID",
            "NAME",
            "METRIC",
            "COMPARATOR",
            "THRESHOLD",
            "ENABLED",
            "SILENCED_UNTIL",
        ],
    );
    for r in &resp.rules {
        let silenced = silenced_display(r);
        let enabled = if r.enabled { "true" } else { "false" };
        t.row(&[
            &r.id,
            &r.name,
            &r.metric,
            &r.comparator,
            &format!("{}", r.threshold),
            enabled,
            &silenced,
        ]);
    }
    t.flush()
}

// ── alerts rules create ───────────────────────────────────────────────────────

fn rules_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("metric").long("metric"))
        .arg(Arg::new("threshold").long("threshold"))
        .arg(Arg::new("comparator").long("comparator"))
        .arg(Arg::new("enabled").long("enabled").default_value("true"));
    let m = parse_or_silent(cmd, args)?;

    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("alerts rules create requires --name=<name>"));
    }
    let metric = m.get_one::<String>("metric").cloned().unwrap_or_default();
    if metric.is_empty() {
        return Err(msg("alerts rules create requires --metric=<metric>"));
    }
    let comparator = m
        .get_one::<String>("comparator")
        .cloned()
        .unwrap_or_default();
    if comparator.is_empty() {
        return Err(msg(
            "alerts rules create requires --comparator=<gt|lt|gte|lte|eq>",
        ));
    }
    if !valid_comparator(&comparator) {
        return Err(msg(format!(
            "alerts rules create: invalid --comparator {:?}; must be one of gt, lt, gte, lte, eq",
            comparator
        )));
    }

    let threshold_str = m
        .get_one::<String>("threshold")
        .cloned()
        .unwrap_or_default();
    let threshold: f64 = if threshold_str.is_empty() {
        0.0
    } else {
        threshold_str.parse::<f64>().map_err(|_| {
            msg(format!(
                "--threshold must be a number, got {:?}",
                threshold_str
            ))
        })?
    };

    let enabled_str = m
        .get_one::<String>("enabled")
        .cloned()
        .unwrap_or_else(|| "true".into());
    let enabled: bool = match enabled_str.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => true,
        "false" | "0" | "no" => false,
        _ => {
            return Err(msg(format!(
                "--enabled must be true or false (got {:?})",
                enabled_str
            )))
        }
    };

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let body = json!({
        "name": name,
        "metric": metric,
        "threshold": threshold,
        "comparator": comparator.to_lowercase(),
        "enabled": enabled,
    });

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        rule: Option<AlertRule>,
    }
    let resp: Resp = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/alerts/rules"),
        Some(body),
    )?;

    if g.json {
        // JSON shape: { rule: AlertRule }
        return print_json(&mut std::io::stdout(), &json!({ "rule": resp.rule }));
    }
    if let Some(r) = &resp.rule {
        println!(
            "Created alert rule {} ({} {} {})",
            r.id, r.metric, r.comparator, r.threshold
        );
    }
    Ok(())
}

// ── alerts rules get ──────────────────────────────────────────────────────────

fn rules_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    // Use a plain Arg for "id" (single value) so flags after it are parsed correctly.
    let cmd = Command::new("alerts rules get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin alerts rules get <id> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        rule: Option<AlertRule>,
    }
    let resp: Resp = c
        .do_json(
            Method::GET,
            &format!("/v1/projects/{ref}/alerts/rules/{id}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "alert rule {:?} not found on project {:?}",
                        id, r#ref
                    ));
                }
            }
            e
        })?;

    if g.json {
        // JSON shape: { rule: AlertRule }
        return print_json(&mut std::io::stdout(), &json!({ "rule": resp.rule }));
    }
    let r = resp
        .rule
        .ok_or_else(|| msg(format!("alert rule {:?} not found", id)))?;
    let silenced = silenced_display(&r);
    let enabled = if r.enabled { "true" } else { "false" };
    println!("id:             {}", r.id);
    println!("name:           {}", r.name);
    println!("metric:         {}", r.metric);
    println!("threshold:      {}", r.threshold);
    println!("comparator:     {}", r.comparator);
    println!("enabled:        {enabled}");
    println!("silenced_until: {silenced}");
    Ok(())
}

// ── alerts rules patch ────────────────────────────────────────────────────────

fn rules_patch(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules patch")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("threshold").long("threshold"))
        .arg(Arg::new("enabled").long("enabled"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin alerts rules patch <id> [--name=<n>] [--threshold=<n>] [--enabled=true|false] [--project=<ref>]"))?;

    // Detect whether --threshold was explicitly provided by scanning args.
    let threshold_set = args
        .iter()
        .any(|a| a.starts_with("--threshold=") || a == "--threshold");

    let mut body = json!({});
    if let Some(name) = m.get_one::<String>("name").filter(|s| !s.is_empty()) {
        body["name"] = json!(name);
    }
    if threshold_set {
        let threshold_str = m
            .get_one::<String>("threshold")
            .cloned()
            .unwrap_or_default();
        let threshold: f64 = threshold_str.parse::<f64>().map_err(|_| {
            msg(format!(
                "--threshold must be a number, got {:?}",
                threshold_str
            ))
        })?;
        body["threshold"] = json!(threshold);
    }
    if let Some(enabled_str) = m.get_one::<String>("enabled") {
        let enabled: bool = match enabled_str.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => true,
            "false" | "0" | "no" => false,
            _ => {
                return Err(msg(format!(
                    "alerts rules patch --enabled must be true or false, got {:?}",
                    enabled_str
                )))
            }
        };
        body["enabled"] = json!(enabled);
    }

    if body.as_object().map(|o| o.is_empty()).unwrap_or(true) {
        return Err(msg(
            "alerts rules patch: at least one of --name, --threshold, --enabled must be provided",
        ));
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        rule: Option<AlertRule>,
    }
    let resp: Resp = c
        .do_json(
            Method::PATCH,
            &format!("/v1/projects/{ref}/alerts/rules/{id}"),
            Some(body),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "alert rule {:?} not found on project {:?}",
                        id, r#ref
                    ));
                }
            }
            e
        })?;

    if g.json {
        // JSON shape: { rule: AlertRule }
        return print_json(&mut std::io::stdout(), &json!({ "rule": resp.rule }));
    }
    println!("Updated alert rule {id}.");
    Ok(())
}

// ── alerts rules delete ───────────────────────────────────────────────────────

fn rules_delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin alerts rules delete <id> [--project=<ref>] [--yes]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete alert rule {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Delete alert rule {:?} on project {:?}? This cannot be undone. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if !line.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        deleted: bool,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        id: String,
    }
    let resp: Resp = c
        .do_json(
            Method::DELETE,
            &format!("/v1/projects/{ref}/alerts/rules/{id}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "alert rule {:?} not found on project {:?}",
                        id, r#ref
                    ));
                }
            }
            e
        })?;

    if g.json {
        // JSON shape: { deleted: bool, id: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted alert rule {id}.");
    Ok(())
}

// ── alerts rules silence ──────────────────────────────────────────────────────

fn rules_silence(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules silence")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("until").long("until"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m.get_one::<String>("id").cloned().ok_or_else(|| {
        msg("usage: basin alerts rules silence <id> --until=<rfc3339> [--project=<ref>]")
    })?;
    let until = m.get_one::<String>("until").cloned().unwrap_or_default();
    if until.is_empty() {
        return Err(msg("alerts rules silence requires --until=<rfc3339>"));
    }
    // Validate RFC3339 format.
    // Use a basic parse: try to parse as datetime. We accept any string
    // that chrono::DateTime can parse in RFC3339 form.
    if chrono::DateTime::parse_from_rfc3339(&until).is_err() {
        return Err(msg(format!(
            "alerts rules silence --until: invalid RFC3339 timestamp {:?}",
            until
        )));
    }

    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default, skip_serializing_if = "String::is_empty")]
        silenced_until: String,
    }
    let body = json!({ "until": until });
    let resp: Resp = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/alerts/rules/{id}/silence"),
            Some(body),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "alert rule {:?} not found on project {:?}",
                        id, r#ref
                    ));
                }
            }
            e
        })?;

    if g.json {
        // JSON shape: { silenced_until: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Alert rule {id} silenced until {}.", resp.silenced_until);
    Ok(())
}

// ── alerts rules unsilence ────────────────────────────────────────────────────

fn rules_unsilence(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts rules unsilence")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("id"));
    let m = parse_or_silent(cmd, args)?;
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin alerts rules unsilence <id> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        silenced: bool,
    }
    let resp: Resp = c
        .do_json(
            Method::POST,
            &format!("/v1/projects/{ref}/alerts/rules/{id}/unsilence"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "alert rule {:?} not found on project {:?}",
                        id, r#ref
                    ));
                }
            }
            e
        })?;

    if g.json {
        // JSON shape: { silenced: false }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let _ = resp;
    println!("Alert rule {id} unsilenced.");
    Ok(())
}

// ── alerts events list ────────────────────────────────────────────────────────

fn events_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("alerts events list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("limit").long("limit").default_value("50"))
        .arg(Arg::new("since").long("since"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let limit = m
        .get_one::<String>("limit")
        .cloned()
        .unwrap_or_else(|| "50".into());
    let since = m.get_one::<String>("since").cloned().unwrap_or_default();
    let c = require_client(g)?;

    let q = query_string(&[("limit", &limit), ("since", &since)]);
    #[derive(Deserialize, Serialize)]
    struct Resp {
        #[serde(default)]
        events: Vec<AlertEvent>,
    }
    let resp: Resp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/alerts/events{q}"),
        None,
    )?;

    if g.json {
        // JSON shape: { events: [ AlertEvent ] }
        return print_json(&mut std::io::stdout(), &json!({ "events": resp.events }));
    }
    if resp.events.is_empty() {
        println!("(no alert events)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "RULE_ID", "FIRED_AT", "RESOLVED_AT", "VALUE"]);
    for ev in &resp.events {
        let resolved = resolved_display(ev);
        t.row(&[
            &ev.id,
            &ev.rule_id,
            &ev.fired_at,
            &resolved,
            &format!("{}", ev.value),
        ]);
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

    fn sample_rule_json(id: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"high-cpu","metric":"cpu_pct","threshold":90,"comparator":"gt","enabled":true}}"#
        )
    }

    fn sample_event_json(id: &str) -> String {
        format!(r#"{{"id":"{id}","rule_id":"r1","fired_at":"2026-05-01T12:00:00Z","value":95.5}}"#)
    }

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn no_args_shows_help_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_alerts(&g, &[]).is_ok());
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_alerts(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_alerts(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    // ── validation: alerts rules create ────────────────────────────────────

    #[test]
    fn create_missing_name_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "create".into(),
                "--project=proj1".into(),
                "--metric=cpu".into(),
                "--threshold=90".into(),
                "--comparator=gt".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name"), "err={err}");
    }

    #[test]
    fn create_missing_metric_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "create".into(),
                "--project=proj1".into(),
                "--name=high-cpu".into(),
                "--threshold=90".into(),
                "--comparator=gt".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--metric"), "err={err}");
    }

    #[test]
    fn create_missing_comparator_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "create".into(),
                "--project=proj1".into(),
                "--name=high-cpu".into(),
                "--metric=cpu".into(),
                "--threshold=90".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--comparator"), "err={err}");
    }

    #[test]
    fn create_invalid_comparator_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "create".into(),
                "--project=proj1".into(),
                "--name=high-cpu".into(),
                "--metric=cpu".into(),
                "--threshold=90".into(),
                "--comparator=neq".into(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("neq") && err.to_string().contains("comparator"),
            "err={err}"
        );
    }

    // ── validation: alerts rules silence ────────────────────────────────────

    #[test]
    fn silence_missing_until_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "silence".into(),
                "--project=proj1".into(),
                "rule-1".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--until"), "err={err}");
    }

    #[test]
    fn silence_invalid_rfc3339_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "silence".into(),
                "--project=proj1".into(),
                "--until=not-a-timestamp".into(),
                "rule-1".into(),
            ],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("RFC3339") || err.to_string().contains("invalid"),
            "err={err}"
        );
    }

    // ── alerts rules list ───────────────────────────────────────────────────

    #[test]
    fn rules_list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules");
            Resp::ok(format!(
                r#"{{"rules":[{},{}]}}"#,
                sample_rule_json("r1"),
                r#"{"id":"r2","name":"low-disk","metric":"disk_free","threshold":10,"comparator":"lt","enabled":false}"#
            ))
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &["rules".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn rules_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"rules":[]}"#));
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &["rules".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn rules_list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"rules":[{}]}}"#, sample_rule_json("r1")))
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();

        #[derive(Deserialize)]
        struct R {
            #[serde(default)]
            rules: Vec<AlertRule>,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: R = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/proj1/alerts/rules",
                None,
            )
            .unwrap();
        print_json(&mut buf, &json!({ "rules": resp.rules })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let rules = v["rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["id"].as_str().unwrap(), "r1");
    }

    // ── alerts rules no project ─────────────────────────────────────────────

    #[test]
    fn rules_list_no_project_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(&g, &["rules".into(), "list".into()]).unwrap_err();
        assert!(err.to_string().contains("project") || !err.to_string().is_empty());
    }

    // ── alerts rules create ─────────────────────────────────────────────────

    #[test]
    fn rules_create_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["comparator"].as_str().unwrap_or(""), "gt");
            Resp::ok(format!(r#"{{"rule":{}}}"#, sample_rule_json("r-new")))
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "create".into(),
                "--project=proj1".into(),
                "--name=high-cpu".into(),
                "--metric=cpu_pct".into(),
                "--threshold=90".into(),
                "--comparator=gt".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rules_create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"rule":{}}}"#, sample_rule_json("r-json")))
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();

        #[derive(Deserialize)]
        struct R {
            #[serde(default)]
            rule: Option<AlertRule>,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: R = c.do_json(
            reqwest::Method::POST,
            "/v1/projects/proj1/alerts/rules",
            Some(json!({"name":"high-cpu","metric":"cpu_pct","threshold":90,"comparator":"gt","enabled":true})),
        ).unwrap();
        print_json(&mut buf, &json!({ "rule": resp.rule })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["rule"]["id"].as_str().unwrap(), "r-json");
    }

    // ── alerts rules get ────────────────────────────────────────────────────

    #[test]
    fn rules_get_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules/r1");
            Resp::ok(format!(r#"{{"rule":{}}}"#, sample_rule_json("r1")))
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "get".into(),
                "--project=proj1".into(),
                "r1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rules_get_404_gives_not_found_message() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"rule not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "get".into(),
                "--project=proj1".into(),
                "missing".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "err={err}");
    }

    // ── alerts rules patch ──────────────────────────────────────────────────

    #[test]
    fn rules_patch_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules/r1");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["name"].as_str().unwrap_or(""), "renamed");
            assert!(!body["enabled"].as_bool().unwrap_or(true));
            Resp::ok(format!(r#"{{"rule":{}}}"#, sample_rule_json("r1")))
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "patch".into(),
                "--project=proj1".into(),
                "--name=renamed".into(),
                "--enabled=false".into(),
                "r1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rules_patch_no_fields_errors() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "patch".into(),
                "--project=proj1".into(),
                "r1".into(),
            ],
        )
        .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn rules_patch_403_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"code":"forbidden","message":"insufficient permissions"}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_alerts(
            &g,
            &[
                "rules".into(),
                "patch".into(),
                "--project=proj1".into(),
                "--name=rename".into(),
                "r1".into(),
            ],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── alerts rules delete ─────────────────────────────────────────────────

    #[test]
    fn rules_delete_with_yes_flag() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules/r1");
            Resp::ok(r#"{"deleted":true,"id":"r1"}"#)
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "r1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rules_delete_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":true,"id":"r1"}"#));
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();

        #[derive(Deserialize, Serialize)]
        struct D {
            deleted: bool,
            #[serde(default)]
            id: String,
        }
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: D = c
            .do_json(
                reqwest::Method::DELETE,
                "/v1/projects/proj1/alerts/rules/r1",
                None,
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["deleted"].as_bool().unwrap());
        assert_eq!(v["id"].as_str().unwrap(), "r1");
    }

    // ── alerts rules silence ────────────────────────────────────────────────

    #[test]
    fn rules_silence_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules/r1/silence");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["until"].as_str().unwrap_or(""), "2026-12-31T00:00:00Z");
            Resp::ok(r#"{"silenced_until":"2026-12-31T00:00:00Z"}"#)
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "silence".into(),
                "--project=proj1".into(),
                "--until=2026-12-31T00:00:00Z".into(),
                "r1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rules_silence_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"silenced_until":"2026-12-31T00:00:00Z"}"#)
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        #[derive(Deserialize, Serialize)]
        struct S {
            #[serde(default)]
            silenced_until: String,
        }
        let resp: S = c
            .do_json(
                reqwest::Method::POST,
                "/v1/projects/proj1/alerts/rules/r1/silence",
                Some(json!({"until":"2026-12-31T00:00:00Z"})),
            )
            .unwrap();
        print_json(&mut buf, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(
            v["silenced_until"].as_str().unwrap(),
            "2026-12-31T00:00:00Z"
        );
    }

    // ── alerts rules unsilence ──────────────────────────────────────────────

    #[test]
    fn rules_unsilence_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/alerts/rules/r1/unsilence");
            Resp::ok(r#"{"silenced":false}"#)
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "rules".into(),
                "unsilence".into(),
                "--project=proj1".into(),
                "r1".into(),
            ],
        )
        .unwrap();
    }

    // ── alerts events list ──────────────────────────────────────────────────

    #[test]
    fn events_list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(
                req.path.contains("/v1/projects/proj1/alerts/events"),
                "path={}",
                req.path
            );
            Resp::ok(format!(
                r#"{{"events":[{},{}]}}"#,
                sample_event_json("e1"),
                sample_event_json("e2")
            ))
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &["events".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn events_list_query_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("limit=10"), "path={}", req.path);
            assert!(req.path.contains("since="), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &[
                "events".into(),
                "list".into(),
                "--project=proj1".into(),
                "--limit=10".into(),
                "--since=2026-01-01T00:00:00Z".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn events_list_empty() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        cmd_alerts(
            &g,
            &["events".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn events_list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"events":[{}]}}"#, sample_event_json("e1")))
        });
        let _g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        #[derive(Deserialize)]
        struct R {
            #[serde(default)]
            events: Vec<AlertEvent>,
        }
        let resp: R = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/proj1/alerts/events",
                None,
            )
            .unwrap();
        print_json(&mut buf, &json!({ "events": resp.events })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let events = v["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"].as_str().unwrap(), "e1");
    }

    #[test]
    fn events_list_422_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"code":"validation_error","message":"invalid since value"}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_alerts(
            &g,
            &["events".into(), "list".into(), "--project=proj1".into()],
        )
        .unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }
}
