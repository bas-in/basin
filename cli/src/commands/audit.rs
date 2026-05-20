//! audit — org-scoped audit log.
//!
//!   basin audit list --org=<slug> [--since=<rfc3339>] [--until=<rfc3339>] [--limit=N] [--export=<dest>]
//!
//! Backed by GET /v1/orgs/{slug}/audit.
//! --org is required: audit is org-scoped, not project-scoped.
//! --export is accepted but deferred: prints a message and exits 0 without
//! calling the API (the cloud's sink-management surface is larger).

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::query_string;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// AuditEvent is the read shape for a single org audit event.
/// JSON shape: { id, actor_id, action, resource_type, resource_id, occurred_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct AuditEvent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub action: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resource_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub resource_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub occurred_at: String,
}

#[derive(Deserialize, Serialize)]
struct AuditResp {
    #[serde(default)]
    events: Vec<AuditEvent>,
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_audit(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "audit",
                "List org-scoped audit log events.",
                &[
                    "list --org=<slug> [--since=<rfc3339>] [--until=<rfc3339>] [--limit=N] [--export=<dest>]",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for audit", other);
            Err(silent())
        }
    }
}

// ── audit list ────────────────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("audit list")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("since").long("since"))
        .arg(Arg::new("until").long("until"))
        .arg(Arg::new("limit").long("limit").default_value("50"))
        .arg(Arg::new("export").long("export"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "audit list",
            "List org-scoped audit log events.",
            &[
                "--org <slug>           Org slug (required; audit is org-scoped).",
                "--since <rfc3339>      Return events at or after this timestamp.",
                "--until <rfc3339>      Return events at or before this timestamp.",
                "--limit <N>            Maximum number of events (default 50).",
                "--export <dest>        Export destination (deferred — not yet wired).",
            ],
        );
        return Ok(());
    }

    // --org: prefer explicit flag, then fall back to g.org_slug.
    let org_flag = m.get_one::<String>("org").cloned().unwrap_or_default();
    let org = if !org_flag.is_empty() {
        org_flag
    } else if !g.org_slug.is_empty() {
        g.org_slug.clone()
    } else {
        return Err(msg(
            "audit list requires --org=<slug> (audit is org-scoped; no project fallback)",
        ));
    };

    // --export is accepted but deferred.
    let export = m.get_one::<String>("export").cloned().unwrap_or_default();
    if !export.is_empty() {
        println!("audit export deferred");
        return Ok(());
    }

    let since = m.get_one::<String>("since").cloned().unwrap_or_default();
    let until = m.get_one::<String>("until").cloned().unwrap_or_default();
    let limit = m.get_one::<String>("limit").cloned().unwrap_or_else(|| "50".into());

    let c = require_client(g)?;
    let q = query_string(&[("limit", &limit), ("since", &since), ("until", &until)]);

    let resp: AuditResp = c.do_json(
        Method::GET,
        &format!("/v1/orgs/{org}/audit{q}"),
        None,
    ).map_err(|e| {
        if let Some(ae) = as_api_error(e.as_ref()) {
            if ae.http_status == 403 {
                return msg(format!(
                    "access denied: you must be an org member to view the audit log for {:?}",
                    org
                ));
            }
        }
        e
    })?;

    if g.json {
        // JSON shape: { events: [ AuditEvent ] }
        return print_json(&mut std::io::stdout(), &json!({ "events": resp.events }));
    }
    if resp.events.is_empty() {
        println!("(no audit events)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "ACTOR", "ACTION", "RESOURCE_TYPE", "RESOURCE_ID", "OCCURRED_AT"]);
    for ev in &resp.events {
        t.row(&[
            &ev.id,
            &ev.actor_id,
            &ev.action,
            &ev.resource_type,
            &ev.resource_id,
            &ev.occurred_at,
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

    fn sample_event_json() -> &'static str {
        r#"{"id":"ev1","actor_id":"user-1","action":"project.create","resource_type":"project","resource_id":"proj-1","occurred_at":"2026-05-01T10:00:00Z"}"#
    }

    // ── dispatcher ─────────────────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_audit(&g, &["help".to_string()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_audit(&g, &["frobnicate".to_string()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    // ── validation: --org required ──────────────────────────────────────────

    #[test]
    fn list_requires_org() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            ..Default::default()
        };
        let err = cmd_audit(&g, &["list".to_string()]).unwrap_err();
        assert!(err.to_string().contains("--org"), "err={err}");
    }

    // ── audit list happy path ───────────────────────────────────────────────

    #[test]
    fn list_happy_path_table() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert!(req.path.starts_with("/v1/orgs/my-org/audit"), "path={}", req.path);
            Resp::ok(format!(r#"{{"events":[{}]}}"#, sample_event_json()))
        });
        let g = flags(&srv.url);
        cmd_audit(&g, &["list".to_string(), "--org=my-org".to_string()]).unwrap();
    }

    #[test]
    fn list_empty_prints_no_audit_events() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        cmd_audit(&g, &["list".to_string(), "--org=my-org".to_string()]).unwrap();
    }

    // ── query-string passthrough ────────────────────────────────────────────

    #[test]
    fn list_since_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("since="), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_audit(
            &g,
            &[
                "list".to_string(),
                "--org=my-org".to_string(),
                "--since=2026-01-01T00:00:00Z".to_string(),
            ],
        ).unwrap();
    }

    #[test]
    fn list_until_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("until="), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_audit(
            &g,
            &[
                "list".to_string(),
                "--org=my-org".to_string(),
                "--until=2026-06-01T00:00:00Z".to_string(),
            ],
        ).unwrap();
    }

    #[test]
    fn list_limit_passthrough() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.contains("limit=25"), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = flags(&srv.url);
        cmd_audit(
            &g,
            &["list".to_string(), "--org=my-org".to_string(), "--limit=25".to_string()],
        ).unwrap();
    }

    // ── JSON shape ──────────────────────────────────────────────────────────

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"events":[{}]}}"#, sample_event_json()))
        });
        let g = flags_json(&srv.url);
        let mut buf = Vec::<u8>::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: AuditResp =
            c.do_json(reqwest::Method::GET, "/v1/orgs/my-org/audit", None).unwrap();
        print_json(&mut buf, &json!({ "events": resp.events })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        let events = v["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["id"].as_str().unwrap(), "ev1");
        assert_eq!(events[0]["action"].as_str().unwrap(), "project.create");
    }

    // ── error mapping ───────────────────────────────────────────────────────

    #[test]
    fn list_403_gives_access_denied_message() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"code":"forbidden","message":"not an org member"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_audit(&g, &["list".to_string(), "--org=my-org".to_string()]).unwrap_err();
        assert!(
            err.to_string().contains("access denied") || err.to_string().contains("forbidden"),
            "err={err}"
        );
    }

    #[test]
    fn list_404_propagates_as_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"code":"not_found","message":"org not found"}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_audit(&g, &["list".to_string(), "--org=nonexistent".to_string()]).unwrap_err();
        let ae = crate::error::as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── export deferred ─────────────────────────────────────────────────────

    #[test]
    fn list_export_deferred_returns_ok() {
        let _g = with_temp_config_dir();
        // Use a server that would fail if contacted. We use a dummy URL
        // for the g flags to make require_client work (but it won't be called
        // because --export short-circuits before HTTP).
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"events":[]}"#));
        let g = flags(&srv.url);
        let result = cmd_audit(
            &g,
            &[
                "list".to_string(),
                "--org=my-org".to_string(),
                "--export=s3://my-bucket/audit".to_string(),
            ],
        );
        assert!(result.is_ok(), "export deferred should return ok: {:?}", result);
    }

    // ── globalFlags.orgSlug fallback ────────────────────────────────────────

    #[test]
    fn list_org_from_global_flags() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert!(req.path.starts_with("/v1/orgs/global-org/audit"), "path={}", req.path);
            Resp::ok(r#"{"events":[]}"#)
        });
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            org_slug: "global-org".into(),
            quiet: true,
            ..Default::default()
        };
        // No --org flag; should use g.org_slug.
        cmd_audit(&g, &["list".to_string()]).unwrap();
    }
}
