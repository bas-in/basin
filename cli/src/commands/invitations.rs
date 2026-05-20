//! invitations — list / resend / revoke pending org invitations.
//!
//! Routes:
//!   GET    /v1/orgs/{slug}/invitations
//!   POST   /v1/orgs/{slug}/invitations/{id}/resend
//!   DELETE /v1/orgs/{slug}/invitations/{id}

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── local response structs ────────────────────────────────────────────

/// Invitation is the read shape from invite + list invitations responses.
/// (Mirrors the Member-file Invitation; defined locally to keep modules
/// independent — they don't import each other.)
#[derive(Debug, Default, Serialize, Deserialize)]
struct Invitation {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    role: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    expires_at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    status: String,
}

#[derive(Deserialize)]
struct InvitationsListResp {
    #[serde(default)]
    invitations: Vec<Invitation>,
}

// ── top-level dispatcher ─────────────────────────────────────────────

pub fn cmd_invitations(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin invitations (list | resend | revoke) ...",
            ))
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "resend" => resend(g, rest),
        "revoke" => revoke(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "invitations",
                "Manage org invitations.",
                &[
                    "list          --org=<slug>              List pending (and expired) invitations.",
                    "resend <id>   --org=<slug>              Resend an invitation email.",
                    "revoke <id>   --org=<slug> [--yes]      Revoke an invitation.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for invitations", other);
            Err(silent())
        }
    }
}

// ── invitations list ─────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("invitations list").arg(Arg::new("org").long("org"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    if org.is_empty() {
        return Err(msg("invitations list requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: InvitationsListResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{org}/invitations"), None)?;
    if g.json {
        // JSON shape: { "invitations": [ { id, email, role, expires_at, status } ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "invitations": resp.invitations }),
        );
    }
    if resp.invitations.is_empty() {
        println!("(no invitations)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "EMAIL", "ROLE", "EXPIRES", "STATUS"]);
    for inv in &resp.invitations {
        t.row(&[&inv.id, &inv.email, &inv.role, &inv.expires_at, &inv.status]);
    }
    t.flush()
}

// ── invitations resend ───────────────────────────────────────────────

fn resend(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("invitations resend")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .unwrap_or_default();
    if org.is_empty() || id.is_empty() {
        return Err(msg("usage: basin invitations resend <id> --org=<slug>"));
    }
    let c = require_client(g)?;

    #[derive(Serialize, Deserialize)]
    struct ResendResp {
        resent: bool,
        id: String,
    }
    let resp: ResendResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/invitations/{id}/resend"),
        None,
    )?;
    if g.json {
        // JSON shape: { "resent": true, "id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Resent invitation {id}.");
    Ok(())
}

// ── invitations revoke ───────────────────────────────────────────────

fn revoke(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("invitations revoke")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .unwrap_or_default();
    if org.is_empty() || id.is_empty() {
        return Err(msg(
            "usage: basin invitations revoke <id> --org=<slug> [--yes]",
        ));
    }

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to revoke invitation {:?} non-interactively under --json",
                id
            )));
        }
        eprint!("Revoke invitation {:?}? [y/N] ", id);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Serialize, Deserialize)]
    struct RevokeResp {
        revoked: bool,
        id: String,
    }
    let resp: RevokeResp =
        c.do_json(Method::DELETE, &format!("/v1/orgs/{org}/invitations/{id}"), None)?;
    if g.json {
        // JSON shape: { "revoked": true, "id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Revoked invitation {id}.");
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::as_api_error;
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

    fn sample_invitation(id: &str, email: &str, role: &str, status: &str) -> String {
        format!(
            r#"{{"id":"{id}","email":"{email}","role":"{role}","expires_at":"2026-06-01T00:00:00Z","status":"{status}"}}"#
        )
    }

    // ── invitations list ──────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/invitations");
            Resp::ok(format!(
                r#"{{"invitations":[{},{}]}}"#,
                sample_invitation("inv-1", "alice@example.com", "admin", "pending"),
                sample_invitation("inv-2", "bob@example.com", "member", "pending"),
            ))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "list: {:?}", err);
    }

    #[test]
    fn list_empty() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"invitations":[]}"#));
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "list empty: {:?}", err);
    }

    #[test]
    fn list_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["list".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn list_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"invitations":[{}]}}"#,
                sample_invitation("inv-1", "alice@example.com", "admin", "pending"),
            ))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let invs = vec![Invitation {
            id: "inv-1".into(),
            email: "alice@example.com".into(),
            role: "admin".into(),
            expires_at: "2026-06-01T00:00:00Z".into(),
            status: "pending".into(),
        }];
        print_json(&mut out, &json!({ "invitations": invs })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["invitations"].is_array());
        assert_eq!(parsed["invitations"][0]["email"], "alice@example.com");
        assert_eq!(parsed["invitations"][0]["status"], "pending");
        let _ = &srv;
    }

    #[test]
    fn list_pending_and_expired() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"invitations":[{},{}]}}"#,
                sample_invitation("inv-p", "pending@example.com", "member", "pending"),
                sample_invitation("inv-e", "expired@example.com", "member", "expired"),
            ))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "list pending+expired: {:?}", err);
    }

    #[test]
    fn list_403() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"Insufficient permissions."}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── invitations resend ────────────────────────────────────────────

    #[test]
    fn resend_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/invitations/inv-1/resend");
            Resp::ok(r#"{"resent":true,"id":"inv-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(
            &g(&srv),
            &["resend".into(), "--org=acme".into(), "inv-1".into()],
        );
        assert!(err.is_ok(), "resend: {:?}", err);
    }

    #[test]
    fn resend_missing_id() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["resend".into(), "--org=acme".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn resend_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["resend".into(), "inv-1".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn resend_json_shape() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"resent":true,"id":"inv-1"}"#));
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R { resent: bool, id: String }
        print_json(&mut out, &R { resent: true, id: "inv-1".into() }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["resent"], true);
        assert_eq!(parsed["id"], "inv-1");
        let _ = &srv;
    }

    #[test]
    fn resend_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"Invitation not found."}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(
            &g(&srv),
            &["resend".into(), "--org=acme".into(), "ghost".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── invitations revoke ────────────────────────────────────────────

    #[test]
    fn revoke_confirmed_yes_flag() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme/invitations/inv-1");
            Resp::ok(r#"{"revoked":true,"id":"inv-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(
            &g(&srv),
            &["revoke".into(), "--org=acme".into(), "--yes".into(), "inv-1".into()],
        );
        assert!(err.is_ok(), "revoke: {:?}", err);
    }

    #[test]
    fn revoke_missing_id() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["revoke".into(), "--org=acme".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn revoke_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { token: "tok".into(), quiet: true, json: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["revoke".into(), "--org=acme".into(), "inv-1".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("confirmation required"));
    }

    #[test]
    fn revoke_json_shape() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"revoked":true,"id":"inv-1"}"#));
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R { revoked: bool, id: String }
        print_json(&mut out, &R { revoked: true, id: "inv-1".into() }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["revoked"], true);
        assert_eq!(parsed["id"], "inv-1");
        let _ = &srv;
    }

    #[test]
    fn revoke_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(404, r#"{"error":{"code":"not_found","message":"Invitation not found."}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_invitations(
            &g(&srv),
            &["revoke".into(), "--org=acme".into(), "--yes".into(), "ghost".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── misc ──────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn no_args_returns_error() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_invitations(&g0, &[]);
        assert!(err.is_err());
    }

    // helper for g_json reference
    #[allow(dead_code)]
    fn _g_json_ref(srv: &TestServer) -> GlobalFlags {
        g_json(srv)
    }
}
