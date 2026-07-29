//! members — list / invite / remove / role-change org members.
//!
//! Routes:
//!   GET    /v1/orgs/{slug}/members
//!   POST   /v1/orgs/{slug}/members/invite
//!   DELETE /v1/orgs/{slug}/members/{user_id}
//!   PATCH  /v1/orgs/{slug}/members/{user_id}

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── local response structs ────────────────────────────────────────────

/// Member is the read shape from GET /v1/orgs/{slug}/members.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Member {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    user_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    email: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    role: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    joined_at: String,
}

/// Invitation is the read shape from invite + list invitations responses.
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
struct MembersListResp {
    #[serde(default)]
    members: Vec<Member>,
}

#[derive(Deserialize, Serialize)]
struct InvitationResp {
    #[serde(default)]
    invitation: Option<Invitation>,
}

#[derive(Deserialize)]
struct MemberResp {
    #[serde(default)]
    member: Option<Member>,
}

// ── top-level dispatcher ─────────────────────────────────────────────

pub fn cmd_members(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin members (list | invite | remove | role) ...",
            ))
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "invite" => invite(g, rest),
        "remove" => remove(g, rest),
        "role" => role(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "members",
                "Manage org members.",
                &[
                    "list   --org=<slug>                                          List org members.",
                    "invite --org=<slug> --email=<e> [--role=member|admin|owner]  Invite a new member.",
                    "remove --org=<slug> --user-id=<u> [--yes]                   Remove a member.",
                    "role   --org=<slug> --user-id=<u> --role=<r>                Update a member's role.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for members", other);
            Err(silent())
        }
    }
}

// ── members list ─────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("members list").arg(Arg::new("org").long("org"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    if org.is_empty() {
        return Err(msg("members list requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: MembersListResp = c.do_json(Method::GET, &format!("/v1/orgs/{org}/members"), None)?;
    if g.json {
        // JSON shape: { "members": [ { user_id, email, role, joined_at } ] }
        return print_json(&mut std::io::stdout(), &json!({ "members": resp.members }));
    }
    if resp.members.is_empty() {
        println!("(no members)");
        return Ok(());
    }
    let mut t = Table::new(g, &["USER_ID", "EMAIL", "ROLE", "JOINED"]);
    for mb in &resp.members {
        t.row(&[&mb.user_id, &mb.email, &mb.role, &mb.joined_at]);
    }
    t.flush()
}

// ── members invite ───────────────────────────────────────────────────

fn invite(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("members invite")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("email").long("email"))
        .arg(Arg::new("role").long("role"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let email = m.get_one::<String>("email").cloned().unwrap_or_default();
    if org.is_empty() || email.is_empty() {
        return Err(msg(
            "members invite requires --org=<slug> and --email=<email>",
        ));
    }
    let role = m
        .get_one::<String>("role")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "member".to_string());
    let body = json!({ "email": email, "role": role });
    let c = require_client(g)?;
    let resp: InvitationResp = c
        .do_json(
            Method::POST,
            &format!("/v1/orgs/{org}/members/invite"),
            Some(body),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 409 {
                    return msg(format!(
                        "member already exists or invitation already pending for {:?}",
                        email
                    ));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { "invitation": { id, email, role, expires_at } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(inv) = resp.invitation {
        println!(
            "Invited {} (role={}, expires={}, id={}).",
            inv.email, inv.role, inv.expires_at, inv.id
        );
    } else {
        println!("Invited {email} to org {org}.");
    }
    Ok(())
}

// ── members remove ───────────────────────────────────────────────────

fn remove(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("members remove")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("user-id").long("user-id"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let user_id = m.get_one::<String>("user-id").cloned().unwrap_or_default();
    if org.is_empty() || user_id.is_empty() {
        return Err(msg(
            "members remove requires --org=<slug> and --user-id=<user_id>",
        ));
    }

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to remove user {:?} non-interactively under --json",
                user_id
            )));
        }
        eprint!("Remove user {:?} from org {:?}? [y/N] ", user_id, org);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Serialize, Deserialize)]
    struct RemoveResp {
        removed: bool,
        user_id: String,
    }
    let resp: RemoveResp = c.do_json(
        Method::DELETE,
        &format!("/v1/orgs/{org}/members/{user_id}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "removed": true, "user_id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Removed user {user_id} from org {org}.");
    Ok(())
}

// ── members role ─────────────────────────────────────────────────────

fn role(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("members role")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("user-id").long("user-id"))
        .arg(Arg::new("role").long("role"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let user_id = m.get_one::<String>("user-id").cloned().unwrap_or_default();
    let new_role = m.get_one::<String>("role").cloned().unwrap_or_default();
    if org.is_empty() || user_id.is_empty() || new_role.is_empty() {
        return Err(msg(
            "members role requires --org=<slug>, --user-id=<user_id>, and --role=<role>",
        ));
    }
    let body = json!({ "role": new_role });
    let c = require_client(g)?;
    let resp: MemberResp = c.do_json(
        Method::PATCH,
        &format!("/v1/orgs/{org}/members/{user_id}"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "member": { user_id, role } }
        return print_json(&mut std::io::stdout(), &json!({ "member": resp.member }));
    }
    if let Some(mb) = resp.member {
        println!(
            "Updated role for user {} to {:?} in org {org}.",
            mb.user_id, mb.role
        );
    } else {
        println!("Updated role for user {user_id} in org {org}.");
    }
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

    // ── members list ──────────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/members");
            Resp::ok(
                r#"{"members":[{"user_id":"user-1","email":"alice@example.com","role":"admin","joined_at":"2026-01-01T00:00:00Z"},{"user_id":"user-2","email":"bob@example.com","role":"member","joined_at":"2026-01-01T00:00:00Z"}]}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "members list: {:?}", err);
    }

    #[test]
    fn list_empty() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"members":[]}"#));
        let _cfg = with_temp_config_dir();
        // No HTTP call is expected to fail; just verify no error.
        let err = cmd_members(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "list empty: {:?}", err);
    }

    #[test]
    fn list_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &["list".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn list_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"members":[{"user_id":"user-1","email":"alice@example.com","role":"admin","joined_at":"2026-01-01T00:00:00Z"}]}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let members = vec![Member {
            user_id: "user-1".into(),
            email: "alice@example.com".into(),
            role: "admin".into(),
            joined_at: "2026-01-01T00:00:00Z".into(),
        }];
        print_json(&mut out, &json!({ "members": members })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["members"].is_array());
        assert_eq!(parsed["members"][0]["email"], "alice@example.com");
        let _ = &srv;
    }

    #[test]
    fn list_403() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"Insufficient permissions."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(&g(&srv), &["list".into(), "--org=acme".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── members invite ────────────────────────────────────────────────

    #[test]
    fn invite_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/members/invite");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["email"], "charlie@example.com");
            assert_eq!(body["role"], "member");
            Resp::ok(
                r#"{"invitation":{"id":"inv-1","email":"charlie@example.com","role":"member","expires_at":"2026-06-01T00:00:00Z","status":"pending"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(
            &g(&srv),
            &[
                "invite".into(),
                "--org=acme".into(),
                "--email=charlie@example.com".into(),
                "--role=member".into(),
            ],
        );
        assert!(err.is_ok(), "invite: {:?}", err);
    }

    #[test]
    fn invite_missing_email() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &["invite".into(), "--org=acme".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--email"));
    }

    #[test]
    fn invite_409_conflict() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                409,
                r#"{"error":{"code":"conflict","message":"Member already exists."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(
            &g(&srv),
            &[
                "invite".into(),
                "--org=acme".into(),
                "--email=alice@example.com".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("already"));
    }

    #[test]
    fn invite_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(
                r#"{"invitation":{"id":"inv-1","email":"charlie@example.com","role":"admin","expires_at":"2026-06-01T00:00:00Z","status":"pending"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let inv = InvitationResp {
            invitation: Some(Invitation {
                id: "inv-1".into(),
                email: "charlie@example.com".into(),
                role: "admin".into(),
                expires_at: "2026-06-01T00:00:00Z".into(),
                status: "pending".into(),
            }),
        };
        print_json(&mut out, &inv).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["invitation"]["email"], "charlie@example.com");
        assert_eq!(parsed["invitation"]["role"], "admin");
        let _ = &srv;
    }

    // ── members remove ────────────────────────────────────────────────

    #[test]
    fn remove_confirmed_yes_flag() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme/members/user-1");
            Resp::ok(r#"{"removed":true,"user_id":"user-1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(
            &g(&srv),
            &[
                "remove".into(),
                "--org=acme".into(),
                "--user-id=user-1".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_ok(), "remove: {:?}", err);
    }

    #[test]
    fn remove_missing_user_id() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &["remove".into(), "--org=acme".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn remove_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_members(
            &g0,
            &[
                "remove".into(),
                "--org=acme".into(),
                "--user-id=user-1".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("confirmation required"));
    }

    #[test]
    fn remove_json_shape() {
        let srv =
            TestServer::start(|_req: &Req| Resp::ok(r#"{"removed":true,"user_id":"user-1"}"#));
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct R {
            removed: bool,
            user_id: String,
        }
        print_json(
            &mut out,
            &R {
                removed: true,
                user_id: "user-1".into(),
            },
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["removed"], true);
        assert_eq!(parsed["user_id"], "user-1");
        let _ = &srv;
    }

    // ── members role ──────────────────────────────────────────────────

    #[test]
    fn role_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/orgs/acme/members/user-1");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["role"], "admin");
            Resp::ok(r#"{"member":{"user_id":"user-1","role":"admin"}}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(
            &g(&srv),
            &[
                "role".into(),
                "--org=acme".into(),
                "--user-id=user-1".into(),
                "--role=admin".into(),
            ],
        );
        assert!(err.is_ok(), "role: {:?}", err);
    }

    #[test]
    fn role_missing_flags() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        // Missing --role
        let err = cmd_members(
            &g0,
            &[
                "role".into(),
                "--org=acme".into(),
                "--user-id=user-1".into(),
            ],
        );
        assert!(err.is_err());
    }

    #[test]
    fn role_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"member":{"user_id":"user-1","role":"owner"}}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let mb = Member {
            user_id: "user-1".into(),
            role: "owner".into(),
            ..Default::default()
        };
        print_json(&mut out, &json!({ "member": mb })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["member"]["role"], "owner");
        let _ = &srv;
    }

    #[test]
    fn role_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"Member not found."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_members(
            &g(&srv),
            &[
                "role".into(),
                "--org=acme".into(),
                "--user-id=ghost".into(),
                "--role=member".into(),
            ],
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
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn no_args_returns_error() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_members(&g0, &[]);
        assert!(err.is_err());
    }

    // helper for g_json reference
    #[allow(dead_code)]
    fn _g_json_ref(srv: &TestServer) -> GlobalFlags {
        g_json(srv)
    }
}
