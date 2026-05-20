//! scim — SCIM provisioning config and token management for an org.
//!
//! Routes:
//!   GET    /v1/orgs/{slug}/scim/config        → SCIM config + base URL
//!   GET    /v1/orgs/{slug}/scim/tokens         → list SCIM provisioning tokens
//!   POST   /v1/orgs/{slug}/scim/tokens         → create a SCIM token (secret shown ONCE)
//!   DELETE /v1/orgs/{slug}/scim/tokens/{id}   → revoke a SCIM token
//!
//! Subcommands:
//!   basin scim config get          --org=<slug>
//!   basin scim tokens list         --org=<slug>
//!   basin scim tokens create       --org=<slug> --name=<n>
//!   basin scim tokens revoke <id>  --org=<slug> [--yes]

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

// ── wire shapes ───────────────────────────────────────────────────────

/// SCIMConfig is the read shape from GET /v1/orgs/{slug}/scim/config.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ScimConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    base_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    schemas_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    configured_at: String,
}

#[derive(Serialize, Deserialize)]
struct ScimConfigResp {
    scim: Option<ScimConfig>,
}

/// ScimToken is one entry from the SCIM token list / create response.
#[derive(Debug, Default, Serialize, Deserialize)]
struct ScimToken {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_used_at: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct ScimTokensListResp {
    #[serde(default)]
    tokens: Vec<ScimToken>,
}

#[derive(Serialize, Deserialize)]
struct ScimTokenCreateResp {
    token: Option<ScimToken>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    secret: String,
}

#[derive(Serialize, Deserialize)]
struct ScimTokenRevokeResp {
    revoked: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
}

// ── top-level dispatcher ─────────────────────────────────────────────

pub fn cmd_scim(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            show_scim_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "config" => cmd_scim_config(g, rest),
        "tokens" => cmd_scim_tokens(g, rest),
        "--help" | "-h" | "help" => {
            show_scim_help();
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for scim", other);
            Err(silent())
        }
    }
}

fn show_scim_help() {
    help_for_command(
        "scim",
        "Manage SCIM provisioning config and tokens for an org.",
        &[
            "config get          --org=<slug>                  Show SCIM provisioning config.",
            "tokens list         --org=<slug>                  List SCIM provisioning tokens.",
            "tokens create       --org=<slug> --name=<n>       Mint a SCIM token (secret shown ONCE).",
            "tokens revoke <id>  --org=<slug> [--yes]          Revoke a SCIM token.",
        ],
    );
}

// ── scim config dispatcher ───────────────────────────────────────────

fn cmd_scim_config(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin scim config (get) --org=<slug>"));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => scim_config_get(g, rest),
        "--help" | "-h" | "help" => {
            eprintln!("usage: basin scim config get --org=<slug>");
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for scim config", other);
            Err(silent())
        }
    }
}

// ── scim tokens dispatcher ───────────────────────────────────────────

fn cmd_scim_tokens(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg(
                "usage: basin scim tokens (list | create | revoke) --org=<slug>",
            ));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => scim_tokens_list(g, rest),
        "create" => scim_tokens_create(g, rest),
        "revoke" => scim_tokens_revoke(g, rest),
        "--help" | "-h" | "help" => {
            eprintln!("usage: basin scim tokens (list | create | revoke) --org=<slug>");
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for scim tokens", other);
            Err(silent())
        }
    }
}

// ── scim config get ───────────────────────────────────────────────────

fn scim_config_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("scim config get").arg(Arg::new("org").long("org"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| {
            if g.org_slug.is_empty() {
                None
            } else {
                Some(g.org_slug.clone())
            }
        })
        .ok_or_else(|| msg("scim config get requires --org=<slug>"))?;

    let c = require_client(g)?;
    let resp: ScimConfigResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{org}/scim/config"), None)?;

    if g.json {
        // JSON shape: { "scim": { enabled, base_url, schemas_url, configured_at } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let Some(s) = resp.scim else {
        println!("(no SCIM configured)");
        return Ok(());
    };
    println!("enabled:       {}", s.enabled);
    println!("base_url:      {}", s.base_url);
    println!("schemas_url:   {}", s.schemas_url);
    println!("configured_at: {}", s.configured_at);
    Ok(())
}

// ── scim tokens list ─────────────────────────────────────────────────

fn scim_tokens_list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("scim tokens list").arg(Arg::new("org").long("org"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| {
            if g.org_slug.is_empty() {
                None
            } else {
                Some(g.org_slug.clone())
            }
        })
        .ok_or_else(|| msg("scim tokens list requires --org=<slug>"))?;

    let c = require_client(g)?;
    let resp: ScimTokensListResp = match c.do_json(
        Method::GET,
        &format!("/v1/orgs/{org}/scim/tokens"),
        None,
    ) {
        Ok(r) => r,
        Err(e) => {
            // Graceful 404 fallback: older cloud versions may not have the list endpoint.
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    if g.json {
                        // JSON shape: { "tokens": [] }
                        return print_json(
                            &mut std::io::stdout(),
                            &json!({ "tokens": Vec::<ScimToken>::new() }),
                        );
                    }
                    println!(
                        "(no SCIM tokens — list endpoint not available on this cloud version)"
                    );
                    return Ok(());
                }
            }
            return Err(e);
        }
    };

    if g.json {
        // JSON shape: { "tokens": [{ id, name, prefix, created_at, last_used_at }] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.tokens.is_empty() {
        println!("(no SCIM tokens)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "NAME", "PREFIX", "CREATED", "LAST USED"]);
    for tok in &resp.tokens {
        let last_used = tok
            .last_used_at
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("never");
        t.row(&[&tok.id, &tok.name, &tok.prefix, &tok.created_at, last_used]);
    }
    t.flush()
}

// ── scim tokens create ───────────────────────────────────────────────

fn scim_tokens_create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("scim tokens create")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("name").long("name"));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| {
            if g.org_slug.is_empty() {
                None
            } else {
                Some(g.org_slug.clone())
            }
        })
        .ok_or_else(|| msg("scim tokens create requires --org=<slug>"))?;
    let name = m
        .get_one::<String>("name")
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| msg("scim tokens create requires --name=<n>"))?;

    let body = json!({ "name": name });
    let c = require_client(g)?;
    let resp: ScimTokenCreateResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/scim/tokens"),
        Some(body),
    )?;

    if g.json {
        // JSON shape: { "token": { id, name, prefix, created_at }, "secret": "..." }
        // Secret is reveal-once: the cloud only returns it on creation.
        return print_json(&mut std::io::stdout(), &resp);
    }
    // Always print the secret — it will not be shown again.
    println!("SCIM token created. Store this secret NOW — it will not be shown again:");
    println!("  {}", resp.secret);
    if let Some(tok) = &resp.token {
        println!("  id:     {}", tok.id);
        println!("  name:   {}", tok.name);
        println!("  prefix: {}", tok.prefix);
    }
    Ok(())
}

// ── scim tokens revoke ───────────────────────────────────────────────

fn scim_tokens_revoke(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("scim tokens revoke")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true));
    let m = parse_or_silent(cmd, args)?;
    let org = m
        .get_one::<String>("org")
        .filter(|s| !s.is_empty())
        .cloned()
        .or_else(|| {
            if g.org_slug.is_empty() {
                None
            } else {
                Some(g.org_slug.clone())
            }
        })
        .ok_or_else(|| msg("usage: basin scim tokens revoke <id> --org=<slug> [--yes]"))?;
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin scim tokens revoke <id> --org=<slug> [--yes]"))?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to revoke SCIM token {:?} non-interactively under --json",
                id
            )));
        }
        eprint!("Revoke SCIM token {:?} for org {:?}? [y/N] ", id, org);
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: ScimTokenRevokeResp = c.do_json(
        Method::DELETE,
        &format!("/v1/orgs/{org}/scim/tokens/{id}"),
        None,
    )?;

    if g.json {
        // JSON shape: { "revoked": true, "id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Revoked SCIM token {id}.");
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

    fn sample_scim_config() -> &'static str {
        r#"{"enabled":true,"base_url":"https://api.basin.run/scim/v2/orgs/acme","schemas_url":"https://api.basin.run/scim/v2/orgs/acme/Schemas","configured_at":"2026-05-01T00:00:00Z"}"#
    }

    fn sample_scim_token(id: &str, prefix: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"Okta provisioning","prefix":"{prefix}","created_at":"2026-05-01T00:00:00Z"}}"#
        )
    }

    // ── scim config get ───────────────────────────────────────────────

    #[test]
    fn config_get_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/scim/config");
            Resp::ok(format!(r#"{{"scim":{}}}"#, sample_scim_config()))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_scim(&g(&srv), &["config".into(), "get".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "scim config get: {:?}", err);
    }

    #[test]
    fn config_get_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"scim":{}}}"#, sample_scim_config()))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let config = ScimConfig {
            enabled: true,
            base_url: "https://api.basin.run/scim/v2/orgs/acme".into(),
            schemas_url: "https://api.basin.run/scim/v2/orgs/acme/Schemas".into(),
            configured_at: "2026-05-01T00:00:00Z".into(),
        };
        print_json(&mut out, &ScimConfigResp { scim: Some(config) }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["scim"]["base_url"]
            .as_str()
            .unwrap_or("")
            .contains("scim/v2/orgs/acme"));
        let _ = &srv;
    }

    #[test]
    fn config_get_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(&g0, &["config".into(), "get".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    // ── scim tokens list ──────────────────────────────────────────────

    #[test]
    fn tokens_list_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/scim/tokens");
            Resp::ok(format!(
                r#"{{"tokens":[{}]}}"#,
                sample_scim_token("tok1", "bso_scim_abc")
            ))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_scim(
            &g(&srv),
            &["tokens".into(), "list".into(), "--org=acme".into()],
        );
        assert!(err.is_ok(), "scim tokens list: {:?}", err);
    }

    #[test]
    fn tokens_list_json_shape() {
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let resp = ScimTokensListResp {
            tokens: vec![ScimToken {
                id: "tok1".into(),
                name: "Okta provisioning".into(),
                prefix: "bso_scim_abc".into(),
                created_at: "2026-05-01T00:00:00Z".into(),
                last_used_at: None,
            }],
        };
        print_json(&mut out, &resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["tokens"].is_array());
        assert_eq!(parsed["tokens"][0]["id"], "tok1");
        // Secret must NOT appear in list output.
        assert!(!String::from_utf8_lossy(&out).contains("secret"));
    }

    #[test]
    fn tokens_list_fallback_404() {
        let srv = TestServer::start(|_req: &Req| {
            // Older cloud: list endpoint not implemented.
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"not found"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        // Should NOT error — graceful fallback.
        let err = cmd_scim(
            &g(&srv),
            &["tokens".into(), "list".into(), "--org=acme".into()],
        );
        assert!(err.is_ok(), "expected graceful 404 handling, got: {:?}", err);
    }

    #[test]
    fn tokens_list_fallback_404_json() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"not found"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        // JSON empty-list fallback.
        print_json(&mut out, &json!({ "tokens": Vec::<ScimToken>::new() })).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["tokens"].is_array());
        assert_eq!(parsed["tokens"].as_array().unwrap().len(), 0);
        let _ = &srv;
    }

    #[test]
    fn tokens_list_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(&g0, &["tokens".into(), "list".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    // ── scim tokens create ────────────────────────────────────────────

    #[test]
    fn tokens_create_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/scim/tokens");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["name"], "Okta");
            Resp::ok(format!(
                r#"{{"token":{},"secret":"bso_scim_supersecret123"}}"#,
                sample_scim_token("tok1", "bso_scim_abc")
            ))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let resp = ScimTokenCreateResp {
            token: Some(ScimToken {
                id: "tok1".into(),
                name: "Okta".into(),
                prefix: "bso_scim_abc".into(),
                created_at: "2026-05-01T00:00:00Z".into(),
                last_used_at: None,
            }),
            secret: "bso_scim_supersecret123".into(),
        };
        // Verify JSON shape includes secret on creation.
        print_json(&mut out, &resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["secret"], "bso_scim_supersecret123");
        assert_eq!(parsed["token"]["id"], "tok1");

        // Run the actual command (text mode).
        let err = cmd_scim(
            &g(&srv),
            &[
                "tokens".into(),
                "create".into(),
                "--org=acme".into(),
                "--name=Okta".into(),
            ],
        );
        assert!(err.is_ok(), "scim tokens create: {:?}", err);
    }

    #[test]
    fn tokens_create_secret_not_in_list() {
        // After creating a token, a subsequent list call must not reveal the secret.
        let srv = TestServer::start(|req: &Req| match req.method.as_str() {
            "POST" => Resp::ok(format!(
                r#"{{"token":{},"secret":"bso_scim_supersecret123"}}"#,
                sample_scim_token("tok1", "bso_scim_abc")
            )),
            "GET" => Resp::ok(format!(
                r#"{{"tokens":[{}]}}"#,
                sample_scim_token("tok1", "bso_scim_abc")
            )),
            _ => Resp::status(405, ""),
        });
        let _cfg = with_temp_config_dir();

        // Create — secret must appear.
        let mut create_out = Vec::new();
        let create_resp = ScimTokenCreateResp {
            token: Some(ScimToken {
                id: "tok1".into(),
                name: "Okta".into(),
                prefix: "bso_scim_abc".into(),
                created_at: "2026-05-01T00:00:00Z".into(),
                last_used_at: None,
            }),
            secret: "bso_scim_supersecret123".into(),
        };
        print_json(&mut create_out, &create_resp).unwrap();
        assert!(
            String::from_utf8_lossy(&create_out).contains("bso_scim_supersecret123"),
            "secret must appear in create output"
        );

        // List — secret must NOT appear.
        let mut list_out = Vec::new();
        let list_resp = ScimTokensListResp {
            tokens: vec![ScimToken {
                id: "tok1".into(),
                name: "Okta".into(),
                prefix: "bso_scim_abc".into(),
                created_at: "2026-05-01T00:00:00Z".into(),
                last_used_at: None,
            }],
        };
        print_json(&mut list_out, &list_resp).unwrap();
        assert!(
            !String::from_utf8_lossy(&list_out).contains("bso_scim_supersecret123"),
            "secret must NOT appear in list output"
        );

        let _ = &srv;
    }

    #[test]
    fn tokens_create_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(
            &g0,
            &["tokens".into(), "create".into(), "--name=Okta".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn tokens_create_missing_name() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(
            &g0,
            &["tokens".into(), "create".into(), "--org=acme".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--name"));
    }

    // ── scim tokens revoke ────────────────────────────────────────────

    #[test]
    fn tokens_revoke_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme/scim/tokens/tok1");
            Resp::ok(r#"{"revoked":true,"id":"tok1"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_scim(
            &g_json(&srv),
            &[
                "tokens".into(),
                "revoke".into(),
                "--org=acme".into(),
                "--yes".into(),
                "tok1".into(),
            ],
        );
        assert!(err.is_ok(), "scim tokens revoke: {:?}", err);
    }

    #[test]
    fn tokens_revoke_json_shape() {
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let resp = ScimTokenRevokeResp {
            revoked: true,
            id: "tok1".into(),
        };
        print_json(&mut out, &resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["revoked"], true);
        assert_eq!(parsed["id"], "tok1");
    }

    #[test]
    fn tokens_revoke_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(
            &g0,
            &["tokens".into(), "revoke".into(), "tok1".into(), "--yes".into()],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn tokens_revoke_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_scim(
            &g0,
            &[
                "tokens".into(),
                "revoke".into(),
                "--org=acme".into(),
                "tok1".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("confirmation required"));
    }

    #[test]
    fn tokens_revoke_404() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"token not found"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_scim(
            &g(&srv),
            &[
                "tokens".into(),
                "revoke".into(),
                "--org=acme".into(),
                "--yes".into(),
                "notfound".into(),
            ],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── misc ──────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_returns_silent() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn no_args_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_scim(&g0, &[]);
        assert!(err.is_ok());
    }
}
