//! saml — Manage SAML SSO configuration for an org.
//!
//! Routes:
//!   GET   /v1/orgs/{slug}/saml         → get SAML config
//!   PATCH /v1/orgs/{slug}/saml         → update SAML config (put)
//!   POST  /v1/orgs/{slug}/saml/test    → test IdP connectivity
//!   POST  /v1/orgs/{slug}/saml/enable  → enable SAML SSO
//!   POST  /v1/orgs/{slug}/saml/disable → disable SAML SSO

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line};
use crate::printerr;

use super::parse_or_silent;

// ── wire shapes ───────────────────────────────────────────────────────

/// SAMLConfig is the read shape from GET /v1/orgs/{slug}/saml.
#[derive(Debug, Default, Serialize, Deserialize)]
struct SamlConfig {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    idp_metadata_url: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    idp_entity_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    sp_metadata_url: String,
    enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    configured_at: String,
}

#[derive(Deserialize, Serialize)]
struct SamlResp {
    #[serde(default)]
    saml: Option<SamlConfig>,
}

// ── top-level dispatcher ─────────────────────────────────────────────

pub fn cmd_saml(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            print_help();
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => saml_get(g, rest),
        "put" => saml_put(g, rest),
        "test" => saml_test(g, rest),
        "enable" => saml_enable(g, rest),
        "disable" => saml_disable(g, rest),
        "--help" | "-h" | "help" => {
            print_help();
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for saml", other);
            Err(silent())
        }
    }
}

fn print_help() {
    use super::help::help_for_command;
    help_for_command(
        "saml",
        "Manage SAML SSO for an org.",
        &[
            "get     --org=<slug>                                           Show SAML config.",
            "put     --org=<slug> --metadata-url=<u> [--entity-id=<id>] [--yes]  Configure SAML IdP.",
            "test    --org=<slug>                                           Test IdP connectivity.",
            "enable  --org=<slug> [--yes]                                   Enable SAML SSO.",
            "disable --org=<slug> [--yes]                                   Disable SAML SSO.",
        ],
    );
}

// ── saml get ─────────────────────────────────────────────────────────

fn saml_get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("saml get").arg(Arg::new("org").long("org"));
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
        .ok_or_else(|| msg("saml get requires --org=<slug>"))?;

    let c = require_client(g)?;
    let resp: SamlResp = c.do_json(Method::GET, &format!("/v1/orgs/{org}/saml"), None)?;

    if g.json {
        // JSON shape: { "saml": { idp_metadata_url, idp_entity_id, sp_metadata_url, enabled, configured_at } | null }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let Some(s) = resp.saml else {
        println!("(no SAML configured)");
        return Ok(());
    };
    println!("idp_metadata_url: {}", s.idp_metadata_url);
    println!("idp_entity_id:    {}", s.idp_entity_id);
    println!("sp_metadata_url:  {}", s.sp_metadata_url);
    println!("enabled:          {}", s.enabled);
    println!("configured_at:    {}", s.configured_at);
    Ok(())
}

// ── saml put ─────────────────────────────────────────────────────────

fn saml_put(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("saml put")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("metadata-url").long("metadata-url"))
        .arg(Arg::new("entity-id").long("entity-id"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
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
        .ok_or_else(|| msg("saml put requires --org=<slug>"))?;

    let metadata_url = m
        .get_one::<String>("metadata-url")
        .filter(|s| !s.is_empty())
        .cloned()
        .ok_or_else(|| msg("saml put requires --metadata-url=<u>"))?;

    let entity_id = m
        .get_one::<String>("entity-id")
        .filter(|s| !s.is_empty())
        .cloned();

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to configure SAML for org {:?} non-interactively under --json",
                org
            )));
        }
        eprint!(
            "Configure SAML IdP for org {:?}? This affects how members log in. [y/N] ",
            org
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let mut body = json!({ "idp_metadata_url": metadata_url });
    if let Some(eid) = entity_id {
        body["idp_entity_id"] = json!(eid);
    }

    let c = require_client(g)?;
    let resp: SamlResp = c.do_json(Method::PATCH, &format!("/v1/orgs/{org}/saml"), Some(body))?;

    if g.json {
        // JSON shape: { "saml": { idp_metadata_url, idp_entity_id, sp_metadata_url, enabled, configured_at } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(s) = resp.saml {
        println!(
            "SAML configured for org {:?} (sp_metadata_url: {}).",
            org, s.sp_metadata_url
        );
    } else {
        println!("SAML configured for org {:?}.", org);
    }
    Ok(())
}

// ── saml test ────────────────────────────────────────────────────────

fn saml_test(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("saml test").arg(Arg::new("org").long("org"));
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
        .ok_or_else(|| msg("saml test requires --org=<slug>"))?;

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct TestResp {
        ok: bool,
        #[serde(default)]
        error: Option<String>,
        #[serde(default, skip_serializing_if = "String::is_empty")]
        tested_at: String,
    }

    let resp: TestResp = c.do_json(Method::POST, &format!("/v1/orgs/{org}/saml/test"), None)?;

    if g.json {
        // JSON shape: { "ok": bool, "error": "..." | null, "tested_at": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.ok {
        println!("SAML test OK (tested_at: {}).", resp.tested_at);
    } else {
        let err_msg = resp.error.as_deref().unwrap_or("(unknown error)");
        println!("SAML test FAILED: {err_msg}");
    }
    Ok(())
}

// ── saml enable ──────────────────────────────────────────────────────

fn saml_enable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("saml enable")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
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
        .ok_or_else(|| msg("saml enable requires --org=<slug>"))?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to enable SAML for org {:?} non-interactively under --json",
                org
            )));
        }
        eprint!(
            "Enable SAML SSO for org {:?}? Members will be required to authenticate via the IdP. [y/N] ",
            org
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct EnableResp {
        enabled: bool,
    }

    let resp: EnableResp = c.do_json(Method::POST, &format!("/v1/orgs/{org}/saml/enable"), None)?;

    if g.json {
        // JSON shape: { "enabled": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let _ = resp;
    println!("SAML SSO enabled for org {:?}.", org);
    Ok(())
}

// ── saml disable ─────────────────────────────────────────────────────

fn saml_disable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("saml disable")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue));
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
        .ok_or_else(|| msg("saml disable requires --org=<slug>"))?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to disable SAML for org {:?} non-interactively under --json",
                org
            )));
        }
        eprint!(
            "Disable SAML SSO for org {:?}? Members will fall back to email/password login. [y/N] ",
            org
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;

    #[derive(Deserialize, Serialize)]
    struct DisableResp {
        enabled: bool,
    }

    let resp: DisableResp =
        c.do_json(Method::POST, &format!("/v1/orgs/{org}/saml/disable"), None)?;

    if g.json {
        // JSON shape: { "enabled": false }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let _ = resp;
    println!("SAML SSO disabled for org {:?}.", org);
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
            token: "bso_saml_test".into(),
            quiet: true,
            ..Default::default()
        }
    }

    fn sample_saml_config() -> &'static str {
        r#"{"idp_metadata_url":"https://idp.example.com/metadata","idp_entity_id":"https://idp.example.com","sp_metadata_url":"https://basin.example.com/sp/metadata","enabled":true,"configured_at":"2024-01-01T00:00:00Z"}"#
    }

    // ── saml get ─────────────────────────────────────────────────────

    #[test]
    fn get_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/saml");
            Resp::ok(format!(r#"{{"saml":{}}}"#, sample_saml_config()))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["get".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "get: {:?}", err);
    }

    #[test]
    fn get_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["get".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn get_null_saml() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"saml":null}"#));
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["get".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "get null saml: {:?}", err);
    }

    #[test]
    fn get_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"saml":{}}}"#, sample_saml_config()))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let s = SamlConfig {
            idp_metadata_url: "https://idp.example.com/metadata".into(),
            idp_entity_id: "https://idp.example.com".into(),
            sp_metadata_url: "https://basin.example.com/sp/metadata".into(),
            enabled: true,
            configured_at: "2024-01-01T00:00:00Z".into(),
        };
        let resp = SamlResp { saml: Some(s) };
        print_json(&mut out, &resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(
            parsed["saml"]["idp_metadata_url"],
            "https://idp.example.com/metadata"
        );
        assert_eq!(parsed["saml"]["enabled"], true);
        let _ = &srv;
    }

    #[test]
    fn get_server_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                500,
                r#"{"error":{"code":"internal","message":"server error"}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["get".into(), "--org=acme".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 500);
    }

    // ── saml put ─────────────────────────────────────────────────────

    #[test]
    fn put_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/orgs/acme/saml");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["idp_metadata_url"], "https://idp.example.com/metadata");
            Resp::ok(format!(r#"{{"saml":{}}}"#, sample_saml_config()))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &[
                "put".into(),
                "--org=acme".into(),
                "--metadata-url=https://idp.example.com/metadata".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_ok(), "put: {:?}", err);
    }

    #[test]
    fn put_with_entity_id() {
        let srv = TestServer::start(|req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["idp_entity_id"], "https://idp.example.com");
            Resp::ok(format!(r#"{{"saml":{}}}"#, sample_saml_config()))
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &[
                "put".into(),
                "--org=acme".into(),
                "--metadata-url=https://idp.example.com/metadata".into(),
                "--entity-id=https://idp.example.com".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_ok(), "put with entity-id: {:?}", err);
    }

    #[test]
    fn put_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(
            &g0,
            &[
                "put".into(),
                "--metadata-url=https://idp.example.com/metadata".into(),
                "--yes".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn put_missing_metadata_url() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["put".into(), "--org=acme".into(), "--yes".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--metadata-url"));
    }

    #[test]
    fn put_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_saml(
            &g0,
            &[
                "put".into(),
                "--org=acme".into(),
                "--metadata-url=https://idp.example.com/metadata".into(),
            ],
        );
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("confirmation required"));
    }

    #[test]
    fn put_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(r#"{{"saml":{}}}"#, sample_saml_config()))
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        let s = SamlConfig {
            idp_metadata_url: "https://idp.example.com/metadata".into(),
            sp_metadata_url: "https://basin.example.com/sp/metadata".into(),
            enabled: false,
            ..Default::default()
        };
        let resp = SamlResp { saml: Some(s) };
        print_json(&mut out, &resp).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(parsed["saml"].is_object());
        let _ = &srv;
    }

    // ── saml test ────────────────────────────────────────────────────

    #[test]
    fn test_happy_path_ok() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/saml/test");
            Resp::ok(r#"{"ok":true,"error":null,"tested_at":"2024-01-01T00:00:00Z"}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["test".into(), "--org=acme".into()]);
        assert!(err.is_ok(), "saml test ok: {:?}", err);
    }

    #[test]
    fn test_happy_path_fail() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"ok":false,"error":"cannot reach IdP","tested_at":""}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["test".into(), "--org=acme".into()]);
        assert!(
            err.is_ok(),
            "saml test (failed IdP) should be Ok: {:?}",
            err
        );
    }

    #[test]
    fn test_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["test".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn test_json_shape() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"ok":true,"error":null,"tested_at":"2024-01-01T00:00:00Z"}"#)
        });
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct T {
            ok: bool,
            error: Option<String>,
            tested_at: String,
        }
        print_json(
            &mut out,
            &T {
                ok: true,
                error: None,
                tested_at: "2024-01-01T00:00:00Z".into(),
            },
        )
        .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["ok"], true);
        let _ = &srv;
    }

    #[test]
    fn test_server_error_mapping() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"saml_not_configured","message":"No SAML config found."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(&g(&srv), &["test".into(), "--org=acme".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 422);
    }

    // ── saml enable ──────────────────────────────────────────────────

    #[test]
    fn enable_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/saml/enable");
            Resp::ok(r#"{"enabled":true}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &["enable".into(), "--org=acme".into(), "--yes".into()],
        );
        assert!(err.is_ok(), "enable: {:?}", err);
    }

    #[test]
    fn enable_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["enable".into(), "--yes".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn enable_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["enable".into(), "--org=acme".into()]);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("confirmation required"));
    }

    #[test]
    fn enable_json_shape() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"enabled":true}"#));
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct E {
            enabled: bool,
        }
        print_json(&mut out, &E { enabled: true }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["enabled"], true);
        let _ = &srv;
    }

    #[test]
    fn enable_server_error_mapping() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"Insufficient permissions."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &["enable".into(), "--org=acme".into(), "--yes".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── saml disable ─────────────────────────────────────────────────

    #[test]
    fn disable_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/saml/disable");
            Resp::ok(r#"{"enabled":false}"#)
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &["disable".into(), "--org=acme".into(), "--yes".into()],
        );
        assert!(err.is_ok(), "disable: {:?}", err);
    }

    #[test]
    fn disable_missing_org() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["disable".into(), "--yes".into()]);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("--org"));
    }

    #[test]
    fn disable_json_requires_yes() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["disable".into(), "--org=acme".into()]);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("confirmation required"));
    }

    #[test]
    fn disable_json_shape() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"enabled":false}"#));
        let _cfg = with_temp_config_dir();
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct D {
            enabled: bool,
        }
        print_json(&mut out, &D { enabled: false }).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(parsed["enabled"], false);
        let _ = &srv;
    }

    #[test]
    fn disable_server_error_mapping() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"Org not found."}}"#,
            )
        });
        let _cfg = with_temp_config_dir();
        let err = cmd_saml(
            &g(&srv),
            &["disable".into(), "--org=acme".into(), "--yes".into()],
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
        let err = cmd_saml(&g0, &["frobnicate".into()]);
        assert!(err.is_err());
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &["help".into()]);
        assert!(err.is_ok());
    }

    #[test]
    fn no_args_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g0 = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        let err = cmd_saml(&g0, &[]);
        assert!(err.is_ok());
    }
}
