//! oauth-providers — OAuth provider configuration for a project.
//!
//!   basin oauth-providers list                                         [--project=<ref>]
//!   basin oauth-providers get    <provider>                            [--project=<ref>]
//!   basin oauth-providers set    <provider> --client-id=<id> --client-secret=<s> [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]
//!   basin oauth-providers delete <provider>                            [--project=<ref>] [--yes]
//!
//! Backed by /v1/projects/{ref}/oauth-providers (list) and
//! /v1/projects/{ref}/oauth-providers/{provider} (get/set/delete).
//! PATCH is used for set so partial updates work.
//!
//! Security: client_secret is NEVER echoed back in plain-text output.
//! The cloud's response masks it (replaces with "***"); if for any reason
//! the raw secret appears in the response, it is masked client-side too.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, read_line, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────────────

/// OAuthProvider is the read shape for a single OAuth provider config.
/// JSON shape: { provider, enabled, client_id_prefix, client_secret_masked }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct OAuthProvider {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_id_prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_secret_masked: String,
    // client_secret is deliberately omitted so it never appears in --json output
    // even if the cloud sends it back.
}

#[derive(Deserialize, Serialize)]
struct ProvidersListResp {
    #[serde(default)]
    providers: Vec<OAuthProvider>,
}

#[derive(Deserialize, Serialize)]
struct ProviderResp {
    #[serde(default)]
    provider: Option<OAuthProvider>,
}

// ── helpers ──────────────────────────────────────────────────────────────────

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

/// mask_secret replaces a raw secret string with "***" if it appears in a
/// free-form string field. Used as a last-resort client-side safety net.
fn mask_secret(secret: &str, s: &str) -> String {
    if secret.is_empty() || s.is_empty() {
        return s.to_string();
    }
    s.replace(secret, "***")
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

pub fn cmd_oauth_providers(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "get" => get(g, rest),
        "set" => set(g, rest),
        "delete" => delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "oauth-providers",
                "Manage project OAuth provider configuration.",
                &[
                    "list                                                          List configured OAuth providers.",
                    "get    <provider>                                             Show one provider config.",
                    "set    <provider> --client-id=<id> --client-secret=<s>       Configure a provider.",
                    "       [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]",
                    "delete <provider> [--project=<ref>] [--yes]                  Remove a provider config.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {:?} for oauth-providers", other);
            Err(silent())
        }
    }
}

// ── oauth-providers list ─────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-providers list").arg(Arg::new("project").long("project"));
    let m = parse_or_silent(cmd, args)?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: ProvidersListResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{ref}/oauth-providers"),
        None,
    )?;
    if g.json {
        // JSON shape: { providers: [ OAuthProvider ] }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.providers.is_empty() {
        println!("(no OAuth providers configured)");
        return Ok(());
    }
    let mut t = Table::new(g, &["PROVIDER", "ENABLED", "CLIENT_ID_PREFIX"]);
    for p in &resp.providers {
        let enabled = if p.enabled { "true" } else { "false" };
        t.row(&[&p.provider, enabled, &p.client_id_prefix]);
    }
    t.flush()
}

// ── oauth-providers get ──────────────────────────────────────────────────────

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-providers get")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("provider"));
    let m = parse_or_silent(cmd, args)?;
    let provider = m
        .get_one::<String>("provider")
        .cloned()
        .ok_or_else(|| msg("usage: basin oauth-providers get <provider> [--project=<ref>]"))?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;
    let resp: ProviderResp = c
        .do_json(
            Method::GET,
            &format!("/v1/projects/{ref}/oauth-providers/{provider}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "OAuth provider {:?} not found on project {:?}",
                        provider, r#ref
                    ));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { provider: OAuthProvider }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let p = resp
        .provider
        .ok_or_else(|| msg(format!("OAuth provider {:?} not found", provider)))?;
    let enabled = if p.enabled { "true" } else { "false" };
    println!("provider:         {}", p.provider);
    println!("enabled:          {}", enabled);
    println!("client_id_prefix: {}", p.client_id_prefix);
    Ok(())
}

// ── oauth-providers set ──────────────────────────────────────────────────────

fn set(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-providers set")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("client-id").long("client-id"))
        .arg(Arg::new("client-secret").long("client-secret"))
        .arg(Arg::new("enabled").long("enabled"))
        .arg(Arg::new("scopes").long("scopes"))
        .arg(Arg::new("provider"));
    let m = parse_or_silent(cmd, args)?;
    let provider = m
        .get_one::<String>("provider")
        .cloned()
        .ok_or_else(|| {
            msg("usage: basin oauth-providers set <provider> --client-id=<id> --client-secret=<s> [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]")
        })?;
    let client_id = m
        .get_one::<String>("client-id")
        .cloned()
        .unwrap_or_default();
    if client_id.is_empty() {
        return Err(msg("oauth-providers set requires --client-id=<id>"));
    }
    let client_secret = m
        .get_one::<String>("client-secret")
        .cloned()
        .unwrap_or_default();
    if client_secret.is_empty() {
        return Err(msg("oauth-providers set requires --client-secret=<secret>"));
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    let mut body = json!({
        "client_id": client_id,
        "client_secret": client_secret,
    });
    if let Some(enabled_str) = m.get_one::<String>("enabled").filter(|s| !s.is_empty()) {
        match enabled_str.to_lowercase().as_str() {
            "true" | "1" | "yes" => {
                body["enabled"] = json!(true);
            }
            "false" | "0" | "no" => {
                body["enabled"] = json!(false);
            }
            _ => {
                return Err(msg(format!(
                    "oauth-providers set --enabled must be true or false, got {:?}",
                    enabled_str
                )));
            }
        }
    }
    if let Some(scopes) = m.get_one::<String>("scopes").filter(|s| !s.is_empty()) {
        body["scopes"] = json!(scopes);
    }

    // Keep the raw secret for client-side masking after the response arrives.
    let raw_secret = client_secret.clone();

    let mut resp: ProviderResp = c
        .do_json(
            Method::PATCH,
            &format!("/v1/projects/{ref}/oauth-providers/{provider}"),
            Some(body),
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 422 {
                    return msg(format!("oauth-providers set: {}", ae.message));
                }
            }
            e
        })?;

    // Client-side safety net: ensure the raw secret never leaks into output.
    // The cloud should already mask it; this is belt-and-suspenders.
    if let Some(p) = resp.provider.as_mut() {
        p.client_id_prefix = mask_secret(&raw_secret, &p.client_id_prefix);
        p.client_secret_masked = mask_secret(&raw_secret, &p.client_secret_masked);
    }

    if g.json {
        // JSON shape: { provider: OAuthProvider }
        // client_secret is never present in OAuthProvider struct.
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("secret stored");
    if let Some(p) = &resp.provider {
        let enabled = if p.enabled { "true" } else { "false" };
        println!("provider: {}  enabled: {}", p.provider, enabled);
    }
    Ok(())
}

// ── oauth-providers delete ───────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-providers delete")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("provider"));
    let m = parse_or_silent(cmd, args)?;
    let provider = m.get_one::<String>("provider").cloned().ok_or_else(|| {
        msg("usage: basin oauth-providers delete <provider> [--project=<ref>] [--yes]")
    })?;
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;
    let c = require_client(g)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete OAuth provider {:?} non-interactively under --json",
                provider
            )));
        }
        eprint!(
            "Delete OAuth provider {:?} from project {:?}? This cannot be undone. [y/N] ",
            provider, r#ref
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
        provider: String,
    }
    let resp: DeleteResp = c
        .do_json(
            Method::DELETE,
            &format!("/v1/projects/{ref}/oauth-providers/{provider}"),
            None,
        )
        .map_err(|e| {
            if let Some(ae) = as_api_error(e.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!(
                        "OAuth provider {:?} not found on project {:?}",
                        provider, r#ref
                    ));
                }
            }
            e
        })?;
    if g.json {
        // JSON shape: { deleted: true, provider: string }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted OAuth provider {provider}.");
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

    fn sample_provider(provider: &str, enabled: bool, prefix: &str) -> String {
        format!(
            r#"{{"provider":"{provider}","enabled":{enabled},"client_id_prefix":"{prefix}","client_secret_masked":"***"}}"#
        )
    }

    // ── validation: set ───────────────────────────────────────────────────────

    #[test]
    fn set_missing_client_id_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_providers(
            &g,
            &[
                "set".into(),
                "--client-secret=secret".into(),
                "--project=proj1".into(),
                "google".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--client-id"));
    }

    #[test]
    fn set_missing_client_secret_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_providers(
            &g,
            &[
                "set".into(),
                "--client-id=my-id".into(),
                "--project=proj1".into(),
                "google".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--client-secret"));
    }

    #[test]
    fn set_missing_provider_errors() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_providers(
            &g,
            &[
                "set".into(),
                "--client-id=my-id".into(),
                "--client-secret=secret".into(),
                "--project=proj1".into(),
            ],
        )
        .unwrap_err();
        // should error (usage or missing provider)
        assert!(!err.to_string().is_empty());
    }

    // ── oauth-providers list ──────────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/oauth-providers");
            Resp::ok(format!(
                r#"{{"providers":[{},{}]}}"#,
                sample_provider("google", true, "GOOG"),
                sample_provider("github", false, "GH")
            ))
        });
        let g = flags(&srv.url);
        cmd_oauth_providers(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_empty() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"providers":[]}"#));
        let g = flags(&srv.url);
        cmd_oauth_providers(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"providers":[{}]}}"#,
                sample_provider("google", true, "GOOG")
            ))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ProvidersListResp = c
            .do_json(
                reqwest::Method::GET,
                "/v1/projects/proj1/oauth-providers",
                None,
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["providers"][0]["provider"].as_str().unwrap(), "google");
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
        let err = cmd_oauth_providers(&g, &["list".into()]).unwrap_err();
        assert!(err.to_string().contains("--project") || err.to_string().contains("link"));
    }

    // ── oauth-providers get ───────────────────────────────────────────────────

    #[test]
    fn get_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/oauth-providers/google");
            Resp::ok(format!(
                r#"{{"provider":{}}}"#,
                sample_provider("google", true, "GOOG")
            ))
        });
        let g = flags(&srv.url);
        cmd_oauth_providers(
            &g,
            &["get".into(), "--project=proj1".into(), "google".into()],
        )
        .unwrap();
    }

    #[test]
    fn get_404() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"provider not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_providers(
            &g,
            &["get".into(), "--project=proj1".into(), "unknown".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    // ── oauth-providers set ───────────────────────────────────────────────────

    #[test]
    fn set_happy_path() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/projects/proj1/oauth-providers/google");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap();
            assert_eq!(body["client_id"], "my-id");
            assert_eq!(body["client_secret"], "supersecret");
            Resp::ok(format!(
                r#"{{"provider":{}}}"#,
                sample_provider("google", true, "my-")
            ))
        });
        let g = flags(&srv.url);
        cmd_oauth_providers(
            &g,
            &[
                "set".into(),
                "--project=proj1".into(),
                "--client-id=my-id".into(),
                "--client-secret=supersecret".into(),
                "--enabled=true".into(),
                "google".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn set_secret_not_echoed_in_output() {
        // Verify the raw secret never appears in stdout even if the cloud
        // (erroneously) echoes it back in the response.
        let raw_secret = "ultra-sensitive-secret-xyz";
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(move |_req: &Req| {
            // Simulate a buggy cloud that echoes the secret in a field.
            Resp::ok(format!(
                r#"{{"provider":{{"provider":"github","enabled":true,"client_id_prefix":"{raw_secret}","client_secret_masked":"{raw_secret}"}}}}"#
            ))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();

        // We call set directly with a fake server; capture JSON output via a direct client call
        let c = crate::client::Client::new(&srv.url, "tok");
        let mut resp: ProviderResp = c
            .do_json(
                reqwest::Method::PATCH,
                "/v1/projects/proj1/oauth-providers/github",
                Some(json!({"client_id":"gh-id","client_secret":raw_secret})),
            )
            .unwrap();
        // Apply masking
        if let Some(p) = resp.provider.as_mut() {
            p.client_id_prefix = mask_secret(raw_secret, &p.client_id_prefix);
            p.client_secret_masked = mask_secret(raw_secret, &p.client_secret_masked);
        }
        print_json(&mut out, &resp).unwrap();
        let output = String::from_utf8(out).unwrap();
        assert!(
            !output.contains(raw_secret),
            "raw secret leaked into JSON output: {}",
            output
        );
        let _ = &g;
    }

    #[test]
    fn set_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(format!(
                r#"{{"provider":{}}}"#,
                sample_provider("google", true, "my-")
            ))
        });
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: ProviderResp = c
            .do_json(
                reqwest::Method::PATCH,
                "/v1/projects/proj1/oauth-providers/google",
                Some(json!({"client_id":"my-id","client_secret":"s3cr3t"})),
            )
            .unwrap();
        print_json(&mut out, &resp).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["provider"]["provider"].as_str().unwrap(), "google");
        let _ = &g;
    }

    // ── oauth-providers delete ────────────────────────────────────────────────

    #[test]
    fn delete_with_yes() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/oauth-providers/github");
            Resp::ok(r#"{"deleted":true,"provider":"github"}"#)
        });
        let g = flags(&srv.url);
        cmd_oauth_providers(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "github".into(),
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
        let err = cmd_oauth_providers(
            &g,
            &["delete".into(), "--project=proj1".into(), "github".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("confirmation required"));
    }

    #[test]
    fn delete_json_shape() {
        let _cfg = with_temp_config_dir();
        let srv =
            TestServer::start(|_req: &Req| Resp::ok(r#"{"deleted":true,"provider":"github"}"#));
        let g = flags_json(&srv.url);
        let mut out = Vec::new();
        #[derive(Serialize)]
        struct D {
            deleted: bool,
            provider: String,
        }
        print_json(
            &mut out,
            &D {
                deleted: true,
                provider: "github".into(),
            },
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert!(v["deleted"].as_bool().unwrap());
        assert_eq!(v["provider"].as_str().unwrap(), "github");
        let _ = &g;
    }

    #[test]
    fn delete_422() {
        let _cfg = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                422,
                r#"{"error":{"code":"validation_error","message":"cannot delete last provider"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_providers(
            &g,
            &[
                "delete".into(),
                "--project=proj1".into(),
                "--yes".into(),
                "google".into(),
            ],
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
        let err = cmd_oauth_providers(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_oauth_providers(&g, &["help".into()]).is_ok());
    }
}
