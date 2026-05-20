//! tokens — `basin tokens (list | create | revoke | show) --org <slug>`
//!
//! Manages org-scoped personal access tokens (PATs).
//!
//!   basin tokens list   --org <slug>
//!   basin tokens create --org <slug> --name <label> [--scope read|write|admin]
//!                                    [--projects <ref,ref>] [--ip <cidr,...>]
//!                                    [--expires <7d|30d|90d|1y|never|RFC3339>]
//!   basin tokens revoke <id> --org <slug>
//!   basin tokens show   <id> --org <slug>
//!
//! Create prints the full token ONCE. Subsequent `show` calls only see
//! the prefix — that's the cloud's reveal-once contract.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────

/// APIToken mirrors models.APIToken minus the secret material.
/// JSON shape: { id, name, description, prefix, scope, scopes,
///               project_ids, ip_allowlist, created_at,
///               expires_at, last_used_at }
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct ApiToken {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub project_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ip_allowlist: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
}

/// CreateTokenResponse is the once-only reveal envelope from
/// POST /v1/orgs/:slug/api-tokens.
#[derive(Debug, Deserialize, serde::Serialize)]
pub struct CreateTokenResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<ApiToken>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub full_token: String,
}

#[derive(Deserialize)]
struct TokensListResp {
    #[serde(default)]
    tokens: Vec<ApiToken>,
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_tokens(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin tokens (list | create | revoke | show) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "revoke" => revoke(g, rest),
        "show" => show(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "tokens",
                "Manage org-scoped personal access tokens (PATs).",
                &[
                    "list   --org <slug>                                          List tokens (revoked filtered out).",
                    "create --org <slug> --name <label> [--scope read|write|admin]",
                    "       [--projects ref,ref] [--ip cidr,...] [--expires <7d|30d|90d|1y|never|RFC3339>]",
                    "                                                             Mint a token (printed ONCE).",
                    "revoke <id> --org <slug>                                     Mark a token revoked.",
                    "show   <id> --org <slug>                                     Print token metadata (prefix only).",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for tokens");
            Err(silent())
        }
    }
}

// ── tokens list ──────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tokens list")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tokens list",
            "List org-scoped tokens (revoked filtered out).",
            &["--org <slug>   Org slug (required)."],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    if org.is_empty() {
        return Err(msg("tokens list requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: TokensListResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{org}/api-tokens"), None)?;
    if g.json {
        // JSON shape: { tokens: [ APIToken ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "tokens": resp.tokens }),
        );
    }
    let mut t = Table::new(g, &["ID", "NAME", "SCOPE", "PREFIX", "EXPIRES", "LAST USED"]);
    for tok in &resp.tokens {
        t.row(&[
            &tok.id,
            &tok.name,
            &tok.scope,
            &tok.prefix,
            &format_expires_at(tok.expires_at.as_deref()),
            &format_expires_at(tok.last_used_at.as_deref()),
        ]);
    }
    t.flush()
}

// ── tokens create ────────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tokens create")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("description").long("description"))
        .arg(Arg::new("scope").long("scope"))
        .arg(Arg::new("projects").long("projects"))
        .arg(Arg::new("ip").long("ip"))
        .arg(Arg::new("expires").long("expires"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tokens create",
            "Mint a new org-scoped token (full token printed ONCE).",
            &[
                "--org <slug>          Org slug (required).",
                "--name <label>        Human-readable label (required).",
                "--description <text>  Optional description.",
                "--scope read|write|admin  Scope band (default: write).",
                "--projects ref,ref    Comma-separated project refs (or empty for all).",
                "--ip cidr,...         Comma-separated CIDR allowlist.",
                "--expires 7d|30d|90d|1y|never|RFC3339  Expiry.",
            ],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if org.is_empty() || name.is_empty() {
        return Err(msg("tokens create requires --org=<slug> and --name=<label>"));
    }
    let desc = m.get_one::<String>("description").cloned().unwrap_or_default();
    let scope = m
        .get_one::<String>("scope")
        .cloned()
        .unwrap_or_else(|| "write".to_string());
    let mut body = json!({
        "name": name,
        "description": desc,
        "scope": scope,
    });
    if let Some(projects) = m.get_one::<String>("projects").filter(|s| !s.is_empty()) {
        body["project_ids"] = json!(split_csv(projects));
    }
    if let Some(ips) = m.get_one::<String>("ip").filter(|s| !s.is_empty()) {
        body["ip_allowlist"] = json!(split_csv(ips));
    }
    if let Some(expires) = m.get_one::<String>("expires").filter(|s| !s.is_empty()) {
        // Heuristic: contains 'T' → treat as RFC3339; else as expires_in.
        if expires.contains('T') {
            body["expires_at"] = json!(expires);
        } else {
            body["expires_in"] = json!(expires);
        }
    }
    let c = require_client(g)?;
    let resp: CreateTokenResponse =
        c.do_json(Method::POST, &format!("/v1/orgs/{org}/api-tokens"), Some(body))?;
    if g.json {
        // JSON shape: CreateTokenResponse — { token: APIToken, full_token: string }
        // (full_token is reveal-once; subsequent reads only see APIToken.Prefix).
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.full_token.is_empty() {
        println!("Token created (no plaintext returned — likely an upstream bug).");
        return Ok(());
    }
    println!("Token created. Copy this NOW — it won't be shown again:");
    println!("  {}", resp.full_token);
    if let Some(tok) = &resp.token {
        println!("  id:      {}", tok.id);
        println!("  scope:   {}", tok.scope);
        println!("  expires: {}", format_expires_at(tok.expires_at.as_deref()));
    }
    Ok(())
}

// ── tokens revoke ────────────────────────────────────────────────────

fn revoke(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tokens revoke")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tokens revoke",
            "Mark an org-scoped token as revoked.",
            &["<id>           Token ID (required).", "--org <slug>    Org slug (required)."],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin tokens revoke <id> --org <slug>"))?;
    if org.is_empty() {
        return Err(msg("usage: basin tokens revoke <id> --org <slug>"));
    }
    let c = require_client(g)?;
    let out: serde_json::Value = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/api-tokens/{id}/revoke"),
        None,
    )?;
    if g.json {
        // JSON shape: passthrough of revoke envelope
        // (commonly { revoked: bool, id: string }).
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Revoked token {id}.");
    Ok(())
}

// ── tokens show ──────────────────────────────────────────────────────

fn show(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("tokens show")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("id").num_args(1..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "tokens show",
            "Print metadata for a specific token (prefix only — secret is never returned).",
            &["<id>           Token ID (required).", "--org <slug>    Org slug (required)."],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let id = m
        .get_many::<String>("id")
        .and_then(|mut v| v.next().cloned())
        .ok_or_else(|| msg("usage: basin tokens show <id> --org <slug>"))?;
    if org.is_empty() {
        return Err(msg("usage: basin tokens show <id> --org <slug>"));
    }
    let c = require_client(g)?;
    // The cloud doesn't expose a per-token GET — pull the list and filter.
    // Cheap (<= a few dozen rows per org).
    let resp: TokensListResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{org}/api-tokens"), None)?;
    for tok in &resp.tokens {
        if tok.id == id {
            if g.json {
                // JSON shape: APIToken — { id, name, description, prefix,
                // scope, scopes, project_ids, ip_allowlist, created_at,
                // expires_at, last_used_at }
                return print_json(&mut std::io::stdout(), tok);
            }
            println!("id:           {}", tok.id);
            println!("name:         {}", tok.name);
            println!("description:  {}", tok.description);
            println!("scope:        {}", tok.scope);
            if !tok.scopes.is_empty() {
                println!("scopes:       {}", tok.scopes.join(","));
            }
            println!("prefix:       {}", tok.prefix);
            println!("created_at:   {}", tok.created_at);
            println!("expires_at:   {}", format_expires_at(tok.expires_at.as_deref()));
            println!("last_used_at: {}", format_expires_at(tok.last_used_at.as_deref()));
            if !tok.project_ids.is_empty() {
                println!("projects:     {}", tok.project_ids.join(","));
            }
            if !tok.ip_allowlist.is_empty() {
                println!("ip_allowlist: {}", tok.ip_allowlist.join(","));
            }
            return Ok(());
        }
    }
    Err(msg(format!("token {id} not found in org {org}")))
}

// ── helpers ──────────────────────────────────────────────────────────

/// format_expires_at collapses an optional timestamp into a display string.
/// A missing / empty value is rendered as "never".
fn format_expires_at(s: Option<&str>) -> String {
    match s {
        None | Some("") => "never".to_string(),
        Some(v) => v.to_string(),
    }
}

/// split_csv splits a comma-separated string, trimming whitespace and
/// dropping empty segments.
fn split_csv(s: &str) -> Vec<String> {
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

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

    fn flags_org(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.to_string(),
            token: "tok".into(),
            org_slug: "acme".into(),
            quiet: true,
            ..Default::default()
        }
    }

    // ── helpers ───────────────────────────────────────────────────────

    #[test]
    fn format_expires_at_none() {
        assert_eq!(format_expires_at(None), "never");
    }

    #[test]
    fn format_expires_at_empty() {
        assert_eq!(format_expires_at(Some("")), "never");
    }

    #[test]
    fn format_expires_at_value() {
        assert_eq!(format_expires_at(Some("2025-01-01T00:00:00Z")), "2025-01-01T00:00:00Z");
    }

    #[test]
    fn split_csv_empty() {
        assert!(split_csv("").is_empty());
    }

    #[test]
    fn split_csv_trims_and_filters() {
        let r = split_csv("a, b , , c");
        assert_eq!(r, vec!["a", "b", "c"]);
    }

    // ── arg-parse errors ─────────────────────────────────────────────

    #[test]
    fn missing_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), quiet: true, ..Default::default() };
        assert!(cmd_tokens(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), quiet: true, ..Default::default() };
        assert!(cmd_tokens(&g, &["frobnicate".into()]).is_err());
    }

    #[test]
    fn list_missing_org_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), quiet: true, ..Default::default() };
        assert!(cmd_tokens(&g, &["list".into()]).is_err());
    }

    #[test]
    fn create_missing_name_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), org_slug: "acme".into(), quiet: true, ..Default::default() };
        assert!(cmd_tokens(&g, &["create".into()]).is_err());
    }

    // ── list happy-path ───────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/api-tokens");
            Resp::ok(r#"{"tokens":[{"id":"tok1","name":"ci","scope":"write","prefix":"bso_"}]}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &["list".into(), "--org=acme".into()],
        )
        .unwrap();
    }

    #[test]
    fn list_empty_tokens() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"tokens":[]}"#));
        cmd_tokens(&flags_org(&srv.url), &["list".into(), "--org=acme".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"tokens":[{"id":"t1","name":"ci","scope":"read","prefix":"bso_"}]}"#)
        });
        let mut g = flags_org(&srv.url);
        g.json = true;
        cmd_tokens(&g, &["list".into(), "--org=acme".into()]).unwrap();
    }

    // ── create happy-path ─────────────────────────────────────────────

    #[test]
    fn create_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/api-tokens");
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(body["name"].as_str().unwrap(), "ci-token");
            Resp::ok(r#"{"token":{"id":"t1","scope":"write","prefix":"bso_"},"full_token":"bso_abc123"}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &["create".into(), "--org=acme".into(), "--name=ci-token".into()],
        )
        .unwrap();
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"token":{"id":"t1","scope":"write","prefix":"bso_"},"full_token":"bso_abc123"}"#)
        });
        let mut g = flags_org(&srv.url);
        g.json = true;
        cmd_tokens(
            &g,
            &["create".into(), "--org=acme".into(), "--name=ci-token".into()],
        )
        .unwrap();
    }

    #[test]
    fn create_with_expires_in() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            // "30d" doesn't contain 'T' → expires_in
            assert_eq!(body["expires_in"].as_str().unwrap(), "30d");
            Resp::ok(r#"{"token":{"id":"t1","scope":"write","prefix":"bso_"},"full_token":"bso_x"}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &[
                "create".into(),
                "--org=acme".into(),
                "--name=t".into(),
                "--expires=30d".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_with_expires_at_rfc3339() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            // Contains 'T' → expires_at
            assert!(body["expires_at"].as_str().unwrap().contains('T'));
            Resp::ok(r#"{"token":{"id":"t1","scope":"write","prefix":"bso_"},"full_token":"bso_x"}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &[
                "create".into(),
                "--org=acme".into(),
                "--name=t".into(),
                "--expires=2026-01-01T00:00:00Z".into(),
            ],
        )
        .unwrap();
    }

    // ── revoke happy-path ─────────────────────────────────────────────

    #[test]
    fn revoke_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/api-tokens/tok1/revoke");
            Resp::ok(r#"{"revoked":true,"id":"tok1"}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &["revoke".into(), "tok1".into(), "--org=acme".into()],
        )
        .unwrap();
    }

    #[test]
    fn revoke_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let err = cmd_tokens(&flags_org(&srv.url), &["revoke".into(), "--org=acme".into()]);
        assert!(err.is_err());
    }

    // ── show happy-path ───────────────────────────────────────────────

    #[test]
    fn show_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"tokens":[{"id":"tok1","name":"ci","scope":"write","prefix":"bso_","created_at":"2024-01-01"}]}"#)
        });
        cmd_tokens(
            &flags_org(&srv.url),
            &["show".into(), "tok1".into(), "--org=acme".into()],
        )
        .unwrap();
    }

    #[test]
    fn show_not_found_is_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"tokens":[]}"#));
        let err = cmd_tokens(
            &flags_org(&srv.url),
            &["show".into(), "unknown-id".into(), "--org=acme".into()],
        );
        assert!(err.is_err());
    }

    // ── server-error mapping ──────────────────────────────────────────

    #[test]
    fn list_server_error_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#)
        });
        let err =
            cmd_tokens(&flags_org(&srv.url), &["list".into(), "--org=acme".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = crate::error::as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── help returns Ok ───────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_tokens(&g, &["help".into()]).is_ok());
    }
}
