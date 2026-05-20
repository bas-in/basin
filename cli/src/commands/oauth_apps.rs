//! oauth-apps — OAuth application management for an org.
//!
//!   basin oauth-apps list                  --org=<slug>
//!   basin oauth-apps get    <id>           --org=<slug>
//!   basin oauth-apps create                --org=<slug> --name=<n> --redirect-uri=<u> [--scopes=<csv>]
//!   basin oauth-apps patch  <id>           --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]
//!   basin oauth-apps rotate <id>           --org=<slug> [--yes]
//!   basin oauth-apps disable <id>          --org=<slug>
//!   basin oauth-apps enable  <id>          --org=<slug>
//!   basin oauth-apps delete  <id>          --org=<slug> [--yes]
//!
//! Cloud routes (mounted under /v1/orgs/{slug}/oauth-apps):
//!   GET    /v1/orgs/{slug}/oauth-apps              → list apps
//!   GET    /v1/orgs/{slug}/oauth-apps/{id}         → get one app
//!   POST   /v1/orgs/{slug}/oauth-apps              → create app (client_secret shown ONCE)
//!   PATCH  /v1/orgs/{slug}/oauth-apps/{id}         → update app
//!   POST   /v1/orgs/{slug}/oauth-apps/{id}/rotate-secret  → rotate client_secret (shown ONCE)
//!   POST   /v1/orgs/{slug}/oauth-apps/{id}/disable → disable app
//!   POST   /v1/orgs/{slug}/oauth-apps/{id}/enable  → enable app
//!   DELETE /v1/orgs/{slug}/oauth-apps/{id}         → delete app
//!
//! Secret-bearing responses (create, rotate) print the client_secret
//! exactly once with a "store now" warning. Subsequent get/list calls
//! only return the client_id — the cloud's reveal-once contract.

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

// ── Wire shapes ──────────────────────────────────────────────────────

/// OAuthApp is the read shape from GET /v1/orgs/{slug}/oauth-apps (list)
/// and the embedded "app" field in create/patch/enable/disable responses.
/// JSON shape: { id, name, client_id, redirect_uri, scopes, enabled, created_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct OAuthApp {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub client_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redirect_uri: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
}

#[derive(Deserialize)]
struct OAuthAppsListResp {
    #[serde(default)]
    apps: Vec<OAuthApp>,
}

#[derive(Deserialize, Serialize)]
struct OAuthAppResp {
    #[serde(default)]
    app: Option<OAuthApp>,
}

/// SecretResp is the reveal-once envelope for create and rotate-secret.
/// JSON shape: { app: OAuthApp, client_secret: string }
#[derive(Deserialize, Serialize)]
struct SecretResp {
    #[serde(default)]
    app: Option<OAuthApp>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    client_secret: String,
}

#[derive(Deserialize, Serialize)]
struct EnabledResp {
    enabled: bool,
}

#[derive(Deserialize, Serialize)]
struct DeletedResp {
    deleted: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_oauth_apps(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            help_for_command(
                "oauth-apps",
                "Manage OAuth applications for an org.",
                HELP_LINES,
            );
            return Ok(());
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "get" => get(g, rest),
        "create" => create(g, rest),
        "patch" => patch(g, rest),
        "rotate" => rotate(g, rest),
        "disable" => disable(g, rest),
        "enable" => enable(g, rest),
        "delete" => delete(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "oauth-apps",
                "Manage OAuth applications for an org.",
                HELP_LINES,
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for oauth-apps");
            Err(silent())
        }
    }
}

const HELP_LINES: &[&str] = &[
    "list                 --org=<slug>                                                 List OAuth apps.",
    "get    <id>          --org=<slug>                                                 Show one OAuth app.",
    "create               --org=<slug> --name=<n> --redirect-uri=<u> [--scopes=<csv>] Create an OAuth app (client_secret shown ONCE).",
    "patch  <id>          --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]  Update an app.",
    "rotate <id>          --org=<slug> [--yes]                                        Rotate client_secret (shown ONCE).",
    "disable <id>         --org=<slug>                                                 Disable an OAuth app.",
    "enable  <id>         --org=<slug>                                                 Enable an OAuth app.",
    "delete  <id>         --org=<slug> [--yes]                                        Delete an OAuth app.",
];

// ── oauth-apps list ──────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps list")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps list",
            "List OAuth applications for an org.",
            &["--org <slug>   Org slug (required)."],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    if org.is_empty() {
        return Err(msg("oauth-apps list requires --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: OAuthAppsListResp =
        c.do_json(Method::GET, &format!("/v1/orgs/{org}/oauth-apps"), None)?;
    if g.json {
        // JSON shape: { "apps": [{ id, name, client_id, redirect_uri, scopes, enabled }] }
        return print_json(&mut std::io::stdout(), &json!({ "apps": resp.apps }));
    }
    if resp.apps.is_empty() {
        println!("(no OAuth apps)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "NAME", "CLIENT_ID", "ENABLED", "REDIRECT_URI"]);
    for app in &resp.apps {
        t.row(&[
            &app.id,
            &app.name,
            &app.client_id,
            &app.enabled.to_string(),
            &app.redirect_uri,
        ]);
    }
    t.flush()
}

// ── oauth-apps get ───────────────────────────────────────────────────

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps get")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps get",
            "Show one OAuth application.",
            &[
                "<id>           App ID (required).",
                "--org <slug>   Org slug (required).",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin oauth-apps get <id> --org=<slug>"))?;
    if org.is_empty() {
        return Err(msg("usage: basin oauth-apps get <id> --org=<slug>"));
    }
    let c = require_client(g)?;
    let resp: OAuthAppResp = c.do_json(
        Method::GET,
        &format!("/v1/orgs/{org}/oauth-apps/{id}"),
        None,
    )?;
    if g.json {
        // JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    let app = resp
        .app
        .ok_or_else(|| msg(format!("OAuth app not found: {id}")))?;
    println!("id:           {}", app.id);
    println!("name:         {}", app.name);
    println!("client_id:    {}", app.client_id);
    println!("redirect_uri: {}", app.redirect_uri);
    println!("enabled:      {}", app.enabled);
    if !app.scopes.is_empty() {
        println!("scopes:       {}", app.scopes.join(", "));
    }
    println!("created_at:   {}", app.created_at);
    Ok(())
}

// ── oauth-apps create ────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps create")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("redirect-uri").long("redirect-uri"))
        .arg(Arg::new("scopes").long("scopes"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps create",
            "Create an OAuth application (client_secret shown ONCE).",
            &[
                "--org <slug>          Org slug (required).",
                "--name <n>            App name (required).",
                "--redirect-uri <u>    Redirect URI (required).",
                "--scopes <csv>        Comma-separated scopes (optional).",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    if org.is_empty() {
        return Err(msg("oauth-apps create requires --org=<slug>"));
    }
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("oauth-apps create requires --name=<n>"));
    }
    let redirect_uri = m
        .get_one::<String>("redirect-uri")
        .cloned()
        .unwrap_or_default();
    if redirect_uri.is_empty() {
        return Err(msg("oauth-apps create requires --redirect-uri=<u>"));
    }
    let scopes_csv = m.get_one::<String>("scopes").cloned().unwrap_or_default();

    let mut body = json!({
        "name": name,
        "redirect_uri": redirect_uri,
    });
    if !scopes_csv.is_empty() {
        let scopes: Vec<&str> = scopes_csv.split(',').map(str::trim).collect();
        body["scopes"] = json!(scopes);
    }

    let c = require_client(g)?;
    let resp: SecretResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/oauth-apps"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled },
        //              "client_secret": "..." }
        // client_secret is reveal-once: the cloud only returns it on creation.
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("OAuth app created. Store this client_secret NOW — it will not be shown again:");
    println!("  {}", resp.client_secret);
    if let Some(app) = &resp.app {
        println!("  id:          {}", app.id);
        println!("  client_id:   {}", app.client_id);
        println!("  name:        {}", app.name);
    }
    Ok(())
}

// ── oauth-apps patch ─────────────────────────────────────────────────

fn patch(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps patch")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("redirect-uri").long("redirect-uri"))
        .arg(Arg::new("scopes").long("scopes"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps patch",
            "Update an OAuth application.",
            &[
                "<id>                  App ID (required).",
                "--org <slug>          Org slug (required).",
                "--name <n>            New app name.",
                "--redirect-uri <u>    New redirect URI.",
                "--scopes <csv>        New scopes (comma-separated).",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m.get_one::<String>("id").cloned();
    if org.is_empty() || id.is_none() {
        return Err(msg(
            "usage: basin oauth-apps patch <id> --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]",
        ));
    }
    let id = id.unwrap();

    let mut body = json!({});
    if let Some(n) = m.get_one::<String>("name").filter(|s| !s.is_empty()) {
        body["name"] = json!(n);
    }
    if let Some(u) = m
        .get_one::<String>("redirect-uri")
        .filter(|s| !s.is_empty())
    {
        body["redirect_uri"] = json!(u);
    }
    if let Some(sc) = m.get_one::<String>("scopes").filter(|s| !s.is_empty()) {
        let scopes: Vec<&str> = sc.split(',').map(str::trim).collect();
        body["scopes"] = json!(scopes);
    }

    let c = require_client(g)?;
    let resp: OAuthAppResp = c.do_json(
        Method::PATCH,
        &format!("/v1/orgs/{org}/oauth-apps/{id}"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled } }
        return print_json(&mut std::io::stdout(), &resp);
    }
    if let Some(app) = &resp.app {
        println!("Updated OAuth app {:?} ({}).", app.name, app.id);
    } else {
        println!("Updated OAuth app {id}.");
    }
    Ok(())
}

// ── oauth-apps rotate ────────────────────────────────────────────────

fn rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps rotate")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps rotate",
            "Rotate the client_secret for an OAuth application (new secret shown ONCE).",
            &[
                "<id>           App ID (required).",
                "--org <slug>   Org slug (required).",
                "--yes          Skip confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m.get_one::<String>("id").cloned();
    if org.is_empty() || id.is_none() {
        return Err(msg(
            "usage: basin oauth-apps rotate <id> --org=<slug> [--yes]",
        ));
    }
    let id = id.unwrap();

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to rotate client_secret for app {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Rotate client_secret for OAuth app {:?} in org {:?}? Existing clients will break. [y/N] ",
            id, org
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: SecretResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/oauth-apps/{id}/rotate-secret"),
        None,
    )?;
    if g.json {
        // JSON shape: { "app": { id, name, client_id, ... }, "client_secret": "..." }
        // client_secret is reveal-once on rotation.
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Client secret rotated. Store this secret NOW — it will not be shown again:");
    println!("  {}", resp.client_secret);
    if let Some(app) = &resp.app {
        println!("  app: {} ({})", app.name, app.id);
    }
    Ok(())
}

// ── oauth-apps disable ───────────────────────────────────────────────

fn disable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps disable")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps disable",
            "Disable an OAuth application.",
            &[
                "<id>           App ID (required).",
                "--org <slug>   Org slug (required).",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m.get_one::<String>("id").cloned();
    if org.is_empty() || id.is_none() {
        return Err(msg("usage: basin oauth-apps disable <id> --org=<slug>"));
    }
    let id = id.unwrap();
    let c = require_client(g)?;
    let resp: EnabledResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/oauth-apps/{id}/disable"),
        None,
    )?;
    if g.json {
        // JSON shape: { "enabled": false }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Disabled OAuth app {id}.");
    Ok(())
}

// ── oauth-apps enable ────────────────────────────────────────────────

fn enable(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps enable")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps enable",
            "Enable an OAuth application.",
            &[
                "<id>           App ID (required).",
                "--org <slug>   Org slug (required).",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m.get_one::<String>("id").cloned();
    if org.is_empty() || id.is_none() {
        return Err(msg("usage: basin oauth-apps enable <id> --org=<slug>"));
    }
    let id = id.unwrap();
    let c = require_client(g)?;
    let resp: EnabledResp = c.do_json(
        Method::POST,
        &format!("/v1/orgs/{org}/oauth-apps/{id}/enable"),
        None,
    )?;
    if g.json {
        // JSON shape: { "enabled": true }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Enabled OAuth app {id}.");
    Ok(())
}

// ── oauth-apps delete ────────────────────────────────────────────────

fn delete(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("oauth-apps delete")
        .arg(Arg::new("id"))
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "oauth-apps delete",
            "Delete an OAuth application permanently.",
            &[
                "<id>           App ID (required).",
                "--org <slug>   Org slug (required).",
                "--yes          Skip confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let org = m.get_one::<String>("org").cloned().unwrap_or_default();
    let id = m.get_one::<String>("id").cloned();
    if org.is_empty() || id.is_none() {
        return Err(msg(
            "usage: basin oauth-apps delete <id> --org=<slug> [--yes]",
        ));
    }
    let id = id.unwrap();

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to delete OAuth app {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Delete OAuth app {:?} from org {:?}? This cannot be undone. [y/N] ",
            id, org
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: DeletedResp = c
        .do_json(
            Method::DELETE,
            &format!("/v1/orgs/{org}/oauth-apps/{id}"),
            None,
        )
        .map_err(|err| {
            if let Some(ae) = as_api_error(err.as_ref()) {
                if ae.http_status == 404 {
                    return msg(format!("OAuth app {:?} not found in org {:?}", id, org));
                }
            }
            err
        })?;
    if g.json {
        // JSON shape: { "deleted": true, "id": "..." }
        return print_json(&mut std::io::stdout(), &resp);
    }
    println!("Deleted OAuth app {id}.");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{Req, Resp, TestServer};

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

    fn sample_app(id: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"My App","client_id":"client_{id}","redirect_uri":"https://app.example.com/callback","scopes":["read","write"],"enabled":true,"created_at":"2026-05-01T00:00:00Z"}}"#
        )
    }

    // ── dispatcher ────────────────────────────────────────────────────

    #[test]
    fn no_args_shows_help_ok() {
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_oauth_apps(&g, &[]).is_ok());
    }

    #[test]
    fn help_subcommand_returns_ok() {
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_oauth_apps(&g, &["help".into()]).is_ok());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    // ── oauth-apps list ───────────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let app1 = sample_app("app1");
        let app2 = sample_app("app2");
        let body = format!(r#"{{"apps":[{app1},{app2}]}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps");
            Resp::ok(body.clone())
        });
        let g = flags(&srv.url);
        cmd_oauth_apps(&g, &["list".into(), "--org=acme".into()]).unwrap();
    }

    #[test]
    fn list_empty_apps() {
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"apps":[]}"#));
        let g = flags(&srv.url);
        cmd_oauth_apps(&g, &["list".into(), "--org=acme".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let app = sample_app("app1");
        let body = format!(r#"{{"apps":[{app}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: OAuthAppsListResp = c
            .do_json(Method::GET, "/v1/orgs/acme/oauth-apps", None)
            .unwrap();
        assert_eq!(resp.apps.len(), 1);
        assert_eq!(resp.apps[0].id, "app1");
        assert_eq!(resp.apps[0].client_id, "client_app1");
        // client_secret must not be present in list shape.
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "apps": resp.apps })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["apps"].is_array());
        let app_v = &v["apps"][0];
        assert!(
            app_v.get("client_secret").is_none(),
            "client_secret must not appear in list JSON"
        );
    }

    #[test]
    fn list_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(&g, &["list".into()]).unwrap_err();
        assert!(err.to_string().contains("--org"), "got: {err}");
    }

    #[test]
    fn list_server_error_returns_api_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_apps(&g, &["list".into(), "--org=acme".into()]).unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── oauth-apps get ────────────────────────────────────────────────

    #[test]
    fn get_happy_path() {
        let app = sample_app("app1");
        let body = format!(r#"{{"app":{app}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1");
            Resp::ok(body.clone())
        });
        let g = flags(&srv.url);
        cmd_oauth_apps(&g, &["get".into(), "--org=acme".into(), "app1".into()]).unwrap();
    }

    #[test]
    fn get_404_returns_api_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"app not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_apps(&g, &["get".into(), "--org=acme".into(), "notfound".into()])
            .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    #[test]
    fn get_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(&g, &["get".into(), "app1".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    // ── oauth-apps create ─────────────────────────────────────────────

    #[test]
    fn create_happy_path_prints_secret() {
        let app = sample_app("app1");
        let body_resp = format!(r#"{{"app":{app},"client_secret":"cs_supersecret456"}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps");
            let b: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(b["name"].as_str().unwrap(), "My App");
            assert_eq!(
                b["redirect_uri"].as_str().unwrap(),
                "https://app.example.com/callback"
            );
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_oauth_apps(
            &g,
            &[
                "create".into(),
                "--org=acme".into(),
                "--name=My App".into(),
                "--redirect-uri=https://app.example.com/callback".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_json_shape_contains_secret() {
        let app = sample_app("app1");
        let body_resp = format!(r#"{{"app":{app},"client_secret":"cs_supersecret456"}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: SecretResp = c
            .do_json(
                Method::POST,
                "/v1/orgs/acme/oauth-apps",
                Some(json!({"name":"My App","redirect_uri":"https://x.com/cb"})),
            )
            .unwrap();
        assert_eq!(resp.client_secret, "cs_supersecret456");
        assert!(resp.app.is_some());
    }

    #[test]
    fn create_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(
            &g,
            &[
                "create".into(),
                "--name=x".into(),
                "--redirect-uri=https://x.com/cb".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--org"), "got: {err}");
    }

    #[test]
    fn create_missing_name_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(
            &g,
            &[
                "create".into(),
                "--org=acme".into(),
                "--redirect-uri=https://x.com/cb".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--name"), "got: {err}");
    }

    #[test]
    fn create_missing_redirect_uri_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(
            &g,
            &["create".into(), "--org=acme".into(), "--name=x".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("--redirect-uri"), "got: {err}");
    }

    // ── oauth-apps patch ──────────────────────────────────────────────

    #[test]
    fn patch_happy_path() {
        let app_json = r#"{"id":"app1","name":"Updated App","client_id":"client_app1","redirect_uri":"https://app.example.com/callback","scopes":["read","write"],"enabled":true,"created_at":"2026-05-01T00:00:00Z"}"#;
        let body_resp = format!(r#"{{"app":{app_json}}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "PATCH");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1");
            let b: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(b["name"].as_str().unwrap(), "Updated App");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_oauth_apps(
            &g,
            &[
                "patch".into(),
                "--org=acme".into(),
                "--name=Updated App".into(),
                "app1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn patch_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err =
            cmd_oauth_apps(&g, &["patch".into(), "app1".into(), "--name=x".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    // ── oauth-apps rotate ─────────────────────────────────────────────

    #[test]
    fn rotate_happy_path_prints_new_secret() {
        let app = sample_app("app1");
        let body_resp = format!(r#"{{"app":{app},"client_secret":"cs_newrotated789"}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1/rotate-secret");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_oauth_apps(
            &g,
            &[
                "rotate".into(),
                "--org=acme".into(),
                "--yes".into(),
                "app1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn rotate_json_requires_yes() {
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err =
            cmd_oauth_apps(&g, &["rotate".into(), "--org=acme".into(), "app1".into()]).unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn rotate_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err =
            cmd_oauth_apps(&g, &["rotate".into(), "app1".into(), "--yes".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    // ── oauth-apps disable / enable ───────────────────────────────────

    #[test]
    fn disable_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1/disable");
            Resp::ok(r#"{"enabled":false}"#)
        });
        let g = flags_json(&srv.url);
        cmd_oauth_apps(&g, &["disable".into(), "--org=acme".into(), "app1".into()]).unwrap();
    }

    #[test]
    fn enable_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1/enable");
            Resp::ok(r#"{"enabled":true}"#)
        });
        let g = flags_json(&srv.url);
        cmd_oauth_apps(&g, &["enable".into(), "--org=acme".into(), "app1".into()]).unwrap();
    }

    #[test]
    fn disable_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_oauth_apps(&g, &["disable".into(), "app1".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    // ── oauth-apps delete ─────────────────────────────────────────────

    #[test]
    fn delete_happy_path() {
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/orgs/acme/oauth-apps/app1");
            Resp::ok(r#"{"deleted":true,"id":"app1"}"#)
        });
        let g = flags_json(&srv.url);
        cmd_oauth_apps(
            &g,
            &[
                "delete".into(),
                "--org=acme".into(),
                "--yes".into(),
                "app1".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn delete_json_requires_yes() {
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err =
            cmd_oauth_apps(&g, &["delete".into(), "--org=acme".into(), "app1".into()]).unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn delete_missing_org_is_error() {
        let g = GlobalFlags {
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err =
            cmd_oauth_apps(&g, &["delete".into(), "app1".into(), "--yes".into()]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn delete_403_returns_api_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"insufficient role"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_apps(
            &g,
            &[
                "delete".into(),
                "--org=acme".into(),
                "--yes".into(),
                "app1".into(),
            ],
        )
        .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    #[test]
    fn delete_404_returns_friendly_error() {
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"app not found"}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_oauth_apps(
            &g,
            &[
                "delete".into(),
                "--org=acme".into(),
                "--yes".into(),
                "missing".into(),
            ],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }
}
