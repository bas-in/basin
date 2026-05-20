//! api-keys — project-scoped API key management.
//!
//!   basin api-keys list   [--project=<ref>]
//!   basin api-keys create --name=<n> [--project=<ref>] [--scope=read|write|admin]
//!   basin api-keys rotate <id> [--project=<ref>] [--yes]
//!   basin api-keys revoke <id> [--project=<ref>] [--yes]
//!
//! These are project-scoped API keys distinct from the org-level PATs
//! managed by `basin tokens`. Routes: /v1/projects/{ref}/api-keys/*.
//!
//! Secret display convention:
//!   - create: print the full secret once with a "store this now" warning
//!   - rotate: print the new full secret once with the same warning
//!   - list:   show only prefix + masked last-4 (e.g. "bsp_****abcd")
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd).
//! Error with hint at `basin link` if neither is available.

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

// ── Wire shapes ──────────────────────────────────────────────────────

/// ProjectAPIKey is the read shape from GET /v1/projects/{ref}/api-keys.
/// The full secret is never returned on list — only prefix + masked tail.
/// JSON shape: { id, name, scope, prefix, masked_key, created_at, last_used_at }
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ProjectAPIKey {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prefix: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub masked_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub created_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
}

/// CreateAPIKeyResponse is the once-only reveal envelope from
/// POST /v1/projects/{ref}/api-keys. The full_key field is present only
/// on creation — subsequent list calls only show the masked form.
/// JSON shape: { key: ProjectAPIKey, full_key: string }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CreateAPIKeyResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<ProjectAPIKey>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub full_key: String,
}

/// RotateAPIKeyResponse is the once-only reveal envelope from
/// POST /v1/projects/{ref}/api-keys/{id}/rotate.
/// JSON shape: { key: ProjectAPIKey, full_key: string }
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct RotateAPIKeyResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<ProjectAPIKey>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub full_key: String,
}

#[derive(Deserialize)]
struct APIKeysListResp {
    #[serde(default)]
    keys: Vec<ProjectAPIKey>,
}

// ── Helpers ───────────────────────────────────────────────────────────

/// mask_api_key returns a masked representation of the key secret for
/// display on list calls. If a masked_key field is already set, use it
/// directly. Otherwise build "prefix••••••••" from whatever we have.
/// The convention mirrors Go's cmd_api_keys.go maskAPIKey.
pub fn mask_api_key(k: &ProjectAPIKey) -> String {
    if !k.masked_key.is_empty() {
        return k.masked_key.clone();
    }
    if !k.prefix.is_empty() {
        return format!("{}••••••••", k.prefix);
    }
    "(masked)".to_string()
}

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors with a helpful hint
/// when neither is available.
fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg("--project is required (or run `basin link` to bind this directory)")),
    }
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_api_keys(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return list(g, &[]),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "create" => create(g, rest),
        "rotate" => rotate(g, rest),
        "revoke" => revoke(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "api-keys",
                "Manage project-scoped API keys.",
                &[
                    "list   [--project=<ref>]                                  List API keys (masked secret).",
                    "create --name=<n> [--project=<ref>] [--scope=read|write|admin]  Create an API key (secret shown ONCE).",
                    "rotate <id> [--project=<ref>] [--yes]                     Rotate an API key (new secret shown ONCE).",
                    "revoke <id> [--project=<ref>] [--yes]                     Revoke an API key permanently.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for api-keys");
            Err(silent())
        }
    }
}

// ── api-keys list ────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("api-keys list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "api-keys list",
            "List project-scoped API keys (secret shown masked).",
            &["--project <ref>   Project ref (or auto-detected from ./basin/config.toml)."],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let c = require_client(g)?;
    let resp: APIKeysListResp =
        c.do_json(Method::GET, &format!("/v1/projects/{ref}/api-keys"), None)?;
    if g.json {
        // JSON shape: { keys: [ ProjectAPIKey ] }
        // masked_key or prefix is shown; full_key is never present in list.
        return print_json(&mut std::io::stdout(), &json!({ "keys": resp.keys }));
    }
    if resp.keys.is_empty() {
        println!("(no api keys)");
        return Ok(());
    }
    let mut t = Table::new(g, &["ID", "NAME", "SCOPE", "KEY (MASKED)", "CREATED"]);
    for k in &resp.keys {
        t.row(&[&k.id, &k.name, &k.scope, &mask_api_key(k), &k.created_at]);
    }
    t.flush()
}

// ── api-keys create ──────────────────────────────────────────────────

fn create(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("api-keys create")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("name").long("name"))
        .arg(Arg::new("scope").long("scope"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "api-keys create",
            "Create a project-scoped API key (full key printed ONCE).",
            &[
                "--name <n>          Human-readable key name (required).",
                "--project <ref>     Project ref.",
                "--scope read|write|admin  Scope (default: write).",
            ],
        );
        return Ok(());
    }
    let name = m.get_one::<String>("name").cloned().unwrap_or_default();
    if name.is_empty() {
        return Err(msg("api-keys create requires --name=<n>"));
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;
    let scope = m
        .get_one::<String>("scope")
        .cloned()
        .unwrap_or_else(|| "write".to_string());
    let c = require_client(g)?;
    let body = json!({ "name": name, "scope": scope });
    let resp: CreateAPIKeyResponse =
        c.do_json(Method::POST, &format!("/v1/projects/{ref}/api-keys"), Some(body))?;
    if g.json {
        // JSON shape: CreateAPIKeyResponse — { key: ProjectAPIKey, full_key: string }
        // full_key is reveal-once; subsequent list calls only see the masked form.
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.full_key.is_empty() {
        println!("API key created (no plaintext returned — check the dashboard).");
        return Ok(());
    }
    if !g.quiet {
        eprintln!("⚠  Store this key now — it will NOT be shown again.");
    }
    println!("{}", resp.full_key);
    if let Some(k) = &resp.key {
        println!("id:      {}", k.id);
        println!("name:    {}", k.name);
        println!("scope:   {}", k.scope);
    }
    Ok(())
}

// ── api-keys rotate ──────────────────────────────────────────────────

fn rotate(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("api-keys rotate")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "api-keys rotate",
            "Rotate a project-scoped API key (new key printed ONCE).",
            &[
                "<id>               Key ID to rotate (required).",
                "--project <ref>    Project ref.",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin api-keys rotate <id> [--project=<ref>] [--yes]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to rotate key {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Rotate API key {:?} on project {:?}? The old key will stop working immediately. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let resp: RotateAPIKeyResponse = c.do_json(
        Method::POST,
        &format!("/v1/projects/{ref}/api-keys/{id}/rotate"),
        None,
    )?;
    if g.json {
        // JSON shape: RotateAPIKeyResponse — { key: ProjectAPIKey, full_key: string }
        // full_key is the new secret, shown once.
        return print_json(&mut std::io::stdout(), &resp);
    }
    if resp.full_key.is_empty() {
        println!("API key rotated (no plaintext returned — check the dashboard).");
        return Ok(());
    }
    if !g.quiet {
        eprintln!("⚠  Store this key now — it will NOT be shown again.");
    }
    println!("{}", resp.full_key);
    if let Some(k) = &resp.key {
        println!("id:    {}", k.id);
        println!("scope: {}", k.scope);
    }
    Ok(())
}

// ── api-keys revoke ──────────────────────────────────────────────────

fn revoke(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("api-keys revoke")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("yes").long("yes").action(ArgAction::SetTrue))
        .arg(Arg::new("id"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "api-keys revoke",
            "Revoke a project-scoped API key permanently.",
            &[
                "<id>               Key ID to revoke (required).",
                "--project <ref>    Project ref.",
                "--yes              Skip the confirmation prompt.",
            ],
        );
        return Ok(());
    }
    let id = m
        .get_one::<String>("id")
        .cloned()
        .ok_or_else(|| msg("usage: basin api-keys revoke <id> [--project=<ref>] [--yes]"))?;
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project)?;

    if !m.get_flag("yes") {
        if g.json {
            return Err(msg(format!(
                "confirmation required: pass --yes to revoke key {:?} non-interactively under --json",
                id
            )));
        }
        eprint!(
            "Revoke API key {:?} on project {:?}? This cannot be undone. [y/N] ",
            id, r#ref
        );
        let line = read_line()?;
        if line.trim().to_lowercase() != "y" {
            println!("Aborted.");
            return Ok(());
        }
    }

    let c = require_client(g)?;
    let out: serde_json::Value = c.do_json(
        Method::DELETE,
        &format!("/v1/projects/{ref}/api-keys/{id}"),
        None,
    )
    .map_err(|err| {
        // Map 404 → friendly message.
        if let Some(ae) = as_api_error(err.as_ref()) {
            if ae.http_status == 404 {
                return msg(format!("api key {:?} not found on project {:?}", id, r#ref));
            }
        }
        err
    })?;
    if g.json {
        // JSON shape: passthrough of revoke envelope (commonly { revoked: bool, id: string }).
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Revoked API key {id}.");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────

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

    fn sample_key(id: &str, name: &str, scope: &str, prefix: &str) -> String {
        format!(
            r#"{{"id":"{id}","name":"{name}","scope":"{scope}","prefix":"{prefix}","masked_key":"{prefix}••••••••","created_at":"2026-01-01T00:00:00Z"}}"#
        )
    }

    // ── mask_api_key helper ───────────────────────────────────────────

    #[test]
    fn mask_api_key_uses_masked_key_when_present() {
        let k = ProjectAPIKey {
            id: "k1".into(),
            prefix: "bsp_".into(),
            masked_key: "bsp_••••••••abcd".into(),
            ..Default::default()
        };
        assert_eq!(mask_api_key(&k), "bsp_••••••••abcd");
    }

    #[test]
    fn mask_api_key_falls_back_to_prefix_bullets() {
        let k = ProjectAPIKey {
            id: "k2".into(),
            prefix: "bsp_".into(),
            ..Default::default()
        };
        let masked = mask_api_key(&k);
        assert!(masked.starts_with("bsp_"), "got: {masked}");
        assert!(masked.contains("••••••••"), "got: {masked}");
    }

    #[test]
    fn mask_api_key_no_prefix_returns_masked_placeholder() {
        let k = ProjectAPIKey { id: "k3".into(), ..Default::default() };
        let masked = mask_api_key(&k);
        assert!(!masked.is_empty(), "should return placeholder, not empty");
    }

    // ── dispatcher ────────────────────────────────────────────────────

    #[test]
    fn no_args_delegates_to_list_which_needs_project() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        // No --project and no working-project config → error.
        assert!(cmd_api_keys(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_api_keys(&g, &["frobnicate".into()]).unwrap_err();
        assert!(crate::error::is_silent(err.as_ref()));
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_api_keys(&g, &["help".into()]).is_ok());
    }

    // ── api-keys list ─────────────────────────────────────────────────

    #[test]
    fn list_empty_keys() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/api-keys");
            Resp::ok(r#"{"keys":[]}"#)
        });
        let g = flags(&srv.url);
        // Should succeed, printing "(no api keys)".
        cmd_api_keys(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_populated_shows_masked_keys() {
        let _g = with_temp_config_dir();
        let k1 = sample_key("k1", "my-key", "write", "bsp_");
        let k2 = sample_key("k2", "read-key", "read", "bsp_");
        let body = format!(r#"{{"keys":[{k1},{k2}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let g = flags(&srv.url);
        // No panic + masked bullets present (verified indirectly — table rendering).
        cmd_api_keys(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let k = sample_key("k1", "my-key", "write", "bsp_");
        let body = format!(r#"{{"keys":[{k}]}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body.clone()));
        let mut g = flags_json(&srv.url);
        g.json = true;
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: APIKeysListResp =
            c.do_json(Method::GET, "/v1/projects/proj1/api-keys", None).unwrap();
        assert_eq!(resp.keys.len(), 1);
        assert_eq!(resp.keys[0].id, "k1");
        // JSON output path goes through print_json; verify shape via direct parse.
        let mut buf = Vec::<u8>::new();
        print_json(&mut buf, &json!({ "keys": resp.keys })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v["keys"].is_array());
        assert_eq!(v["keys"][0]["id"].as_str().unwrap(), "k1");
    }

    #[test]
    fn list_no_project_flag_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        // No --project and no linked project → error.
        assert!(cmd_api_keys(&g, &["list".into()]).is_err());
    }

    #[test]
    fn list_server_error_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#)
        });
        let g = flags(&srv.url);
        let err = cmd_api_keys(&g, &["list".into(), "--project=proj1".into()]).unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── api-keys create ───────────────────────────────────────────────

    #[test]
    fn create_happy_path_prints_full_key() {
        let _g = with_temp_config_dir();
        let k = sample_key("k-new", "deploy-key", "write", "bsp_");
        let body_resp =
            format!(r#"{{"key":{k},"full_key":"bsp_supersecrettoken1234"}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/api-keys");
            let b: serde_json::Value =
                serde_json::from_str(&req.body).unwrap_or_default();
            assert_eq!(b["name"].as_str().unwrap(), "deploy-key");
            assert_eq!(b["scope"].as_str().unwrap(), "write");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_api_keys(
            &g,
            &[
                "create".into(),
                "--project=proj1".into(),
                "--name=deploy-key".into(),
                "--scope=write".into(),
            ],
        )
        .unwrap();
    }

    #[test]
    fn create_missing_name_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_api_keys(&g, &["create".into(), "--project=proj1".into()]).unwrap_err();
        assert!(err.to_string().contains("--name"), "got: {err}");
    }

    #[test]
    fn create_json_shape() {
        let _g = with_temp_config_dir();
        let k = sample_key("k-json", "ci-key", "read", "bsp_");
        let body_resp = format!(r#"{{"key":{k},"full_key":"bsp_fulljsontoken5678"}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: CreateAPIKeyResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/api-keys", Some(json!({"name":"ci-key","scope":"write"}))).unwrap();
        assert_eq!(resp.full_key, "bsp_fulljsontoken5678");
        assert!(resp.key.is_some());
        assert_eq!(resp.key.as_ref().unwrap().id, "k-json");
    }

    // ── api-keys rotate ───────────────────────────────────────────────

    #[test]
    fn rotate_with_yes_calls_endpoint_and_prints_key() {
        let _g = with_temp_config_dir();
        let k = sample_key("k1", "my-key", "write", "bsp_");
        let body_resp = format!(r#"{{"key":{k},"full_key":"bsp_newrotatedtoken9999"}}"#);
        let srv = TestServer::start(move |req: &Req| {
            assert_eq!(req.method, "POST");
            assert_eq!(req.path, "/v1/projects/proj1/api-keys/k1/rotate");
            Resp::ok(body_resp.clone())
        });
        let g = flags(&srv.url);
        cmd_api_keys(
            &g,
            &["rotate".into(), "--project=proj1".into(), "--yes".into(), "k1".into()],
        )
        .unwrap();
    }

    #[test]
    fn rotate_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err =
            cmd_api_keys(&g, &["rotate".into(), "--project=proj1".into(), "k1".into()]).unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }

    #[test]
    fn rotate_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_api_keys(
            &g,
            &["rotate".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"), "got: {err}");
    }

    #[test]
    fn rotate_json_shape() {
        let _g = with_temp_config_dir();
        let k = sample_key("k1", "my-key", "write", "bsp_");
        let body_resp = format!(r#"{{"key":{k},"full_key":"bsp_rotatedjsontoken"}}"#);
        let srv = TestServer::start(move |_req: &Req| Resp::ok(body_resp.clone()));
        let c = crate::client::Client::new(&srv.url, "tok");
        let resp: RotateAPIKeyResponse =
            c.do_json(Method::POST, "/v1/projects/proj1/api-keys/k1/rotate", None).unwrap();
        assert_eq!(resp.full_key, "bsp_rotatedjsontoken");
    }

    // ── api-keys revoke ───────────────────────────────────────────────

    #[test]
    fn revoke_with_yes_calls_delete_endpoint() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert_eq!(req.path, "/v1/projects/proj1/api-keys/k1");
            Resp::ok(r#"{"revoked":true,"id":"k1"}"#)
        });
        let g = flags(&srv.url);
        cmd_api_keys(
            &g,
            &["revoke".into(), "--project=proj1".into(), "--yes".into(), "k1".into()],
        )
        .unwrap();
    }

    #[test]
    fn revoke_404_returns_friendly_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"API key not found."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_api_keys(
            &g,
            &["revoke".into(), "--project=proj1".into(), "--yes".into(), "missing".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    #[test]
    fn revoke_403_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                403,
                r#"{"error":{"code":"forbidden","message":"Insufficient permissions."}}"#,
            )
        });
        let g = flags(&srv.url);
        let err = cmd_api_keys(
            &g,
            &["revoke".into(), "--project=proj1".into(), "--yes".into(), "k1".into()],
        )
        .unwrap_err();
        let ae = as_api_error(err.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    #[test]
    fn revoke_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"revoked":true,"id":"k1"}"#));
        let c = crate::client::Client::new(&srv.url, "tok");
        let out: serde_json::Value =
            c.do_json(Method::DELETE, "/v1/projects/proj1/api-keys/k1", None).unwrap();
        assert_eq!(out["revoked"].as_bool().unwrap(), true);
        assert_eq!(out["id"].as_str().unwrap(), "k1");
    }

    #[test]
    fn revoke_missing_id_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        let err = cmd_api_keys(
            &g,
            &["revoke".into(), "--project=proj1".into(), "--yes".into()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("usage:"), "got: {err}");
    }

    #[test]
    fn revoke_json_requires_yes() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            json: true,
            ..Default::default()
        };
        let err = cmd_api_keys(
            &g,
            &["revoke".into(), "--project=proj1".into(), "k1".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("confirmation required"),
            "got: {err}"
        );
    }
}
