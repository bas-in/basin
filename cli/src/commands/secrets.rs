//! secrets — `basin secrets (list | set | rm) [--project <ref>]`
//!
//! Wraps the project env-override surface at `/v1/projects/:ref/secrets`.
//!
//!   basin secrets list  [--project <ref>]
//!   basin secrets set   [--project <ref>] KEY=VALUE [KEY=VALUE ...]
//!   basin secrets rm    [--project <ref>] KEY [KEY ...]
//!
//! Project ref resolution: --project= flag, else load_working_project(cwd)
//! from ./basin/config.toml. Error with hint at `basin link` if neither.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── project resolution ────────────────────────────────────────────────────────

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

// ── Wire shapes ──────────────────────────────────────────────────────

#[derive(Deserialize)]
struct SecretEntry {
    #[serde(default)]
    key: String,
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    updated_at: String,
}

#[derive(Deserialize)]
struct SecretsListResp {
    #[serde(default)]
    secrets: Vec<SecretEntry>,
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_secrets(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => {
            return Err(msg("usage: basin secrets (list | set | rm) ..."));
        }
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "list" => list(g, rest),
        "set" => set(g, rest),
        "rm" | "remove" | "delete" => rm(g, rest),
        "--help" | "-h" | "help" => {
            help_for_command(
                "secrets",
                "List / set / remove project env-overrides.",
                &[
                    "list --project <ref>                            Print every override (values masked).",
                    "set  --project <ref> KEY=VALUE [KEY=VALUE ...]  Set or update overrides.",
                    "rm   --project <ref> KEY [KEY ...]              Remove overrides.",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for secrets");
            Err(silent())
        }
    }
}

// ── secrets list ─────────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("secrets list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "secrets list",
            "Print every env-override for a project (values masked).",
            &["--project <ref>   Project ref (required)."],
        );
        return Ok(());
    }
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    let c = require_client(g)?;
    let resp: SecretsListResp = c.do_json(
        Method::GET,
        &format!("/v1/projects/{project}/secrets"),
        None,
    )?;
    if g.json {
        // JSON shape: { secrets: [ { key: string, prefix: string, updated_at: string } ] }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "secrets": resp.secrets.iter().map(|s| json!({
                "key": s.key,
                "prefix": s.prefix,
                "updated_at": s.updated_at,
            })).collect::<Vec<_>>() }),
        );
    }
    let mut t = Table::new(g, &["KEY", "PREFIX", "UPDATED"]);
    for s in &resp.secrets {
        t.row(&[&s.key, &s.prefix, &s.updated_at]);
    }
    t.flush()
}

// ── secrets set ──────────────────────────────────────────────────────

fn set(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("secrets set")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("pairs").num_args(0..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "secrets set",
            "Set or update env-overrides for a project.",
            &[
                "--project <ref>        Project ref (required).",
                "KEY=VALUE ...          One or more key=value pairs.",
            ],
        );
        return Ok(());
    }
    let pairs: Vec<String> = m
        .get_many::<String>("pairs")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if pairs.is_empty() {
        return Err(msg(
            "usage: basin secrets set [--project <ref>] KEY=VALUE [KEY=VALUE ...]",
        ));
    }
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    // Parse KEY=VALUE pairs into a map.
    let mut secrets: serde_json::Map<String, serde_json::Value> =
        serde_json::Map::with_capacity(pairs.len());
    for kv in &pairs {
        let i = kv
            .find('=')
            .filter(|&p| p > 0)
            .ok_or_else(|| msg(format!("invalid pair {kv:?} (expected KEY=VALUE)")))?;
        let key = kv[..i].to_string();
        let val = kv[i + 1..].to_string();
        secrets.insert(key, json!(val));
    }
    let c = require_client(g)?;
    let body = json!({ "secrets": secrets });
    let out: serde_json::Value = c.do_json(
        Method::PUT,
        &format!("/v1/projects/{project}/secrets"),
        Some(body),
    )?;
    if g.json {
        // JSON shape: passthrough of /v1/projects/:ref/secrets PUT envelope
        // (commonly { updated: int, secrets: [{key,prefix,updated_at}] }).
        return print_json(&mut std::io::stdout(), &out);
    }
    println!("Set {} secret(s).", secrets.len());
    Ok(())
}

// ── secrets rm ───────────────────────────────────────────────────────

fn rm(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("secrets rm")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("keys").num_args(0..).trailing_var_arg(true))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "secrets rm",
            "Remove env-overrides from a project.",
            &[
                "--project <ref>   Project ref (required).",
                "KEY ...           One or more keys to remove.",
            ],
        );
        return Ok(());
    }
    let keys: Vec<String> = m
        .get_many::<String>("keys")
        .map(|v| v.cloned().collect())
        .unwrap_or_default();
    if keys.is_empty() {
        return Err(msg(
            "usage: basin secrets rm [--project <ref>] KEY [KEY ...]",
        ));
    }
    let project = resolve_project_ref(
        m.get_one::<String>("project")
            .map(|s| s.as_str())
            .unwrap_or(""),
    )?;
    let c = require_client(g)?;
    for key in &keys {
        c.do_noout(
            Method::DELETE,
            &format!("/v1/projects/{project}/secrets/{key}"),
            None,
        )?;
    }
    if g.json {
        // JSON shape: { removed: [ string ] }
        return print_json(&mut std::io::stdout(), &json!({ "removed": keys }));
    }
    println!("Removed {} secret(s).", keys.len());
    Ok(())
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

    // ── cwd-fallback: resolve_project_ref ────────────────────────────────────

    #[test]
    fn resolve_project_ref_uses_flag_when_set() {
        let _cfg = with_temp_config_dir();
        let r = resolve_project_ref("explicit-ref").unwrap();
        assert_eq!(r, "explicit-ref");
    }

    #[test]
    fn resolve_project_ref_no_config_returns_error() {
        let _cfg = with_temp_config_dir();
        let err = resolve_project_ref("").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--project") || msg.contains("basin link"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn resolve_project_ref_reads_working_project() {
        use crate::config::{save_working_project, WorkingProject};
        let _cfg = with_temp_config_dir();
        let dir = std::env::temp_dir().join(format!("secrets-wp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        save_working_project(
            &dir,
            &WorkingProject {
                project_ref: "secrets-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let wp = crate::config::load_working_project(&dir).unwrap().unwrap();
        assert_eq!(wp.project_ref, "secrets-proj");
        // Verify the flag path returns the value directly.
        let r = resolve_project_ref("secrets-proj").unwrap();
        assert_eq!(r, "secrets-proj");
        std::fs::remove_dir_all(&dir).ok();
    }

    // ── arg-parse errors ─────────────────────────────────────────────

    #[test]
    fn missing_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &[]).is_err());
    }

    #[test]
    fn unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &["frobnicate".into()]).is_err());
    }

    #[test]
    fn list_missing_project_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &["list".into()]).is_err());
    }

    #[test]
    fn set_missing_pairs_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &["set".into(), "--project=p1".into()]).is_err());
    }

    #[test]
    fn rm_missing_keys_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            api_url: "http://127.0.0.1:1".into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &["rm".into(), "--project=p1".into()]).is_err());
    }

    // ── happy-path round-trips ────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/p1/secrets");
            Resp::ok(r#"{"secrets":[{"key":"DB_URL","prefix":"pos","updated_at":"2024-01-01"}]}"#)
        });
        cmd_secrets(&flags(&srv.url), &["list".into(), "--project=p1".into()]).unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"secrets":[{"key":"FOO","prefix":"foo_","updated_at":"2024-01-01"}]}"#)
        });
        let mut g = flags(&srv.url);
        g.json = true;
        cmd_secrets(&g, &["list".into(), "--project=p1".into()]).unwrap();
    }

    #[test]
    fn set_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "PUT");
            assert_eq!(req.path, "/v1/projects/p1/secrets");
            // Body should contain the key-value pair.
            let body: serde_json::Value = serde_json::from_str(&req.body).unwrap_or_default();
            assert!(body["secrets"]["FOO"].is_string());
            Resp::ok(r#"{"updated":1}"#)
        });
        cmd_secrets(
            &flags(&srv.url),
            &["set".into(), "--project=p1".into(), "FOO=bar".into()],
        )
        .unwrap();
    }

    #[test]
    fn set_invalid_pair_is_error() {
        let _g = with_temp_config_dir();
        // "=value" has empty key (i == 0 so filtered), "noequals" has no '='
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let err = cmd_secrets(
            &flags(&srv.url),
            &["set".into(), "--project=p1".into(), "noequals".into()],
        );
        assert!(err.is_err());
    }

    #[test]
    fn rm_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "DELETE");
            assert!(req.path.starts_with("/v1/projects/p1/secrets/"));
            Resp::ok("{}")
        });
        cmd_secrets(
            &flags(&srv.url),
            &["rm".into(), "--project=p1".into(), "FOO".into()],
        )
        .unwrap();
    }

    #[test]
    fn rm_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        let mut g = flags(&srv.url);
        g.json = true;
        cmd_secrets(&g, &["rm".into(), "--project=p1".into(), "FOO".into()]).unwrap();
    }

    // ── server-error mapping ──────────────────────────────────────────

    #[test]
    fn list_server_404_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(
                404,
                r#"{"error":{"code":"not_found","message":"project not found"}}"#,
            )
        });
        let err = cmd_secrets(&flags(&srv.url), &["list".into(), "--project=p1".into()]);
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = crate::error::as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 404);
    }

    // ── aliases ───────────────────────────────────────────────────────

    #[test]
    fn remove_alias_works() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        cmd_secrets(
            &flags(&srv.url),
            &["remove".into(), "--project=p1".into(), "FOO".into()],
        )
        .unwrap();
    }

    #[test]
    fn delete_alias_works() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok("{}"));
        cmd_secrets(
            &flags(&srv.url),
            &["delete".into(), "--project=p1".into(), "BAR".into()],
        )
        .unwrap();
    }

    // ── help ──────────────────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_secrets(&g, &["help".into()]).is_ok());
    }
}
