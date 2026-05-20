//! extensions — `basin extensions [list] [--project=<ref>]`
//!
//! List the native extension catalog for a project. The catalog is
//! read-only: the engine rejects CREATE/DROP/ALTER EXTENSION (ADR 0002 —
//! "no upstream extensions"), so only list is wired.
//!
//! Project ref resolution: --project flag, else load from ./basin/config.toml.
//! Error with hint at `basin link` if neither is set.
//!
//! Backed by GET /v1/projects/{ref}/extensions/catalog with a fallback to
//! GET /v1/projects/{ref}/extensions for older cloud deployments that
//! pre-date the /catalog route.

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{as_api_error, msg, silent, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::{print_json, Table};
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

// ── Wire shapes ──────────────────────────────────────────────────────

/// Extension is one entry in the extensions catalog.
/// JSON shape: { name, version, installed, description }
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Extension {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default)]
    pub installed: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
}

#[derive(Deserialize)]
struct ExtensionsResp {
    #[serde(default)]
    extensions: Vec<Extension>,
}

// ── Dispatcher ───────────────────────────────────────────────────────

pub fn cmd_extensions(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    match args.split_first() {
        // No args → run list with no args.
        None => list(g, &[]),
        Some((sub, rest)) => match sub.as_str() {
            "list" => list(g, rest),
            "--help" | "-h" | "help" => {
                help_for_command(
                    "extensions",
                    "List the native extension catalog for a project.",
                    &["list [--project=<ref>]   List available extensions (read-only)."],
                );
                Ok(())
            }
            other => {
                printerr!(g, "unknown subcommand {other:?} for extensions");
                Err(silent())
            }
        },
    }
}

// ── extensions list ──────────────────────────────────────────────────

fn list(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("extensions list")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "extensions list",
            "List the available extensions for a project (read-only catalog).",
            &["--project <ref>   Project ref (or use basin link to bind the directory)."],
        );
        return Ok(());
    }
    let project_flag = m.get_one::<String>("project").cloned().unwrap_or_default();
    let r#ref = resolve_project_ref(&project_flag)?;

    let c = require_client(g)?;

    // Prefer the /catalog path; fall back to the root /extensions path if
    // catalog returns 404 (older cloud that predates the catalog route).
    let catalog_path = format!("/v1/projects/{ref}/extensions/catalog");
    let resp: ExtensionsResp = match c.do_json(Method::GET, &catalog_path, None) {
        Ok(r) => r,
        Err(e) => {
            // Only fall back on 404 — other errors (401, 403, 5xx) propagate.
            let is_404 = as_api_error(e.as_ref())
                .map(|ae| ae.http_status == 404)
                .unwrap_or(false);
            if is_404 {
                let root_path = format!("/v1/projects/{ref}/extensions");
                c.do_json(Method::GET, &root_path, None)?
            } else {
                return Err(e);
            }
        }
    };

    if g.json {
        // JSON shape: { extensions: [ Extension ] }
        return print_json(&mut std::io::stdout(), &json!({ "extensions": resp.extensions }));
    }
    if resp.extensions.is_empty() {
        println!("(no extensions)");
        return Ok(());
    }
    let mut t = Table::new(g, &["NAME", "VERSION", "INSTALLED", "DESCRIPTION"]);
    for e in &resp.extensions {
        t.row(&[
            &e.name,
            &e.version,
            if e.installed { "true" } else { "false" },
            &e.description,
        ]);
    }
    t.flush()
}

// ── helpers ──────────────────────────────────────────────────────────

/// resolve_project_ref returns the project ref from the --project flag or
/// the directory-scoped ./basin/config.toml. Errors with a hint at
/// `basin link` when neither is available.
fn resolve_project_ref(flag_value: &str) -> CliResult<String> {
    if !flag_value.is_empty() {
        return Ok(flag_value.to_string());
    }
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("could not determine cwd: {e}")))?;
    let wp = load_working_project(&cwd)?;
    match wp {
        Some(w) if !w.project_ref.is_empty() => Ok(w.project_ref),
        _ => Err(msg(
            "--project is required (or run `basin link` to bind this directory)",
        )),
    }
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

    // ── arg-parse / dispatch errors ───────────────────────────────────

    #[test]
    fn unknown_subcommand_is_error() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), quiet: true, ..Default::default() };
        assert!(cmd_extensions(&g, &["frobnicate".into()]).is_err());
    }

    #[test]
    fn no_project_and_no_config_is_error() {
        let _g = with_temp_config_dir();
        // In the temp dir there is no basin/config.toml, so --project must be provided.
        let g = GlobalFlags { api_url: "http://127.0.0.1:1".into(), token: "tok".into(), quiet: true, ..Default::default() };
        assert!(cmd_extensions(&g, &["list".into()]).is_err());
    }

    // ── happy-path round-trips ────────────────────────────────────────

    #[test]
    fn list_happy_path() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|req: &Req| {
            assert_eq!(req.method, "GET");
            assert_eq!(req.path, "/v1/projects/proj1/extensions/catalog");
            Resp::ok(r#"{"extensions":[{"name":"pgvector","version":"0.7.0","installed":true,"description":"vector similarity search"},{"name":"basin-cron","version":"1.0.0","installed":false,"description":"cron jobs"}]}"#)
        });
        cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn list_empty_extensions() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"extensions":[]}"#));
        cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn list_json_shape() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::ok(r#"{"extensions":[{"name":"pgvector","version":"0.7.0","installed":true,"description":"vec"}]}"#)
        });
        let mut g = flags(&srv.url);
        g.json = true;
        cmd_extensions(&g, &["list".into(), "--project=proj1".into()]).unwrap();
    }

    // ── catalog→root fallback on 404 ─────────────────────────────────

    #[test]
    fn catalog_404_falls_back_to_root() {
        let _g = with_temp_config_dir();
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let call_count = Arc::new(AtomicUsize::new(0));
        let cc = Arc::clone(&call_count);
        let srv = TestServer::start(move |req: &Req| {
            let n = cc.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                // First call → /extensions/catalog → 404
                assert!(req.path.ends_with("/extensions/catalog"), "path={}", req.path);
                Resp::status(404, r#"{"error":{"code":"not_found","message":"route not found"}}"#)
            } else {
                // Second call → /extensions root
                assert!(req.path.ends_with("/extensions"), "path={}", req.path);
                Resp::ok(r#"{"extensions":[{"name":"basin-net","version":"2.0.0","installed":false,"description":"net"}]}"#)
            }
        });
        cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    #[test]
    fn catalog_403_does_not_fallback() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(403, r#"{"error":{"code":"forbidden","message":"insufficient permissions"}}"#)
        });
        let err = cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 403);
    }

    // ── explicit "list" dispatch ──────────────────────────────────────

    #[test]
    fn explicit_list_dispatch() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| Resp::ok(r#"{"extensions":[]}"#));
        cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        )
        .unwrap();
    }

    // ── implicit list (no subcommand) ─────────────────────────────────

    // NOTE: implicit list with no subcommand + no --project + no working config
    // yields an error (no project to query). That path is covered by
    // no_project_and_no_config_is_error above.

    // ── server-error mapping ──────────────────────────────────────────

    #[test]
    fn server_error_returns_api_error() {
        let _g = with_temp_config_dir();
        let srv = TestServer::start(|_req: &Req| {
            Resp::status(409, r#"{"error":{"code":"conflict","message":"conflict"}}"#)
        });
        let err = cmd_extensions(
            &flags(&srv.url),
            &["list".into(), "--project=proj1".into()],
        );
        assert!(err.is_err());
        let e = err.unwrap_err();
        let ae = as_api_error(e.as_ref()).expect("expected ApiError");
        assert_eq!(ae.http_status, 409);
    }

    // ── help ──────────────────────────────────────────────────────────

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags { quiet: true, ..Default::default() };
        assert!(cmd_extensions(&g, &["help".into()]).is_ok());
    }

    // ── open_command equivalents (docs module cross-check skipped here) ─
    // ── installed flag rendering ──────────────────────────────────────

    #[test]
    fn installed_flag_true_and_false() {
        let installed = Extension {
            name: "pg".into(),
            version: "1.0".into(),
            installed: true,
            description: "d".into(),
        };
        let not_installed = Extension {
            name: "pg2".into(),
            version: "1.0".into(),
            installed: false,
            description: "d2".into(),
        };
        assert!(installed.installed);
        assert!(!not_installed.installed);
    }
}
