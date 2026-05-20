//! link — `basin link --project=<ref> [--force]`
//!
//! Validates the project ref via `GET /v1/projects/{ref}`, then writes
//! `project_ref` into `./basin/config.toml`. Refuses to overwrite an
//! existing binding unless `--force` is supplied.
//!
//! CWD-dependent logic lives in `run_link(…, cwd)` so tests can pass a
//! tempdir path directly without touching the process-global cwd.

use std::path::Path;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::config::{load_working_project, save_working_project};
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::print_json;
use crate::printinfo;

use super::help::help_for_command;
use super::parse_or_silent;

#[derive(Deserialize)]
struct ProjectResp {
    #[serde(default)]
    project: Option<LinkedProject>,
}

/// A minimal project shape we need for the `link` display.
#[derive(Deserialize, Default)]
struct LinkedProject {
    #[serde(default, rename = "ref")]
    r#ref: String,
    #[serde(default)]
    name: String,
}

pub fn cmd_link(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("link")
        .arg(Arg::new("project").long("project"))
        .arg(Arg::new("force").long("force").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "link",
            "Bind this directory to a remote project.",
            &[
                "--project=<ref>   Project ref to link (required).",
                "--org=<slug>      Org slug (optional, for disambiguation).",
                "--force           Overwrite an existing project binding.",
            ],
        );
        return Ok(());
    }
    let project = m.get_one::<String>("project").cloned().unwrap_or_default();
    if project.is_empty() {
        return Err(msg("link: --project=<ref> is required"));
    }
    let force = m.get_flag("force");
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("link: could not determine working directory: {e}")))?;
    run_link(g, &cwd, &project, force)
}

/// run_link performs the actual link logic rooted at `cwd`.
/// Extracted so tests can supply a temp path without process-global chdir.
fn run_link(g: &GlobalFlags, cwd: &Path, project_ref: &str, force: bool) -> CliResult<()> {
    let basin_dir = cwd.join("basin");

    // Require basin/ to already exist (created by `basin init`).
    if !basin_dir.is_dir() {
        if g.json {
            // JSON shape: { "linked": false, "error": string }
            return print_json(
                &mut std::io::stdout(),
                &json!({
                    "linked": false,
                    "error": "./basin/ not found — run `basin init` first",
                }),
            );
        }
        return Err(msg("link: ./basin/ not found — run `basin init` first"));
    }

    // Load current config to detect an existing binding.
    let mut wp = load_working_project(cwd)
        .map_err(|e| msg(format!("link: {e}")))?
        .unwrap_or_default();

    let prev_ref = wp.project_ref.clone();

    // Already linked to a *different* ref? Refuse unless --force.
    if !prev_ref.is_empty() && prev_ref != project_ref && !force {
        if g.json {
            // JSON shape: { "linked": false, "error": string, "current_ref": string }
            return print_json(
                &mut std::io::stdout(),
                &json!({
                    "linked": false,
                    "error": format!("already linked to {:?} — use --force to overwrite", prev_ref),
                    "current_ref": prev_ref,
                }),
            );
        }
        return Err(msg(format!(
            "link: already linked to {:?} — use --force to overwrite",
            prev_ref
        )));
    }

    // Validate the ref against the cloud.
    let c = require_client(g)?;
    let resp: ProjectResp =
        match c.do_json(Method::GET, &format!("/v1/projects/{project_ref}"), None) {
            Ok(r) => r,
            Err(e) => {
                // Downcast to ApiError to check for 404.
                let is_404 = e
                    .downcast_ref::<crate::error::ApiError>()
                    .map(|ae| ae.http_status == 404)
                    .unwrap_or(false);
                if is_404 {
                    if g.json {
                        // JSON shape: { "linked": false, "error": string, "project_ref": string }
                        return print_json(
                            &mut std::io::stdout(),
                            &json!({
                                "linked": false,
                                "error": format!("project {:?} not found", project_ref),
                                "project_ref": project_ref,
                            }),
                        );
                    }
                    return Err(msg(format!("link: project {:?} not found", project_ref)));
                }
                return Err(e);
            }
        };

    // Write the binding (preserving other fields).
    wp.project_ref = project_ref.to_string();
    save_working_project(cwd, &wp)
        .map_err(|e| msg(format!("link: write config.toml: {e}")))?;

    if g.json {
        // JSON shape: { "project_ref": string, "linked": true, "previous_ref": string }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "project_ref": project_ref,
                "linked": true,
                "previous_ref": prev_ref,
            }),
        );
    }

    if !prev_ref.is_empty() && prev_ref != project_ref {
        printinfo!(g, "Re-linked: {} → {}", prev_ref, project_ref);
    } else {
        let name = resp
            .project
            .as_ref()
            .filter(|p| !p.name.is_empty())
            .map(|p| format!("{} ({})", p.name, project_ref))
            .unwrap_or_else(|| project_ref.to_string());
        printinfo!(g, "Linked to project {}.", name);
    }
    println!("project_ref = {project_ref}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkingProject;
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("basin-link-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn scaffold(cwd: &Path) {
        // Create basin/migrations/ and an empty config.toml.
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        save_working_project(cwd, &WorkingProject::default()).unwrap();
    }

    fn srv_known(refs: &[&str]) -> TestServer {
        let refs: Vec<String> = refs.iter().map(|r| r.to_string()).collect();
        TestServer::start(move |req: &Req| {
            let prefix = "/v1/projects/";
            if req.method == "GET" && req.path.starts_with(prefix) {
                let r = &req.path[prefix.len()..];
                let r = r.split('/').next().unwrap_or("");
                if refs.iter().any(|k| k == r) {
                    return Resp::ok(format!(
                        r#"{{"project":{{"ref":"{r}","name":"Demo Project","region":"jnb","status":"active"}}}}"#
                    ));
                }
                return Resp::status(
                    404,
                    r#"{"error":{"code":"not_found","message":"project not found"}}"#,
                );
            }
            Resp::status(404, r#""#)
        })
    }

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags { api_url: url.into(), token: "tok".into(), quiet: true, ..Default::default() }
    }

    #[test]
    fn link_success_writes_project_ref() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("success");
        scaffold(&cwd);
        let srv = srv_known(&["proj-1"]);
        let g = flags(&srv.url);

        run_link(&g, &cwd, "proj-1", false).unwrap();

        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "proj-1");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_not_found_errors_with_ref_name() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("notfound");
        scaffold(&cwd);
        let srv = srv_known(&[]);
        let g = flags(&srv.url);

        let err = run_link(&g, &cwd, "does-not-exist", false).unwrap_err();
        assert!(
            err.to_string().contains("does-not-exist"),
            "error should mention the ref: {err}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_already_linked_no_force() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("alreadylinked");
        scaffold(&cwd);
        save_working_project(&cwd, &WorkingProject { project_ref: "proj-1".into(), ..Default::default() }).unwrap();

        let srv = srv_known(&["proj-2"]);
        let g = flags(&srv.url);

        let err = run_link(&g, &cwd, "proj-2", false).unwrap_err();
        assert!(err.to_string().contains("--force"), "error should mention --force: {err}");

        // Binding unchanged.
        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "proj-1");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_already_linked_with_force() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("force");
        scaffold(&cwd);
        save_working_project(&cwd, &WorkingProject { project_ref: "proj-1".into(), ..Default::default() }).unwrap();

        let srv = srv_known(&["proj-2"]);
        let g = flags(&srv.url);

        run_link(&g, &cwd, "proj-2", true).unwrap();

        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "proj-2");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_missing_basin_dir() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("nodir");
        // No scaffold — basin/ does not exist.
        let srv = srv_known(&["proj-1"]);
        let g = flags(&srv.url);

        let err = run_link(&g, &cwd, "proj-1", false).unwrap_err();
        assert!(
            err.to_string().contains("basin init"),
            "error should hint at `basin init`: {err}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_same_ref_idempotent() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("samref");
        scaffold(&cwd);
        save_working_project(&cwd, &WorkingProject { project_ref: "same-proj".into(), ..Default::default() }).unwrap();

        let srv = srv_known(&["same-proj"]);
        let g = flags(&srv.url);

        run_link(&g, &cwd, "same-proj", false).unwrap();

        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "same-proj");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_preserves_existing_fields() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("preserve");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: String::new(),
                default_branch: "dev".into(),
                engine_version_pin: "0.5.0".into(),
            },
        )
        .unwrap();

        let srv = srv_known(&["proj-x"]);
        let g = flags(&srv.url);

        run_link(&g, &cwd, "proj-x", false).unwrap();

        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "proj-x");
        assert_eq!(wp.default_branch, "dev");
        assert_eq!(wp.engine_version_pin, "0.5.0");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_json_success_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-ok");
        scaffold(&cwd);
        let srv = srv_known(&["proj-json"]);
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };

        // run_link writes to stdout; we just verify it returns Ok.
        run_link(&g, &cwd, "proj-json", false).unwrap();
        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "proj-json");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_json_404_envelope() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-404");
        scaffold(&cwd);
        let srv = srv_known(&[]);
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };

        // With --json, 404 writes JSON to stdout and returns Ok.
        run_link(&g, &cwd, "ghost", false).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_json_already_linked_no_force_envelope() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-already");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject { project_ref: "existing-proj".into(), ..Default::default() },
        )
        .unwrap();

        let srv = srv_known(&["new-proj"]);
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };

        // With --json, already-linked conflict writes JSON and returns Ok.
        run_link(&g, &cwd, "new-proj", false).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_json_force_overwrite_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-force");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject { project_ref: "old-proj".into(), ..Default::default() },
        )
        .unwrap();

        let srv = srv_known(&["new-proj"]);
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };

        run_link(&g, &cwd, "new-proj", true).unwrap();
        let wp = load_working_project(&cwd).unwrap().unwrap();
        assert_eq!(wp.project_ref, "new-proj");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_writes_config_toml_file() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("toml");
        scaffold(&cwd);
        let srv = srv_known(&["toml-proj"]);
        let g = flags(&srv.url);

        run_link(&g, &cwd, "toml-proj", false).unwrap();

        let toml = std::fs::read_to_string(cwd.join("basin/config.toml")).unwrap();
        assert!(
            toml.contains("toml-proj"),
            "config.toml does not contain project ref: {toml:?}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn link_missing_project_flag() {
        let _cfg = with_temp_config_dir();
        // --project is required; cmd_link with empty project should error.
        let g = GlobalFlags { quiet: true, ..Default::default() };
        let err = cmd_link(&g, &[]).unwrap_err();
        assert!(err.to_string().contains("--project"), "error should mention --project: {err}");
    }
}
