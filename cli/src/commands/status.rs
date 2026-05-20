//! status — `basin status`
//!
//! Reads `./basin/config.toml` for the linked project ref, then calls:
//!   - `GET /v1/projects/{ref}` for live status
//!   - `GET /admin/v1/projects/{ref}/migrations` for remote migrations list
//!
//! Compares the remote migration set against `./basin/migrations/*.sql`
//! to produce a drift summary (applied / local_only / remote_only).
//!
//! CWD-dependent logic lives in `run_status(…, cwd)` so tests can pass a
//! tempdir path directly without touching the process-global cwd.

use std::path::Path;

use clap::{Arg, ArgAction, Command};
use reqwest::Method;
use serde::Deserialize;
use serde_json::json;

use crate::config::load_working_project;
use crate::error::{msg, CliResult};
use crate::global::{require_client, GlobalFlags};
use crate::output::print_json;
use crate::printerr;

use super::help::help_for_command;
use super::parse_or_silent;

#[derive(Deserialize, Default)]
struct StatusProjectResp {
    #[serde(default)]
    project: Option<StatusProject>,
}

#[derive(Deserialize, Default)]
struct StatusProject {
    #[serde(default, rename = "ref")]
    r#ref: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    default_branch: String,
}

#[derive(Deserialize, Default)]
struct MigrationsResp {
    #[serde(default)]
    migrations: Vec<MigrationRow>,
}

/// MigrationRow mirrors the remote migration record. Only `id` and
/// `source` are used for drift analysis.
#[derive(Deserialize, Default)]
struct MigrationRow {
    #[serde(default)]
    id: String,
    #[serde(default)]
    source: String,
}

pub fn cmd_status(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("status").arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "status",
            "Show the linked project's live status and migration drift.",
            &[],
        );
        return Ok(());
    }
    let cwd = std::env::current_dir().map_err(|e| {
        msg(format!(
            "status: could not determine working directory: {e}"
        ))
    })?;
    run_status(g, &cwd)
}

/// run_status performs the actual status logic rooted at `cwd`.
/// Extracted so tests can supply a temp path without process-global chdir.
fn run_status(g: &GlobalFlags, cwd: &Path) -> CliResult<()> {
    // Load the working project config.
    let wp = load_working_project(cwd).map_err(|e| msg(format!("status: {e}")))?;

    // Not linked — exit 0 with a clear message.
    let project_ref = match wp.as_ref().map(|w| w.project_ref.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            if g.json {
                // JSON shape: { "linked": false }
                return print_json(&mut std::io::stdout(), &json!({ "linked": false }));
            }
            println!("no project linked in this directory");
            return Ok(());
        }
    };

    let c = require_client(g)?;

    // Fetch project metadata.
    let proj_resp: StatusProjectResp = c
        .do_json(Method::GET, &format!("/v1/projects/{project_ref}"), None)
        .map_err(|e| msg(format!("status: fetch project: {e}")))?;
    let proj = proj_resp.project.unwrap_or_else(|| StatusProject {
        r#ref: project_ref.clone(),
        ..Default::default()
    });

    // Fetch remote migrations list (non-fatal on failure).
    let remote_migrations: Vec<MigrationRow> = match c.do_json::<MigrationsResp>(
        Method::GET,
        &format!("/admin/v1/projects/{project_ref}/migrations"),
        None,
    ) {
        Ok(r) => r.migrations,
        Err(e) => {
            printerr!(g, "warning: could not fetch remote migrations: {e}");
            Vec::new()
        }
    };

    // Collect local migration filenames (*.sql) in ./basin/migrations/.
    let local_files = collect_local_migrations(cwd);

    // Build sets for drift analysis.
    // Convention: compare bare filename of local files against
    // basename of MigrationRow.source (or fall back to ID when empty).
    let remote_set: std::collections::HashSet<String> =
        remote_migrations.iter().map(migration_key).collect();

    let local_set: std::collections::HashSet<String> = local_files
        .iter()
        .filter_map(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        })
        .collect();

    let applied: usize = local_set.iter().filter(|k| remote_set.contains(*k)).count();
    let local_only: usize = local_set
        .iter()
        .filter(|k| !remote_set.contains(*k))
        .count();
    let remote_only: usize = remote_set
        .iter()
        .filter(|k| !local_set.contains(*k))
        .count();

    let status = if proj.status.is_empty() {
        "unknown".to_string()
    } else {
        proj.status.clone()
    };

    if g.json {
        // JSON shape: { "linked": true, "project_ref": string, "status": string,
        //               "region": string, "default_branch": string,
        //               "migrations": { "applied": int, "local_only": int, "remote_only": int } }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "linked": true,
                "project_ref": project_ref,
                "status": status,
                "region": proj.region,
                "default_branch": proj.default_branch,
                "migrations": {
                    "applied": applied,
                    "local_only": local_only,
                    "remote_only": remote_only,
                },
            }),
        );
    }

    // Human-readable output.
    println!("project:        {project_ref}");
    if !proj.name.is_empty() {
        println!("name:           {}", proj.name);
    }
    println!("status:         {status}");
    if !proj.region.is_empty() {
        println!("region:         {}", proj.region);
    }
    if !proj.default_branch.is_empty() {
        println!("default_branch: {}", proj.default_branch);
    }
    println!();
    println!("migrations:");
    println!("  applied:      {applied}");
    if local_only > 0 {
        println!("  local_only:   {local_only}  (pending push)");
    } else {
        println!("  local_only:   {local_only}");
    }
    if remote_only > 0 {
        println!("  remote_only:  {remote_only}  (run `basin db pull` to sync)");
    } else {
        println!("  remote_only:  {remote_only}");
    }

    Ok(())
}

/// collect_local_migrations returns a sorted list of absolute paths for
/// every `*.sql` file in `<cwd>/basin/migrations/`. Returns an empty Vec
/// (not an error) when the directory is absent or empty.
fn collect_local_migrations(cwd: &Path) -> Vec<std::path::PathBuf> {
    let dir = cwd.join("basin").join("migrations");
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(_) => return Vec::new(),
    };
    let mut files: Vec<std::path::PathBuf> = entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            !e.file_type().map(|t| t.is_dir()).unwrap_or(true)
                && e.file_name().to_string_lossy().ends_with(".sql")
        })
        .map(|e| e.path())
        .collect();
    files.sort();
    files
}

/// migration_key returns the identifier used to match a remote MigrationRow
/// against a local *.sql filename. Uses the basename of `source` when set,
/// otherwise falls back to `id`.
fn migration_key(m: &MigrationRow) -> String {
    if !m.source.is_empty() {
        return std::path::Path::new(&m.source)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(&m.source)
            .to_string();
    }
    m.id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{save_working_project, WorkingProject};
    use crate::testutil::{with_temp_config_dir, Req, Resp, TestServer};

    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("basin-status-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn scaffold(cwd: &Path) {
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        save_working_project(cwd, &WorkingProject::default()).unwrap();
    }

    /// Build a TestServer that serves the two status endpoints.
    fn status_server(proj_json: Option<&'static str>, mig_json: &'static str) -> TestServer {
        TestServer::start(move |req: &Req| {
            let path = &req.path;
            if req.method == "GET" && path.ends_with("/migrations") {
                return Resp::ok(format!(r#"{{"migrations":{mig_json}}}"#));
            }
            if req.method == "GET" && path.contains("/v1/projects/") {
                match proj_json {
                    Some(p) => return Resp::ok(format!(r#"{{"project":{p}}}"#)),
                    None => {
                        return Resp::status(
                            404,
                            r#"{"error":{"code":"not_found","message":"not found"}}"#,
                        )
                    }
                }
            }
            Resp::status(404, r#""#)
        })
    }

    fn flags(url: &str) -> GlobalFlags {
        GlobalFlags {
            api_url: url.into(),
            token: "tok".into(),
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn status_not_linked_prose() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("notlinked");
        scaffold(&cwd);

        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        // Should return Ok(()) without error.
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_not_linked_json() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("notlinked-json");
        scaffold(&cwd);

        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_clean_migrations() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("clean");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "clean-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        std::fs::write(
            cwd.join("basin/migrations/0001_init.sql"),
            b"CREATE TABLE foo (id int);",
        )
        .unwrap();

        let srv = status_server(
            Some(
                r#"{"ref":"clean-proj","name":"Clean","region":"jnb","status":"active","default_branch":"main"}"#,
            ),
            r#"[{"id":"m1","source":"0001_init.sql"}]"#,
        );
        let g = flags(&srv.url);
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_drift_counts() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("drift");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "drift-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        // Two local files.
        std::fs::write(cwd.join("basin/migrations/0001_init.sql"), b"--").unwrap();
        std::fs::write(cwd.join("basin/migrations/0002_add_col.sql"), b"--").unwrap();

        // Remote: 0001 applied + one remote-only.
        let srv = status_server(
            Some(r#"{"ref":"drift-proj","status":"active","region":"jnb"}"#),
            r#"[{"id":"m1","source":"0001_init.sql"},{"id":"m2","source":"0003_remote_only.sql"}]"#,
        );
        let g = flags(&srv.url);
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_paused_project() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("paused");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "paused-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();

        let srv = status_server(
            Some(r#"{"ref":"paused-proj","status":"paused","region":"jnb"}"#),
            r#"[]"#,
        );
        let g = flags(&srv.url);
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_json_output_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-shape");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "json-proj".into(),
                ..Default::default()
            },
        )
        .unwrap();
        std::fs::write(cwd.join("basin/migrations/0001_init.sql"), b"--").unwrap();

        let srv = status_server(
            Some(
                r#"{"ref":"json-proj","status":"active","region":"us-east","default_branch":"main"}"#,
            ),
            r#"[{"id":"m1","source":"0001_init.sql"}]"#,
        );
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };
        // Returns Ok; JSON goes to stdout.
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_missing_basin_dir_returns_not_linked() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("nodir");
        // No scaffold — no basin/ directory.
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        // load_working_project returns Ok(None) when not found.
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_json_drift_counts() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-drift");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "drift-json".into(),
                ..Default::default()
            },
        )
        .unwrap();
        // Two local-only files.
        std::fs::write(cwd.join("basin/migrations/local_a.sql"), b"--").unwrap();
        std::fs::write(cwd.join("basin/migrations/local_b.sql"), b"--").unwrap();

        // Three remote-only migrations.
        let srv = status_server(
            Some(r#"{"ref":"drift-json","status":"active"}"#),
            r#"[{"id":"r1","source":"remote_x.sql"},{"id":"r2","source":"remote_y.sql"},{"id":"r3","source":"remote_z.sql"}]"#,
        );
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn status_remote_only_no_source_falls_back_to_id() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("id-fallback");
        scaffold(&cwd);
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "id-fallback".into(),
                ..Default::default()
            },
        )
        .unwrap();
        // No local files.

        let srv = status_server(
            Some(r#"{"ref":"id-fallback","status":"active"}"#),
            r#"[{"id":"m-abc"}]"#,
        );
        let g = GlobalFlags {
            api_url: srv.url.clone(),
            token: "tok".into(),
            json: true,
            quiet: true,
            ..Default::default()
        };
        run_status(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn migration_key_uses_source_basename() {
        let m = MigrationRow {
            id: "some-id".into(),
            source: "/long/path/0001_init.sql".into(),
        };
        assert_eq!(migration_key(&m), "0001_init.sql");
    }

    #[test]
    fn migration_key_falls_back_to_id() {
        let m = MigrationRow {
            id: "m-abc".into(),
            source: String::new(),
        };
        assert_eq!(migration_key(&m), "m-abc");
    }
}
