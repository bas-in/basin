//! unlink — `basin unlink`
//!
//! Removes only the `project_ref` line from `./basin/config.toml`;
//! keeps `migrations/` and `seed.sql` intact. Idempotent: if
//! `project_ref` is already absent, exits 0 without rewriting anything.
//!
//! CWD-dependent logic lives in `run_unlink(…, cwd)` so tests can pass a
//! tempdir path directly without touching the process-global cwd.

use std::path::Path;

use clap::{Arg, ArgAction, Command};
use serde_json::json;

use crate::config::{load_working_project, save_working_project};
use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;
use crate::printinfo;

use super::help::help_for_command;
use super::parse_or_silent;

pub fn cmd_unlink(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("unlink").arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "unlink",
            "Remove the project binding from ./basin/config.toml.",
            &[],
        );
        return Ok(());
    }
    let cwd = std::env::current_dir().map_err(|e| {
        msg(format!(
            "unlink: could not determine working directory: {e}"
        ))
    })?;
    run_unlink(g, &cwd)
}

/// run_unlink performs the actual unlink logic rooted at `cwd`.
/// Extracted so tests can supply a temp path without process-global chdir.
fn run_unlink(g: &GlobalFlags, cwd: &Path) -> CliResult<()> {
    let wp = load_working_project(cwd).map_err(|e| msg(format!("unlink: {e}")))?;

    // Not linked — idempotent success (do NOT rewrite the file).
    let project_ref = match wp.as_ref().map(|w| w.project_ref.as_str()) {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            if g.json {
                // JSON shape: { ok: bool, was_linked: bool }
                return print_json(
                    &mut std::io::stdout(),
                    &json!({ "ok": true, "was_linked": false }),
                );
            }
            printinfo!(g, "Not linked to any project (nothing to do).");
            return Ok(());
        }
    };

    // Clear the binding while preserving other fields.
    let mut updated = wp.unwrap_or_default();
    updated.project_ref = String::new();
    save_working_project(cwd, &updated)
        .map_err(|e| msg(format!("unlink: write config.toml: {e}")))?;

    if g.json {
        // JSON shape: { ok: bool, was_linked: bool, unlinked_ref: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "ok": true,
                "was_linked": true,
                "unlinked_ref": project_ref,
            }),
        );
    }
    println!("Unlinked from project {project_ref}.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{save_working_project, WorkingProject};
    use crate::testutil::with_temp_config_dir;

    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("basin-unlink-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn scaffold_linked(cwd: &Path, project_ref: &str) {
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        std::fs::write(cwd.join("basin/seed.sql"), b"-- seed data\n").unwrap();
        save_working_project(
            cwd,
            &WorkingProject {
                project_ref: project_ref.to_string(),
                default_branch: "main".into(),
                ..Default::default()
            },
        )
        .unwrap();
    }

    fn scaffold_empty(cwd: &Path) {
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        std::fs::write(cwd.join("basin/seed.sql"), b"-- seed data\n").unwrap();
        save_working_project(cwd, &WorkingProject::default()).unwrap();
    }

    fn quiet_g() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn unlink_linked_success() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("success");
        scaffold_linked(&cwd, "my-proj");

        run_unlink(&quiet_g(), &cwd).unwrap();

        let wp = crate::config::load_working_project(&cwd).unwrap();
        let ref_val = wp.as_ref().map(|w| w.project_ref.as_str()).unwrap_or("");
        assert!(ref_val.is_empty(), "project_ref not cleared: {ref_val:?}");

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn unlink_not_linked_no_op() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("noop");
        scaffold_empty(&cwd);

        let config_path = cwd.join("basin/config.toml");
        let mtime_before = std::fs::metadata(&config_path).unwrap().modified().unwrap();

        // Small sleep to ensure mtime would change if written.
        std::thread::sleep(std::time::Duration::from_millis(10));

        run_unlink(&quiet_g(), &cwd).unwrap();

        let mtime_after = std::fs::metadata(&config_path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "config.toml was rewritten on no-op unlink (mtime changed)"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn unlink_json_linked_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-linked");
        scaffold_linked(&cwd, "proj-abc");

        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        run_unlink(&g, &cwd).unwrap();

        // Verify project_ref is cleared on disk.
        let wp = crate::config::load_working_project(&cwd).unwrap();
        let ref_val = wp.as_ref().map(|w| w.project_ref.as_str()).unwrap_or("");
        assert!(
            ref_val.is_empty(),
            "project_ref should be cleared after json unlink"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn unlink_json_not_linked_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-noop");
        scaffold_empty(&cwd);

        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        run_unlink(&g, &cwd).unwrap();

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn unlink_preserves_migrations_and_seed() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("preserve");
        scaffold_linked(&cwd, "preserve-test");

        // Place a migration file.
        let mig = cwd.join("basin/migrations/0001_init.sql");
        std::fs::write(&mig, b"CREATE TABLE foo (id int);").unwrap();

        run_unlink(&quiet_g(), &cwd).unwrap();

        // Migration must still exist.
        assert!(mig.exists(), "migration file removed by unlink");
        // seed.sql must still exist.
        assert!(
            cwd.join("basin/seed.sql").exists(),
            "seed.sql removed by unlink"
        );
        // config.toml must exist and have no project_ref.
        let toml = std::fs::read_to_string(cwd.join("basin/config.toml")).unwrap();
        assert!(
            !toml.contains("project_ref"),
            "project_ref still in config.toml: {toml:?}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn unlink_preserves_other_fields() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("fields");
        // Link with extra fields.
        std::fs::create_dir_all(cwd.join("basin/migrations")).unwrap();
        save_working_project(
            &cwd,
            &WorkingProject {
                project_ref: "field-proj".into(),
                default_branch: "dev".into(),
                engine_version_pin: "1.2.3".into(),
            },
        )
        .unwrap();

        run_unlink(&quiet_g(), &cwd).unwrap();

        let wp = crate::config::load_working_project(&cwd).unwrap().unwrap();
        assert!(wp.project_ref.is_empty(), "project_ref not cleared");
        assert_eq!(wp.default_branch, "dev", "default_branch was lost");
        assert_eq!(
            wp.engine_version_pin, "1.2.3",
            "engine_version_pin was lost"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }
}
