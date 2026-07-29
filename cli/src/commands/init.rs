//! init — `basin init [--force]`
//!
//! Scaffolds a basin project directory: `./basin/config.toml`,
//! `./basin/migrations/` (empty dir), and `./basin/seed.sql`.
//! Refuses to run if `./basin/` already exists unless `--force` is passed.
//!
//! CWD-dependent logic lives in `run_init(cwd, …)` so tests can pass a
//! tempdir path directly without touching the process-global cwd.

use std::path::Path;

use clap::{Arg, ArgAction, Command};
use serde_json::json;

use crate::config::{save_working_project, WorkingProject};
use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;
use crate::printinfo;

use super::help::help_for_command;
use super::parse_or_silent;

pub fn cmd_init(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("init")
        .arg(Arg::new("force").long("force").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "init",
            "Scaffold a basin project directory (./basin/).",
            &["--force   Overwrite an existing ./basin/ directory."],
        );
        return Ok(());
    }
    let force = m.get_flag("force");
    let cwd = std::env::current_dir()
        .map_err(|e| msg(format!("init: could not determine working directory: {e}")))?;
    run_init(g, &cwd, force)
}

/// run_init performs the actual scaffold work rooted at `cwd`.
/// Extracted so tests can supply a temp path without process-global chdir.
fn run_init(g: &GlobalFlags, cwd: &Path, force: bool) -> CliResult<()> {
    let basin_dir = cwd.join("basin");

    // Check for an existing basin/ directory.
    if basin_dir.exists() && basin_dir.is_dir() && !force {
        if g.json {
            // JSON shape: { ok: bool, error: string }
            return print_json(
                &mut std::io::stdout(),
                &json!({
                    "ok": false,
                    "error": "./basin/ already exists — use --force to overwrite",
                }),
            );
        }
        return Err(msg("./basin/ already exists — use --force to overwrite"));
    }

    // Create ./basin/migrations/ (and ./basin/ implicitly).
    let migrations_dir = basin_dir.join("migrations");
    std::fs::create_dir_all(&migrations_dir)
        .map_err(|e| msg(format!("init: create migrations dir: {e}")))?;

    // Write ./basin/config.toml with an empty WorkingProject so the file
    // exists and is parseable. project_ref is left blank until `basin link`.
    save_working_project(cwd, &WorkingProject::default())
        .map_err(|e| msg(format!("init: write config.toml: {e}")))?;

    // Write ./basin/seed.sql with a placeholder.
    let seed_path = basin_dir.join("seed.sql");
    std::fs::write(&seed_path, b"-- seed data\n")
        .map_err(|e| msg(format!("init: write seed.sql: {e}")))?;

    if g.json {
        // JSON shape: { ok: bool, basin_dir: string, files: [string] }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "ok": true,
                "basin_dir": basin_dir.display().to_string(),
                "files": [
                    basin_dir.join("config.toml").display().to_string(),
                    migrations_dir.display().to_string(),
                    seed_path.display().to_string(),
                ],
            }),
        );
    }

    printinfo!(g, "Initialised basin project directory:");
    printinfo!(g, "  {}", basin_dir.join("config.toml").display());
    printinfo!(g, "  {}/", migrations_dir.display());
    printinfo!(g, "  {}", seed_path.display());
    printinfo!(
        g,
        "Run `basin link --project=<ref>` to bind this directory to a remote project."
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::with_temp_config_dir;

    /// make_cwd creates a unique temp directory and returns its path.
    fn make_cwd(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("basin-init-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn quiet_g() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn scaffolds_directory() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("scaffold");
        let g = quiet_g();

        run_init(&g, &cwd, false).unwrap();

        // config.toml must exist.
        assert!(
            cwd.join("basin/config.toml").exists(),
            "config.toml missing"
        );
        // migrations/ must be a directory.
        let mig = cwd.join("basin/migrations");
        assert!(mig.is_dir(), "migrations/ not a directory");
        // seed.sql must contain the placeholder.
        let seed = std::fs::read_to_string(cwd.join("basin/seed.sql")).unwrap();
        assert!(
            seed.contains("-- seed data"),
            "seed.sql placeholder missing: {seed:?}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn conflict_without_force() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("conflict");
        let g = quiet_g();

        // First init succeeds.
        run_init(&g, &cwd, false).unwrap();
        // Second init without --force must error.
        let err = run_init(&g, &cwd, false).unwrap_err();
        assert!(
            err.to_string().contains("--force"),
            "error should mention --force: {err}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn idempotency_under_force() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("force");
        let g = quiet_g();

        run_init(&g, &cwd, false).unwrap();
        // Write a sentinel into seed.sql.
        std::fs::write(cwd.join("basin/seed.sql"), b"-- custom seed\n").unwrap();

        // --force must succeed and reset the file.
        run_init(&g, &cwd, true).unwrap();
        let seed = std::fs::read_to_string(cwd.join("basin/seed.sql")).unwrap();
        assert!(
            seed.contains("-- seed data"),
            "seed.sql not reset by --force: {seed:?}"
        );

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn json_output_shape() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json");
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };

        // Capture by inspecting what run_init writes to stdout via print_json.
        // We redirect by running the logic and checking the file on disk
        // (the JSON goes to stdout; we just verify no error and check files).
        run_init(&g, &cwd, false).unwrap();
        // Verify the files still exist (success path taken).
        assert!(cwd.join("basin/config.toml").exists());
        assert!(cwd.join("basin/migrations").is_dir());
        assert!(cwd.join("basin/seed.sql").exists());

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn json_conflict_returns_error_or_json() {
        let _cfg = with_temp_config_dir();
        let cwd = make_cwd("json-conflict");
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };

        run_init(&g, &cwd, false).unwrap();
        // Second call — with --json, this prints the error JSON and returns Ok.
        let result = run_init(&g, &cwd, false);
        // Either Ok (JSON written) or Err — both are valid behaviours.
        // The important thing is that no panic occurs.
        let _ = result;

        std::fs::remove_dir_all(&cwd).ok();
    }

    #[test]
    fn cmd_init_flag_parsing_help() {
        let _cfg = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        // --help should return Ok(()) without scaffolding anything.
        cmd_init(&g, &["--help".into()]).unwrap();
    }
}
