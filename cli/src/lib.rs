//! basin_cli — library crate exposing the CLI's internal modules so
//! integration tests (and property/fuzz tests in `tests/`) can drive
//! parsers, the HTTP client wrapper, and the config layer in-process.
//!
//! The `basin` binary in `src/main.rs` is a thin shim over [`run`].

#![allow(dead_code)]

pub mod client;
pub mod commands;
pub mod config;
pub mod error;
pub mod fetch;
pub mod global;
pub mod output;
pub mod telemetry;
pub mod types;
pub mod version_check;
pub mod version_warn;

#[cfg(test)]
mod testutil;

use std::time::Instant;

use error::CliResult;
use global::parse_global_flags;
use telemetry::emit_telemetry;
use version_warn::{warn_if_self_update_available, warn_if_version_out_of_window};

/// run is the in-process entry point: drives global-flag parsing,
/// subcommand dispatch, and telemetry emission. The bin's `main` is a
/// one-liner around this so test code can invoke it without
/// `process::exit`.
pub fn run(argv: &[String]) -> CliResult<()> {
    let (g, rest) = parse_global_flags(argv)?;
    if rest.is_empty() {
        commands::help::print_top_level_help(&mut std::io::stdout());
        return Ok(());
    }
    let name = rest[0].clone();
    let args = rest[1..].to_vec();

    // CLI↔cloud support-window check. Soft-fail, side-effect-only.
    // Skipped for the no-API local commands.
    match name.as_str() {
        "help" | "version" | "logout" | "completion" | "config" => {}
        _ => {
            warn_if_version_out_of_window(&g);
            warn_if_self_update_available(&g);
        }
    }

    let start = Instant::now();
    for c in commands::all() {
        if c.name == name {
            let run_err = (c.run)(&g, &args);
            emit_telemetry(&g, &name, start.elapsed(), run_err.is_ok());
            return run_err;
        }
    }
    eprintln!("basin: unknown command {name:?}\n");
    commands::help::print_top_level_help(&mut std::io::stderr());
    Err(error::silent())
}

/// is_silent_err re-exports the bin-side helper so the thin
/// `src/main.rs` shim can suppress the error print for the silent
/// sentinel without re-declaring the error module.
pub use error::is_silent as is_silent_err;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::is_silent;
    use crate::testutil::with_temp_config_dir;

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn version_runs_without_network() {
        let _g = with_temp_config_dir();
        assert!(run(&argv(&["version"])).is_ok());
    }

    #[test]
    fn help_and_empty_run_clean() {
        let _g = with_temp_config_dir();
        assert!(run(&argv(&["help"])).is_ok());
        assert!(run(&[]).is_ok());
    }

    #[test]
    fn unknown_command_is_silent_error() {
        let _g = with_temp_config_dir();
        let err = run(&argv(&["definitely-not-a-command"])).unwrap_err();
        assert!(is_silent(err.as_ref()));
    }
}
