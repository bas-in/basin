//! Command basin is the basin-cloud CLI.
//!
//! Modeled on `gh` / `flyctl` / `supabase`. Subcommand dispatch is a
//! flat table at the top level; each `commands/*.rs` file owns one
//! handler and parses its own flags (clap builder, `no_binary_name`).
//!
//! Auth is via personal access tokens (PATs), the same `bso_org_<32>`
//! shape the dashboard mints under Org → API Tokens. Lookup order:
//!   1. --token=<value> flag
//!   2. $BASIN_TOKEN env var
//!   3. tokens.<--org slug> entry from ~/.config/basin/config.json
//!   4. default_token entry from the same file
//!
//! API endpoint defaults to https://api.basin.run, override with
//! $BASIN_API or --api-url. Every command honours --json, -q/--quiet,
//! and --no-color.

// Transitional during the Go→Rust port: the client/config/output layers
// expose foundation helpers (working-project I/O, endpoint-absent
// sentinels, TableInfo, …) consumed by command modules as each is
// ported. Remove this once the port reaches parity.
#![allow(dead_code)]

mod client;
mod commands;
mod config;
mod error;
mod fetch;
mod global;
mod output;
mod telemetry;
mod types;
mod version_check;
mod version_warn;

#[cfg(test)]
mod testutil;

use std::time::Instant;

use error::{is_silent, CliResult};
use global::parse_global_flags;
use telemetry::emit_telemetry;
use version_warn::{warn_if_self_update_available, warn_if_version_out_of_window};

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(&argv) {
        if !is_silent(e.as_ref()) {
            eprintln!("basin: {e}");
        }
        std::process::exit(1);
    }
}

/// run is split out from main so test code can drive the CLI in-process
/// (without process::exit).
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

#[cfg(test)]
mod tests {
    use super::*;
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
