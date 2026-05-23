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
//!
//! The dispatch surface itself lives in the `basin_cli` library crate
//! (see `src/lib.rs`) so integration/property tests in `tests/` can
//! invoke parsers and the client wrapper without spawning a subprocess.
//! `main` stays a `process::exit`-only shim.

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = basin_cli::run(&argv) {
        if !basin_cli::is_silent_err(e.as_ref()) {
            eprintln!("basin: {e}");
        }
        std::process::exit(1);
    }
}
