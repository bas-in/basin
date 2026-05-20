//! help — top-level help printer + per-subcommand help routing.

use std::io::Write;

use crate::error::{silent, CliResult};
use crate::global::{default_api_url, GlobalFlags};

use super::all;

/// cmd_help dispatches to the requested subcommand's --help, or prints
/// the top-level summary when called without args.
pub fn cmd_help(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    if args.is_empty() {
        print_top_level_help(&mut std::io::stdout());
        return Ok(());
    }
    let target = &args[0];
    for c in all() {
        if c.name == *target {
            if c.name == "help" {
                print_top_level_help(&mut std::io::stdout());
                return Ok(());
            }
            return (c.run)(g, &["--help".to_string()]);
        }
    }
    eprintln!("basin: unknown command {target:?}");
    Err(silent())
}

/// print_top_level_help renders the summary every subcommand inherits.
pub fn print_top_level_help<W: Write>(w: &mut W) {
    let _ = writeln!(w, "basin — manage basin-cloud projects from the terminal.");
    let _ = writeln!(w);
    let _ = writeln!(w, "USAGE");
    let _ = writeln!(w, "  basin [global flags] <command> [args]");
    let _ = writeln!(w);
    let _ = writeln!(w, "GLOBAL FLAGS");
    let _ = writeln!(w, "  --api-url=<url>    Override the API endpoint (default {}).", default_api_url());
    let _ = writeln!(w, "  --token=<pat>      Use this PAT instead of the on-disk config.");
    let _ = writeln!(w, "  --org=<slug>       Use the per-org token from the config file.");
    let _ = writeln!(w, "  --json             Emit machine-readable JSON.");
    let _ = writeln!(w, "  -q, --quiet        Suppress non-essential output.");
    let _ = writeln!(w, "  --no-color         Disable ANSI colour in tables.");
    let _ = writeln!(w, "  -h, --help         Show this help.");
    let _ = writeln!(w);
    let _ = writeln!(w, "ENVIRONMENT");
    let _ = writeln!(w, "  BASIN_TOKEN        Same as --token.");
    let _ = writeln!(w, "  BASIN_API          Same as --api-url.");
    let _ = writeln!(w, "  XDG_CONFIG_HOME    Where to look for config.json (default ~/.config).");
    let _ = writeln!(w, "  NO_COLOR           Disable ANSI colour (any non-empty value).");
    let _ = writeln!(w);
    let _ = writeln!(w, "COMMANDS");
    let cmds = all();
    let width = cmds.iter().map(|c| c.name.len()).max().unwrap_or(0);
    for c in &cmds {
        let _ = writeln!(w, "  {:width$}  {}", c.name, c.help, width = width);
    }
    let _ = writeln!(w);
    let _ = writeln!(w, "Run `basin help <command>` for command-specific flags.");
}

/// help_for_command prints a short help block. Each command calls this
/// from its own --help branch.
pub fn help_for_command(name: &str, summary: &str, lines: &[&str]) {
    println!("{summary}");
    println!();
    println!("USAGE");
    println!("  basin {name} [args]");
    if !lines.is_empty() {
        println!();
        println!("FLAGS / SUBCOMMANDS");
        for ln in lines {
            println!("  {}", ln.trim_end());
        }
    }
}
