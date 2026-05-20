//! logout — remove a token entry from the config file. With --all it
//! wipes every token. Idempotent: no-ops cleanly when absent.

use clap::{Arg, ArgAction, Command};
use serde_json::json;

use crate::config::remove_token;
use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;

use super::help::help_for_command;
use super::login::org_or_default;
use super::parse_or_silent;

pub fn cmd_logout(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("logout")
        .arg(Arg::new("org").long("org"))
        .arg(Arg::new("all").long("all").action(ArgAction::SetTrue))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "logout",
            "Forget the locally-stored personal access token.",
            &[
                "--org=<slug>   Wipe the per-org entry. Default wipes default_token.",
                "--all          Wipe every stored token.",
            ],
        );
        return Ok(());
    }
    let org = m
        .get_one::<String>("org")
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| g.org_slug.clone());
    let all = m.get_flag("all");
    let target = if all { "*".to_string() } else { org.clone() };
    remove_token(&target).map_err(|e| msg(format!("write config: {e}")))?;
    if g.json {
        // JSON shape: { signed_out: bool, scope: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "signed_out": true, "scope": org_or_default(&org) }),
        );
    }
    if all {
        println!("Cleared every stored token.");
    } else {
        println!("Cleared {}.", org_or_default(&org));
    }
    Ok(())
}
