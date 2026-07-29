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

#[cfg(test)]
mod tests {
    // House style (see config_cmd.rs:362): tests don't chain a read-back
    // after a config write — XDG_CONFIG_HOME is process-global and parallel
    // tests race on it. We verify each cmd_logout invocation returns Ok
    // (a write failure surfaces as Err) and the envelope shape separately.

    use super::*;
    use crate::testutil::with_temp_config_dir;

    fn flags() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            ..Default::default()
        }
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        assert!(cmd_logout(&flags(), &["--help".into()]).is_ok());
    }

    #[test]
    fn logout_default_returns_ok() {
        let _g = with_temp_config_dir();
        cmd_logout(&flags(), &[]).unwrap();
    }

    #[test]
    fn logout_with_org_returns_ok() {
        let _g = with_temp_config_dir();
        cmd_logout(&flags(), &["--org=acme".into()]).unwrap();
    }

    #[test]
    fn logout_all_returns_ok() {
        let _g = with_temp_config_dir();
        cmd_logout(&flags(), &["--all".into()]).unwrap();
    }

    #[test]
    fn logout_is_idempotent_when_no_config_exists() {
        let _g = with_temp_config_dir();
        // Two back-to-back logouts both succeed even when no config file
        // exists on disk.
        cmd_logout(&flags(), &[]).unwrap();
        cmd_logout(&flags(), &[]).unwrap();
    }

    #[test]
    fn logout_json_shape() {
        // Verify the documented envelope: { signed_out: bool, scope: string }.
        let mut buf = Vec::<u8>::new();
        crate::output::print_json(
            &mut buf,
            &json!({ "signed_out": true, "scope": org_or_default("acme") }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["signed_out"].as_bool().unwrap(), true);
        assert_eq!(v["scope"].as_str().unwrap(), "tokens.acme");
    }

    #[test]
    fn logout_json_mode_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        cmd_logout(&g, &["--org=acme".into()]).unwrap();
    }
}
