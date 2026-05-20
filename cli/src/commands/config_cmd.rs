//! config — read/write CLI config flags.
//!
//!   basin config get telemetry
//!   basin config set telemetry on|off
//!   basin config show
//!
//! Read-only by default. `set` persists via write_config_file. Today
//! only `telemetry` is a boolean; the dispatch is open for future flags.

use serde_json::json;

use crate::config::{read_config_file, write_config_file, ConfigFile};
use crate::error::{msg, silent, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;
use crate::printerr;

use super::help::help_for_command;

pub fn cmd_config(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let (sub, rest) = match args.split_first() {
        None => return show(g),
        Some((s, r)) => (s.as_str(), r),
    };
    match sub {
        "get" => get(g, rest),
        "set" => set(g, rest),
        "show" => show(g),
        "--help" | "-h" | "help" => {
            help_for_command(
                "config",
                "Read or write CLI config values.",
                &[
                    "show                          Print the on-disk config.",
                    "get <key>                     Print one key.",
                    "set <key> <value>             Persist a key.",
                    "",
                    "Keys:",
                    "  telemetry                   on|off  (anonymous usage pings; default off)",
                ],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown subcommand {other:?} for config");
            Err(silent())
        }
    }
}

fn show(g: &GlobalFlags) -> CliResult<()> {
    let cf = read_config_file()?.unwrap_or_default();
    if g.json {
        // JSON shape: { telemetry: bool, default_org: string, api_url: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({
                "telemetry": cf.telemetry,
                "default_org": cf.default_org,
                "api_url": cf.api_url,
            }),
        );
    }
    println!("telemetry:   {}", on_off(cf.telemetry));
    println!("default_org: {}", cf.default_org);
    println!("api_url:     {}", cf.api_url);
    Ok(())
}

fn get(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let key = args
        .first()
        .ok_or_else(|| msg("usage: basin config get <key>"))?;
    let cf = read_config_file()?.unwrap_or_default();
    match key.as_str() {
        "telemetry" => {
            if g.json {
                // JSON shape: { telemetry: bool }
                return print_json(
                    &mut std::io::stdout(),
                    &json!({ "telemetry": cf.telemetry }),
                );
            }
            println!("{}", on_off(cf.telemetry));
            Ok(())
        }
        "default_org" => {
            println!("{}", cf.default_org);
            Ok(())
        }
        "api_url" => {
            println!("{}", cf.api_url);
            Ok(())
        }
        other => Err(msg(format!(
            "unknown key {other:?} (try: telemetry, default_org, api_url)"
        ))),
    }
}

fn set(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    if args.len() < 2 {
        return Err(msg("usage: basin config set <key> <value>"));
    }
    let (key, val) = (&args[0], &args[1]);
    let mut cf: ConfigFile = read_config_file()?.unwrap_or_default();
    match key.as_str() {
        "telemetry" => {
            let b = parse_on_off(val)
                .ok_or_else(|| msg(format!("telemetry must be on|off (got {val:?})")))?;
            cf.telemetry = b;
        }
        "default_org" => cf.default_org = val.clone(),
        "api_url" => cf.api_url = val.clone(),
        other => {
            return Err(msg(format!(
                "unknown key {other:?} (try: telemetry, default_org, api_url)"
            )))
        }
    }
    write_config_file(&cf).map_err(|e| msg(format!("write config: {e}")))?;
    if g.json {
        // JSON shape: { set: true, key: string, value: string }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "set": true, "key": key, "value": val }),
        );
    }
    println!("Set {key} = {val}.");
    Ok(())
}

/// on_off renders a bool as "on" / "off".
pub fn on_off(b: bool) -> &'static str {
    if b {
        "on"
    } else {
        "off"
    }
}

/// parse_on_off accepts on/off, true/false, yes/no, 1/0 (case-insensitive).
pub fn parse_on_off(s: &str) -> Option<bool> {
    match s.trim().to_lowercase().as_str() {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    }
}
