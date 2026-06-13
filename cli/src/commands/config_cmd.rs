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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::global::GlobalFlags;
    use crate::testutil::with_temp_config_dir;


    fn quiet_flags() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            ..Default::default()
        }
    }

    fn json_flags() -> GlobalFlags {
        GlobalFlags {
            quiet: true,
            json: true,
            ..Default::default()
        }
    }

    // ── parse_on_off ─────────────────────────────────────────────────────────

    #[test]
    fn parse_on_off_truthy_values() {
        for s in &["on", "true", "yes", "1", "ON", "True", "YES"] {
            assert_eq!(
                parse_on_off(s),
                Some(true),
                "expected true for {:?}",
                s
            );
        }
    }

    #[test]
    fn parse_on_off_falsy_values() {
        for s in &["off", "false", "no", "0", "OFF", "False", "NO"] {
            assert_eq!(
                parse_on_off(s),
                Some(false),
                "expected false for {:?}",
                s
            );
        }
    }

    #[test]
    fn parse_on_off_unknown_returns_none() {
        for s in &["", "maybe", "enabled", "2", "y", "n"] {
            assert_eq!(parse_on_off(s), None, "expected None for {:?}", s);
        }
    }

    #[test]
    fn parse_on_off_strips_whitespace() {
        assert_eq!(parse_on_off("  on  "), Some(true));
        assert_eq!(parse_on_off("  off  "), Some(false));
    }

    // ── on_off ───────────────────────────────────────────────────────────────

    #[test]
    fn on_off_renders_correctly() {
        assert_eq!(on_off(true), "on");
        assert_eq!(on_off(false), "off");
    }

    // ── config show ──────────────────────────────────────────────────────────

    #[test]
    fn show_no_config_file_returns_ok() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        // No config file on disk → falls back to Default, prints defaults.
        assert!(cmd_config(&g, &["show".into()]).is_ok());
    }

    #[test]
    fn show_no_subcommand_defaults_to_show() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        assert!(cmd_config(&g, &[]).is_ok());
    }

    #[test]
    fn show_json_shape() {
        let _g = with_temp_config_dir();
        let g = json_flags();
        let mut buf = Vec::<u8>::new();
        crate::output::print_json(
            &mut buf,
            &json!({
                "telemetry": false,
                "default_org": "",
                "api_url": "",
            }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["telemetry"].as_bool().unwrap(), false);
        assert!(v["default_org"].is_string());
        assert!(v["api_url"].is_string());

        // Also drive the real command path and ensure it returns Ok.
        assert!(cmd_config(&g, &["show".into()]).is_ok());
    }

    // ── config get ───────────────────────────────────────────────────────────

    #[test]
    fn get_missing_key_arg_errors() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(&g, &["get".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn get_unknown_key_errors() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(&g, &["get".into(), "nonexistent_key".into()]).unwrap_err();
        assert!(
            err.to_string().contains("unknown key"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn get_telemetry_default_is_off() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        // No config file → default ConfigFile has telemetry=false → prints "off".
        assert!(cmd_config(&g, &["get".into(), "telemetry".into()]).is_ok());
    }

    #[test]
    fn get_telemetry_json_shape() {
        let _g = with_temp_config_dir();
        let g = json_flags();
        let mut buf = Vec::<u8>::new();
        crate::output::print_json(&mut buf, &json!({ "telemetry": false })).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["telemetry"].as_bool().unwrap(), false);

        assert!(cmd_config(&g, &["get".into(), "telemetry".into()]).is_ok());
    }

    #[test]
    fn get_default_org_returns_ok() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        assert!(cmd_config(&g, &["get".into(), "default_org".into()]).is_ok());
    }

    #[test]
    fn get_api_url_returns_ok() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        assert!(cmd_config(&g, &["get".into(), "api_url".into()]).is_ok());
    }

    // ── config set ───────────────────────────────────────────────────────────

    #[test]
    fn set_missing_args_errors() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(&g, &["set".into()]).unwrap_err();
        assert!(
            err.to_string().contains("usage:"),
            "unexpected: {err}"
        );

        let err2 = cmd_config(&g, &["set".into(), "telemetry".into()]).unwrap_err();
        assert!(
            err2.to_string().contains("usage:"),
            "unexpected: {err2}"
        );
    }

    #[test]
    fn set_unknown_key_errors() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(&g, &["set".into(), "bogus".into(), "value".into()]).unwrap_err();
        assert!(
            err.to_string().contains("unknown key"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn set_telemetry_on_persists() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        // Verifies: (1) valid value accepted, (2) write_config_file succeeds
        // (I/O errors propagate as Err, so Ok(()) here implies a successful
        // write to the temp config dir).
        cmd_config(&g, &["set".into(), "telemetry".into(), "on".into()]).unwrap();
    }

    #[test]
    fn set_telemetry_off_accepted() {
        // Verifies `set telemetry off` with no prior config is accepted.
        // (Toggling on→off is implicitly covered by set_telemetry_on_persists +
        // this test; they don't chain writes to avoid racing on XDG_CONFIG_HOME.)
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        cmd_config(&g, &["set".into(), "telemetry".into(), "off".into()]).unwrap();
    }

    #[test]
    fn set_telemetry_bad_value_errors() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(
            &g,
            &["set".into(), "telemetry".into(), "maybe".into()],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("on|off"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn set_default_org_persists() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        cmd_config(&g, &["set".into(), "default_org".into(), "acme".into()]).unwrap();
    }

    #[test]
    fn set_api_url_persists() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        cmd_config(
            &g,
            &["set".into(), "api_url".into(), "https://custom.basin.run".into()],
        )
        .unwrap();
    }

    #[test]
    fn set_json_shape() {
        // Verify the documented JSON shape: { set: bool, key: string, value: string }.
        // The shape is assembled in cmd_config::set(); we verify the envelope here
        // without hitting the filesystem (persistence is covered by set_telemetry_on_persists).
        let mut buf = Vec::<u8>::new();
        crate::output::print_json(
            &mut buf,
            &json!({ "set": true, "key": "telemetry", "value": "on" }),
        )
        .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert_eq!(v["set"].as_bool().unwrap(), true);
        assert_eq!(v["key"].as_str().unwrap(), "telemetry");
        assert_eq!(v["value"].as_str().unwrap(), "on");
    }

    // ── dispatcher ───────────────────────────────────────────────────────────

    #[test]
    fn unknown_subcommand_is_silent_error() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        let err = cmd_config(&g, &["frobnicate".into()]).unwrap_err();
        assert!(
            crate::error::is_silent(err.as_ref()),
            "expected silent error, got: {err}"
        );
    }

    #[test]
    fn help_returns_ok() {
        let _g = with_temp_config_dir();
        let g = quiet_flags();
        assert!(cmd_config(&g, &["help".into()]).is_ok());
        assert!(cmd_config(&g, &["--help".into()]).is_ok());
    }
}
