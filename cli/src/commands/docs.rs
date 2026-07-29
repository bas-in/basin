//! docs — `basin docs [<subcommand>]`
//!
//! With no arguments, opens https://docs.basin.run/cli.
//! With a subcommand name, opens https://docs.basin.run/cli/<subcommand>.
//!
//! Under --json the command prints the URL + `"opened": false` and does
//! NOT launch a browser (CI-safe path).
//!
//! Browser launch is OS-dispatched via std::env::consts::OS:
//!   - "macos"   → open <url>
//!   - "windows" → cmd /c start <url>
//!   - else      → xdg-open <url>

use clap::{Arg, ArgAction, Command};
use serde_json::json;

use crate::error::{msg, CliResult};
use crate::global::GlobalFlags;
use crate::output::print_json;

use super::help::help_for_command;
use super::parse_or_silent;

const DOCS_BASE_URL: &str = "https://docs.basin.run/cli";

pub fn cmd_docs(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let cmd = Command::new("docs")
        .arg(Arg::new("subcommand").num_args(0..=1))
        .arg(Arg::new("help").long("help").action(ArgAction::SetTrue));
    let m = parse_or_silent(cmd, args)?;
    if m.get_flag("help") {
        help_for_command(
            "docs",
            "Open the Basin CLI documentation in your browser.",
            &[
                "[<subcommand>]   Open the docs page for a specific subcommand.",
                "                  Omit to open https://docs.basin.run/cli.",
                "--json           Print { \"url\": \"...\", \"opened\": false } without launching a browser.",
            ],
        );
        return Ok(());
    }

    // First non-flag positional argument is the subcommand name.
    let sub = m
        .get_one::<String>("subcommand")
        .cloned()
        .unwrap_or_default();

    // Build the target URL.
    let url = if sub.is_empty() {
        DOCS_BASE_URL.to_string()
    } else {
        format!("{DOCS_BASE_URL}/{sub}")
    };

    // Under --json: print metadata but do NOT spawn the browser.
    // CI environments can OOM trying to launch a graphical browser, so we
    // suppress it unconditionally under --json.
    if g.json {
        // JSON shape: { "url": "https://docs.basin.run/cli[/<subcommand>]", "opened": false }
        return print_json(
            &mut std::io::stdout(),
            &json!({ "url": url, "opened": false }),
        );
    }

    // Human path: emit a progress line then spawn the browser.
    eprintln!("Opening {url} ...");
    let (prog, prog_args) = open_command(std::env::consts::OS, &url);
    let mut cmd = std::process::Command::new(&prog);
    cmd.args(&prog_args);
    cmd.spawn()
        .map_err(|e| msg(format!("docs: could not open browser: {e}")))?;
    Ok(())
}

/// open_command maps an OS name to the (program, args) pair used to open a
/// URL in the default browser. This is a pure function so tests can verify
/// the dispatch without actually spawning a process.
///
/// OS name follows std::env::consts::OS conventions:
///   "macos"   → ("open",    [url])
///   "windows" → ("cmd",     ["/c", "start", url])
///   else      → ("xdg-open",[url])
pub fn open_command(os: &str, url: &str) -> (String, Vec<String>) {
    match os {
        "macos" => ("open".to_string(), vec![url.to_string()]),
        "windows" => (
            "cmd".to_string(),
            vec!["/c".to_string(), "start".to_string(), url.to_string()],
        ),
        _ => ("xdg-open".to_string(), vec![url.to_string()]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::with_temp_config_dir;

    // ── open_command dispatch ─────────────────────────────────────────

    #[test]
    fn open_command_macos() {
        let (prog, args) = open_command("macos", "https://docs.basin.run/cli");
        assert_eq!(prog, "open");
        assert_eq!(args, vec!["https://docs.basin.run/cli"]);
    }

    #[test]
    fn open_command_windows() {
        let (prog, args) = open_command("windows", "https://docs.basin.run/cli");
        assert_eq!(prog, "cmd");
        assert_eq!(args, vec!["/c", "start", "https://docs.basin.run/cli"]);
    }

    #[test]
    fn open_command_linux() {
        let (prog, args) = open_command("linux", "https://docs.basin.run/cli");
        assert_eq!(prog, "xdg-open");
        assert_eq!(args, vec!["https://docs.basin.run/cli"]);
    }

    #[test]
    fn open_command_freebsd() {
        let (prog, args) = open_command("freebsd", "https://docs.basin.run/cli/db");
        assert_eq!(prog, "xdg-open");
        assert_eq!(args, vec!["https://docs.basin.run/cli/db"]);
    }

    #[test]
    fn open_command_unknown_falls_back_to_xdg() {
        let (prog, args) = open_command("plan9", "https://docs.basin.run/cli");
        assert_eq!(prog, "xdg-open");
        assert_eq!(args, vec!["https://docs.basin.run/cli"]);
    }

    // ── URL construction ─────────────────────────────────────────────

    #[test]
    fn base_url_constant() {
        assert_eq!(DOCS_BASE_URL, "https://docs.basin.run/cli");
    }

    #[test]
    fn url_with_subcommand() {
        let cases = &[
            ("db", "https://docs.basin.run/cli/db"),
            ("branches", "https://docs.basin.run/cli/branches"),
            ("gen", "https://docs.basin.run/cli/gen"),
            ("migrations", "https://docs.basin.run/cli/migrations"),
        ];
        for (sub, want) in cases {
            let got = format!("{DOCS_BASE_URL}/{sub}");
            assert_eq!(&got, want, "sub={sub}");
        }
    }

    // ── JSON shape ───────────────────────────────────────────────────

    #[test]
    fn json_no_subcommand_no_browser() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        // Must succeed without spawning a browser (json mode).
        cmd_docs(&g, &[]).unwrap();
    }

    #[test]
    fn json_with_subcommand() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            json: true,
            quiet: true,
            ..Default::default()
        };
        // "db" subcommand — must not error.
        cmd_docs(&g, &["db".into()]).unwrap();
    }

    #[test]
    fn json_shape_url_opened_false() {
        // Verify the JSON shape directly (without running cmd_docs which would
        // touch stdout and potentially need a real URL).
        let url = "https://docs.basin.run/cli/branches".to_string();
        let v = json!({ "url": url, "opened": false });
        assert_eq!(
            v["url"].as_str().unwrap(),
            "https://docs.basin.run/cli/branches"
        );
        assert!(!v["opened"].as_bool().unwrap());
    }

    // ── help returns Ok ──────────────────────────────────────────────

    #[test]
    fn help_flag_returns_ok() {
        let _g = with_temp_config_dir();
        let g = GlobalFlags {
            quiet: true,
            ..Default::default()
        };
        assert!(cmd_docs(&g, &["--help".into()]).is_ok());
    }
}
