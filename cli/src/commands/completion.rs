//! completion — emit shell completion scripts (bash / zsh / fish).
//!
//! Scripts are hand-rolled (no template engine) and the subcommand
//! names come from [`all`] so completion never drifts from dispatch.

use std::io::Write;

use crate::error::{silent, CliResult};
use crate::global::GlobalFlags;
use crate::printerr;

use super::all;
use super::help::help_for_command;

/// GLOBAL_FLAG_NAMES is the static list rendered into every shell's
/// flag-completion table. Kept in sync with parse_global_flags by hand.
const GLOBAL_FLAG_NAMES: &[&str] =
    &["--api-url", "--token", "--org", "--json", "--quiet", "--no-color", "--help"];

pub fn cmd_completion(g: &GlobalFlags, args: &[String]) -> CliResult<()> {
    let shell = match args.first() {
        None => {
            help_for_command(
                "completion",
                "Print a shell completion script for bash, zsh, or fish.",
                &[
                    "bash    Print bash completion (source from /etc/bash_completion.d/basin).",
                    "zsh     Print zsh completion (drop in $fpath as _basin).",
                    "fish    Print fish completion (~/.config/fish/completions/basin.fish).",
                ],
            );
            return Ok(());
        }
        Some(s) => s.as_str(),
    };
    let mut out = std::io::stdout();
    match shell {
        "bash" => write_bash(&mut out),
        "zsh" => write_zsh(&mut out),
        "fish" => write_fish(&mut out),
        "--help" | "-h" | "help" => {
            help_for_command(
                "completion",
                "Print a shell completion script for bash, zsh, or fish.",
                &["bash    Print bash completion.", "zsh     Print zsh completion.", "fish    Print fish completion."],
            );
            Ok(())
        }
        other => {
            printerr!(g, "unknown shell {other:?} (expected bash, zsh, or fish)");
            Err(silent())
        }
    }
}

fn command_names() -> Vec<&'static str> {
    all().into_iter().map(|c| c.name).collect()
}

fn write_bash<W: Write>(w: &mut W) -> CliResult<()> {
    let cmds = command_names().join(" ");
    let flags = GLOBAL_FLAG_NAMES.join(" ");
    write!(
        w,
        r#"# bash completion for basin                              -*- shell-script -*-
_basin() {{
    local cur prev words cword
    COMPREPLY=()
    cur="${{COMP_WORDS[COMP_CWORD]}}"
    prev="${{COMP_WORDS[COMP_CWORD-1]}}"
    local commands="{cmds}"
    local global_flags="{flags}"
    if [ "${{COMP_CWORD}}" -eq 1 ]; then
        COMPREPLY=( $(compgen -W "${{commands}} ${{global_flags}}" -- "${{cur}}") )
        return 0
    fi
    if [[ "${{cur}}" == -* ]]; then
        COMPREPLY=( $(compgen -W "${{global_flags}}" -- "${{cur}}") )
        return 0
    fi
    COMPREPLY=( $(compgen -f -- "${{cur}}") )
    return 0
}}
complete -F _basin basin
"#
    )?;
    Ok(())
}

fn write_zsh<W: Write>(w: &mut W) -> CliResult<()> {
    let mut b = String::new();
    b.push_str("#compdef basin\n");
    b.push_str("# zsh completion for basin                                -*- shell-script -*-\n");
    b.push_str("_basin() {\n    local -a commands\n    commands=(\n");
    for c in all() {
        b.push_str(&format!("        '{}:{}'\n", c.name, escape_zsh(c.help)));
    }
    b.push_str("    )\n    _arguments -C \\\n");
    for f in GLOBAL_FLAG_NAMES {
        b.push_str(&format!("        '{f}[global flag]' \\\n"));
    }
    b.push_str("        '1: :->command' \\\n        '*::arg:->args'\n");
    b.push_str("    case $state in\n");
    b.push_str("        command) _describe 'command' commands ;;\n");
    b.push_str("        args)    _files ;;\n");
    b.push_str("    esac\n}\n_basin \"$@\"\n");
    w.write_all(b.as_bytes())?;
    Ok(())
}

fn write_fish<W: Write>(w: &mut W) -> CliResult<()> {
    let mut b = String::from("# fish completion for basin\n");
    for c in all() {
        b.push_str(&format!(
            "complete -c basin -n '__fish_use_subcommand' -a '{}' -d '{}'\n",
            c.name,
            escape_fish(c.help)
        ));
    }
    for f in GLOBAL_FLAG_NAMES {
        let long = f.trim_start_matches("--");
        b.push_str(&format!("complete -c basin -l {long} -d 'global flag'\n"));
    }
    w.write_all(b.as_bytes())?;
    Ok(())
}

fn escape_zsh(s: &str) -> String {
    s.replace('\'', r"'\''")
}

fn escape_fish(s: &str) -> String {
    s.replace('\'', r"\'")
}
