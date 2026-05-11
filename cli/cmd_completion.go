// cmd_completion — emit shell completion scripts.
//
//	basin completion bash
//	basin completion zsh
//	basin completion fish
//
// Prints a self-contained script to stdout. The user installs it
// per-shell:
//
//	# bash
//	basin completion bash > /etc/bash_completion.d/basin
//	# zsh
//	basin completion zsh  > "${fpath[1]}/_basin"
//	# fish
//	basin completion fish > ~/.config/fish/completions/basin.fish
//
// Scripts are hand-rolled (no text/template) so we don't take a single
// extra import and so the bytes match what `flyctl completion` ships.
// Subcommand names come from commands() so we never drift from the
// dispatch table.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func cmdCompletion(g *globalFlags, args []string) error {
	_ = g
	if len(args) == 0 {
		helpForCommand("completion", "Print a shell completion script for bash, zsh, or fish.", []string{
			"bash    Print bash completion (source from /etc/bash_completion.d/basin).",
			"zsh     Print zsh completion (drop in $fpath as _basin).",
			"fish    Print fish completion (~/.config/fish/completions/basin.fish).",
		})
		return nil
	}
	switch args[0] {
	case "bash":
		return writeBashCompletion(os.Stdout)
	case "zsh":
		return writeZshCompletion(os.Stdout)
	case "fish":
		return writeFishCompletion(os.Stdout)
	case "--help", "-h", "help":
		helpForCommand("completion", "Print a shell completion script for bash, zsh, or fish.", []string{
			"bash    Print bash completion.",
			"zsh     Print zsh completion.",
			"fish    Print fish completion.",
		})
		return nil
	default:
		printErr(g, "unknown shell %q (expected bash, zsh, or fish)", args[0])
		return errSilent
	}
}

// commandNames returns the canonical subcommand list, lifted from
// commands() so the completion stays in sync with dispatch.
func commandNames() []string {
	cmds := commands()
	out := make([]string, 0, len(cmds))
	for _, c := range cmds {
		out = append(out, c.name)
	}
	return out
}

// globalFlagNames is the static list rendered into every shell's
// flag-completion table. Kept in sync with parseGlobalFlags by hand —
// the set is tiny and rarely changes.
var globalFlagNames = []string{
	"--api-url", "--token", "--org", "--json", "--quiet", "--no-color", "--help",
}

// writeBashCompletion emits a bash completion function bound to
// `_basin` + registered via `complete -F`.
func writeBashCompletion(w io.Writer) error {
	cmds := strings.Join(commandNames(), " ")
	flags := strings.Join(globalFlagNames, " ")
	_, err := fmt.Fprintf(w, `# bash completion for basin                              -*- shell-script -*-
_basin() {
    local cur prev words cword
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    local commands="%s"
    local global_flags="%s"
    # First positional after "basin" → top-level command.
    if [ "${COMP_CWORD}" -eq 1 ]; then
        COMPREPLY=( $(compgen -W "${commands} ${global_flags}" -- "${cur}") )
        return 0
    fi
    # Otherwise let the shell complete files / flags generically.
    if [[ "${cur}" == -* ]]; then
        COMPREPLY=( $(compgen -W "${global_flags}" -- "${cur}") )
        return 0
    fi
    COMPREPLY=( $(compgen -f -- "${cur}") )
    return 0
}
complete -F _basin basin
`, cmds, flags)
	return err
}

// writeZshCompletion emits a zsh #compdef function. Uses `_arguments`
// + a tiny dispatch table so completion can branch on the first
// positional.
func writeZshCompletion(w io.Writer) error {
	var b strings.Builder
	b.WriteString("#compdef basin\n")
	b.WriteString("# zsh completion for basin                                -*- shell-script -*-\n")
	b.WriteString("_basin() {\n")
	b.WriteString("    local -a commands\n")
	b.WriteString("    commands=(\n")
	for _, c := range commands() {
		b.WriteString(fmt.Sprintf("        '%s:%s'\n", c.name, escapeZsh(c.help)))
	}
	b.WriteString("    )\n")
	b.WriteString("    _arguments -C \\\n")
	for _, f := range globalFlagNames {
		b.WriteString(fmt.Sprintf("        '%s[global flag]' \\\n", f))
	}
	b.WriteString("        '1: :->command' \\\n")
	b.WriteString("        '*::arg:->args'\n")
	b.WriteString("    case $state in\n")
	b.WriteString("        command) _describe 'command' commands ;;\n")
	b.WriteString("        args)    _files ;;\n")
	b.WriteString("    esac\n")
	b.WriteString("}\n")
	b.WriteString("_basin \"$@\"\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// writeFishCompletion emits fish's `complete` lines. One per command,
// keyed on `__fish_use_subcommand` so suggestions only show at the
// right position.
func writeFishCompletion(w io.Writer) error {
	var b strings.Builder
	b.WriteString("# fish completion for basin\n")
	for _, c := range commands() {
		b.WriteString(fmt.Sprintf("complete -c basin -n '__fish_use_subcommand' -a '%s' -d '%s'\n",
			c.name, escapeFish(c.help)))
	}
	for _, f := range globalFlagNames {
		// Strip the leading "--" — fish wants the long-form bare.
		long := strings.TrimPrefix(f, "--")
		b.WriteString(fmt.Sprintf("complete -c basin -l %s -d 'global flag'\n", long))
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// escapeZsh escapes single quotes for the zsh `commands` array.
func escapeZsh(s string) string {
	return strings.ReplaceAll(s, "'", `'\''`)
}

// escapeFish escapes single quotes for fish's `-d` argument.
func escapeFish(s string) string {
	return strings.ReplaceAll(s, "'", `\'`)
}
