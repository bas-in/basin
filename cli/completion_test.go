// completion_test — shape-pin the bash/zsh/fish completion output.
//
// We don't run the scripts (no shell available in CI on every matrix),
// but we do assert that:
//
//   - Every subcommand from commands() appears in every emitted script.
//   - The shell-specific header line ("complete -F", "#compdef", etc) is
//     present so the user knows the format wasn't truncated.
//   - Unknown shells exit with errSilent (handled in main.go) rather
//     than panicking.
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBashCompletionContainsEveryCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := writeBashCompletion(&buf); err != nil {
		t.Fatalf("writeBashCompletion: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "complete -F _basin basin") {
		t.Errorf("bash completion missing `complete -F` registration:\n%s", out)
	}
	for _, c := range commands() {
		if !strings.Contains(out, c.name) {
			t.Errorf("bash completion missing command %q", c.name)
		}
	}
	for _, f := range globalFlagNames {
		if !strings.Contains(out, f) {
			t.Errorf("bash completion missing flag %q", f)
		}
	}
}

func TestZshCompletionContainsEveryCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := writeZshCompletion(&buf); err != nil {
		t.Fatalf("writeZshCompletion: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "#compdef basin") {
		t.Errorf("zsh completion missing #compdef header:\n%s", out)
	}
	for _, c := range commands() {
		if !strings.Contains(out, "'"+c.name+":") {
			t.Errorf("zsh completion missing command %q", c.name)
		}
	}
}

func TestFishCompletionContainsEveryCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := writeFishCompletion(&buf); err != nil {
		t.Fatalf("writeFishCompletion: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "complete -c basin") {
		t.Errorf("fish completion missing `complete -c basin`:\n%s", out)
	}
	for _, c := range commands() {
		if !strings.Contains(out, "-a '"+c.name+"'") {
			t.Errorf("fish completion missing command %q", c.name)
		}
	}
}

func TestCompletionUnknownShellFails(t *testing.T) {
	g := &globalFlags{}
	if err := cmdCompletion(g, []string{"powershell"}); err == nil {
		t.Errorf("expected errSilent for unknown shell, got nil")
	}
}

func TestCompletionNoArgsPrintsHelp(t *testing.T) {
	g := &globalFlags{}
	// No args path: prints help to stdout/stderr. We just confirm it
	// returns nil — the help body is rendered by helpForCommand and
	// has its own coverage elsewhere.
	if err := cmdCompletion(g, nil); err != nil {
		t.Errorf("expected nil from `completion` (no args), got %v", err)
	}
}

func TestEscapeZshEscapesSingleQuotes(t *testing.T) {
	in := "it's a test"
	got := escapeZsh(in)
	if !strings.Contains(got, `'\''`) {
		t.Errorf("escapeZsh(%q) = %q; expected '\\'' encoding", in, got)
	}
}

func TestEscapeFishEscapesSingleQuotes(t *testing.T) {
	in := "it's a test"
	got := escapeFish(in)
	if !strings.Contains(got, `\'`) {
		t.Errorf("escapeFish(%q) = %q; expected backslash-escape", in, got)
	}
}
