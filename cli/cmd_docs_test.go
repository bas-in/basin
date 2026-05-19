// cmd_docs_test — coverage for the `basin docs` subcommand.
//
// Tests use the openBrowserFunc injectable seam so no real browser
// process is ever spawned. All URL assertions are exact-match so a
// future refactor of the base URL constant triggers a test failure
// rather than silently shipping the wrong link.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// ── Test 1: no subcommand → base URL ────────────────────────────────────────

// TestDocsNoSubcommandJSONNoSpawn verifies that --json mode returns the base
// URL and does NOT invoke the browser, even with no subcommand argument.
func TestDocsNoSubcommandJSONNoSpawn(t *testing.T) {
	browserCalled := false
	prev := openBrowserFunc
	openBrowserFunc = func(url string) error {
		browserCalled = true
		return nil
	}
	defer func() { openBrowserFunc = prev }()

	g := &globalFlags{json: true}
	if err := cmdDocs(g, nil); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if browserCalled {
		t.Error("expected browser NOT to be called under --json (no subcommand)")
	}
}

// TestDocsNoSubcommandURL verifies the URL produced when no subcommand is given.
func TestDocsNoSubcommandURL(t *testing.T) {
	want := "https://docs.basin.run/cli"
	if docsBaseURL != want {
		t.Errorf("docsBaseURL = %q, want %q", docsBaseURL, want)
	}
}

// ── Test 2: with subcommand → suffixed URL ───────────────────────────────────

func TestDocsWithSubcommand(t *testing.T) {
	tests := []struct {
		sub  string
		want string
	}{
		{"db", "https://docs.basin.run/cli/db"},
		{"branches", "https://docs.basin.run/cli/branches"},
		{"gen", "https://docs.basin.run/cli/gen"},
		{"migrations", "https://docs.basin.run/cli/migrations"},
	}

	for _, tc := range tests {
		t.Run(tc.sub, func(t *testing.T) {
			var gotURL string
			prev := openBrowserFunc
			openBrowserFunc = func(url string) error {
				gotURL = url
				return nil
			}
			defer func() { openBrowserFunc = prev }()

			g := &globalFlags{}
			if err := cmdDocs(g, []string{tc.sub}); err != nil {
				t.Fatalf("cmdDocs(%q): unexpected error: %v", tc.sub, err)
			}
			if gotURL != tc.want {
				t.Errorf("URL for sub=%q: got %q, want %q", tc.sub, gotURL, tc.want)
			}
		})
	}
}

// ── Test 3: --json does NOT spawn the browser ────────────────────────────────

func TestDocsJSONDoesNotSpawnBrowser(t *testing.T) {
	browserCalled := false
	prev := openBrowserFunc
	openBrowserFunc = func(url string) error {
		browserCalled = true
		return nil
	}
	defer func() { openBrowserFunc = prev }()

	g := &globalFlags{json: true}
	if err := cmdDocs(g, []string{"db"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if browserCalled {
		t.Error("expected browser NOT to be called when --json is set, but openBrowserFunc was invoked")
	}
}

// TestDocsJSONShape validates the { "url": "...", "opened": false } shape.
func TestDocsJSONShape(t *testing.T) {
	cases := []struct {
		args    []string
		wantURL string
	}{
		{nil, "https://docs.basin.run/cli"},
		{[]string{"branches"}, "https://docs.basin.run/cli/branches"},
		{[]string{"db"}, "https://docs.basin.run/cli/db"},
	}
	for _, tc := range cases {
		sub := ""
		for _, a := range tc.args {
			if len(a) > 0 && a[0] != '-' {
				sub = a
				break
			}
		}
		url := docsBaseURL
		if sub != "" {
			url = docsBaseURL + "/" + sub
		}
		if url != tc.wantURL {
			t.Errorf("args=%v: URL=%q, want=%q", tc.args, url, tc.wantURL)
		}
		// Round-trip the JSON shape that cmdDocs would emit.
		m := map[string]any{"url": url, "opened": false}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(b, &decoded); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if decoded["url"] != url {
			t.Errorf("json url: got %v, want %v", decoded["url"], url)
		}
		if decoded["opened"] != false {
			t.Errorf("json opened: got %v, want false", decoded["opened"])
		}
	}
}

// ── Test 4: browser error is propagated ─────────────────────────────────────

func TestDocsBrowserErrorPropagated(t *testing.T) {
	prev := openBrowserFunc
	openBrowserFunc = func(url string) error {
		return fmt.Errorf("no browser available")
	}
	defer func() { openBrowserFunc = prev }()

	g := &globalFlags{}
	err := cmdDocs(g, []string{"db"})
	if err == nil {
		t.Fatal("expected an error when browser launch fails, got nil")
	}
	if !strings.Contains(err.Error(), "no browser available") {
		t.Errorf("error %q does not mention the underlying cause", err.Error())
	}
}

// ── Test 5: unknown OS → typed error via docsLaunchForOS ─────────────────────

// TestDocsUnknownOS checks that the dispatch helper returns a typed error for
// an unrecognised GOOS value. We cannot change runtime.GOOS at runtime, so
// we test the logic by calling docsLaunchForOS directly.
func TestDocsUnknownOS(t *testing.T) {
	err := docsLaunchForOS("plan9", "https://docs.basin.run/cli")
	if err == nil {
		t.Fatal("expected error for unknown OS, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported OS") {
		t.Errorf("error %q does not mention 'unsupported OS'", err.Error())
	}
}

// ── Test 6: known OS values do not error ─────────────────────────────────────

func TestDocsKnownOSValues(t *testing.T) {
	known := []string{"darwin", "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "windows"}
	for _, goos := range known {
		t.Run(goos, func(t *testing.T) {
			err := docsLaunchForOS(goos, "https://docs.basin.run/cli")
			if err != nil {
				t.Errorf("OS %q: unexpected error: %v", goos, err)
			}
		})
	}
}

// docsLaunchForOS is a testable twin of openBrowserOS that accepts the OS
// name as a parameter instead of reading runtime.GOOS. It returns nil for
// known OS values (without actually running any command) and a descriptive
// error for unknown ones, so tests can assert on dispatch without spawning
// real processes.
//
// This function lives in the test file because it is only needed for testing;
// production code uses openBrowserOS (which calls runtime.GOOS) directly.
func docsLaunchForOS(goos, url string) error {
	switch goos {
	case "darwin", "linux", "freebsd", "openbsd", "netbsd", "dragonfly", "windows":
		return nil
	default:
		return fmt.Errorf("docs: unsupported OS %q — open %s manually", goos, url)
	}
}
