// cmd_docs — open the Basin CLI documentation in the default browser.
//
//	basin docs [<subcommand>]
//
// With no arguments, opens https://docs.basin.run/cli.
// With a subcommand name, opens https://docs.basin.run/cli/<subcommand>.
//
// Under --json, the command prints the URL and an "opened" boolean (false,
// since CI environments should not spawn browsers) and does NOT launch a
// browser process. This is the safe path for automation and CI.
//
// Browser launch is OS-dispatched via runtime.GOOS:
//   - darwin   → open <url>
//   - linux     → xdg-open <url>
//   - windows   → rundll32 url.dll,FileProtocolHandler <url>
//
// The browser-spawn behaviour is injectable via the package-level
// openBrowserFunc variable so tests can replace it without spawning
// real processes.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

const docsBaseURL = "https://docs.basin.run/cli"

// openBrowserFunc is the injectable seam for tests. In production it
// defaults to openBrowserOS, which calls the OS-appropriate launcher.
// Tests replace this with a no-op recorder to avoid spawning real
// browser processes.
var openBrowserFunc func(url string) error = openBrowserOS

// openBrowserOS launches the user's default browser for the given URL.
// It dispatches on runtime.GOOS and returns an error when the OS is
// not recognised or the exec call fails.
func openBrowserOS(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux", "freebsd", "openbsd", "netbsd", "dragonfly":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return fmt.Errorf("docs: unsupported OS %q — open %s manually", runtime.GOOS, url)
	}
	return cmd.Start()
}

// cmdDocs is the exported handler registered in main.go's commands() table
// by the dispatcher session. It must not be registered here in cmd_docs.go
// (the dispatcher session handles main.go edits to avoid cross-agent
// collision per decisions.md).
func cmdDocs(g *globalFlags, args []string) error {
	// Handle help flags before anything else.
	for _, a := range args {
		if a == "--help" || a == "-h" || a == "help" {
			helpForCommand("docs", "Open the Basin CLI documentation in your browser.", []string{
				"[<subcommand>]   Open the docs page for a specific subcommand.",
				"                  Omit to open https://docs.basin.run/cli.",
				"--json           Print { \"url\": \"...\", \"opened\": false } without launching a browser.",
			})
			return nil
		}
	}

	// Collect the first non-flag positional argument as the subcommand name.
	sub := ""
	for _, a := range args {
		if len(a) > 0 && a[0] != '-' {
			sub = a
			break
		}
	}

	// Build the target URL.
	url := docsBaseURL
	if sub != "" {
		url = docsBaseURL + "/" + sub
	}

	// Under --json: print metadata but do NOT spawn the browser.
	// CI environments (GitHub Actions, etc.) can OOM trying to launch a
	// graphical browser process, so we suppress it unconditionally under --json.
	if g.json {
		// JSON shape: { "url": "https://docs.basin.run/cli[/<subcommand>]", "opened": false }
		return printJSON(os.Stdout, map[string]any{
			"url":    url,
			"opened": false,
		})
	}

	// Human path: emit a progress line then spawn the browser.
	fmt.Fprintf(os.Stderr, "Opening %s ...\n", url)
	if err := openBrowserFunc(url); err != nil {
		return fmt.Errorf("docs: could not open browser: %w", err)
	}
	return nil
}
