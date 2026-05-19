// Command basin is the basin-cloud CLI.
//
// Modeled on `gh` / `flyctl` / `supabase`. Stdlib-only by design — no
// cobra, no urfave/cli — so the binary stays small and the build has
// zero new module dependencies. Subcommand dispatch is a flat switch
// at the top level; each cmd_*.go file owns its own flag.FlagSet.
//
// Auth is via personal access tokens (PATs), the same `bso_org_<32>`
// shape the dashboard mints under Org → API Tokens. Lookup order:
//
//  1. --token=<value> flag
//  2. $BASIN_TOKEN env var
//  3. tokens.<--org slug> entry from ~/.config/basin/config.json
//  4. default_token entry from the same file
//
// API endpoint defaults to https://api.basin.run, override with
// $BASIN_API or --api-url=...  Every command honours --json (machine
// output), -q/--quiet (suppress non-essential prose), and --no-color.
package main

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// version / buildDate / buildOSArch are stamped via -ldflags at build
// time. The Makefile target threads -X main.version=<sha> so a
// distributable binary can identify itself; uninjected the literals
// here serve as a sane "dev" fallback.
var (
	version     = "dev"
	buildDate   = "unknown"
	buildOSArch = ""
)

// commandHandler is the per-subcommand entrypoint. Every cmd_*.go
// file exposes one of these. The dispatch table in main() keeps the
// list of canonical commands small + alphabetical.
type commandHandler func(g *globalFlags, args []string) error

// commandEntry is one row of the subcommand dispatch table.
type commandEntry struct {
	name string
	help string
	run  commandHandler
}

// commands returns the canonical subcommand table. Wrapped in a
// function (instead of a package-level var) so the initialization
// graph stays acyclic — cmdHelp itself ranges over the result.
// Order shapes the `basin --help` summary so we keep it grouped:
// auth → org/project reads → mutations → engine surfaces → utilities.
func commands() []commandEntry {
	return []commandEntry{
		{"login", "Sign in by pasting a personal access token.", cmdLogin},
		{"logout", "Forget the locally-stored token.", cmdLogout},
		{"whoami", "Print the active user + accessible orgs.", cmdWhoami},
		{"orgs", "List / get / create / update / delete orgs + branding.", cmdOrgs},
		{"members", "List / invite / remove / role-change org members.", cmdMembers},
		{"invitations", "List / resend / revoke pending org invitations.", cmdInvitations},
		{"projects", "List / get / create / delete / pause / resume projects.", cmdProjects},
		{"init", "Scaffold a basin project directory (./basin/).", cmdInit},
		{"link", "Bind ./basin/ to a remote project (writes project_ref).", cmdLink},
		{"status", "Show the linked project + migration drift summary.", cmdStatus},
		{"unlink", "Remove the project binding from ./basin/config.toml.", cmdUnlink},
		{"sql", "Run a SQL query against a project.", cmdSQL},
		{"tables", "List / describe tables; show paginated rows.", cmdTables},
		{"migrations", "List / apply / new / get / diff / rollback DDL migrations.", cmdMigrations},
		{"branches", "List / create / get / merge / delete / events on project branches.", cmdBranches},
		{"gen", "Generate language bindings (typescript / go / python) from schema.", cmdGen},
		{"snapshots", "List / create / restore project snapshots.", cmdSnapshots},
		{"backups", "Manage backup policy / snapshots / restore jobs.", cmdBackups},
		{"logs", "Stream live project logs (or poll history).", cmdLogs},
		{"api-keys", "List / create / rotate / revoke project-scoped API keys.", cmdAPIKeys},
		{"pgwire", "Show / reveal / rotate pgwire credentials and engine JWT keys.", cmdPgwire},
		{"secrets", "List / set / remove project env-overrides.", cmdSecrets},
		{"tokens", "Manage org-scoped personal access tokens.", cmdTokens},
		{"config", "Read or write CLI config (telemetry, default org).", cmdConfig},
		{"completion", "Print a shell completion script for bash, zsh, or fish.", cmdCompletion},
		{"version", "Print binary version + build metadata.", cmdVersion},
		{"help", "Show help for the CLI or a subcommand.", cmdHelp},
	}
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		// Exit-only errors from the dispatch path are formatted with the
		// shared error printer (see shared.go). "silent" sentinel skips
		// the print so handlers that already wrote a tailored message
		// don't double up.
		if !errors.Is(err, errSilent) {
			fmt.Fprintln(os.Stderr, "basin:", err)
		}
		os.Exit(1)
	}
}

// run is split out from main() so test code can drive the CLI in
// process (without os.Exit).
func run(argv []string) error {
	g, rest, err := parseGlobalFlags(argv)
	if err != nil {
		return err
	}
	if len(rest) == 0 {
		printTopLevelHelp(os.Stdout)
		return nil
	}
	name, args := rest[0], rest[1:]
	// CLI↔cloud support-window check. Soft-fail, side-effect-only;
	// emits at most one line of stderr per command. Skipped for the
	// no-API local commands (help, version, logout) where a network
	// round-trip + warning would only add noise.
	switch name {
	case "help", "version", "logout", "completion", "config":
		// no-op — local-only commands, no network round-trip needed.
	default:
		warnIfVersionOutOfWindow(g)
		warnIfSelfUpdateAvailable(g)
	}
	// Wrap the dispatch in a telemetry envelope. emitTelemetry is a
	// best-effort fire-and-forget; opt-in (off by default), soft-fails
	// on 404 so a cloud that doesn't expose `/v1/cli/telemetry` yet
	// doesn't poison the user's command exit.
	start := time.Now()
	var runErr error
	for _, c := range commands() {
		if c.name == name {
			runErr = c.run(g, args)
			emitTelemetry(g, name, time.Since(start), runErr)
			return runErr
		}
	}
	// Unknown subcommand: print a helpful message + the canonical list.
	fmt.Fprintf(os.Stderr, "basin: unknown command %q\n\n", name)
	printTopLevelHelp(os.Stderr)
	return errSilent
}
