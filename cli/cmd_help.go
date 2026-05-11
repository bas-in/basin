// cmd_help — top-level help printer + per-subcommand help routing.
//
// `basin help` (no args) prints the same shape `basin --help`
// renders. `basin help <cmd>` prints the targeted subcommand's
// help by re-running it with --help (each command supports the
// same convention via its own flag.FlagSet).
package main

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

// cmdHelp dispatches to the requested subcommand's --help, or prints
// the top-level summary when called without args.
func cmdHelp(g *globalFlags, args []string) error {
	_ = g
	if len(args) == 0 {
		printTopLevelHelp(os.Stdout)
		return nil
	}
	target := args[0]
	for _, c := range commands() {
		if c.name == target {
			// Avoid recursion: don't loop back into cmdHelp on
			// `basin help help` — print the top-level summary instead.
			if c.name == "help" {
				printTopLevelHelp(os.Stdout)
				return nil
			}
			return c.run(g, []string{"--help"})
		}
	}
	fmt.Fprintf(os.Stderr, "basin: unknown command %q\n", target)
	return errSilent
}

// printTopLevelHelp renders the summary every subcommand inherits.
// Kept stable so a tool like `man basin` (when we eventually ship a
// manpage) can grep against it.
func printTopLevelHelp(w io.Writer) {
	fmt.Fprintln(w, "basin — manage basin-cloud projects from the terminal.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "USAGE")
	fmt.Fprintln(w, "  basin [global flags] <command> [args]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "GLOBAL FLAGS")
	fmt.Fprintf(w, "  --api-url=<url>    Override the API endpoint (default %s).\n", defaultAPIURL())
	fmt.Fprintln(w, "  --token=<pat>      Use this PAT instead of the on-disk config.")
	fmt.Fprintln(w, "  --org=<slug>       Use the per-org token from the config file.")
	fmt.Fprintln(w, "  --json             Emit machine-readable JSON.")
	fmt.Fprintln(w, "  -q, --quiet        Suppress non-essential output.")
	fmt.Fprintln(w, "  --no-color         Disable ANSI colour in tables.")
	fmt.Fprintln(w, "  -h, --help         Show this help.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "ENVIRONMENT")
	fmt.Fprintln(w, "  BASIN_TOKEN        Same as --token.")
	fmt.Fprintln(w, "  BASIN_API          Same as --api-url.")
	fmt.Fprintln(w, "  XDG_CONFIG_HOME    Where to look for config.json (default ~/.config).")
	fmt.Fprintln(w, "  NO_COLOR           Disable ANSI colour (any non-empty value).")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "COMMANDS")
	cmds := commands()
	width := 0
	for _, c := range cmds {
		if len(c.name) > width {
			width = len(c.name)
		}
	}
	for _, c := range cmds {
		fmt.Fprintf(w, "  %-*s  %s\n", width, c.name, c.help)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Run `basin help <command>` for command-specific flags.")
}

// cmdVersion prints version + buildDate + os/arch. No network.
func cmdVersion(g *globalFlags, args []string) error {
	if g.json {
		// JSON shape: { version: string, date: string, os_arch: string, go_build: string }
		return printJSON(os.Stdout, map[string]string{
			"version":  version,
			"date":     buildDate,
			"os_arch":  osArch(),
			"go_build": runtime.Version(),
		})
	}
	fmt.Fprintf(os.Stdout, "basin %s built %s for %s (%s)\n",
		version, buildDate, osArch(), runtime.Version())
	_ = args
	return nil
}

// osArch returns "darwin/arm64" / "linux/amd64". buildOSArch is
// allowed to override (so a cross-compiled binary can advertise the
// target it was built for if the build script wants to be explicit),
// but defaults to the runtime values for a sensible local build.
func osArch() string {
	if buildOSArch != "" {
		return buildOSArch
	}
	return runtime.GOOS + "/" + runtime.GOARCH
}

// helpForCommand prints a short help block. Each cmd_*.go calls
// this from its own flag-parsing branch when --help fires.
func helpForCommand(name, summary string, lines []string) {
	fmt.Println(summary)
	fmt.Println()
	fmt.Println("USAGE")
	fmt.Println("  basin " + name + " [args]")
	if len(lines) > 0 {
		fmt.Println()
		fmt.Println("FLAGS / SUBCOMMANDS")
		for _, ln := range lines {
			fmt.Println("  " + strings.TrimRight(ln, " "))
		}
	}
}
