package main

import (
	"flag"
	"fmt"
	"os"
)

// cmdLogout removes the token entry from the config file. With --all
// it wipes every token. Idempotent: no-ops cleanly when the entry
// doesn't exist.
func cmdLogout(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("logout", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	orgFlag := fs.String("org", g.orgSlug, "Wipe tokens.<slug> instead of default_token.")
	allFlag := fs.Bool("all", false, "Wipe every stored token.")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("logout", "Forget the locally-stored personal access token.", []string{
			"--org=<slug>   Wipe the per-org entry. Default wipes default_token.",
			"--all          Wipe every stored token.",
		})
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	target := *orgFlag
	if *allFlag {
		target = "*"
	}
	if err := removeToken(target); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	if g.json {
		// JSON shape: { signed_out: bool, scope: string }
		return printJSON(os.Stdout, map[string]any{"signed_out": true, "scope": orgOrDefault(*orgFlag)})
	}
	if *allFlag {
		fmt.Fprintln(os.Stdout, "Cleared every stored token.")
	} else {
		fmt.Fprintln(os.Stdout, "Cleared "+orgOrDefault(*orgFlag)+".")
	}
	return nil
}
