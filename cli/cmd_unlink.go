// cmd_unlink — remove the project_ref binding from ./basin/config.toml.
//
//	basin unlink
//
// Removes only the project_ref line; keeps migrations/ and seed.sql.
// Idempotent: if project_ref is already absent, exits 0 without error.
package main

import (
	"flag"
	"fmt"
	"os"
)

func cmdUnlink(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("unlink", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("unlink", "Remove the project binding from ./basin/config.toml.", nil)
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("unlink: could not determine working directory: %w", err)
	}

	wp, err := loadWorkingProject(cwd)
	if err != nil {
		return fmt.Errorf("unlink: %w", err)
	}

	// Not linked — idempotent success.
	if wp == nil || wp.ProjectRef == "" {
		if g.json {
			// JSON shape: { ok: bool, was_linked: bool }
			return printJSON(os.Stdout, map[string]any{
				"ok":         true,
				"was_linked": false,
			})
		}
		printInfo(g, "Not linked to any project (nothing to do).")
		return nil
	}

	prev := wp.ProjectRef
	wp.ProjectRef = ""
	if err := saveWorkingProject(cwd, *wp); err != nil {
		return fmt.Errorf("unlink: write config.toml: %w", err)
	}

	if g.json {
		// JSON shape: { ok: bool, was_linked: bool, unlinked_ref: string }
		return printJSON(os.Stdout, map[string]any{
			"ok":           true,
			"was_linked":   true,
			"unlinked_ref": prev,
		})
	}
	fmt.Fprintf(os.Stdout, "Unlinked from project %s.\n", prev)
	return nil
}
