// cmd_init — scaffold a basin project directory.
//
//	basin init [--force]
//
// Creates ./basin/config.toml, ./basin/migrations/ (empty dir), and
// ./basin/seed.sql. Refuses if ./basin/ already exists unless --force
// is passed.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func cmdInit(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	force := fs.Bool("force", false, "Overwrite an existing ./basin/ directory.")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("init", "Scaffold a basin project directory (./basin/).", []string{
			"--force   Overwrite an existing ./basin/ directory.",
		})
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
		return fmt.Errorf("init: could not determine working directory: %w", err)
	}

	basinDir := filepath.Join(cwd, "basin")

	// Check for existing basin/ directory.
	if st, err := os.Stat(basinDir); err == nil && st.IsDir() {
		if !*force {
			if g.json {
				// JSON shape: { ok: bool, error: string }
				return printJSON(os.Stdout, map[string]any{
					"ok":    false,
					"error": "./basin/ already exists — use --force to overwrite",
				})
			}
			return fmt.Errorf("./basin/ already exists — use --force to overwrite")
		}
	}

	// Create ./basin/migrations/ (and ./basin/ implicitly).
	migrationsDir := filepath.Join(basinDir, "migrations")
	if err := os.MkdirAll(migrationsDir, 0o755); err != nil {
		return fmt.Errorf("init: create migrations dir: %w", err)
	}

	// Write ./basin/config.toml with an empty workingProject so the
	// file exists and is parseable. project_ref is left blank until
	// `basin link` writes it.
	if err := saveWorkingProject(cwd, workingProject{}); err != nil {
		return fmt.Errorf("init: write config.toml: %w", err)
	}

	// Write ./basin/seed.sql with a placeholder.
	seedPath := filepath.Join(basinDir, "seed.sql")
	if err := os.WriteFile(seedPath, []byte("-- seed data\n"), 0o644); err != nil {
		return fmt.Errorf("init: write seed.sql: %w", err)
	}

	if g.json {
		// JSON shape: { ok: bool, basin_dir: string, files: [string] }
		return printJSON(os.Stdout, map[string]any{
			"ok":        true,
			"basin_dir": basinDir,
			"files": []string{
				filepath.Join(basinDir, "config.toml"),
				migrationsDir,
				seedPath,
			},
		})
	}
	printInfo(g, "Initialised basin project directory:")
	printInfo(g, "  %s", filepath.Join(basinDir, "config.toml"))
	printInfo(g, "  %s/", migrationsDir)
	printInfo(g, "  %s", seedPath)
	printInfo(g, "Run `basin link --project=<ref>` to bind this directory to a remote project.")
	return nil
}
