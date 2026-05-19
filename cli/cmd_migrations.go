// cmd_migrations — project DDL-snapshot timeline.
//
//	basin migrations list                  --project <ref>
//	basin migrations apply   <id>          --project <ref>
//	basin migrations new     <name>
//	basin migrations get     <id>          --project <ref>
//	basin migrations diff                  --project <ref>
//	basin migrations rollback <id>         --project <ref>  [--yes]
//
// All cloud calls use the /admin/v1/projects/{ref}/migrations family —
// where the cloud router actually mounts these handlers today
// (basin-cloud server.go ~line 1163). See decisions.md.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// MigrationDetail is the full shape returned by GET /admin/v1/projects/{ref}/migrations/{id}.
type MigrationDetail struct {
	ID          string `json:"id,omitempty"`
	Status      string `json:"status,omitempty"`
	AppliedAt   string `json:"applied_at,omitempty"`
	SQL         string `json:"sql,omitempty"`
	Source      string `json:"source,omitempty"`
	Summary     string `json:"summary,omitempty"`
	Op          string `json:"op,omitempty"`
	Table       string `json:"table,omitempty"`
	At          string `json:"at,omitempty"`
	Author      string `json:"author,omitempty"`
}

func cmdMigrations(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin migrations (list | apply | new | get | diff | rollback) ...")
	}
	switch args[0] {
	case "list":
		return cmdMigrationsList(g, args[1:])
	case "apply":
		return cmdMigrationsApply(g, args[1:])
	case "new":
		return cmdMigrationsNew(g, args[1:])
	case "get":
		return cmdMigrationsGet(g, args[1:])
	case "diff":
		return cmdMigrationsDiff(g, args[1:])
	case "rollback":
		return cmdMigrationsRollback(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("migrations", "Manage project migrations (DDL snapshots).", []string{
			"list                  --project <ref>          List all migrations, newest first.",
			"apply <id>            --project <ref>          Apply (roll forward to) a migration.",
			"new   <name>                                   Create a new local migration file.",
			"get   <id>            --project <ref>          Show a single migration's details.",
			"diff                  --project <ref>          Show schema drift vs applied migrations.",
			"rollback <id>         --project <ref> [--yes]  Roll back a migration.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for migrations", args[0])
	}
}

func cmdMigrationsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations list", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Migrations []MigrationRow `json:"migrations"`
	}
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { migrations: [ MigrationRow ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "ID", "AT", "OP", "TABLE", "SUMMARY")
	for _, m := range resp.Migrations {
		t.row(m.ID, m.At, m.Op, m.Table, m.Summary)
	}
	return t.flush()
}

func cmdMigrationsApply(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations apply", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin migrations apply <id> [--project <ref>]")
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "POST", "/admin/v1/projects/"+ref+"/migrations/"+id+"/rollback", nil, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of rollback envelope
		// (engine-defined; commonly { applied: bool, snapshot_id: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Applied migration %s.\n", id)
	return nil
}

// slugRunRe matches two or more consecutive underscores (after invalid chars replaced).
var slugRunRe = regexp.MustCompile(`_{2,}`)

// toSlug converts a name to a safe filename slug: only ASCII a-z, 0-9 and
// underscore are allowed; everything else (including uppercase, spaces,
// special chars, and non-ASCII letters) is replaced with underscore. Runs of
// underscores are collapsed; leading/trailing underscores are trimmed.
func toSlug(name string) string {
	// Map: ASCII a-z/0-9 → keep lowercased; ASCII A-Z → lowercase;
	// everything else (spaces, punctuation, non-ASCII) → underscore.
	s := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' {
			return r
		}
		if r >= 'A' && r <= 'Z' {
			return unicode.ToLower(r)
		}
		if r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, name)
	// Replace runs of two or more underscores with a single underscore.
	s = slugRunRe.ReplaceAllString(s, "_")
	// Trim leading/trailing underscores.
	s = strings.Trim(s, "_")
	if s == "" {
		s = "migration"
	}
	return s
}

func cmdMigrationsNew(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations new", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	nameArgs := fs.Args()
	if len(nameArgs) == 0 {
		return fmt.Errorf("usage: basin migrations new <name>")
	}
	name := strings.Join(nameArgs, " ")
	slug := toSlug(name)

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("migrations new: could not determine working directory: %w", err)
	}

	dir := filepath.Join(cwd, "basin", "migrations")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("migrations new: could not create migrations dir: %w", err)
	}

	// Pick a timestamp, bumping by 1s on collision.
	now := time.Now().UTC()
	ts := now.Unix()
	var path string
	for {
		filename := fmt.Sprintf("%d_%s.sql", ts, slug)
		path = filepath.Join(dir, filename)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			break
		}
		ts++
	}

	created := time.Unix(ts, 0).UTC()
	header := fmt.Sprintf("-- migration: %s\n-- created: %s\n\n",
		name, created.Format(time.RFC3339))

	if err := os.WriteFile(path, []byte(header), 0o644); err != nil {
		return fmt.Errorf("migrations new: write file: %w", err)
	}

	if g.json {
		// JSON shape: { "path": string, "name": string, "slug": string, "timestamp": int64 }
		return printJSON(os.Stdout, map[string]any{
			"path":      path,
			"name":      name,
			"slug":      slug,
			"timestamp": ts,
		})
	}
	fmt.Fprintln(os.Stdout, path)
	return nil
}

func cmdMigrationsGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations get", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin migrations get <id> [--project <ref>]")
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var detail MigrationDetail
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations/"+id, nil, &detail); err != nil {
		return err
	}
	if g.json {
		// JSON shape: MigrationDetail { id, status, applied_at, sql, source, summary, op, table, at, author }
		return printJSON(os.Stdout, detail)
	}
	fmt.Fprintf(os.Stdout, "id:         %s\n", detail.ID)
	fmt.Fprintf(os.Stdout, "status:     %s\n", detail.Status)
	if detail.AppliedAt != "" {
		fmt.Fprintf(os.Stdout, "applied_at: %s\n", detail.AppliedAt)
	}
	if detail.At != "" {
		fmt.Fprintf(os.Stdout, "at:         %s\n", detail.At)
	}
	if detail.Op != "" {
		fmt.Fprintf(os.Stdout, "op:         %s\n", detail.Op)
	}
	if detail.Table != "" {
		fmt.Fprintf(os.Stdout, "table:      %s\n", detail.Table)
	}
	if detail.Summary != "" {
		fmt.Fprintf(os.Stdout, "summary:    %s\n", detail.Summary)
	}
	if detail.SQL != "" {
		fmt.Fprintf(os.Stdout, "\n-- SQL:\n%s\n", detail.SQL)
	}
	return nil
}

func cmdMigrationsDiff(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations diff", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp map[string]any
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations/diff", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of diff envelope (commonly { diff: string, sql: string, changes: [...] })
		return printJSON(os.Stdout, resp)
	}
	// Try common diff-body keys in priority order.
	for _, key := range []string{"diff", "sql", "body"} {
		if v, ok := resp[key]; ok {
			if s, ok := v.(string); ok && s != "" {
				fmt.Fprint(os.Stdout, s)
				if !strings.HasSuffix(s, "\n") {
					fmt.Fprintln(os.Stdout)
				}
				return nil
			}
		}
	}
	fmt.Fprintln(os.Stdout, "(no diff)")
	return nil
}

func cmdMigrationsRollback(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations rollback", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin migrations rollback <id> [--project <ref>] [--yes]")
	}
	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	id := rest[0]

	// Confirmation prompt — skip under --yes or --json (non-interactive).
	if !*yes && !g.json {
		fmt.Fprintf(os.Stderr, "Roll back migration %s on project %s? [y/N] ", id, ref)
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
			if answer != "y" && answer != "yes" {
				fmt.Fprintln(os.Stdout, "Rollback aborted.")
				return nil
			}
		}
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "POST", "/admin/v1/projects/"+ref+"/migrations/"+id+"/rollback", nil, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of rollback envelope
		// (engine-defined; commonly { rolled_back: bool, migration_id: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Rolled back migration %s.\n", id)
	return nil
}
