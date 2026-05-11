// cmd_migrations — project DDL-snapshot timeline.
//
//	basin migrations list   --project <ref>
//	basin migrations apply  <id>      --project <ref>     (rollback to snapshot)
//
// "Apply" maps onto the cloud's POST /admin/v1/projects/:ref/migrations/:id/rollback —
// rolling forward to a previously-recorded snapshot. The engine
// records every DDL as a snapshot so this is the same surface the
// dashboard's Migration Manager hits.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

func cmdMigrations(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin migrations (list | apply) ...")
	}
	switch args[0] {
	case "list":
		return cmdMigrationsList(g, args[1:])
	case "apply":
		return cmdMigrationsApply(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("migrations", "List or apply project migrations (DDL snapshots).", []string{
			"list  --project <ref>          Show every recorded snapshot, newest first.",
			"apply <id> --project <ref>     Roll forward to the named snapshot.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for migrations", args[0])
	}
}

func cmdMigrationsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("migrations list", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
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
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+*project+"/migrations", nil, &resp); err != nil {
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
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin migrations apply <id> --project <ref>")
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "POST", "/admin/v1/projects/"+*project+"/migrations/"+id+"/rollback", nil, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of rollback envelope
		// (engine-defined; commonly { applied: bool, snapshot_id: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Rolled forward to snapshot %s.\n", id)
	return nil
}
