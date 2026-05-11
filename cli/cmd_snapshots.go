// cmd_snapshots — backup snapshots + restore.
//
//	basin snapshots list    --project <ref>
//	basin snapshots create  --project <ref> [--name "before-rls"]
//	basin snapshots restore <id> --project <ref> [--yes]    confirm-by-typing-ref
//
// Restore is destructive: we require the user to type the project's
// ref exactly, mirroring the dashboard's confirm-by-name modal.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdSnapshots(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin snapshots (list | create | restore) ...")
	}
	switch args[0] {
	case "list":
		return cmdSnapshotsList(g, args[1:])
	case "create":
		return cmdSnapshotsCreate(g, args[1:])
	case "restore":
		return cmdSnapshotsRestore(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("snapshots", "List / create / restore project snapshots.", []string{
			"list    --project <ref>                             List snapshots, newest first.",
			"create  --project <ref> [--name <description>]      Create a manual snapshot.",
			"restore <id> --project <ref> [--yes]                Restore (confirm-by-ref).",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for snapshots", args[0])
	}
}

func cmdSnapshotsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("snapshots list", flag.ContinueOnError)
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
		Snapshots []*Snapshot `json:"snapshots"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+*project+"/backups/snapshots", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { snapshots: [ Snapshot ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "ID", "ORIGIN", "STATUS", "CREATED", "SIZE", "DESCRIPTION")
	for _, s := range resp.Snapshots {
		t.row(s.ID, s.Origin, s.Status, s.CreatedAt, s.SizeBytes, s.Description)
	}
	return t.flush()
}

func cmdSnapshotsCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("snapshots create", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	name := fs.String("name", "", "Description (free-form).")
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
	ctx, cancel := context.WithTimeout(context.Background(), 60_000_000_000)
	defer cancel()
	body := map[string]any{}
	if *name != "" {
		body["description"] = *name
	}
	var resp map[string]any
	if err := c.do(ctx, "POST", "/v1/projects/"+*project+"/backups/snapshots", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { snapshot: Snapshot, engine_msg?: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Snapshot create requested.")
	if snap, ok := resp["snapshot"].(map[string]any); ok {
		if id, _ := snap["id"].(string); id != "" {
			fmt.Fprintf(os.Stdout, "  id:     %s\n", id)
		}
		if status, _ := snap["status"].(string); status != "" {
			fmt.Fprintf(os.Stdout, "  status: %s\n", status)
		}
	}
	if msg, ok := resp["engine_msg"].(string); ok && msg != "" {
		fmt.Fprintf(os.Stdout, "  engine: %s\n", msg)
	}
	return nil
}

func cmdSnapshotsRestore(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("snapshots restore", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	yes := fs.Bool("yes", false, "Skip the confirm-by-ref prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin snapshots restore <id> --project <ref>")
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	if !*yes {
		fmt.Fprintf(os.Stderr, "Restore overwrites the project's data plane. Re-type the project ref %q to confirm:\n> ", *project)
		typed, err := readLine()
		if err != nil {
			return err
		}
		if strings.TrimSpace(typed) != *project {
			return fmt.Errorf("ref mismatch — aborted")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120_000_000_000)
	defer cancel()
	body := map[string]any{
		"source_kind": "snapshot",
		"snapshot_id": id,
	}
	var out map[string]any
	if err := c.do(ctx, "POST", "/v1/projects/"+*project+"/backups/restore", body, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of /v1/projects/:ref/backups/restore envelope
		// (commonly { restore: { id, status }, engine_msg?: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Restore from snapshot %s requested.\n", id)
	return nil
}
