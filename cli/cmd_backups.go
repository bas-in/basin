// cmd_backups — backup policy, per-project snapshots, and restore jobs.
//
//	basin backups policy get                             [--project=<ref>]
//	basin backups policy set --retention-days=<n> --schedule=<cron> [--enabled=true|false] [--project=<ref>]
//	basin backups snapshots list                         [--project=<ref>]
//	basin backups snapshots create [--label=<text>]      [--project=<ref>]
//	basin backups snapshots expire <id> [--yes]          [--project=<ref>]
//	basin backups restore --from=<snapshot_id> [--into=<branch_ref>] [--yes] [--project=<ref>]
//	basin backups restore-jobs list                      [--project=<ref>]
//
// Routes: /v1/projects/{ref}/backups/policy, /backups/snapshots,
//         /backups/snapshots/{id}/expire, /backups/restore, /backups/restore/jobs
//
// Note: this is the project-wide DDL+data backup surface. It is entirely
// separate from the per-table snapshot surface in cmd_snapshots.go
// (/v1/projects/{ref}/tables/{name}/snapshots). Treat them as distinct.
//
// Project ref resolution order:
//  1. --project=<ref> flag
//  2. loadWorkingProject(cwd) — ./basin/config.toml (via resolveProjectRef)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────────

// BackupPolicy is the read/write shape for GET|PUT /v1/projects/{ref}/backups/policy.
// JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
type BackupPolicy struct {
	RetentionDays int    `json:"retention_days"`
	Schedule      string `json:"schedule"`
	Enabled       bool   `json:"enabled"`
}

// BackupSnapshot is one row from GET /v1/projects/{ref}/backups/snapshots
// and the embedded "snapshot" field in create responses.
// JSON shape: { "id": "...", "label": "...", "size_bytes": N, "created_at": "..." }
type BackupSnapshot struct {
	ID        string `json:"id,omitempty"`
	Label     string `json:"label,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
}

// RestoreJob is one row from GET /v1/projects/{ref}/backups/restore/jobs.
// JSON shape: { "id":"...", "status":"...", "started_at":"...", "completed_at":"...", "snapshot_id":"..." }
type RestoreJob struct {
	ID          string `json:"id,omitempty"`
	Status      string `json:"status,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	SnapshotID  string `json:"snapshot_id,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────────

// cmdBackups is the top-level dispatcher for `basin backups …`.
// It is exported so the dispatcher session can register it in main.go.
func cmdBackups(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin backups (policy | snapshots | restore | restore-jobs) ...")
	}
	switch args[0] {
	case "policy":
		return cmdBackupsPolicy(g, args[1:])
	case "snapshots":
		return cmdBackupsSnapshots(g, args[1:])
	case "restore-jobs":
		return cmdBackupsRestoreJobsDispatch(g, args[1:])
	case "restore":
		return cmdBackupsRestore(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("backups", "Manage project backup policy, snapshots, and restore jobs.", []string{
			"policy get                                   [--project=<ref>]  Get the backup policy.",
			"policy set --retention-days=<n> --schedule=<cron> [--enabled=true|false] [--project=<ref>]  Update the backup policy.",
			"snapshots list                               [--project=<ref>]  List backup snapshots.",
			"snapshots create [--label=<text>]            [--project=<ref>]  Create a manual backup snapshot.",
			"snapshots expire <id> [--yes]                [--project=<ref>]  Expire (delete) a backup snapshot.",
			"restore --from=<snapshot_id> [--into=<branch_ref>] [--yes] [--project=<ref>]  Restore from a snapshot.",
			"restore-jobs list                            [--project=<ref>]  List restore jobs.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for backups", args[0])
	}
}

// ── backups policy ───────────────────────────────────────────────────────

func cmdBackupsPolicy(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin backups policy (get | set) ...")
	}
	switch args[0] {
	case "get":
		return cmdBackupsPolicyGet(g, args[1:])
	case "set":
		return cmdBackupsPolicySet(g, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q for backups policy", args[0])
	}
}

func cmdBackupsPolicyGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups policy get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
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
	var policy BackupPolicy
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/backups/policy", nil, &policy); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
		return printJSON(os.Stdout, policy)
	}
	fmt.Fprintf(os.Stdout, "retention_days: %d\n", policy.RetentionDays)
	fmt.Fprintf(os.Stdout, "schedule:       %s\n", policy.Schedule)
	fmt.Fprintf(os.Stdout, "enabled:        %v\n", policy.Enabled)
	return nil
}

func cmdBackupsPolicySet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups policy set", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	retentionDays := fs.Int("retention-days", 0, "Retention period in days (required, >0).")
	schedule := fs.String("schedule", "", "Cron expression for scheduled backups (required).")
	enabled := fs.String("enabled", "true", "Whether scheduled backups are enabled (true|false).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	// Client-side validation before any HTTP call.
	if *retentionDays <= 0 {
		return fmt.Errorf("--retention-days must be greater than 0 (got %d)", *retentionDays)
	}
	if strings.TrimSpace(*schedule) == "" {
		return fmt.Errorf("--schedule must be a non-empty cron expression")
	}

	enabledBool, err := strconv.ParseBool(*enabled)
	if err != nil {
		return fmt.Errorf("--enabled must be true or false (got %q)", *enabled)
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

	body := BackupPolicy{
		RetentionDays: *retentionDays,
		Schedule:      *schedule,
		Enabled:       enabledBool,
	}
	var resp BackupPolicy
	if err := c.do(ctx, "PUT", "/v1/projects/"+ref+"/backups/policy", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "retention_days": N, "schedule": "...", "enabled": bool }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "retention_days: %d\n", resp.RetentionDays)
	fmt.Fprintf(os.Stdout, "schedule:       %s\n", resp.Schedule)
	fmt.Fprintf(os.Stdout, "enabled:        %v\n", resp.Enabled)
	return nil
}

// ── backups snapshots ────────────────────────────────────────────────────

func cmdBackupsSnapshots(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin backups snapshots (list | create | expire) ...")
	}
	switch args[0] {
	case "list":
		return cmdBackupsSnapshotsList(g, args[1:])
	case "create":
		return cmdBackupsSnapshotsCreate(g, args[1:])
	case "expire":
		return cmdBackupsSnapshotsExpire(g, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q for backups snapshots", args[0])
	}
}

func cmdBackupsSnapshotsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups snapshots list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
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
		Snapshots []*BackupSnapshot `json:"snapshots"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/backups/snapshots", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "snapshots": [ { "id":"...", "label":"...", "size_bytes":N, "created_at":"..." } ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Snapshots) == 0 {
		fmt.Fprintln(os.Stdout, "(no backup snapshots)")
		return nil
	}
	t := newTable(g, "ID", "LABEL", "SIZE_BYTES", "CREATED")
	for _, s := range resp.Snapshots {
		t.row(s.ID, s.Label, s.SizeBytes, s.CreatedAt)
	}
	return t.flush()
}

func cmdBackupsSnapshotsCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups snapshots create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	label := fs.String("label", "", "Optional label for the snapshot.")
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
	// Snapshot creation can be slow — use a longer timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 60_000_000_000) // 60s
	defer cancel()

	body := map[string]any{}
	if *label != "" {
		body["label"] = *label
	}

	var resp struct {
		Snapshot *BackupSnapshot `json:"snapshot"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/backups/snapshots", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "snapshot": { "id":"...", "label":"...", "size_bytes":N, "created_at":"..." } }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Backup snapshot create requested.")
	if s := resp.Snapshot; s != nil {
		if s.ID != "" {
			fmt.Fprintf(os.Stdout, "  id:    %s\n", s.ID)
		}
		if s.Label != "" {
			fmt.Fprintf(os.Stdout, "  label: %s\n", s.Label)
		}
	}
	return nil
}

func cmdBackupsSnapshotsExpire(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups snapshots expire", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin backups snapshots expire <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to expire snapshot %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Expire backup snapshot %q for project %q? This cannot be undone. [y/N] ", id, ref)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Expired bool   `json:"expired"`
		ID      string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/backups/snapshots/"+id+"/expire", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "expired": true, "id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Expired backup snapshot %s.\n", id)
	return nil
}

// ── backups restore ──────────────────────────────────────────────────────

func cmdBackupsRestore(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	from := fs.String("from", "", "Snapshot ID to restore from (required).")
	into := fs.String("into", "", "Branch ref to restore into (optional; omit for same project).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	if *from == "" {
		return fmt.Errorf("--from is required (snapshot ID to restore from)")
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to restore non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Restore project %q from snapshot %q? This will overwrite data. [y/N] ", ref, *from)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	// Restore can be slow — use a longer timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 120_000_000_000) // 2 min
	defer cancel()

	body := map[string]any{
		"snapshot_id": *from,
	}
	if *into != "" {
		body["into_branch_ref"] = *into
	}

	var resp struct {
		RestoreJobID string `json:"restore_job_id"`
		Status       string `json:"status"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/backups/restore", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "restore_job_id": "...", "status": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Restore job started: %s (status: %s)\n", resp.RestoreJobID, resp.Status)
	return nil
}

// ── backups restore-jobs ─────────────────────────────────────────────────

func cmdBackupsRestoreJobsDispatch(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin backups restore-jobs (list) ...")
	}
	switch args[0] {
	case "list":
		return cmdBackupsRestoreJobsList(g, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q for backups restore-jobs", args[0])
	}
}

func cmdBackupsRestoreJobsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("backups restore-jobs list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
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
		Jobs []*RestoreJob `json:"jobs"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/backups/restore/jobs", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "jobs": [ { "id":"...", "status":"...", "started_at":"...", "completed_at":"...", "snapshot_id":"..." } ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Jobs) == 0 {
		fmt.Fprintln(os.Stdout, "(no restore jobs)")
		return nil
	}
	t := newTable(g, "ID", "STATUS", "STARTED", "COMPLETED", "SNAPSHOT")
	for _, j := range resp.Jobs {
		completed := "—"
		if j.CompletedAt != "" {
			completed = j.CompletedAt
		}
		t.row(j.ID, j.Status, j.StartedAt, completed, j.SnapshotID)
	}
	return t.flush()
}
