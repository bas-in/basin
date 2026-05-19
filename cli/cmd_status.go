// cmd_status — show the current directory's project binding and health.
//
//	basin status
//
// Reads ./basin/config.toml for the linked project ref, then calls
// GET /v1/projects/{ref} for live status + GET /admin/v1/projects/{ref}/migrations
// to compare against ./basin/migrations/*.sql for drift detection.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func cmdStatus(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("status", "Show the linked project's live status and migration drift.", nil)
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
		return fmt.Errorf("status: could not determine working directory: %w", err)
	}

	// Load the working project config.
	wp, err := loadWorkingProject(cwd)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}

	// Not linked — exit 0 with a clear message.
	if wp == nil || wp.ProjectRef == "" {
		if g.json {
			// JSON shape: { "linked": false }
			return printJSON(os.Stdout, map[string]any{
				"linked": false,
			})
		}
		fmt.Fprintln(os.Stdout, "no project linked in this directory")
		return nil
	}

	ref := wp.ProjectRef

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	// Fetch project metadata.
	var projResp struct {
		Project *Project `json:"project"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref, nil, &projResp); err != nil {
		return fmt.Errorf("status: fetch project: %w", err)
	}
	proj := projResp.Project
	if proj == nil {
		proj = &Project{Ref: ref}
	}

	// Fetch remote migrations list.
	var migResp struct {
		Migrations []MigrationRow `json:"migrations"`
	}
	if err := c.do(ctx, "GET", "/admin/v1/projects/"+ref+"/migrations", nil, &migResp); err != nil {
		// Non-fatal: degrade gracefully — show project info, skip drift.
		printErr(g, "warning: could not fetch remote migrations: %v", err)
	}

	// Collect local migration filenames (*.sql) in ./basin/migrations/.
	localFiles, _ := collectLocalMigrations(cwd)

	// Build sets for drift analysis.
	// Convention: compare the bare filename (no directory component) of
	// local files against the basename of MigrationRow.Source. When Source
	// is empty, fall back to the ID as the remote key.
	remoteSet := make(map[string]struct{}, len(migResp.Migrations))
	for _, m := range migResp.Migrations {
		key := migrationKey(m)
		remoteSet[key] = struct{}{}
	}

	localSet := make(map[string]struct{}, len(localFiles))
	for _, f := range localFiles {
		localSet[filepath.Base(f)] = struct{}{}
	}

	// applied = in both local + remote; local_only = only local; remote_only = only remote.
	applied := 0
	localOnly := 0
	remoteOnly := 0

	for k := range localSet {
		if _, ok := remoteSet[k]; ok {
			applied++
		} else {
			localOnly++
		}
	}
	for k := range remoteSet {
		if _, ok := localSet[k]; !ok {
			remoteOnly++
		}
	}

	status := proj.Status
	if status == "" {
		status = "unknown"
	}
	region := proj.Region
	defaultBranch := proj.DefaultBranch

	if g.json {
		// JSON shape: { "linked": true, "project_ref": string, "status": string, "region": string, "default_branch": string, "migrations": { "applied": int, "local_only": int, "remote_only": int } }
		return printJSON(os.Stdout, map[string]any{
			"linked":         true,
			"project_ref":    ref,
			"status":         status,
			"region":         region,
			"default_branch": defaultBranch,
			"migrations": map[string]any{
				"applied":     applied,
				"local_only":  localOnly,
				"remote_only": remoteOnly,
			},
		})
	}

	// Human-readable output.
	fmt.Fprintf(os.Stdout, "project:        %s\n", ref)
	if proj.Name != "" {
		fmt.Fprintf(os.Stdout, "name:           %s\n", proj.Name)
	}
	fmt.Fprintf(os.Stdout, "status:         %s\n", status)
	if region != "" {
		fmt.Fprintf(os.Stdout, "region:         %s\n", region)
	}
	if defaultBranch != "" {
		fmt.Fprintf(os.Stdout, "default_branch: %s\n", defaultBranch)
	}
	fmt.Fprintln(os.Stdout)
	fmt.Fprintf(os.Stdout, "migrations:\n")
	fmt.Fprintf(os.Stdout, "  applied:      %d\n", applied)
	if localOnly > 0 {
		fmt.Fprintf(os.Stdout, "  local_only:   %d  (pending push)\n", localOnly)
	} else {
		fmt.Fprintf(os.Stdout, "  local_only:   %d\n", localOnly)
	}
	if remoteOnly > 0 {
		fmt.Fprintf(os.Stdout, "  remote_only:  %d  (run `basin db pull` to sync)\n", remoteOnly)
	} else {
		fmt.Fprintf(os.Stdout, "  remote_only:  %d\n", remoteOnly)
	}

	return nil
}

// collectLocalMigrations returns a sorted slice of absolute paths for
// every *.sql file in <cwd>/basin/migrations/. Returns an empty slice
// (not an error) when the directory is absent or empty.
func collectLocalMigrations(cwd string) ([]string, error) {
	dir := filepath.Join(cwd, "basin", "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

// migrationKey returns the identifier used to match a remote MigrationRow
// against a local *.sql filename. We use the basename of Source when set
// (matching the filename the migration was uploaded from), otherwise
// fall back to the ID so every remote row still participates in the
// drift count.
func migrationKey(m MigrationRow) string {
	if m.Source != "" {
		return filepath.Base(m.Source)
	}
	return m.ID
}
