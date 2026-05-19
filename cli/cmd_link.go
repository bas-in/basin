// cmd_link — bind the current directory to a remote project.
//
//	basin link --project=<ref> [--org=<slug>] [--force]
//
// Validates the ref via GET /v1/projects/{ref}, then writes project_ref
// into ./basin/config.toml. Refuses to overwrite an existing binding
// unless --force is supplied.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

func cmdLink(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("link", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	projectFlag := fs.String("project", "", "Project ref to link (required).")
	forceFlag := fs.Bool("force", false, "Overwrite an existing project binding.")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("link", "Bind this directory to a remote project.", []string{
			"--project=<ref>   Project ref to link (required).",
			"--org=<slug>      Org slug (optional, for disambiguation).",
			"--force           Overwrite an existing project binding.",
		})
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}
	if *projectFlag == "" {
		return fmt.Errorf("link: --project=<ref> is required")
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("link: could not determine working directory: %w", err)
	}

	// Require the basin/ directory to already exist (created by `basin init`).
	basinDir := filepath.Join(cwd, "basin")
	if st, err := os.Stat(basinDir); err != nil || !st.IsDir() {
		if g.json {
			// JSON shape: { "linked": false, "error": string }
			return printJSON(os.Stdout, map[string]any{
				"linked": false,
				"error":  "./basin/ not found — run `basin init` first",
			})
		}
		return fmt.Errorf("link: ./basin/ not found — run `basin init` first")
	}

	// Load current config to detect an existing binding.
	wp, err := loadWorkingProject(cwd)
	if err != nil {
		return fmt.Errorf("link: %w", err)
	}
	if wp == nil {
		wp = &workingProject{}
	}

	ref := *projectFlag
	prevRef := wp.ProjectRef

	// Already linked to a different ref? Refuse unless --force.
	if prevRef != "" && prevRef != ref && !*forceFlag {
		if g.json {
			// JSON shape: { "linked": false, "error": string, "current_ref": string }
			return printJSON(os.Stdout, map[string]any{
				"linked":      false,
				"error":       fmt.Sprintf("already linked to %q — use --force to overwrite", prevRef),
				"current_ref": prevRef,
			})
		}
		return fmt.Errorf("link: already linked to %q — use --force to overwrite", prevRef)
	}

	// Validate the ref against the cloud.
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Project *Project `json:"project"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == http.StatusNotFound {
			if g.json {
				// JSON shape: { "linked": false, "error": string, "project_ref": string }
				return printJSON(os.Stdout, map[string]any{
					"linked":      false,
					"error":       fmt.Sprintf("project %q not found", ref),
					"project_ref": ref,
				})
			}
			return fmt.Errorf("link: project %q not found", ref)
		}
		return err
	}

	// Write the binding.
	wp.ProjectRef = ref
	if err := saveWorkingProject(cwd, *wp); err != nil {
		return fmt.Errorf("link: write config.toml: %w", err)
	}

	if g.json {
		// JSON shape: { "project_ref": string, "linked": true, "previous_ref": string }
		return printJSON(os.Stdout, map[string]any{
			"project_ref":  ref,
			"linked":       true,
			"previous_ref": prevRef,
		})
	}
	if prevRef != "" && prevRef != ref {
		printInfo(g, "Re-linked: %s → %s", prevRef, ref)
	} else {
		name := ref
		if resp.Project != nil && resp.Project.Name != "" {
			name = resp.Project.Name + " (" + ref + ")"
		}
		printInfo(g, "Linked to project %s.", name)
	}
	fmt.Fprintf(os.Stdout, "project_ref = %s\n", ref)
	return nil
}
