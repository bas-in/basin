// cmd_extensions — list the native extension catalog for a project.
//
//	basin extensions list [--project=<ref>]
//
// Backed by GET /v1/projects/{ref}/extensions/catalog.
// The catalog is read-only: the engine rejects CREATE/DROP/ALTER EXTENSION
// (ADR 0002 — "no upstream extensions"), so only list is wired.
// The cloud also mounts GET /extensions at the root of the project route
// group; the catalog path is preferred.
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// Extension is one entry in the extensions catalog.
// JSON shape: { name, version, installed, description }
type Extension struct {
	Name        string `json:"name,omitempty"`
	Version     string `json:"version,omitempty"`
	Installed   bool   `json:"installed"`
	Description string `json:"description,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdExtensions(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdExtensionsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdExtensionsList(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("extensions", "List the native extension catalog for a project.", []string{
			"list [--project=<ref>]   List available extensions (read-only).",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for extensions", args[0])
		return errSilent
	}
}

// ── extensions list ──────────────────────────────────────────────────

func cmdExtensionsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("extensions list", flag.ContinueOnError)
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
		Extensions []*Extension `json:"extensions"`
	}

	// Prefer the /catalog path; fall back to the root /extensions path
	// if catalog returns 404 (older cloud that predates the catalog route).
	catalogPath := "/v1/projects/" + ref + "/extensions/catalog"
	err = c.do(ctx, "GET", catalogPath, nil, &resp)
	if err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			// Fallback to root /extensions route.
			rootPath := "/v1/projects/" + ref + "/extensions"
			err = c.do(ctx, "GET", rootPath, nil, &resp)
		}
		if err != nil {
			return err
		}
	}

	if g.json {
		// JSON shape: { extensions: [ Extension ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Extensions) == 0 {
		fmt.Fprintln(os.Stdout, "(no extensions)")
		return nil
	}
	t := newTable(g, "NAME", "VERSION", "INSTALLED", "DESCRIPTION")
	for _, e := range resp.Extensions {
		installed := "false"
		if e.Installed {
			installed = "true"
		}
		t.row(e.Name, e.Version, installed, e.Description)
	}
	return t.flush()
}
