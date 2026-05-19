// cmd_projects — projects subcommand.
//
//	basin projects list   [--org <slug>]
//	basin projects get    <ref>
//	basin projects create <name> --org <slug> [--region <code>]
//	basin projects delete <ref>            (confirm-by-typing-name)
//	basin projects pause  <ref>
//	basin projects resume <ref>
//
// Delete is destructive: we require the user to retype the project's
// name exactly, mirroring the dashboard's confirm-by-name modal.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdProjects(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdProjectsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdProjectsList(g, args[1:])
	case "get":
		return cmdProjectsGet(g, args[1:])
	case "create":
		return cmdProjectsCreate(g, args[1:])
	case "delete":
		return cmdProjectsDelete(g, args[1:])
	case "pause":
		return cmdProjectsPauseResume(g, args[1:], "pause")
	case "resume":
		return cmdProjectsPauseResume(g, args[1:], "resume")
	case "transfers":
		return cmdProjectsTransfers(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("projects", "List / get / create / delete / pause / resume projects.", []string{
			"list   [--org <slug>]                                List projects.",
			"get    <ref>                                         Show one project.",
			"create <name> --org <slug> [--region <code>]         Create a project.",
			"delete <ref> [--yes]                                 Delete a project (confirm by name).",
			"pause  <ref>                                         Pause a project's data plane.",
			"resume <ref>                                         Resume a paused project.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for projects", args[0])
		return errSilent
	}
}

func cmdProjectsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *orgFlag == "" {
		return fmt.Errorf("projects list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Projects []*Project `json:"projects"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*orgFlag+"/projects", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { projects: [ Project ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "REF", "NAME", "REGION", "STATUS", "CREATED")
	for _, p := range resp.Projects {
		t.row(p.Ref, p.Name, p.Region, p.Status, p.CreatedAt)
	}
	return t.flush()
}

func cmdProjectsGet(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin projects get <ref>")
	}
	ref := args[0]
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
		return err
	}
	if g.json {
		// JSON shape: { project: Project }
		return printJSON(os.Stdout, resp)
	}
	p := resp.Project
	if p == nil {
		fmt.Fprintln(os.Stdout, "(empty)")
		return nil
	}
	fmt.Fprintf(os.Stdout, "ref:             %s\n", p.Ref)
	fmt.Fprintf(os.Stdout, "name:            %s\n", p.Name)
	fmt.Fprintf(os.Stdout, "region:          %s\n", p.Region)
	fmt.Fprintf(os.Stdout, "status:          %s\n", p.Status)
	fmt.Fprintf(os.Stdout, "id:              %s\n", p.ID)
	fmt.Fprintf(os.Stdout, "engine_tenant:   %s\n", p.EngineTenantID)
	fmt.Fprintf(os.Stdout, "created_at:      %s\n", p.CreatedAt)
	return nil
}

func cmdProjectsCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	region := fs.String("region", "", "Region code (defaults to org default).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin projects create <name> --org <slug> [--region <code>]")
	}
	if *orgFlag == "" {
		return fmt.Errorf("projects create requires --org=<slug>")
	}
	name := strings.Join(rest, " ")

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60_000_000_000) // 60s — engine provisioning is slow
	defer cancel()
	body := map[string]any{"name": name}
	if *region != "" {
		body["region"] = *region
	}
	var resp CreateProjectResponse
	if err := c.do(ctx, "POST", "/v1/orgs/"+*orgFlag+"/projects", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { project: Project, keys: map[string]any, pgwire: Pgwire }
		// (anon/service keys + pgwire connection string are reveal-once).
		return printJSON(os.Stdout, resp)
	}
	if resp.Project != nil {
		fmt.Fprintf(os.Stdout, "Created project %s (%s) in %s.\n",
			resp.Project.Name, resp.Project.Ref, resp.Project.Region)
	}
	if resp.Pgwire != nil && resp.Pgwire.ConnectionURL != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "Connection URL (shown ONCE — store it now):")
		fmt.Fprintln(os.Stdout, "  "+resp.Pgwire.ConnectionURL)
	}
	if anon, ok := resp.Keys["anon"].(string); ok && anon != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "API keys (also shown ONCE):")
		fmt.Fprintln(os.Stdout, "  anon: "+anon)
	}
	return nil
}

func cmdProjectsDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("projects delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	yes := fs.Bool("yes", false, "Skip the confirm-by-name prompt (assume the user typed it).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin projects delete <ref>")
	}
	ref := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	// Always fetch first so we know the canonical name to compare against.
	var meta struct {
		Project *Project `json:"project"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref, nil, &meta); err != nil {
		return err
	}
	if meta.Project == nil {
		return fmt.Errorf("project not found: %s", ref)
	}

	if !*yes {
		fmt.Fprintf(os.Stderr, "About to delete project %q (%s). Re-type the name to confirm:\n> ",
			meta.Project.Name, meta.Project.Ref)
		typed, err := readLine()
		if err != nil {
			return err
		}
		if strings.TrimSpace(typed) != meta.Project.Name {
			return fmt.Errorf("name mismatch — aborted")
		}
	}

	q := queryString(map[string]string{"confirm_name": meta.Project.Name})
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+q, nil, nil); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { deleted: bool, ref: string }
		return printJSON(os.Stdout, map[string]any{"deleted": true, "ref": ref})
	}
	fmt.Fprintf(os.Stdout, "Deleted project %s.\n", ref)
	return nil
}

func cmdProjectsPauseResume(g *globalFlags, args []string, action string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin projects %s <ref>", action)
	}
	ref := args[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/"+action, nil, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of /v1/projects/:ref/pause|resume envelope
		// (engine-defined; commonly { ok: bool, status: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "%s: %s ok.\n", ref, action)
	return nil
}
