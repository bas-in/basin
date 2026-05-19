// cmd_rls — row-level security management for a project table.
//
//	basin rls enable   <table> [--project=<ref>]
//	basin rls disable  <table> [--project=<ref>]
//	basin rls policies list   <table> [--project=<ref>]
//	basin rls policies create <table> --name=<n> --command=<cmd> --using=<sql>
//	                          [--with-check=<sql>] [--roles=<csv>] [--project=<ref>]
//	basin rls policies drop   <table> --name=<n> [--project=<ref>] [--yes]
//
// Endpoint family — canonical per-table routes:
//
//	POST   /v1/projects/{ref}/tables/{name}/rls/enable
//	POST   /v1/projects/{ref}/tables/{name}/rls/disable
//	GET    /v1/projects/{ref}/tables/{name}/policies
//	POST   /v1/projects/{ref}/tables/{name}/policies      body { name, command, using, with_check, roles }
//	DELETE /v1/projects/{ref}/tables/{name}/policies/{policy_name}
//
// Note: The cloud also mounts a project-level /v1/projects/{ref}/rls/*
// surface (used by the dashboard's visual editor). The CLI uses the
// per-table routes above — they are the stable public contract and map
// directly to the table-scoped subcommand UX.
//
// Project ref resolution order:
//  1. --project=<ref> flag
//  2. loadWorkingProject(cwd) — ./basin/config.toml
//  3. Error with hint to run `basin link`
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// RLSStatus is the JSON shape returned by rls/enable and rls/disable.
// JSON shape: { "enabled": bool, "table": string }
type RLSStatus struct {
	Enabled bool   `json:"enabled"`
	Table   string `json:"table"`
}

// Policy is one row returned by GET …/policies.
// JSON shape: { name, command, using, with_check, roles }
type Policy struct {
	Name      string   `json:"name"`
	Command   string   `json:"command"`
	Using     string   `json:"using"`
	WithCheck string   `json:"with_check,omitempty"`
	Roles     []string `json:"roles,omitempty"`
}

// policyCreateResponse is the JSON shape returned by POST …/policies.
// JSON shape: { "policy": Policy }
type policyCreateResponse struct {
	Policy *Policy `json:"policy"`
}

// policyDropResponse is the JSON shape returned by DELETE …/policies/{name}.
// JSON shape: { "dropped": bool, "name": string }
type policyDropResponse struct {
	Dropped bool   `json:"dropped"`
	Name    string `json:"name"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdRLS(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin rls (enable | disable | policies) <table> ...")
	}
	switch args[0] {
	case "enable":
		return cmdRLSEnable(g, args[1:])
	case "disable":
		return cmdRLSDisable(g, args[1:])
	case "policies":
		return cmdRLSPolicies(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("rls", "Enable / disable / manage RLS policies on a project table.", []string{
			"enable   <table> [--project=<ref>]                                         Enable RLS.",
			"disable  <table> [--project=<ref>]                                         Disable RLS.",
			"policies list   <table> [--project=<ref>]                                  List policies.",
			"policies create <table> --name=<n> --command=<cmd> --using=<sql>           Create a policy.",
			"                        [--with-check=<sql>] [--roles=<csv>]",
			"policies drop   <table> --name=<n> [--project=<ref>] [--yes]               Drop a policy.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for rls", args[0])
	}
}

// ── rls enable ───────────────────────────────────────────────────────

func cmdRLSEnable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rls enable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rls enable <table> [--project=<ref>]")
	}
	table := rest[0]

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
	var resp RLSStatus
	if err := c.do(ctx, "POST",
		"/v1/projects/"+ref+"/tables/"+table+"/rls/enable",
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": true, "table": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "RLS enabled on table %s.\n", table)
	return nil
}

// ── rls disable ──────────────────────────────────────────────────────

func cmdRLSDisable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rls disable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rls disable <table> [--project=<ref>]")
	}
	table := rest[0]

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
	var resp RLSStatus
	if err := c.do(ctx, "POST",
		"/v1/projects/"+ref+"/tables/"+table+"/rls/disable",
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": false, "table": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "RLS disabled on table %s.\n", table)
	return nil
}

// ── rls policies dispatcher ───────────────────────────────────────────

func cmdRLSPolicies(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin rls policies (list | create | drop) <table> ...")
	}
	switch args[0] {
	case "list":
		return cmdRLSPoliciesList(g, args[1:])
	case "create":
		return cmdRLSPoliciesCreate(g, args[1:])
	case "drop":
		return cmdRLSPoliciesDrop(g, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q for rls policies", args[0])
	}
}

// ── rls policies list ────────────────────────────────────────────────

func cmdRLSPoliciesList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rls policies list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rls policies list <table> [--project=<ref>]")
	}
	table := rest[0]

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
		Policies []*Policy `json:"policies"`
	}
	if err := c.do(ctx, "GET",
		"/v1/projects/"+ref+"/tables/"+table+"/policies",
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "policies": [{ name, command, using, with_check, roles }] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Policies) == 0 {
		fmt.Fprintln(os.Stdout, "(no policies)")
		return nil
	}
	t := newTable(g, "NAME", "COMMAND", "ROLES", "USING")
	for _, p := range resp.Policies {
		roles := strings.Join(p.Roles, ",")
		if roles == "" {
			roles = "—"
		}
		t.row(p.Name, p.Command, roles, p.Using)
	}
	return t.flush()
}

// ── rls policies create ──────────────────────────────────────────────

func cmdRLSPoliciesCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rls policies create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	name := fs.String("name", "", "Policy name (required).")
	command := fs.String("command", "", "Policy command: select|insert|update|delete|all (required).")
	using := fs.String("using", "", "USING expression (SQL predicate, required).")
	withCheck := fs.String("with-check", "", "WITH CHECK expression (SQL predicate, optional).")
	roles := fs.String("roles", "", "Comma-separated list of roles (optional; empty means all roles).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rls policies create <table> --name=<n> --command=<cmd> --using=<sql> [--project=<ref>]")
	}
	table := rest[0]

	// Validate required flags before any HTTP call.
	if *name == "" {
		return fmt.Errorf("rls policies create: --name is required")
	}
	if *command == "" {
		return fmt.Errorf("rls policies create: --command is required (select|insert|update|delete|all)")
	}
	validCommands := map[string]bool{
		"select": true, "insert": true, "update": true, "delete": true, "all": true,
	}
	if !validCommands[strings.ToLower(*command)] {
		return fmt.Errorf("rls policies create: --command must be one of: select, insert, update, delete, all")
	}
	if *using == "" {
		return fmt.Errorf("rls policies create: --using is required")
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	body := map[string]any{
		"name":    *name,
		"command": strings.ToLower(*command),
		"using":   *using,
	}
	if *withCheck != "" {
		body["with_check"] = *withCheck
	}
	if *roles != "" {
		var roleList []string
		for _, r := range strings.Split(*roles, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				roleList = append(roleList, r)
			}
		}
		body["roles"] = roleList
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp policyCreateResponse
	if err := c.do(ctx, "POST",
		"/v1/projects/"+ref+"/tables/"+table+"/policies",
		body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "policy": { name, command, using, with_check, roles } }
		return printJSON(os.Stdout, resp)
	}
	pName := *name
	if resp.Policy != nil {
		pName = resp.Policy.Name
	}
	fmt.Fprintf(os.Stdout, "Created policy %q on table %s.\n", pName, table)
	return nil
}

// ── rls policies drop ────────────────────────────────────────────────

func cmdRLSPoliciesDrop(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rls policies drop", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	name := fs.String("name", "", "Policy name to drop (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rls policies drop <table> --name=<n> [--project=<ref>] [--yes]")
	}
	table := rest[0]

	if *name == "" {
		return fmt.Errorf("rls policies drop: --name is required")
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
			return fmt.Errorf("confirmation required: pass --yes to drop policy %q non-interactively under --json", *name)
		}
		fmt.Fprintf(os.Stderr, "Drop policy %q from table %s? This cannot be undone. [y/N] ", *name, table)
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
	var resp policyDropResponse
	if err := c.do(ctx, "DELETE",
		"/v1/projects/"+ref+"/tables/"+table+"/policies/"+*name,
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "dropped": true, "name": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Dropped policy %q from table %s.\n", *name, table)
	return nil
}
