package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// OrgBranding is the read/write shape for /v1/orgs/{slug}/branding.
type OrgBranding struct {
	LogoURL        string `json:"logo_url,omitempty"`
	PrimaryColor   string `json:"primary_color,omitempty"`
	SecondaryColor string `json:"secondary_color,omitempty"`
}

// cmdOrgs covers the org management surface:
//
//	basin orgs list                                   — prints every org the caller can see
//	basin orgs show <slug>                            — prints one org
//	basin orgs create --slug=<s> --name=<n> [--plan=free|pro]
//	basin orgs update <slug> [--name=<n>] [--plan=<p>]
//	basin orgs delete <slug> [--yes]
//	basin orgs branding {get|put|delete} <slug> [--logo-url=...] [--primary-color=...] [--secondary-color=...]
func cmdOrgs(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdOrgsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdOrgsList(g, args[1:])
	case "show":
		return cmdOrgsShow(g, args[1:])
	case "create":
		return cmdOrgsCreate(g, args[1:])
	case "update":
		return cmdOrgsUpdate(g, args[1:])
	case "delete":
		return cmdOrgsDelete(g, args[1:])
	case "branding":
		return cmdOrgsBranding(g, args[1:])
	case "ownership-transfers":
		return cmdOrgsOwnershipTransfers(g, args[1:])
	case "incoming-project-transfers":
		return cmdOrgsIncomingProjectTransfers(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("orgs", "List, show, create, update, delete organizations and manage branding.", []string{
			"list                                    List orgs the caller can see.",
			"show <slug>                             Show one org by slug.",
			"create --slug=<s> --name=<n> [--plan=free|pro]  Create a new org.",
			"update <slug> [--name=<n>] [--plan=<p>]         Update an org.",
			"delete <slug> [--yes]                           Delete an org (destructive).",
			"branding get <slug>                             Get org branding.",
			"branding put <slug> [--logo-url=...] [--primary-color=...] [--secondary-color=...]  Set org branding.",
			"branding delete <slug>                          Remove org branding.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for orgs", args[0])
		return errSilent
	}
}

func cmdOrgsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Orgs []*Org `json:"orgs"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { orgs: [ Org ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "SLUG", "NAME", "PLAN", "ID")
	for _, o := range resp.Orgs {
		t.row(o.Slug, o.Name, o.Plan, o.ID)
	}
	return t.flush()
}

func cmdOrgsShow(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin orgs show <slug>")
	}
	slug := args[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Org *Org `json:"org"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+slug, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { org: Org }
		return printJSON(os.Stdout, resp)
	}
	o := resp.Org
	if o == nil {
		fmt.Fprintln(os.Stdout, "(empty)")
		return nil
	}
	fmt.Fprintf(os.Stdout, "slug:           %s\n", o.Slug)
	fmt.Fprintf(os.Stdout, "name:           %s\n", o.Name)
	fmt.Fprintf(os.Stdout, "plan:           %s\n", o.Plan)
	fmt.Fprintf(os.Stdout, "id:             %s\n", o.ID)
	if o.BillingEmail != "" {
		fmt.Fprintf(os.Stdout, "billing_email:  %s\n", o.BillingEmail)
	}
	return nil
}

// ── orgs create ──────────────────────────────────────────────────────

func cmdOrgsCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	slug := fs.String("slug", "", "Org slug (required, unique).")
	name := fs.String("name", "", "Display name (required).")
	plan := fs.String("plan", "", "Plan: free | pro (optional).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *slug == "" || *name == "" {
		return fmt.Errorf("orgs create requires --slug=<slug> and --name=<name>")
	}
	body := map[string]any{
		"slug": *slug,
		"name": *name,
	}
	if *plan != "" {
		body["plan"] = *plan
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Org *Org `json:"org"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs", body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 409 {
			return fmt.Errorf("org slug conflict: an org with slug %q already exists", *slug)
		}
		return err
	}
	if g.json {
		// JSON shape: { "org": { id, slug, name, plan } }
		return printJSON(os.Stdout, resp)
	}
	o := resp.Org
	if o == nil {
		fmt.Fprintln(os.Stdout, "Org created.")
		return nil
	}
	fmt.Fprintf(os.Stdout, "Created org %q (%s).\n", o.Slug, o.ID)
	return nil
}

// ── orgs update ──────────────────────────────────────────────────────

func cmdOrgsUpdate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs update", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	name := fs.String("name", "", "New display name.")
	plan := fs.String("plan", "", "New plan: free | pro.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs update <slug> [--name=<n>] [--plan=<p>]")
	}
	slug := rest[0]
	body := map[string]any{}
	if *name != "" {
		body["name"] = *name
	}
	if *plan != "" {
		body["plan"] = *plan
	}
	if len(body) == 0 {
		return fmt.Errorf("orgs update: nothing to update — pass --name or --plan")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Org *Org `json:"org"`
	}
	if err := c.do(ctx, "PATCH", "/v1/orgs/"+slug, body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "org": { id, slug, name, plan } }
		return printJSON(os.Stdout, resp)
	}
	o := resp.Org
	if o == nil {
		fmt.Fprintf(os.Stdout, "Updated org %s.\n", slug)
		return nil
	}
	fmt.Fprintf(os.Stdout, "Updated org %q.\n", o.Slug)
	return nil
}

// ── orgs delete ──────────────────────────────────────────────────────

func cmdOrgsDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	yes := fs.Bool("yes", false, "Skip confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs delete <slug> [--yes]")
	}
	slug := rest[0]

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to delete org %q non-interactively under --json", slug)
		}
		fmt.Fprintf(os.Stderr, "Delete org %q? This cannot be undone. [y/N] ", slug)
		line, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(line)) != "y" {
			fmt.Fprintln(os.Stdout, "Aborted.")
			return nil
		}
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Deleted bool   `json:"deleted"`
		Slug    string `json:"slug"`
	}
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+slug, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "deleted": true, "slug": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted org %s.\n", slug)
	return nil
}

// ── orgs branding ────────────────────────────────────────────────────

func cmdOrgsBranding(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin orgs branding {get|put|delete} <slug> [flags]")
	}
	switch args[0] {
	case "get":
		return cmdOrgsBrandingGet(g, args[1:])
	case "put":
		return cmdOrgsBrandingPut(g, args[1:])
	case "delete":
		return cmdOrgsBrandingDelete(g, args[1:])
	default:
		return fmt.Errorf("unknown branding subcommand %q; want get|put|delete", args[0])
	}
}

func cmdOrgsBrandingGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs branding get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs branding get <slug>")
	}
	slug := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Branding *OrgBranding `json:"branding"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+slug+"/branding", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "branding": { logo_url, primary_color, secondary_color } }
		return printJSON(os.Stdout, resp)
	}
	b := resp.Branding
	if b == nil {
		fmt.Fprintln(os.Stdout, "(no branding set)")
		return nil
	}
	fmt.Fprintf(os.Stdout, "logo_url:        %s\n", b.LogoURL)
	fmt.Fprintf(os.Stdout, "primary_color:   %s\n", b.PrimaryColor)
	fmt.Fprintf(os.Stdout, "secondary_color: %s\n", b.SecondaryColor)
	return nil
}

func cmdOrgsBrandingPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs branding put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	logoURL := fs.String("logo-url", "", "Logo image URL.")
	primaryColor := fs.String("primary-color", "", "Primary brand colour (e.g. #1a2b3c).")
	secondaryColor := fs.String("secondary-color", "", "Secondary brand colour.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs branding put <slug> [--logo-url=...] [--primary-color=...] [--secondary-color=...]")
	}
	slug := rest[0]
	body := map[string]any{}
	if *logoURL != "" {
		body["logo_url"] = *logoURL
	}
	if *primaryColor != "" {
		body["primary_color"] = *primaryColor
	}
	if *secondaryColor != "" {
		body["secondary_color"] = *secondaryColor
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Branding *OrgBranding `json:"branding"`
	}
	if err := c.do(ctx, "PUT", "/v1/orgs/"+slug+"/branding", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "branding": { logo_url, primary_color, secondary_color } }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Updated branding for org %s.\n", slug)
	return nil
}

func cmdOrgsBrandingDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("orgs branding delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin orgs branding delete <slug>")
	}
	slug := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp map[string]any
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+slug+"/branding", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of branding delete envelope.
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted branding for org %s.\n", slug)
	return nil
}
