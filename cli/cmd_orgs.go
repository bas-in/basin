package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// cmdOrgs covers the read-only org surface:
//
//	basin orgs list                 — prints every org the caller can see
//	basin orgs show <slug>          — prints one org
//
// Org create/update/delete intentionally don't ship in the CLI yet —
// they're rare operator-side gestures, and the dashboard's UI is the
// safer place for the irreversible flavours.
func cmdOrgs(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdOrgsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdOrgsList(g, args[1:])
	case "show":
		return cmdOrgsShow(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("orgs", "List or show organizations.", []string{
			"list                List orgs the caller can see.",
			"show <slug>         Show one org by slug.",
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
