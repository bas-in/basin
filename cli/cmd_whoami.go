package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// cmdWhoami prints the active user + every org accessible by the
// stored token. Reads from /auth/v1/user (user shape) + /v1/orgs
// (org list). Two requests in series; small, fine for an interactive
// command.
func cmdWhoami(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("whoami", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("whoami", "Print the active user + accessible orgs.", nil)
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var me struct {
		User *User `json:"user"`
	}
	if err := c.do(ctx, "GET", "/auth/v1/user", nil, &me); err != nil {
		return err
	}
	var orgsResp struct {
		Orgs []*Org `json:"orgs"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs", nil, &orgsResp); err != nil {
		// Don't fail whoami on org-list errors; surface the user we
		// did manage to load.
		printErr(g, "warning: could not list orgs: %v", err)
	}

	if g.json {
		// JSON shape: { user: User, orgs: [ Org ] }
		return printJSON(os.Stdout, map[string]any{
			"user": me.User,
			"orgs": orgsResp.Orgs,
		})
	}

	if me.User != nil {
		label := me.User.Name
		if label == "" {
			label = me.User.Email
		}
		fmt.Fprintf(os.Stdout, "%s <%s>\n", label, me.User.Email)
	}
	if len(orgsResp.Orgs) == 0 {
		fmt.Fprintln(os.Stdout, "  (no orgs)")
		return nil
	}
	t := newTable(g, "SLUG", "NAME", "PLAN", "ID")
	for _, o := range orgsResp.Orgs {
		t.row(o.Slug, o.Name, o.Plan, o.ID)
	}
	return t.flush()
}
