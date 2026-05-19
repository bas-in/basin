// cmd_invitations — org invitation management.
//
//	basin invitations list   --org=<slug>
//	basin invitations resend <id> --org=<slug>
//	basin invitations revoke <id> --org=<slug> [--yes]
//
// Routes:
//
//	GET    /v1/orgs/{slug}/invitations
//	POST   /v1/orgs/{slug}/invitations/{id}/resend
//	DELETE /v1/orgs/{slug}/invitations/{id}
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// cmdInvitations is the top-level dispatcher for the invitations subcommand.
func cmdInvitations(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin invitations (list | resend | revoke) ...")
	}
	switch args[0] {
	case "list":
		return cmdInvitationsList(g, args[1:])
	case "resend":
		return cmdInvitationsResend(g, args[1:])
	case "revoke":
		return cmdInvitationsRevoke(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("invitations", "Manage org invitations.", []string{
			"list          --org=<slug>              List pending (and expired) invitations.",
			"resend <id>   --org=<slug>              Resend an invitation email.",
			"revoke <id>   --org=<slug> [--yes]      Revoke an invitation.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for invitations", args[0])
	}
}

// ── invitations list ─────────────────────────────────────────────────

func cmdInvitationsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("invitations list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("invitations list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Invitations []*Invitation `json:"invitations"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/invitations", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "invitations": [ { id, email, role, expires_at, status } ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Invitations) == 0 {
		fmt.Fprintln(os.Stdout, "(no invitations)")
		return nil
	}
	t := newTable(g, "ID", "EMAIL", "ROLE", "EXPIRES", "STATUS")
	for _, inv := range resp.Invitations {
		t.row(inv.ID, inv.Email, inv.Role, inv.ExpiresAt, inv.Status)
	}
	return t.flush()
}

// ── invitations resend ───────────────────────────────────────────────

func cmdInvitationsResend(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("invitations resend", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin invitations resend <id> --org=<slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Resent bool   `json:"resent"`
		ID     string `json:"id"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/invitations/"+id+"/resend", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "resent": true, "id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Resent invitation %s.\n", id)
	return nil
}

// ── invitations revoke ───────────────────────────────────────────────

func cmdInvitationsRevoke(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("invitations revoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin invitations revoke <id> --org=<slug> [--yes]")
	}
	id := rest[0]

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to revoke invitation %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Revoke invitation %q? [y/N] ", id)
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
		Revoked bool   `json:"revoked"`
		ID      string `json:"id"`
	}
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+*org+"/invitations/"+id, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "revoked": true, "id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Revoked invitation %s.\n", id)
	return nil
}
