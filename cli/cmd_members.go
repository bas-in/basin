// cmd_members — org member management.
//
//	basin members list   --org=<slug>
//	basin members invite --org=<slug> --email=<e> [--role=member|admin|owner]
//	basin members remove --org=<slug> --user-id=<u> [--yes]
//	basin members role   --org=<slug> --user-id=<u> --role=<r>
//
// Routes:
//
//	GET    /v1/orgs/{slug}/members
//	POST   /v1/orgs/{slug}/members/invite
//	DELETE /v1/orgs/{slug}/members/{user_id}
//	PATCH  /v1/orgs/{slug}/members/{user_id}
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// Member is the read shape from GET /v1/orgs/{slug}/members.
type Member struct {
	UserID   string `json:"user_id,omitempty"`
	Email    string `json:"email,omitempty"`
	Role     string `json:"role,omitempty"`
	JoinedAt string `json:"joined_at,omitempty"`
}

// Invitation is the read shape from invite + list invitations responses.
type Invitation struct {
	ID        string `json:"id,omitempty"`
	Email     string `json:"email,omitempty"`
	Role      string `json:"role,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
	Status    string `json:"status,omitempty"`
}

// cmdMembers is the top-level dispatcher for the members subcommand.
func cmdMembers(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin members (list | invite | remove | role) ...")
	}
	switch args[0] {
	case "list":
		return cmdMembersList(g, args[1:])
	case "invite":
		return cmdMembersInvite(g, args[1:])
	case "remove":
		return cmdMembersRemove(g, args[1:])
	case "role":
		return cmdMembersRole(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("members", "Manage org members.", []string{
			"list   --org=<slug>                                        List org members.",
			"invite --org=<slug> --email=<e> [--role=member|admin|owner]  Invite a new member.",
			"remove --org=<slug> --user-id=<u> [--yes]                 Remove a member.",
			"role   --org=<slug> --user-id=<u> --role=<r>              Update a member's role.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for members", args[0])
	}
}

// ── members list ─────────────────────────────────────────────────────

func cmdMembersList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("members list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("members list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Members []*Member `json:"members"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/members", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "members": [ { user_id, email, role, joined_at } ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Members) == 0 {
		fmt.Fprintln(os.Stdout, "(no members)")
		return nil
	}
	t := newTable(g, "USER_ID", "EMAIL", "ROLE", "JOINED")
	for _, m := range resp.Members {
		t.row(m.UserID, m.Email, m.Role, m.JoinedAt)
	}
	return t.flush()
}

// ── members invite ───────────────────────────────────────────────────

func cmdMembersInvite(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("members invite", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	email := fs.String("email", "", "Email address to invite (required).")
	role := fs.String("role", "member", "Role to assign: member | admin | owner.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" || *email == "" {
		return fmt.Errorf("members invite requires --org=<slug> and --email=<email>")
	}
	body := map[string]any{
		"email": *email,
		"role":  *role,
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Invitation *Invitation `json:"invitation"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/members/invite", body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 409 {
			return fmt.Errorf("member already exists or invitation already pending for %q", *email)
		}
		return err
	}
	if g.json {
		// JSON shape: { "invitation": { id, email, role, expires_at } }
		return printJSON(os.Stdout, resp)
	}
	inv := resp.Invitation
	if inv == nil {
		fmt.Fprintf(os.Stdout, "Invited %s to org %s.\n", *email, *org)
		return nil
	}
	fmt.Fprintf(os.Stdout, "Invited %s (role=%s, expires=%s, id=%s).\n",
		inv.Email, inv.Role, inv.ExpiresAt, inv.ID)
	return nil
}

// ── members remove ───────────────────────────────────────────────────

func cmdMembersRemove(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("members remove", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	userID := fs.String("user-id", "", "User ID to remove (required).")
	yes := fs.Bool("yes", false, "Skip confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" || *userID == "" {
		return fmt.Errorf("members remove requires --org=<slug> and --user-id=<user_id>")
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to remove user %q non-interactively under --json", *userID)
		}
		fmt.Fprintf(os.Stderr, "Remove user %q from org %q? [y/N] ", *userID, *org)
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
		Removed bool   `json:"removed"`
		UserID  string `json:"user_id"`
	}
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+*org+"/members/"+*userID, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "removed": true, "user_id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Removed user %s from org %s.\n", *userID, *org)
	return nil
}

// ── members role ─────────────────────────────────────────────────────

func cmdMembersRole(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("members role", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	userID := fs.String("user-id", "", "User ID whose role to update (required).")
	role := fs.String("role", "", "New role: member | admin | owner (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" || *userID == "" || *role == "" {
		return fmt.Errorf("members role requires --org=<slug>, --user-id=<user_id>, and --role=<role>")
	}
	body := map[string]any{
		"role": *role,
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Member *Member `json:"member"`
	}
	if err := c.do(ctx, "PATCH", "/v1/orgs/"+*org+"/members/"+*userID, body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "member": { user_id, role } }
		return printJSON(os.Stdout, resp)
	}
	m := resp.Member
	if m == nil {
		fmt.Fprintf(os.Stdout, "Updated role for user %s in org %s.\n", *userID, *org)
		return nil
	}
	fmt.Fprintf(os.Stdout, "Updated role for user %s to %q in org %s.\n", m.UserID, m.Role, *org)
	return nil
}
