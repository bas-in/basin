// cmd_tokens — manage org-scoped personal access tokens.
//
//	basin tokens list   --org <slug>
//	basin tokens create --org <slug> --name <label> [--scope read|write|admin]
//	                                 [--projects <ref,ref>] [--ip <cidr,...>]
//	                                 [--expires <7d|30d|90d|1y|never|RFC3339>]
//	basin tokens revoke <id> --org <slug>
//	basin tokens show   <id> --org <slug>
//
// Create prints the FullToken once. Subsequent `show` calls only see
// the prefix — that's the cloud's reveal-once contract.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdTokens(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin tokens (list | create | revoke | show) ...")
	}
	switch args[0] {
	case "list":
		return cmdTokensList(g, args[1:])
	case "create":
		return cmdTokensCreate(g, args[1:])
	case "revoke":
		return cmdTokensRevoke(g, args[1:])
	case "show":
		return cmdTokensShow(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("tokens", "Manage org-scoped personal access tokens (PATs).", []string{
			"list   --org <slug>                                          List tokens (revoked filtered out).",
			"create --org <slug> --name <label> [--scope read|write|admin]",
			"       [--projects ref,ref] [--ip cidr,...] [--expires <7d|30d|90d|1y|never|RFC3339>]",
			"                                                             Mint a token (printed ONCE).",
			"revoke <id> --org <slug>                                     Mark a token revoked.",
			"show   <id> --org <slug>                                     Print token metadata (prefix only).",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for tokens", args[0])
	}
}

func cmdTokensList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tokens list", flag.ContinueOnError)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *orgFlag == "" {
		return fmt.Errorf("tokens list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Tokens []*APIToken `json:"tokens"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*orgFlag+"/api-tokens", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { tokens: [ APIToken ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "ID", "NAME", "SCOPE", "PREFIX", "EXPIRES", "LAST USED")
	for _, tok := range resp.Tokens {
		t.row(tok.ID, tok.Name, tok.Scope, tok.Prefix,
			formatExpiresAt(tok.ExpiresAt), formatExpiresAt(tok.LastUsedAt))
	}
	return t.flush()
}

func cmdTokensCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tokens create", flag.ContinueOnError)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	name := fs.String("name", "", "Human-readable label (required).")
	desc := fs.String("description", "", "Optional description.")
	scope := fs.String("scope", "write", "Scope band: read | write | admin.")
	projects := fs.String("projects", "", "Comma-separated project refs (or empty for all).")
	ips := fs.String("ip", "", "Comma-separated CIDR allowlist (e.g. 1.2.3.0/24,10.0.0.1/32).")
	expires := fs.String("expires", "", "7d / 30d / 90d / 1y / never, or RFC3339 absolute timestamp.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *orgFlag == "" || *name == "" {
		return fmt.Errorf("tokens create requires --org=<slug> and --name=<label>")
	}
	body := map[string]any{
		"name":        *name,
		"description": *desc,
		"scope":       *scope,
	}
	if *projects != "" {
		body["project_ids"] = splitCSV(*projects)
	}
	if *ips != "" {
		body["ip_allowlist"] = splitCSV(*ips)
	}
	if *expires != "" {
		// The handler accepts both "30d" (expires_in) and an RFC3339
		// string (expires_at). Heuristic: contains 'T' → treat as
		// RFC3339; else as expires_in.
		if strings.Contains(*expires, "T") {
			body["expires_at"] = *expires
		} else {
			body["expires_in"] = *expires
		}
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp CreateTokenResponse
	if err := c.do(ctx, "POST", "/v1/orgs/"+*orgFlag+"/api-tokens", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: CreateTokenResponse — { token: APIToken, full_token: string }
		// (full_token is reveal-once; subsequent reads only see APIToken.Prefix).
		return printJSON(os.Stdout, resp)
	}
	if resp.FullToken == "" {
		fmt.Fprintln(os.Stdout, "Token created (no plaintext returned — likely an upstream bug).")
		return nil
	}
	fmt.Fprintln(os.Stdout, "Token created. Copy this NOW — it won't be shown again:")
	fmt.Fprintln(os.Stdout, "  "+resp.FullToken)
	if resp.Token != nil {
		fmt.Fprintf(os.Stdout, "  id:      %s\n", resp.Token.ID)
		fmt.Fprintf(os.Stdout, "  scope:   %s\n", resp.Token.Scope)
		fmt.Fprintf(os.Stdout, "  expires: %s\n", formatExpiresAt(resp.Token.ExpiresAt))
	}
	return nil
}

func cmdTokensRevoke(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tokens revoke", flag.ContinueOnError)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *orgFlag == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin tokens revoke <id> --org <slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "POST", "/v1/orgs/"+*orgFlag+"/api-tokens/"+id+"/revoke", nil, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of revoke envelope
		// (commonly { revoked: bool, id: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Revoked token %s.\n", id)
	return nil
}

func cmdTokensShow(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tokens show", flag.ContinueOnError)
	orgFlag := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *orgFlag == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin tokens show <id> --org <slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	// The cloud doesn't expose a per-token GET — pull the list and
	// filter. Cheap (<= a few dozen rows per org).
	var resp struct {
		Tokens []*APIToken `json:"tokens"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*orgFlag+"/api-tokens", nil, &resp); err != nil {
		return err
	}
	for _, tok := range resp.Tokens {
		if tok.ID == id {
			if g.json {
				// JSON shape: APIToken — { id, name, description, prefix,
				// scope, scopes, project_ids, ip_allowlist, created_at,
				// expires_at, last_used_at }
				return printJSON(os.Stdout, tok)
			}
			fmt.Fprintf(os.Stdout, "id:           %s\n", tok.ID)
			fmt.Fprintf(os.Stdout, "name:         %s\n", tok.Name)
			fmt.Fprintf(os.Stdout, "description:  %s\n", tok.Description)
			fmt.Fprintf(os.Stdout, "scope:        %s\n", tok.Scope)
			if len(tok.Scopes) > 0 {
				fmt.Fprintf(os.Stdout, "scopes:       %s\n", strings.Join(tok.Scopes, ","))
			}
			fmt.Fprintf(os.Stdout, "prefix:       %s\n", tok.Prefix)
			fmt.Fprintf(os.Stdout, "created_at:   %s\n", tok.CreatedAt)
			fmt.Fprintf(os.Stdout, "expires_at:   %s\n", formatExpiresAt(tok.ExpiresAt))
			fmt.Fprintf(os.Stdout, "last_used_at: %s\n", formatExpiresAt(tok.LastUsedAt))
			if len(tok.ProjectIDs) > 0 {
				fmt.Fprintf(os.Stdout, "projects:     %s\n", strings.Join(tok.ProjectIDs, ","))
			}
			if len(tok.IPAllowlist) > 0 {
				fmt.Fprintf(os.Stdout, "ip_allowlist: %s\n", strings.Join(tok.IPAllowlist, ","))
			}
			return nil
		}
	}
	return fmt.Errorf("token %s not found in org %s", id, *orgFlag)
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
