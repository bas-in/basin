// cmd_scim — SCIM provisioning config and token management for an org.
//
//	basin scim config get          --org=<slug>
//	basin scim tokens list         --org=<slug>
//	basin scim tokens create       --org=<slug> --name=<n>
//	basin scim tokens revoke <id>  --org=<slug> [--yes]
//
// Cloud routes (mounted under /v1/orgs/{slug}):
//
//	GET    /v1/orgs/{slug}/scim/config      → get SCIM config + base URL
//	GET    /v1/orgs/{slug}/scim/tokens      → list SCIM provisioning tokens
//	POST   /v1/orgs/{slug}/scim/tokens      → create a SCIM token (secret shown ONCE)
//	DELETE /v1/orgs/{slug}/scim/tokens/{id} → revoke a SCIM token
//
// Note: GET /v1/orgs/{slug}/scim/tokens may not be implemented on all
// cloud versions. The CLI falls back gracefully to a 404 → empty list.
//
// Secret-bearing responses (tokens create) print the plaintext secret
// exactly once with a "store now" warning. Subsequent list/get calls
// only show the token prefix — the cloud's reveal-once contract.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// SCIMConfig is the read shape from GET /v1/orgs/{slug}/scim/config.
// JSON shape: { enabled, base_url, schemas_url, configured_at }
type SCIMConfig struct {
	Enabled      bool   `json:"enabled"`
	BaseURL      string `json:"base_url,omitempty"`
	SchemasURL   string `json:"schemas_url,omitempty"`
	ConfiguredAt string `json:"configured_at,omitempty"`
}

// SCIMToken is one entry from the SCIM token list.
// JSON shape: { id, name, prefix, created_at, last_used_at }
type SCIMToken struct {
	ID         string  `json:"id,omitempty"`
	Name       string  `json:"name,omitempty"`
	Prefix     string  `json:"prefix,omitempty"`
	CreatedAt  string  `json:"created_at,omitempty"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdSCIM(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("scim", "Manage SCIM provisioning config and tokens for an org.", []string{
			"config get          --org=<slug>                  Show SCIM provisioning config.",
			"tokens list         --org=<slug>                  List SCIM provisioning tokens.",
			"tokens create       --org=<slug> --name=<n>       Mint a SCIM token (secret shown ONCE).",
			"tokens revoke <id>  --org=<slug> [--yes]          Revoke a SCIM token.",
		})
		return nil
	}
	switch args[0] {
	case "config":
		return cmdSCIMConfig(g, args[1:])
	case "tokens":
		return cmdSCIMTokens(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("scim", "Manage SCIM provisioning config and tokens for an org.", []string{
			"config get          --org=<slug>                  Show SCIM provisioning config.",
			"tokens list         --org=<slug>                  List SCIM provisioning tokens.",
			"tokens create       --org=<slug> --name=<n>       Mint a SCIM token (secret shown ONCE).",
			"tokens revoke <id>  --org=<slug> [--yes]          Revoke a SCIM token.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for scim", args[0])
		return errSilent
	}
}

func cmdSCIMConfig(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin scim config (get) --org=<slug>")
	}
	switch args[0] {
	case "get":
		return cmdSCIMConfigGet(g, args[1:])
	case "--help", "-h", "help":
		fmt.Fprintln(os.Stderr, "usage: basin scim config get --org=<slug>")
		return nil
	default:
		printErr(g, "unknown subcommand %q for scim config", args[0])
		return errSilent
	}
}

func cmdSCIMTokens(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin scim tokens (list | create | revoke) --org=<slug>")
	}
	switch args[0] {
	case "list":
		return cmdSCIMTokensList(g, args[1:])
	case "create":
		return cmdSCIMTokensCreate(g, args[1:])
	case "revoke":
		return cmdSCIMTokensRevoke(g, args[1:])
	case "--help", "-h", "help":
		fmt.Fprintln(os.Stderr, "usage: basin scim tokens (list | create | revoke) --org=<slug>")
		return nil
	default:
		printErr(g, "unknown subcommand %q for scim tokens", args[0])
		return errSilent
	}
}

// ── scim config get ───────────────────────────────────────────────────

func cmdSCIMConfigGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("scim config get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("scim config get requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		SCIM *SCIMConfig `json:"scim"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/scim/config", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "scim": { enabled, base_url, schemas_url, configured_at } }
		return printJSON(os.Stdout, resp)
	}
	if resp.SCIM == nil {
		fmt.Fprintln(os.Stdout, "(no SCIM configured)")
		return nil
	}
	s := resp.SCIM
	fmt.Fprintf(os.Stdout, "enabled:       %v\n", s.Enabled)
	fmt.Fprintf(os.Stdout, "base_url:      %s\n", s.BaseURL)
	fmt.Fprintf(os.Stdout, "schemas_url:   %s\n", s.SchemasURL)
	fmt.Fprintf(os.Stdout, "configured_at: %s\n", s.ConfiguredAt)
	return nil
}

// ── scim tokens list ─────────────────────────────────────────────────

func cmdSCIMTokensList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("scim tokens list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("scim tokens list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Tokens []*SCIMToken `json:"tokens"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/scim/tokens", nil, &resp); err != nil {
		// If the cloud doesn't expose a list endpoint yet (404), return
		// an empty list so the caller's tooling doesn't break.
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			if g.json {
				// JSON shape: { "tokens": [] }
				return printJSON(os.Stdout, resp)
			}
			fmt.Fprintln(os.Stdout, "(no SCIM tokens — list endpoint not available on this cloud version)")
			return nil
		}
		return err
	}
	if g.json {
		// JSON shape: { "tokens": [{ id, name, prefix, created_at, last_used_at }] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Tokens) == 0 {
		fmt.Fprintln(os.Stdout, "(no SCIM tokens)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "PREFIX", "CREATED", "LAST USED")
	for _, tok := range resp.Tokens {
		lastUsed := "never"
		if tok.LastUsedAt != nil && *tok.LastUsedAt != "" {
			lastUsed = *tok.LastUsedAt
		}
		t.row(tok.ID, tok.Name, tok.Prefix, tok.CreatedAt, lastUsed)
	}
	return t.flush()
}

// ── scim tokens create ───────────────────────────────────────────────

func cmdSCIMTokensCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("scim tokens create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	name := fs.String("name", "", "Human-readable label for the token (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("scim tokens create requires --org=<slug>")
	}
	if *name == "" {
		return fmt.Errorf("scim tokens create requires --name=<n>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	body := map[string]any{
		"name": *name,
	}
	var resp struct {
		Token  *SCIMToken `json:"token"`
		Secret string     `json:"secret,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/scim/tokens", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "token": { id, name, prefix, created_at }, "secret": "..." }
		// Secret is reveal-once: the cloud only returns it on creation.
		return printJSON(os.Stdout, resp)
	}
	// Always print the secret — it will not be shown again.
	fmt.Fprintln(os.Stdout, "SCIM token created. Store this secret NOW — it will not be shown again:")
	fmt.Fprintln(os.Stdout, "  "+resp.Secret)
	if resp.Token != nil {
		fmt.Fprintf(os.Stdout, "  id:     %s\n", resp.Token.ID)
		fmt.Fprintf(os.Stdout, "  name:   %s\n", resp.Token.Name)
		fmt.Fprintf(os.Stdout, "  prefix: %s\n", resp.Token.Prefix)
	}
	return nil
}

// ── scim tokens revoke ───────────────────────────────────────────────

func cmdSCIMTokensRevoke(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("scim tokens revoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin scim tokens revoke <id> --org=<slug> [--yes]")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to revoke SCIM token %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Revoke SCIM token %q for org %q? [y/N] ", id, *org)
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

	var resp struct {
		Revoked bool   `json:"revoked"`
		ID      string `json:"id,omitempty"`
	}
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+*org+"/scim/tokens/"+id, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "revoked": true, "id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Revoked SCIM token %s.\n", id)
	return nil
}
