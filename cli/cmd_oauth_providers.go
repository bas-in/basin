// cmd_oauth_providers — OAuth provider configuration for a project.
//
//	basin oauth-providers list                                         [--project=<ref>]
//	basin oauth-providers get    <provider>                            [--project=<ref>]
//	basin oauth-providers set    <provider> --client-id=<id> --client-secret=<s> [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]
//	basin oauth-providers delete <provider>                            [--project=<ref>] [--yes]
//
// Backed by /v1/projects/{ref}/oauth-providers (list),
// /v1/projects/{ref}/oauth-providers/{provider} (get/set/delete).
// PATCH is used for set so partial updates work.
//
// Security: client_secret is NEVER echoed back in plain-text output.
// The cloud's response masks it (replaces with "***"); if for any reason
// the raw secret appears in the response, it is masked client-side too.
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// OAuthProvider is the read shape for a single OAuth provider config.
// JSON shape: { provider, enabled, client_id_prefix, client_secret_masked }
type OAuthProvider struct {
	Provider           string `json:"provider,omitempty"`
	Enabled            bool   `json:"enabled"`
	ClientIDPrefix     string `json:"client_id_prefix,omitempty"`
	ClientSecretMasked string `json:"client_secret_masked,omitempty"`
	// client_secret is deliberately omitted from the struct so it never
	// appears in --json output even if the cloud sends it back.
}

// maskSecret replaces a raw secret string with "***" if it appears in a
// free-form string field. Used as a last-resort client-side safety net.
func maskSecret(secret, s string) string {
	if secret == "" || s == "" {
		return s
	}
	return strings.ReplaceAll(s, secret, "***")
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdOAuthProviders(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdOAuthProvidersList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdOAuthProvidersList(g, args[1:])
	case "get":
		return cmdOAuthProvidersGet(g, args[1:])
	case "set":
		return cmdOAuthProvidersSet(g, args[1:])
	case "delete":
		return cmdOAuthProvidersDelete(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("oauth-providers", "Manage project OAuth provider configuration.", []string{
			"list                                                          List configured OAuth providers.",
			"get    <provider>                                             Show one provider config.",
			"set    <provider> --client-id=<id> --client-secret=<s>       Configure a provider.",
			"       [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]",
			"delete <provider> [--project=<ref>] [--yes]                  Remove a provider config.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for oauth-providers", args[0])
		return errSilent
	}
}

// ── oauth-providers list ─────────────────────────────────────────────

func cmdOAuthProvidersList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-providers list", flag.ContinueOnError)
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
		Providers []*OAuthProvider `json:"providers"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/oauth-providers", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { providers: [ OAuthProvider ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Providers) == 0 {
		fmt.Fprintln(os.Stdout, "(no OAuth providers configured)")
		return nil
	}
	t := newTable(g, "PROVIDER", "ENABLED", "CLIENT_ID_PREFIX")
	for _, p := range resp.Providers {
		enabled := "false"
		if p.Enabled {
			enabled = "true"
		}
		t.row(p.Provider, enabled, p.ClientIDPrefix)
	}
	return t.flush()
}

// ── oauth-providers get ──────────────────────────────────────────────

func cmdOAuthProvidersGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-providers get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-providers get <provider> [--project=<ref>]")
	}
	provider := rest[0]

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
		Provider *OAuthProvider `json:"provider"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/oauth-providers/"+provider, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("OAuth provider %q not found on project %q", provider, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { provider: OAuthProvider }
		return printJSON(os.Stdout, resp)
	}
	p := resp.Provider
	if p == nil {
		return fmt.Errorf("OAuth provider %q not found", provider)
	}
	enabled := "false"
	if p.Enabled {
		enabled = "true"
	}
	fmt.Fprintf(os.Stdout, "provider:         %s\n", p.Provider)
	fmt.Fprintf(os.Stdout, "enabled:          %s\n", enabled)
	fmt.Fprintf(os.Stdout, "client_id_prefix: %s\n", p.ClientIDPrefix)
	return nil
}

// ── oauth-providers set ──────────────────────────────────────────────

func cmdOAuthProvidersSet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-providers set", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	clientID := fs.String("client-id", "", "OAuth client ID (required).")
	clientSecret := fs.String("client-secret", "", "OAuth client secret (required).")
	enabledStr := fs.String("enabled", "", "Enable or disable the provider: true|false.")
	scopes := fs.String("scopes", "", "Comma-separated list of OAuth scopes.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-providers set <provider> --client-id=<id> --client-secret=<s> [--enabled=true|false] [--scopes=<csv>] [--project=<ref>]")
	}
	provider := rest[0]
	if *clientID == "" {
		return fmt.Errorf("oauth-providers set requires --client-id=<id>")
	}
	if *clientSecret == "" {
		return fmt.Errorf("oauth-providers set requires --client-secret=<secret>")
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

	body := map[string]any{
		"client_id":     *clientID,
		"client_secret": *clientSecret,
	}
	if *enabledStr != "" {
		switch strings.ToLower(*enabledStr) {
		case "true", "1", "yes":
			body["enabled"] = true
		case "false", "0", "no":
			body["enabled"] = false
		default:
			return fmt.Errorf("oauth-providers set --enabled must be true or false, got %q", *enabledStr)
		}
	}
	if *scopes != "" {
		body["scopes"] = *scopes
	}

	// Keep the raw secret in memory for client-side masking after we send it.
	rawSecret := *clientSecret

	var resp struct {
		Provider *OAuthProvider `json:"provider"`
	}
	if err := c.do(ctx, "PATCH", "/v1/projects/"+ref+"/oauth-providers/"+provider, body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 422 {
			return fmt.Errorf("oauth-providers set: %s", ae.Message)
		}
		return err
	}

	// Client-side safety net: ensure the raw secret never leaks.
	// The cloud should already mask it in the response; this is belt-and-
	// suspenders. We apply maskSecret to the free-form ClientIDPrefix field
	// which is the only string field that could theoretically carry it.
	if resp.Provider != nil {
		resp.Provider.ClientIDPrefix = maskSecret(rawSecret, resp.Provider.ClientIDPrefix)
		resp.Provider.ClientSecretMasked = maskSecret(rawSecret, resp.Provider.ClientSecretMasked)
	}

	if g.json {
		// JSON shape: { provider: OAuthProvider }
		// client_secret is never present in OAuthProvider struct.
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "secret stored\n")
	if resp.Provider != nil {
		enabled := "false"
		if resp.Provider.Enabled {
			enabled = "true"
		}
		fmt.Fprintf(os.Stdout, "provider: %s  enabled: %s\n", resp.Provider.Provider, enabled)
	}
	return nil
}

// ── oauth-providers delete ───────────────────────────────────────────

func cmdOAuthProvidersDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-providers delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-providers delete <provider> [--project=<ref>] [--yes]")
	}
	provider := rest[0]

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
			return fmt.Errorf("confirmation required: pass --yes to delete OAuth provider %q non-interactively under --json", provider)
		}
		fmt.Fprintf(os.Stderr, "Delete OAuth provider %q from project %q? This cannot be undone. [y/N] ", provider, ref)
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
		Deleted  bool   `json:"deleted"`
		Provider string `json:"provider,omitempty"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/oauth-providers/"+provider, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("OAuth provider %q not found on project %q", provider, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { deleted: true, provider: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted OAuth provider %s.\n", provider)
	return nil
}
