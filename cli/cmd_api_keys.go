// cmd_api_keys — project-scoped API key management.
//
//	basin api-keys list   [--project=<ref>]
//	basin api-keys create --name=<n> [--project=<ref>] [--scope=read|write|admin]
//	basin api-keys rotate <id> [--project=<ref>] [--yes]
//	basin api-keys revoke <id> [--project=<ref>] [--yes]
//
// These are project-scoped API keys distinct from the org-level PATs
// managed by `basin tokens`. Routes: /v1/projects/{ref}/api-keys/*.
//
// Secret display convention:
//   - create: print the full secret once with a "store this now" warning
//   - rotate: print the new full secret once with the same warning
//   - list:   show only prefix + masked last-4 (e.g. "bsp_****abcd")
//
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is available.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// ProjectAPIKey is the read shape from GET /v1/projects/{ref}/api-keys.
// The full secret is never returned on list — only prefix + masked tail.
type ProjectAPIKey struct {
	ID        string  `json:"id,omitempty"`
	Name      string  `json:"name,omitempty"`
	Scope     string  `json:"scope,omitempty"`
	Prefix    string  `json:"prefix,omitempty"`
	MaskedKey string  `json:"masked_key,omitempty"`
	CreatedAt string  `json:"created_at,omitempty"`
	LastUsedAt *string `json:"last_used_at,omitempty"`
}

// CreateAPIKeyResponse is the once-only reveal envelope from
// POST /v1/projects/{ref}/api-keys. The full_key field is present only
// on creation — subsequent list calls only show the masked form.
type CreateAPIKeyResponse struct {
	Key     *ProjectAPIKey `json:"key,omitempty"`
	FullKey string         `json:"full_key,omitempty"`
}

// RotateAPIKeyResponse is the once-only reveal envelope from
// POST /v1/projects/{ref}/api-keys/{id}/rotate.
type RotateAPIKeyResponse struct {
	Key     *ProjectAPIKey `json:"key,omitempty"`
	FullKey string         `json:"full_key,omitempty"`
}

// maskAPIKey returns a masked representation of the key secret for display
// on list calls. If a masked_key field is already set, use it directly.
// Otherwise build "prefix****last4" from whatever we have.
// The convention mirrors cmd_tokens.go's "prefix" display.
func maskAPIKey(k *ProjectAPIKey) string {
	if k == nil {
		return ""
	}
	if k.MaskedKey != "" {
		return k.MaskedKey
	}
	if k.Prefix != "" {
		return k.Prefix + "••••••••"
	}
	return "(masked)"
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdAPIKeys(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdAPIKeysList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdAPIKeysList(g, args[1:])
	case "create":
		return cmdAPIKeysCreate(g, args[1:])
	case "rotate":
		return cmdAPIKeysRotate(g, args[1:])
	case "revoke":
		return cmdAPIKeysRevoke(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("api-keys", "Manage project-scoped API keys.", []string{
			"list   [--project=<ref>]                                  List API keys (masked secret).",
			"create --name=<n> [--project=<ref>] [--scope=read|write|admin]  Create an API key (secret shown ONCE).",
			"rotate <id> [--project=<ref>] [--yes]                     Rotate an API key (new secret shown ONCE).",
			"revoke <id> [--project=<ref>] [--yes]                     Revoke an API key permanently.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for api-keys", args[0])
		return errSilent
	}
}

// ── api-keys list ────────────────────────────────────────────────────

func cmdAPIKeysList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("api-keys list", flag.ContinueOnError)
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
		Keys []*ProjectAPIKey `json:"keys"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/api-keys", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { keys: [ ProjectAPIKey ] }
		// masked_key or prefix is shown; full_key is never present in list.
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Keys) == 0 {
		fmt.Fprintln(os.Stdout, "(no api keys)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "SCOPE", "KEY (MASKED)", "CREATED")
	for _, k := range resp.Keys {
		t.row(k.ID, k.Name, k.Scope, maskAPIKey(k), k.CreatedAt)
	}
	return t.flush()
}

// ── api-keys create ──────────────────────────────────────────────────

func cmdAPIKeysCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("api-keys create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	name := fs.String("name", "", "Human-readable key name (required).")
	scope := fs.String("scope", "write", "Scope: read | write | admin.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *name == "" {
		return fmt.Errorf("api-keys create requires --name=<n>")
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
		"name":  *name,
		"scope": *scope,
	}
	var resp CreateAPIKeyResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/api-keys", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: CreateAPIKeyResponse — { key: ProjectAPIKey, full_key: string }
		// full_key is reveal-once; subsequent list calls only see the masked form.
		return printJSON(os.Stdout, resp)
	}
	if resp.FullKey == "" {
		fmt.Fprintln(os.Stdout, "API key created (no plaintext returned — check the dashboard).")
		return nil
	}
	if !g.quiet {
		fmt.Fprintln(os.Stderr, "⚠  Store this key now — it will NOT be shown again.")
	}
	fmt.Fprintln(os.Stdout, resp.FullKey)
	if resp.Key != nil {
		fmt.Fprintf(os.Stdout, "id:      %s\n", resp.Key.ID)
		fmt.Fprintf(os.Stdout, "name:    %s\n", resp.Key.Name)
		fmt.Fprintf(os.Stdout, "scope:   %s\n", resp.Key.Scope)
	}
	return nil
}

// ── api-keys rotate ──────────────────────────────────────────────────

func cmdAPIKeysRotate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("api-keys rotate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin api-keys rotate <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to rotate key %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Rotate API key %q on project %q? The old key will stop working immediately. [y/N] ", id, ref)
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

	var resp RotateAPIKeyResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/api-keys/"+id+"/rotate", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: RotateAPIKeyResponse — { key: ProjectAPIKey, full_key: string }
		// full_key is the new secret, shown once.
		return printJSON(os.Stdout, resp)
	}
	if resp.FullKey == "" {
		fmt.Fprintln(os.Stdout, "API key rotated (no plaintext returned — check the dashboard).")
		return nil
	}
	if !g.quiet {
		fmt.Fprintln(os.Stderr, "⚠  Store this key now — it will NOT be shown again.")
	}
	fmt.Fprintln(os.Stdout, resp.FullKey)
	if resp.Key != nil {
		fmt.Fprintf(os.Stdout, "id:    %s\n", resp.Key.ID)
		fmt.Fprintf(os.Stdout, "scope: %s\n", resp.Key.Scope)
	}
	return nil
}

// ── api-keys revoke ──────────────────────────────────────────────────

func cmdAPIKeysRevoke(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("api-keys revoke", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin api-keys revoke <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to revoke key %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Revoke API key %q on project %q? This cannot be undone. [y/N] ", id, ref)
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

	var out map[string]any
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/api-keys/"+id, nil, &out); err != nil {
		// Map 404 → friendly message.
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("api key %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: passthrough of revoke envelope (commonly { revoked: bool, id: string }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Revoked API key %s.\n", id)
	return nil
}
