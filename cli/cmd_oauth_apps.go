// cmd_oauth_apps — OAuth application management for an org.
//
//	basin oauth-apps list                 --org=<slug>
//	basin oauth-apps get    <id>          --org=<slug>
//	basin oauth-apps create               --org=<slug> --name=<n> --redirect-uri=<u> [--scopes=<csv>]
//	basin oauth-apps patch  <id>          --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]
//	basin oauth-apps rotate <id>          --org=<slug> [--yes]
//	basin oauth-apps disable <id>         --org=<slug>
//	basin oauth-apps enable  <id>         --org=<slug>
//	basin oauth-apps delete  <id>         --org=<slug> [--yes]
//
// Cloud routes (mounted under /v1/orgs/{slug}):
//
//	GET    /v1/orgs/{slug}/oauth-apps              → list apps
//	GET    /v1/orgs/{slug}/oauth-apps/{id}         → get one app
//	POST   /v1/orgs/{slug}/oauth-apps              → create app (client_secret shown ONCE)
//	PATCH  /v1/orgs/{slug}/oauth-apps/{id}         → update app
//	POST   /v1/orgs/{slug}/oauth-apps/{id}/rotate-secret  → rotate client_secret (shown ONCE)
//	POST   /v1/orgs/{slug}/oauth-apps/{id}/disable → disable app
//	POST   /v1/orgs/{slug}/oauth-apps/{id}/enable  → enable app
//	DELETE /v1/orgs/{slug}/oauth-apps/{id}         → delete app
//
// Secret-bearing responses (create, rotate) print the client_secret
// exactly once with a "store now" warning. Subsequent get/list calls
// only return the client_id — the cloud's reveal-once contract.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// OAuthApp is the read shape from GET /v1/orgs/{slug}/oauth-apps (list)
// and the embedded "app" field in create/patch/enable/disable responses.
// JSON shape: { id, name, client_id, redirect_uri, scopes, enabled }
type OAuthApp struct {
	ID          string   `json:"id,omitempty"`
	Name        string   `json:"name,omitempty"`
	ClientID    string   `json:"client_id,omitempty"`
	RedirectURI string   `json:"redirect_uri,omitempty"`
	Scopes      []string `json:"scopes,omitempty"`
	Enabled     bool     `json:"enabled"`
	CreatedAt   string   `json:"created_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdOAuthApps(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("oauth-apps", "Manage OAuth applications for an org.", []string{
			"list                 --org=<slug>                                  List OAuth apps.",
			"get    <id>          --org=<slug>                                  Show one OAuth app.",
			"create               --org=<slug> --name=<n> --redirect-uri=<u>   Create an OAuth app (client_secret shown ONCE).",
			"patch  <id>          --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]  Update an app.",
			"rotate <id>          --org=<slug> [--yes]                          Rotate client_secret (shown ONCE).",
			"disable <id>         --org=<slug>                                  Disable an OAuth app.",
			"enable  <id>         --org=<slug>                                  Enable an OAuth app.",
			"delete  <id>         --org=<slug> [--yes]                          Delete an OAuth app.",
		})
		return nil
	}
	switch args[0] {
	case "list":
		return cmdOAuthAppsList(g, args[1:])
	case "get":
		return cmdOAuthAppsGet(g, args[1:])
	case "create":
		return cmdOAuthAppsCreate(g, args[1:])
	case "patch":
		return cmdOAuthAppsPatch(g, args[1:])
	case "rotate":
		return cmdOAuthAppsRotate(g, args[1:])
	case "disable":
		return cmdOAuthAppsDisable(g, args[1:])
	case "enable":
		return cmdOAuthAppsEnable(g, args[1:])
	case "delete":
		return cmdOAuthAppsDelete(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("oauth-apps", "Manage OAuth applications for an org.", []string{
			"list                 --org=<slug>                                  List OAuth apps.",
			"get    <id>          --org=<slug>                                  Show one OAuth app.",
			"create               --org=<slug> --name=<n> --redirect-uri=<u>   Create an OAuth app (client_secret shown ONCE).",
			"patch  <id>          --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]  Update an app.",
			"rotate <id>          --org=<slug> [--yes]                          Rotate client_secret (shown ONCE).",
			"disable <id>         --org=<slug>                                  Disable an OAuth app.",
			"enable  <id>         --org=<slug>                                  Enable an OAuth app.",
			"delete  <id>         --org=<slug> [--yes]                          Delete an OAuth app.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for oauth-apps", args[0])
		return errSilent
	}
}

// ── oauth-apps list ──────────────────────────────────────────────────

func cmdOAuthAppsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("oauth-apps list requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Apps []*OAuthApp `json:"apps"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/oauth-apps", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "apps": [{ id, name, client_id, redirect_uri, scopes, enabled }] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Apps) == 0 {
		fmt.Fprintln(os.Stdout, "(no OAuth apps)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "CLIENT_ID", "ENABLED", "REDIRECT_URI")
	for _, app := range resp.Apps {
		t.row(app.ID, app.Name, app.ClientID, fmt.Sprintf("%v", app.Enabled), app.RedirectURI)
	}
	return t.flush()
}

// ── oauth-apps get ───────────────────────────────────────────────────

func cmdOAuthAppsGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps get <id> --org=<slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		App *OAuthApp `json:"app"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/oauth-apps/"+id, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled } }
		return printJSON(os.Stdout, resp)
	}
	if resp.App == nil {
		return fmt.Errorf("OAuth app not found: %s", id)
	}
	app := resp.App
	fmt.Fprintf(os.Stdout, "id:           %s\n", app.ID)
	fmt.Fprintf(os.Stdout, "name:         %s\n", app.Name)
	fmt.Fprintf(os.Stdout, "client_id:    %s\n", app.ClientID)
	fmt.Fprintf(os.Stdout, "redirect_uri: %s\n", app.RedirectURI)
	fmt.Fprintf(os.Stdout, "enabled:      %v\n", app.Enabled)
	if len(app.Scopes) > 0 {
		fmt.Fprintf(os.Stdout, "scopes:       %s\n", strings.Join(app.Scopes, ", "))
	}
	fmt.Fprintf(os.Stdout, "created_at:   %s\n", app.CreatedAt)
	return nil
}

// ── oauth-apps create ────────────────────────────────────────────────

func cmdOAuthAppsCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	name := fs.String("name", "", "App name (required).")
	redirectURI := fs.String("redirect-uri", "", "Redirect URI (required).")
	scopes := fs.String("scopes", "", "Comma-separated scopes (optional).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("oauth-apps create requires --org=<slug>")
	}
	if *name == "" {
		return fmt.Errorf("oauth-apps create requires --name=<n>")
	}
	if *redirectURI == "" {
		return fmt.Errorf("oauth-apps create requires --redirect-uri=<u>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	body := map[string]any{
		"name":         *name,
		"redirect_uri": *redirectURI,
	}
	if *scopes != "" {
		body["scopes"] = splitCSV(*scopes)
	}

	var resp struct {
		App          *OAuthApp `json:"app"`
		ClientSecret string    `json:"client_secret,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/oauth-apps", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled },
		//              "client_secret": "..." }
		// client_secret is reveal-once: the cloud only returns it on creation.
		return printJSON(os.Stdout, resp)
	}
	// Always print the secret — it will not be shown again.
	fmt.Fprintln(os.Stdout, "OAuth app created. Store this client_secret NOW — it will not be shown again:")
	fmt.Fprintln(os.Stdout, "  "+resp.ClientSecret)
	if resp.App != nil {
		fmt.Fprintf(os.Stdout, "  id:          %s\n", resp.App.ID)
		fmt.Fprintf(os.Stdout, "  client_id:   %s\n", resp.App.ClientID)
		fmt.Fprintf(os.Stdout, "  name:        %s\n", resp.App.Name)
	}
	return nil
}

// ── oauth-apps patch ─────────────────────────────────────────────────

func cmdOAuthAppsPatch(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps patch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	name := fs.String("name", "", "New app name.")
	redirectURI := fs.String("redirect-uri", "", "New redirect URI.")
	scopes := fs.String("scopes", "", "New scopes (comma-separated).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps patch <id> --org=<slug> [--name=<n>] [--redirect-uri=<u>] [--scopes=<csv>]")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	body := map[string]any{}
	if *name != "" {
		body["name"] = *name
	}
	if *redirectURI != "" {
		body["redirect_uri"] = *redirectURI
	}
	if *scopes != "" {
		body["scopes"] = splitCSV(*scopes)
	}

	var resp struct {
		App *OAuthApp `json:"app"`
	}
	if err := c.do(ctx, "PATCH", "/v1/orgs/"+*org+"/oauth-apps/"+id, body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "app": { id, name, client_id, redirect_uri, scopes, enabled } }
		return printJSON(os.Stdout, resp)
	}
	if resp.App != nil {
		fmt.Fprintf(os.Stdout, "Updated OAuth app %q (%s).\n", resp.App.Name, resp.App.ID)
	} else {
		fmt.Fprintf(os.Stdout, "Updated OAuth app %s.\n", id)
	}
	return nil
}

// ── oauth-apps rotate ────────────────────────────────────────────────

func cmdOAuthAppsRotate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps rotate", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps rotate <id> --org=<slug> [--yes]")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to rotate client_secret for app %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Rotate client_secret for OAuth app %q in org %q? Existing clients will break. [y/N] ", id, *org)
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
		App          *OAuthApp `json:"app"`
		ClientSecret string    `json:"client_secret,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/oauth-apps/"+id+"/rotate-secret", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "app": { id, name, client_id, ... }, "client_secret": "..." }
		// client_secret is reveal-once on rotation.
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Client secret rotated. Store this secret NOW — it will not be shown again:")
	fmt.Fprintln(os.Stdout, "  "+resp.ClientSecret)
	if resp.App != nil {
		fmt.Fprintf(os.Stdout, "  app: %s (%s)\n", resp.App.Name, resp.App.ID)
	}
	return nil
}

// ── oauth-apps disable ───────────────────────────────────────────────

func cmdOAuthAppsDisable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps disable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps disable <id> --org=<slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Enabled bool `json:"enabled"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/oauth-apps/"+id+"/disable", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": false }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Disabled OAuth app %s.\n", id)
	return nil
}

// ── oauth-apps enable ────────────────────────────────────────────────

func cmdOAuthAppsEnable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps enable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps enable <id> --org=<slug>")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Enabled bool `json:"enabled"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/oauth-apps/"+id+"/enable", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Enabled OAuth app %s.\n", id)
	return nil
}

// ── oauth-apps delete ────────────────────────────────────────────────

func cmdOAuthAppsDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("oauth-apps delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *org == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin oauth-apps delete <id> --org=<slug> [--yes]")
	}
	id := rest[0]
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to delete OAuth app %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Delete OAuth app %q from org %q? This cannot be undone. [y/N] ", id, *org)
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
		Deleted bool   `json:"deleted"`
		ID      string `json:"id,omitempty"`
	}
	if err := c.do(ctx, "DELETE", "/v1/orgs/"+*org+"/oauth-apps/"+id, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "deleted": true, "id": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted OAuth app %s.\n", id)
	return nil
}
