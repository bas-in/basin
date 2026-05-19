// cmd_saml — SAML SSO configuration for an org.
//
//	basin saml get     --org=<slug>
//	basin saml put     --org=<slug> --metadata-url=<u> [--entity-id=<id>] [--yes]
//	basin saml test    --org=<slug>
//	basin saml enable  --org=<slug> [--yes]
//	basin saml disable --org=<slug> [--yes]
//
// Cloud routes (mounted under /v1/orgs/{slug}):
//
//	GET   /v1/orgs/{slug}/saml             → get SAML config
//	PATCH /v1/orgs/{slug}/saml             → update SAML config (put)
//	POST  /v1/orgs/{slug}/saml/test        → test IdP connectivity
//	POST  /v1/orgs/{slug}/saml/enable      → enable SAML SSO
//	POST  /v1/orgs/{slug}/saml/disable     → disable SAML SSO
//
// All subcommands require --org=<slug>. Destructive operations
// (put, enable, disable) prompt for confirmation unless --yes is passed.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// SAMLConfig is the read shape from GET /v1/orgs/{slug}/saml.
// JSON shape: { idp_metadata_url, idp_entity_id, sp_metadata_url, enabled, configured_at }
type SAMLConfig struct {
	IDPMetadataURL string `json:"idp_metadata_url,omitempty"`
	IDPEntityID    string `json:"idp_entity_id,omitempty"`
	SPMetadataURL  string `json:"sp_metadata_url,omitempty"`
	Enabled        bool   `json:"enabled"`
	ConfiguredAt   string `json:"configured_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdSAML(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("saml", "Manage SAML SSO for an org.", []string{
			"get     --org=<slug>                                       Show SAML config.",
			"put     --org=<slug> --metadata-url=<u> [--entity-id=<id>] [--yes]  Configure SAML IdP.",
			"test    --org=<slug>                                       Test IdP connectivity.",
			"enable  --org=<slug> [--yes]                               Enable SAML SSO.",
			"disable --org=<slug> [--yes]                               Disable SAML SSO.",
		})
		return nil
	}
	switch args[0] {
	case "get":
		return cmdSAMLGet(g, args[1:])
	case "put":
		return cmdSAMLPut(g, args[1:])
	case "test":
		return cmdSAMLTest(g, args[1:])
	case "enable":
		return cmdSAMLEnable(g, args[1:])
	case "disable":
		return cmdSAMLDisable(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("saml", "Manage SAML SSO for an org.", []string{
			"get     --org=<slug>                                       Show SAML config.",
			"put     --org=<slug> --metadata-url=<u> [--entity-id=<id>] [--yes]  Configure SAML IdP.",
			"test    --org=<slug>                                       Test IdP connectivity.",
			"enable  --org=<slug> [--yes]                               Enable SAML SSO.",
			"disable --org=<slug> [--yes]                               Disable SAML SSO.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for saml", args[0])
		return errSilent
	}
}

// ── saml get ─────────────────────────────────────────────────────────

func cmdSAMLGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("saml get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("saml get requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		SAML *SAMLConfig `json:"saml"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/saml", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "saml": { idp_metadata_url, idp_entity_id, sp_metadata_url, enabled, configured_at } | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.SAML == nil {
		fmt.Fprintln(os.Stdout, "(no SAML configured)")
		return nil
	}
	s := resp.SAML
	fmt.Fprintf(os.Stdout, "idp_metadata_url: %s\n", s.IDPMetadataURL)
	fmt.Fprintf(os.Stdout, "idp_entity_id:    %s\n", s.IDPEntityID)
	fmt.Fprintf(os.Stdout, "sp_metadata_url:  %s\n", s.SPMetadataURL)
	fmt.Fprintf(os.Stdout, "enabled:          %v\n", s.Enabled)
	fmt.Fprintf(os.Stdout, "configured_at:    %s\n", s.ConfiguredAt)
	return nil
}

// ── saml put ─────────────────────────────────────────────────────────

func cmdSAMLPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("saml put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	metadataURL := fs.String("metadata-url", "", "IdP SAML metadata URL (required).")
	entityID := fs.String("entity-id", "", "IdP entity ID (optional; inferred from metadata when absent).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("saml put requires --org=<slug>")
	}
	if *metadataURL == "" {
		return fmt.Errorf("saml put requires --metadata-url=<u>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to configure SAML non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Configure SAML IdP for org %q? This affects how members log in. [y/N] ", *org)
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

	body := map[string]any{
		"idp_metadata_url": *metadataURL,
	}
	if *entityID != "" {
		body["idp_entity_id"] = *entityID
	}

	var resp struct {
		SAML *SAMLConfig `json:"saml"`
	}
	if err := c.do(ctx, "PATCH", "/v1/orgs/"+*org+"/saml", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "saml": { idp_metadata_url, idp_entity_id, sp_metadata_url, enabled, configured_at } }
		return printJSON(os.Stdout, resp)
	}
	if resp.SAML != nil {
		fmt.Fprintf(os.Stdout, "SAML configured for org %q (sp_metadata_url: %s)\n",
			*org, resp.SAML.SPMetadataURL)
	} else {
		fmt.Fprintf(os.Stdout, "SAML configured for org %q.\n", *org)
	}
	return nil
}

// ── saml test ────────────────────────────────────────────────────────

func cmdSAMLTest(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("saml test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("saml test requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		OK       bool    `json:"ok"`
		Error    *string `json:"error"`
		TestedAt string  `json:"tested_at,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/saml/test", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "ok": bool, "error": "..." | null, "tested_at": "..." }
		return printJSON(os.Stdout, resp)
	}
	if resp.OK {
		fmt.Fprintf(os.Stdout, "SAML test OK (tested_at: %s)\n", resp.TestedAt)
	} else {
		errMsg := "(unknown error)"
		if resp.Error != nil {
			errMsg = *resp.Error
		}
		fmt.Fprintf(os.Stdout, "SAML test FAILED: %s\n", errMsg)
	}
	return nil
}

// ── saml enable ──────────────────────────────────────────────────────

func cmdSAMLEnable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("saml enable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("saml enable requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to enable SAML non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Enable SAML SSO for org %q? Members will be required to authenticate via the IdP. [y/N] ", *org)
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
		Enabled bool `json:"enabled"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/saml/enable", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "SAML SSO enabled for org %q.\n", *org)
	return nil
}

// ── saml disable ─────────────────────────────────────────────────────

func cmdSAMLDisable(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("saml disable", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", g.orgSlug, "Org slug (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *org == "" {
		return fmt.Errorf("saml disable requires --org=<slug>")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to disable SAML non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Disable SAML SSO for org %q? Members will fall back to email/password login. [y/N] ", *org)
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
		Enabled bool `json:"enabled"`
	}
	if err := c.do(ctx, "POST", "/v1/orgs/"+*org+"/saml/disable", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "enabled": false }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "SAML SSO disabled for org %q.\n", *org)
	return nil
}
