// cmd_byo_engine — BYO self-hosted Basin engine management.
//
//	basin byo engine get    [--project=<ref>]
//	basin byo engine put    --url=<u> --shared-key=<k> [--project=<ref>] [--yes]
//	basin byo engine probe  [--project=<ref>]
//	basin byo engine delete [--project=<ref>] [--yes]
//
// Cloud routes:
//
//	GET    /v1/projects/{ref}/byo-engine        → get engine config
//	PUT    /v1/projects/{ref}/byo-engine        → set engine config
//	DELETE /v1/projects/{ref}/byo-engine        → remove engine config
//	POST   /v1/projects/{ref}/byo-engine/probe  → probe connectivity
//
// shared-key is NEVER echoed in any printed output.
// Called from cmdBYO in cmd_byo_bucket.go.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// BYOEngineConfig is the read shape for GET /v1/projects/{ref}/byo-engine.
// JSON shape: { url, configured_at }
// shared_key is NEVER returned by the server or printed by the CLI.
type BYOEngineConfig struct {
	URL          string `json:"url,omitempty"`
	ConfiguredAt string `json:"configured_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

// cmdBYOEngine is called from cmdBYO in cmd_byo_bucket.go.
func cmdBYOEngine(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("byo engine", "Manage a self-hosted Basin engine for a project.", []string{
			"get    [--project=<ref>]                                         Show current engine config.",
			"put    --url=<u> --shared-key=<k> [--project=<ref>] [--yes]     Configure a BYO engine.",
			"probe  [--project=<ref>]                                         Test connectivity.",
			"delete [--project=<ref>] [--yes]                                 Remove engine config.",
		})
		return nil
	}
	switch args[0] {
	case "get":
		return cmdBYOEngineGet(g, args[1:])
	case "put":
		return cmdBYOEnginePut(g, args[1:])
	case "probe":
		return cmdBYOEngineProbe(g, args[1:])
	case "delete":
		return cmdBYOEngineDelete(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("byo engine", "Manage a self-hosted Basin engine for a project.", []string{
			"get    [--project=<ref>]                                         Show current engine config.",
			"put    --url=<u> --shared-key=<k> [--project=<ref>] [--yes]     Configure a BYO engine.",
			"probe  [--project=<ref>]                                         Test connectivity.",
			"delete [--project=<ref>] [--yes]                                 Remove engine config.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for byo engine", args[0])
		return errSilent
	}
}

// ── byo engine get ───────────────────────────────────────────────────

func cmdBYOEngineGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo engine get", flag.ContinueOnError)
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
		Engine *BYOEngineConfig `json:"engine"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/byo-engine", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "engine": { url, configured_at } | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.Engine == nil {
		fmt.Fprintln(os.Stdout, "(no BYO engine configured)")
		return nil
	}
	e := resp.Engine
	fmt.Fprintf(os.Stdout, "url:           %s\n", e.URL)
	fmt.Fprintf(os.Stdout, "configured_at: %s\n", e.ConfiguredAt)
	return nil
}

// ── byo engine put ───────────────────────────────────────────────────

func cmdBYOEnginePut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo engine put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	engineURL := fs.String("url", "", "Engine base URL (required).")
	sharedKey := fs.String("shared-key", "", "Shared HMAC key for request signing (required). NEVER echoed.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	if *engineURL == "" {
		return fmt.Errorf("byo engine put requires --url=<engine-url>")
	}
	if *sharedKey == "" {
		return fmt.Errorf("byo engine put requires --shared-key=<key>")
	}

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
			return fmt.Errorf("confirmation required: pass --yes to configure BYO engine non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Configure BYO engine %q on project %q? This rewires query routing. [y/N] ", *engineURL, ref)
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
		"url":        *engineURL,
		"shared_key": *sharedKey,
	}

	var resp struct {
		Engine *BYOEngineConfig `json:"engine"`
	}
	if err := c.do(ctx, "PUT", "/v1/projects/"+ref+"/byo-engine", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "engine": { url, configured_at } }
		// shared_key is NEVER included in the response shape.
		return printJSON(os.Stdout, resp)
	}
	if resp.Engine != nil {
		// Only print non-sensitive fields — shared_key NEVER echoed.
		fmt.Fprintf(os.Stdout, "BYO engine configured: url=%s\n", resp.Engine.URL)
	} else {
		fmt.Fprintln(os.Stdout, "BYO engine configured.")
	}
	return nil
}

// ── byo engine probe ─────────────────────────────────────────────────

func cmdBYOEngineProbe(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo engine probe", flag.ContinueOnError)
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
		OK        bool    `json:"ok"`
		LatencyMs float64 `json:"latency_ms"`
		Error     *string `json:"error"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/byo-engine/probe", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.OK {
		fmt.Fprintf(os.Stdout, "Engine probe OK (latency=%.1fms)\n", resp.LatencyMs)
	} else {
		errMsg := "(unknown error)"
		if resp.Error != nil {
			errMsg = *resp.Error
		}
		fmt.Fprintf(os.Stdout, "Engine probe FAILED: %s\n", errMsg)
	}
	return nil
}

// ── byo engine delete ────────────────────────────────────────────────

func cmdBYOEngineDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo engine delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
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

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to remove BYO engine config non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Remove BYO engine config on project %q? Queries will revert to the basin-managed engine. [y/N] ", ref)
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
		Removed bool `json:"removed"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/byo-engine", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "removed": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "BYO engine config removed.")
	return nil
}
