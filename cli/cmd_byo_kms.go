// cmd_byo_kms — BYO KMS (customer-managed encryption key) management.
//
//	basin byo kms get         [--project=<ref>]
//	basin byo kms put         --provider=<aws|gcp|azure> --key-id=<id>
//	                          [--auth-json=<path|->] [--project=<ref>] [--yes]
//	basin byo kms rotate      [--project=<ref>] [--yes]
//	basin byo kms verify      [--project=<ref>]
//	basin byo kms audit       [--project=<ref>] [--limit=N] [--since=<rfc3339>]
//	basin byo kms delete      [--project=<ref>] [--yes]
//	basin byo kms cache-stats [--project=<ref>]
//
// Cloud routes (mounted under /v1/projects/{ref}/encryption):
//
//	GET    /v1/projects/{ref}/encryption           → get KMS config
//	POST   /v1/projects/{ref}/encryption           → set/update KMS config (put)
//	DELETE /v1/projects/{ref}/encryption           → delete KMS config
//	POST   /v1/projects/{ref}/encryption/verify    → verify key connectivity
//	POST   /v1/projects/{ref}/encryption/rotate    → rotate key version
//	GET    /v1/projects/{ref}/encryption/audit     → audit log
//	GET    /v1/projects/{ref}/encryption/cache-stats → key-cache stats
//
// Auth blob handling:
//   - --auth-json=<path> reads a JSON credentials file from disk.
//   - --auth-json=- reads the JSON credentials blob from stdin.
//   - The auth blob is NEVER echoed in output — only its presence is
//     acknowledged. This prevents accidental leakage of cloud credentials
//     in terminal scrollback or CI logs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// BYOKMSConfig is the read shape for GET /v1/projects/{ref}/encryption.
// JSON shape: { provider, key_id, configured_at }
type BYOKMSConfig struct {
	Provider     string `json:"provider,omitempty"`
	KeyID        string `json:"key_id,omitempty"`
	ConfiguredAt string `json:"configured_at,omitempty"`
}

// KMSAuditEvent is one entry from GET /v1/projects/{ref}/encryption/audit.
// JSON shape: { id, action, at, actor_id, key_version }
type KMSAuditEvent struct {
	ID         string `json:"id,omitempty"`
	Action     string `json:"action,omitempty"`
	At         string `json:"at,omitempty"`
	ActorID    string `json:"actor_id,omitempty"`
	KeyVersion string `json:"key_version,omitempty"`
}

// validBYOKMSProviders is the allowed set for --provider.
var validBYOKMSProviders = map[string]bool{
	"aws": true, "gcp": true, "azure": true,
}

// ── Dispatcher ───────────────────────────────────────────────────────

// cmdBYOKMS is called from cmdBYO in cmd_byo_bucket.go.
func cmdBYOKMS(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("byo kms", "Manage a customer-managed KMS encryption key.", []string{
			"get         [--project=<ref>]                                             Show current KMS config.",
			"put         --provider=<aws|gcp|azure> --key-id=<id>                     Configure a KMS key.",
			"            [--auth-json=<path|->] [--project=<ref>] [--yes]",
			"rotate      [--project=<ref>] [--yes]                                     Rotate the active key version.",
			"verify      [--project=<ref>]                                             Test key connectivity.",
			"audit       [--project=<ref>] [--limit=N] [--since=<rfc3339>]            List KMS audit events.",
			"delete      [--project=<ref>] [--yes]                                     Remove KMS config.",
			"cache-stats [--project=<ref>]                                             Show key-cache hit/miss stats.",
		})
		return nil
	}
	switch args[0] {
	case "get":
		return cmdBYOKMSGet(g, args[1:])
	case "put":
		return cmdBYOKMSPut(g, args[1:])
	case "rotate":
		return cmdBYOKMSRotate(g, args[1:])
	case "verify":
		return cmdBYOKMSVerify(g, args[1:])
	case "audit":
		return cmdBYOKMSAudit(g, args[1:])
	case "delete":
		return cmdBYOKMSDelete(g, args[1:])
	case "cache-stats":
		return cmdBYOKMSCacheStats(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("byo kms", "Manage a customer-managed KMS encryption key.", []string{
			"get         [--project=<ref>]                                             Show current KMS config.",
			"put         --provider=<aws|gcp|azure> --key-id=<id>                     Configure a KMS key.",
			"            [--auth-json=<path|->] [--project=<ref>] [--yes]",
			"rotate      [--project=<ref>] [--yes]                                     Rotate the active key version.",
			"verify      [--project=<ref>]                                             Test key connectivity.",
			"audit       [--project=<ref>] [--limit=N] [--since=<rfc3339>]            List KMS audit events.",
			"delete      [--project=<ref>] [--yes]                                     Remove KMS config.",
			"cache-stats [--project=<ref>]                                             Show key-cache hit/miss stats.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for byo kms", args[0])
		return errSilent
	}
}

// ── byo kms get ──────────────────────────────────────────────────────

func cmdBYOKMSGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms get", flag.ContinueOnError)
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
		KMS *BYOKMSConfig `json:"kms"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/encryption", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "kms": { provider, key_id, configured_at } | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.KMS == nil {
		fmt.Fprintln(os.Stdout, "(no BYO KMS configured)")
		return nil
	}
	k := resp.KMS
	fmt.Fprintf(os.Stdout, "provider:      %s\n", k.Provider)
	fmt.Fprintf(os.Stdout, "key_id:        %s\n", k.KeyID)
	fmt.Fprintf(os.Stdout, "configured_at: %s\n", k.ConfiguredAt)
	return nil
}

// ── byo kms put ──────────────────────────────────────────────────────

// readAuthBlob reads the auth JSON blob from either a file path or stdin
// (when path == "-"). Returns the parsed JSON as a map so we can embed
// it in the request body. Returns nil when path is empty.
// IMPORTANT: the blob is NEVER printed back to the user.
func readAuthBlob(path string) (map[string]any, error) {
	if path == "" {
		return nil, nil
	}

	var raw []byte
	var err error
	if path == "-" {
		raw, err = io.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("byo kms put: reading auth JSON from stdin: %w", err)
		}
	} else {
		raw, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("byo kms put: reading auth JSON from %q: %w", path, err)
		}
	}

	raw = []byte(strings.TrimSpace(string(raw)))
	if len(raw) == 0 {
		return nil, fmt.Errorf("byo kms put: auth JSON is empty")
	}

	var blob map[string]any
	if err := json.Unmarshal(raw, &blob); err != nil {
		return nil, fmt.Errorf("byo kms put: auth JSON is not valid JSON: %w", err)
	}
	return blob, nil
}

func cmdBYOKMSPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	provider := fs.String("provider", "", "KMS provider: aws, gcp, or azure (required).")
	keyID := fs.String("key-id", "", "KMS key identifier (required).")
	authJSON := fs.String("auth-json", "", "Path to credentials JSON file (or - for stdin). NEVER echoed.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	if *provider == "" {
		return fmt.Errorf("byo kms put requires --provider=<aws|gcp|azure>")
	}
	if !validBYOKMSProviders[strings.ToLower(*provider)] {
		return fmt.Errorf("byo kms put: invalid --provider %q; must be one of aws, gcp, azure", *provider)
	}
	if *keyID == "" {
		return fmt.Errorf("byo kms put requires --key-id=<id>")
	}

	// Read auth blob (may be nil) — NEVER print its contents.
	authBlob, err := readAuthBlob(*authJSON)
	if err != nil {
		return err
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
			return fmt.Errorf("confirmation required: pass --yes to configure BYO KMS non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Configure BYO KMS key %q (%s) on project %q? [y/N] ", *keyID, *provider, ref)
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
		"provider": strings.ToLower(*provider),
		"key_id":   *keyID,
	}
	if authBlob != nil {
		// Embed the auth credentials blob — NEVER reflected back in output.
		body["auth"] = authBlob
	}

	var resp struct {
		KMS *BYOKMSConfig `json:"kms"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/encryption", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "kms": { provider, key_id, configured_at } }
		// auth blob is NEVER included in the response shape.
		return printJSON(os.Stdout, resp)
	}
	if resp.KMS != nil {
		// Only print non-sensitive fields — auth blob NEVER echoed.
		fmt.Fprintf(os.Stdout, "BYO KMS configured: provider=%s key_id=%s\n",
			resp.KMS.Provider, resp.KMS.KeyID)
	} else {
		fmt.Fprintln(os.Stdout, "BYO KMS configured.")
	}
	return nil
}

// ── byo kms rotate ───────────────────────────────────────────────────

func cmdBYOKMSRotate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms rotate", flag.ContinueOnError)
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
			return fmt.Errorf("confirmation required: pass --yes to rotate the KMS key non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Rotate BYO KMS key on project %q? This re-encrypts the data-encryption key. [y/N] ", ref)
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
		Rotated       bool   `json:"rotated"`
		NewKeyVersion string `json:"new_key_version,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/encryption/rotate", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "rotated": true, "new_key_version": "..." }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "KMS key rotated. New version: %s\n", resp.NewKeyVersion)
	return nil
}

// ── byo kms verify ───────────────────────────────────────────────────

func cmdBYOKMSVerify(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms verify", flag.ContinueOnError)
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
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/encryption/verify", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.OK {
		fmt.Fprintf(os.Stdout, "KMS key verify OK (latency=%.1fms)\n", resp.LatencyMs)
	} else {
		errMsg := "(unknown error)"
		if resp.Error != nil {
			errMsg = *resp.Error
		}
		fmt.Fprintf(os.Stdout, "KMS key verify FAILED: %s\n", errMsg)
	}
	return nil
}

// ── byo kms audit ────────────────────────────────────────────────────

func cmdBYOKMSAudit(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms audit", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	limit := fs.Int("limit", 50, "Maximum number of events to return.")
	since := fs.String("since", "", "Return events at or after this RFC3339 timestamp.")
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

	q := queryString(map[string]string{
		"limit": fmt.Sprintf("%d", *limit),
		"since": *since,
	})
	var resp struct {
		Events []*KMSAuditEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/encryption/audit"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "events": [{ id, action, at, actor_id, key_version }] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no KMS audit events)")
		return nil
	}
	t := newTable(g, "ID", "ACTION", "AT", "ACTOR_ID", "KEY_VERSION")
	for _, ev := range resp.Events {
		t.row(ev.ID, ev.Action, ev.At, ev.ActorID, ev.KeyVersion)
	}
	return t.flush()
}

// ── byo kms delete ───────────────────────────────────────────────────

func cmdBYOKMSDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms delete", flag.ContinueOnError)
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
			return fmt.Errorf("confirmation required: pass --yes to delete BYO KMS config non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Delete BYO KMS config on project %q? Data will revert to the default cluster key. [y/N] ", ref)
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
		Deleted bool `json:"deleted"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/encryption", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "deleted": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "BYO KMS config deleted.")
	return nil
}

// ── byo kms cache-stats ──────────────────────────────────────────────

func cmdBYOKMSCacheStats(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo kms cache-stats", flag.ContinueOnError)
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
		Hits    int `json:"hits"`
		Misses  int `json:"misses"`
		Size    int `json:"size"`
		MaxSize int `json:"max_size"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/encryption/cache-stats", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "hits": N, "misses": N, "size": N, "max_size": N }
		return printJSON(os.Stdout, resp)
	}
	hitRate := 0.0
	if total := resp.Hits + resp.Misses; total > 0 {
		hitRate = float64(resp.Hits) / float64(total) * 100
	}
	fmt.Fprintf(os.Stdout, "hits:     %d\n", resp.Hits)
	fmt.Fprintf(os.Stdout, "misses:   %d\n", resp.Misses)
	fmt.Fprintf(os.Stdout, "hit_rate: %.1f%%\n", hitRate)
	fmt.Fprintf(os.Stdout, "size:     %d\n", resp.Size)
	fmt.Fprintf(os.Stdout, "max_size: %d\n", resp.MaxSize)
	return nil
}
