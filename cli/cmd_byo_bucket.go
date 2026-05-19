// cmd_byo_bucket — BYO surfaces: bucket, KMS, and self-hosted engine.
//
//	basin byo bucket get    [--project=<ref>]
//	basin byo bucket put    --provider=<s3|r2|tigris|minio> --bucket=<n> --region=<r> \
//	                        --access-key=<id> --secret-key=<s> [--endpoint=<u>] \
//	                        [--project=<ref>] [--yes]
//	basin byo bucket probe  [--project=<ref>]
//	basin byo bucket delete [--project=<ref>] [--yes]
//
//	basin byo kms  (dispatched to cmdBYOKMS in cmd_byo_kms.go)
//	basin byo engine (dispatched to cmdBYOEngine in cmd_byo_engine.go)
//
// Cloud routes:
//
//	GET    /v1/projects/{ref}/storage        → get storage config
//	PUT    /v1/projects/{ref}/storage        → set storage config
//	POST   /v1/projects/{ref}/storage/probe  → probe connectivity
//	DELETE /v1/projects/{ref}/storage        → revoke storage config
//
// The top-level dispatcher cmdBYO lives here (single entry point).
// cmdBYOKMS and cmdBYOEngine are defined in their own files and called
// from cmdBYO.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// BYOStorageConfig is the read shape for GET /v1/projects/{ref}/storage.
// JSON shape: { provider, bucket, region, configured_at }
type BYOStorageConfig struct {
	Provider     string `json:"provider,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	Region       string `json:"region,omitempty"`
	ConfiguredAt string `json:"configured_at,omitempty"`
}

// validBYOStorageProviders is the allowed set for --provider.
var validBYOStorageProviders = map[string]bool{
	"s3": true, "r2": true, "tigris": true, "minio": true,
}

// ── Top-level BYO dispatcher ─────────────────────────────────────────

// cmdBYO is the single top-level entry point for `basin byo`.
// It routes to cmdBYOBucket, cmdBYOKMS, or cmdBYOEngine.
func cmdBYO(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("byo", "Manage bring-your-own infra surfaces (bucket, KMS, engine).", []string{
			"bucket  get|put|probe|delete  [--project=<ref>]   Manage a customer-owned object storage bucket.",
			"kms     get|put|rotate|verify|audit|delete|cache-stats  [--project=<ref>]  Manage a customer-managed KMS key.",
			"engine  get|put|probe|delete  [--project=<ref>]   Manage a self-hosted Basin engine.",
		})
		return nil
	}
	switch args[0] {
	case "bucket":
		return cmdBYOBucket(g, args[1:])
	case "kms":
		return cmdBYOKMS(g, args[1:])
	case "engine":
		return cmdBYOEngine(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("byo", "Manage bring-your-own infra surfaces (bucket, KMS, engine).", []string{
			"bucket  get|put|probe|delete  [--project=<ref>]   Manage a customer-owned object storage bucket.",
			"kms     get|put|rotate|verify|audit|delete|cache-stats  [--project=<ref>]  Manage a customer-managed KMS key.",
			"engine  get|put|probe|delete  [--project=<ref>]   Manage a self-hosted Basin engine.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for byo", args[0])
		return errSilent
	}
}

// ── byo bucket dispatcher ────────────────────────────────────────────

func cmdBYOBucket(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("byo bucket", "Manage a customer-owned object storage bucket for Parquet data.", []string{
			"get    [--project=<ref>]                                              Show current bucket config.",
			"put    --provider=<s3|r2|tigris|minio> --bucket=<n> --region=<r>     Configure a bucket.",
			"       --access-key=<id> --secret-key=<s> [--endpoint=<u>]",
			"       [--project=<ref>] [--yes]",
			"probe  [--project=<ref>]                                              Test connectivity.",
			"delete [--project=<ref>] [--yes]                                      Remove bucket config.",
		})
		return nil
	}
	switch args[0] {
	case "get":
		return cmdBYOBucketGet(g, args[1:])
	case "put":
		return cmdBYOBucketPut(g, args[1:])
	case "probe":
		return cmdBYOBucketProbe(g, args[1:])
	case "delete":
		return cmdBYOBucketDelete(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("byo bucket", "Manage a customer-owned object storage bucket for Parquet data.", []string{
			"get    [--project=<ref>]                                              Show current bucket config.",
			"put    --provider=<s3|r2|tigris|minio> --bucket=<n> --region=<r>     Configure a bucket.",
			"       --access-key=<id> --secret-key=<s> [--endpoint=<u>]",
			"       [--project=<ref>] [--yes]",
			"probe  [--project=<ref>]                                              Test connectivity.",
			"delete [--project=<ref>] [--yes]                                      Remove bucket config.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for byo bucket", args[0])
		return errSilent
	}
}

// ── byo bucket get ───────────────────────────────────────────────────

func cmdBYOBucketGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo bucket get", flag.ContinueOnError)
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
		Storage *BYOStorageConfig `json:"storage"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/storage", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "storage": { provider, bucket, region, configured_at } | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.Storage == nil {
		fmt.Fprintln(os.Stdout, "(no BYO bucket configured)")
		return nil
	}
	s := resp.Storage
	fmt.Fprintf(os.Stdout, "provider:      %s\n", s.Provider)
	fmt.Fprintf(os.Stdout, "bucket:        %s\n", s.Bucket)
	fmt.Fprintf(os.Stdout, "region:        %s\n", s.Region)
	fmt.Fprintf(os.Stdout, "configured_at: %s\n", s.ConfiguredAt)
	return nil
}

// ── byo bucket put ───────────────────────────────────────────────────

func cmdBYOBucketPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo bucket put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	provider := fs.String("provider", "", "Storage provider: s3, r2, tigris, or minio (required).")
	bucket := fs.String("bucket", "", "Bucket name (required).")
	region := fs.String("region", "", "Bucket region (required).")
	accessKey := fs.String("access-key", "", "Access key ID (required).")
	secretKey := fs.String("secret-key", "", "Secret access key (required).")
	endpoint := fs.String("endpoint", "", "Custom endpoint URL (optional; required for minio).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	// Validate required fields before any HTTP call.
	if *provider == "" {
		return fmt.Errorf("byo bucket put requires --provider=<s3|r2|tigris|minio>")
	}
	if !validBYOStorageProviders[strings.ToLower(*provider)] {
		return fmt.Errorf("byo bucket put: invalid --provider %q; must be one of s3, r2, tigris, minio", *provider)
	}
	if *bucket == "" {
		return fmt.Errorf("byo bucket put requires --bucket=<name>")
	}
	if *region == "" {
		return fmt.Errorf("byo bucket put requires --region=<region>")
	}
	if *accessKey == "" {
		return fmt.Errorf("byo bucket put requires --access-key=<id>")
	}
	if *secretKey == "" {
		return fmt.Errorf("byo bucket put requires --secret-key=<secret>")
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
			return fmt.Errorf("confirmation required: pass --yes to configure BYO bucket non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Configure BYO bucket %q (%s) on project %q? This rewires the data plane. [y/N] ", *bucket, *provider, ref)
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
		"provider":   strings.ToLower(*provider),
		"bucket":     *bucket,
		"region":     *region,
		"access_key": *accessKey,
		"secret_key": *secretKey,
	}
	if *endpoint != "" {
		body["endpoint"] = *endpoint
	}

	var resp struct {
		Storage *BYOStorageConfig `json:"storage"`
	}
	if err := c.do(ctx, "PUT", "/v1/projects/"+ref+"/storage", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "storage": { provider, bucket, region, configured_at } }
		// secret_key is NEVER included in the response shape.
		return printJSON(os.Stdout, resp)
	}
	if resp.Storage != nil {
		// Never echo the secret key.
		fmt.Fprintf(os.Stdout, "BYO bucket configured: provider=%s bucket=%s region=%s\n",
			resp.Storage.Provider, resp.Storage.Bucket, resp.Storage.Region)
	} else {
		fmt.Fprintln(os.Stdout, "BYO bucket configured.")
	}
	return nil
}

// ── byo bucket probe ─────────────────────────────────────────────────

func cmdBYOBucketProbe(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo bucket probe", flag.ContinueOnError)
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
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/storage/probe", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "ok": bool, "latency_ms": N, "error": "..." | null }
		return printJSON(os.Stdout, resp)
	}
	if resp.OK {
		fmt.Fprintf(os.Stdout, "Bucket probe OK (latency=%.1fms)\n", resp.LatencyMs)
	} else {
		errMsg := "(unknown error)"
		if resp.Error != nil {
			errMsg = *resp.Error
		}
		fmt.Fprintf(os.Stdout, "Bucket probe FAILED: %s\n", errMsg)
	}
	return nil
}

// ── byo bucket delete ────────────────────────────────────────────────

func cmdBYOBucketDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("byo bucket delete", flag.ContinueOnError)
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
			return fmt.Errorf("confirmation required: pass --yes to remove BYO bucket config non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Remove BYO bucket config on project %q? This disconnects the data plane. [y/N] ", ref)
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
		Revoked bool `json:"revoked"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/storage", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "revoked": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "BYO bucket config removed.")
	return nil
}
