// cmd_secrets — project env-overrides ("secrets").
//
//	basin secrets list   --project <ref>
//	basin secrets set    --project <ref> KEY=VALUE [KEY=VALUE ...]
//	basin secrets rm     --project <ref> KEY [KEY ...]
//
// Wraps the project_settings env-override surface. The endpoint
// is /v1/projects/:ref/secrets — when the cluster doesn't expose
// it yet we surface the typed 404 so the operator knows to upgrade
// the backend rather than blame the CLI.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdSecrets(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin secrets (list | set | rm) ...")
	}
	switch args[0] {
	case "list":
		return cmdSecretsList(g, args[1:])
	case "set":
		return cmdSecretsSet(g, args[1:])
	case "rm", "remove", "delete":
		return cmdSecretsRm(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("secrets", "List / set / remove project env-overrides.", []string{
			"list --project <ref>                            Print every override (values masked).",
			"set  --project <ref> KEY=VALUE [KEY=VALUE ...]  Set or update overrides.",
			"rm   --project <ref> KEY [KEY ...]              Remove overrides.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for secrets", args[0])
	}
}

func cmdSecretsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("secrets list", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Secrets []struct {
			Key       string `json:"key"`
			Prefix    string `json:"prefix"`
			UpdatedAt string `json:"updated_at"`
		} `json:"secrets"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+*project+"/secrets", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { secrets: [ { key: string, prefix: string, updated_at: string } ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "KEY", "PREFIX", "UPDATED")
	for _, s := range resp.Secrets {
		t.row(s.Key, s.Prefix, s.UpdatedAt)
	}
	return t.flush()
}

func cmdSecretsSet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("secrets set", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *project == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin secrets set --project <ref> KEY=VALUE [KEY=VALUE ...]")
	}
	pairs := make(map[string]string, len(rest))
	for _, kv := range rest {
		i := strings.IndexByte(kv, '=')
		if i <= 0 {
			return fmt.Errorf("invalid pair %q (expected KEY=VALUE)", kv)
		}
		pairs[kv[:i]] = kv[i+1:]
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var out map[string]any
	if err := c.do(ctx, "PUT", "/v1/projects/"+*project+"/secrets", map[string]any{"secrets": pairs}, &out); err != nil {
		return err
	}
	if g.json {
		// JSON shape: passthrough of /v1/projects/:ref/secrets PUT envelope
		// (commonly { updated: int, secrets: [{key,prefix,updated_at}] }).
		return printJSON(os.Stdout, out)
	}
	fmt.Fprintf(os.Stdout, "Set %d secret(s).\n", len(pairs))
	return nil
}

func cmdSecretsRm(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("secrets rm", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if *project == "" || len(rest) == 0 {
		return fmt.Errorf("usage: basin secrets rm --project <ref> KEY [KEY ...]")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	for _, key := range rest {
		if err := c.do(ctx, "DELETE", "/v1/projects/"+*project+"/secrets/"+key, nil, nil); err != nil {
			return err
		}
	}
	if g.json {
		// JSON shape: { removed: [ string ] }
		return printJSON(os.Stdout, map[string]any{"removed": rest})
	}
	fmt.Fprintf(os.Stdout, "Removed %d secret(s).\n", len(rest))
	return nil
}
