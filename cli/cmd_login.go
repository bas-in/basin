// cmd_login / cmd_logout — credential lifecycle.
//
// `basin login` prompts the user to paste a personal access token,
// validates it by calling /auth/v1/user, and writes it to disk under
// either `default_token` (no --org) or `tokens.<slug>`. `basin
// logout` wipes the matching entry.
//
// We intentionally don't open a browser flow yet — the dashboard's
// PAT mint UI is the single source of credentials, and a CLI-driven
// device-code dance can be a follow-up once the auth server grows
// the matching endpoints.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

func cmdLogin(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("login", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	tokenFlag := fs.String("token", "", "PAT to store (skips the interactive prompt).")
	orgFlag := fs.String("org", g.orgSlug, "Store under tokens.<slug> instead of default_token.")
	apiURL := fs.String("api-url", g.apiURL, "API endpoint to validate against.")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("login", "Sign in by pasting a personal access token.", []string{
			"--token=<pat>     Provide the token non-interactively.",
			"--org=<slug>      Store under tokens.<slug> (per-org). Default writes default_token.",
			"--api-url=<url>   Validate against this endpoint.",
		})
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}

	token := *tokenFlag
	if token == "" {
		fmt.Fprintln(os.Stderr, "Paste a personal access token (mint one at /app/org/<slug>/api-tokens):")
		fmt.Fprint(os.Stderr, "> ")
		t, err := readLine()
		if err != nil {
			return err
		}
		token = strings.TrimSpace(t)
	}
	if token == "" {
		return errors.New("no token provided")
	}
	if !strings.HasPrefix(token, "bso_org_") {
		printErr(g, "warning: token doesn't start with 'bso_org_' — is this the right value?")
	}

	c := NewClient(*apiURL, token)
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var me struct {
		User *User `json:"user"`
	}
	if err := c.do(ctx, "GET", "/auth/v1/user", nil, &me); err != nil {
		// Surface the typed code so the operator can see "expired" /
		// "invalid" rather than a flat HTTP 401.
		printErr(g, "validation failed: %v", err)
		return errSilent
	}

	cf, path, err := upsertToken(*orgFlag, token)
	if err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	// Optionally remember the org as default for future invocations
	// where --org isn't passed.
	if *orgFlag != "" && cf.DefaultOrg == "" {
		cf.DefaultOrg = *orgFlag
		if err := writeConfigFile(cf); err != nil {
			return fmt.Errorf("write config: %w", err)
		}
	}

	if g.json {
		// JSON shape: { signed_in: bool, user: User, config_path: string, stored_under: string }
		return printJSON(os.Stdout, map[string]any{
			"signed_in":    true,
			"user":         me.User,
			"config_path":  path,
			"stored_under": orgOrDefault(*orgFlag),
		})
	}
	if me.User != nil && me.User.Email != "" {
		fmt.Fprintf(os.Stdout, "Signed in as %s. Token saved to %s (%s).\n",
			me.User.Email, path, orgOrDefault(*orgFlag))
	} else {
		fmt.Fprintf(os.Stdout, "Signed in. Token saved to %s (%s).\n",
			path, orgOrDefault(*orgFlag))
	}
	return nil
}

// orgOrDefault is a tiny render helper.
func orgOrDefault(org string) string {
	if org == "" {
		return "default_token"
	}
	return "tokens." + org
}

// readLine pulls a single line from stdin. Trims the trailing
// newline. Returns "" + io.EOF when the user hits Ctrl-D without
// typing anything.
func readLine() (string, error) {
	r := bufio.NewReader(os.Stdin)
	s, err := r.ReadString('\n')
	if err != nil && s == "" {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}
