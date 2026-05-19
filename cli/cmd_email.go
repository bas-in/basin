// cmd_email — project email template configuration and allowance.
//
//	basin email templates get    [--project=<ref>]
//	basin email templates put    --kind=<k> --subject=<s> --body=<b> [--project=<ref>]
//	basin email templates delete [--project=<ref>] [--yes]
//	basin email templates test   --kind=<k> --to=<email>            [--project=<ref>]
//	basin email allowance        [--project=<ref>]
//
// Cloud routes (server.go ~line 738):
//   GET    /v1/projects/{ref}/email-config        → templates get
//   PATCH  /v1/projects/{ref}/email-config        → templates put (partial update)
//   DELETE /v1/projects/{ref}/email-config        → templates delete (reset to defaults)
//   POST   /v1/projects/{ref}/email-config/test   → templates test
//   GET    /v1/projects/{ref}/email/allowance     → allowance
//
// Note: the cloud mounts the email template surface under /email-config
// (not /email/templates). The CLI surfaces it as `email templates ...`
// for user-friendliness; the underlying path is /email-config.
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

// EmailAllowance is the read shape from GET /email/allowance.
// JSON shape: { limit, used, reset_at }
type EmailAllowance struct {
	Limit   int    `json:"limit"`
	Used    int    `json:"used"`
	ResetAt string `json:"reset_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdEmail(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("email", "Manage project email templates and view allowance.", []string{
			"templates get    [--project=<ref>]                           Get all email templates.",
			"templates put    --kind=<k> --subject=<s> --body=<b>        Update a template kind.",
			"                 [--project=<ref>]",
			"templates delete [--project=<ref>] [--yes]                  Reset templates to defaults.",
			"templates test   --kind=<k> --to=<email> [--project=<ref>]  Send a test email.",
			"allowance        [--project=<ref>]                           Show email send allowance.",
		})
		return nil
	}
	switch args[0] {
	case "templates":
		return cmdEmailTemplates(g, args[1:])
	case "allowance":
		return cmdEmailAllowance(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("email", "Manage project email templates and view allowance.", []string{
			"templates get    [--project=<ref>]                           Get all email templates.",
			"templates put    --kind=<k> --subject=<s> --body=<b>        Update a template kind.",
			"                 [--project=<ref>]",
			"templates delete [--project=<ref>] [--yes]                  Reset templates to defaults.",
			"templates test   --kind=<k> --to=<email> [--project=<ref>]  Send a test email.",
			"allowance        [--project=<ref>]                           Show email send allowance.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for email", args[0])
		return errSilent
	}
}

// cmdEmailTemplates dispatches email templates sub-subcommands.
func cmdEmailTemplates(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdEmailTemplatesGet(g, nil)
	}
	switch args[0] {
	case "get":
		return cmdEmailTemplatesGet(g, args[1:])
	case "put":
		return cmdEmailTemplatesPut(g, args[1:])
	case "delete":
		return cmdEmailTemplatesDelete(g, args[1:])
	case "test":
		return cmdEmailTemplatesTest(g, args[1:])
	default:
		printErr(g, "unknown subcommand %q for email templates", args[0])
		return errSilent
	}
}

// ── email templates get ──────────────────────────────────────────────

func cmdEmailTemplatesGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("email templates get", flag.ContinueOnError)
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

	// Response is a freeform map keyed by template kind.
	var resp struct {
		Templates map[string]any `json:"templates"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/email-config", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { templates: { <kind>: { subject, body } } }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Templates) == 0 {
		fmt.Fprintln(os.Stdout, "(no email templates configured)")
		return nil
	}
	t := newTable(g, "KIND", "SUBJECT")
	for kind, v := range resp.Templates {
		subject := ""
		if m, ok := v.(map[string]any); ok {
			if s, ok := m["subject"].(string); ok {
				subject = s
			}
		}
		t.row(kind, subject)
	}
	return t.flush()
}

// ── email templates put ──────────────────────────────────────────────

func cmdEmailTemplatesPut(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("email templates put", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	kind := fs.String("kind", "", "Template kind, e.g. confirmation, recovery (required).")
	subject := fs.String("subject", "", "Email subject line (required).")
	body := fs.String("body", "", "Email body HTML/text (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *kind == "" {
		return fmt.Errorf("email templates put requires --kind=<kind>")
	}
	if *subject == "" {
		return fmt.Errorf("email templates put requires --subject=<subject>")
	}
	if *body == "" {
		return fmt.Errorf("email templates put requires --body=<body>")
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

	reqBody := map[string]any{
		"templates": map[string]any{
			*kind: map[string]any{
				"subject": *subject,
				"body":    *body,
			},
		},
	}
	var resp struct {
		Templates map[string]any `json:"templates"`
	}
	if err := c.do(ctx, "PATCH", "/v1/projects/"+ref+"/email-config", reqBody, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { templates: { <kind>: { subject, body } } }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Updated email template %q.\n", *kind)
	return nil
}

// ── email templates delete ───────────────────────────────────────────

func cmdEmailTemplatesDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("email templates delete", flag.ContinueOnError)
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
			return fmt.Errorf("confirmation required: pass --yes to reset email templates non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Reset email templates to defaults for project %q? [y/N] ", ref)
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
		ResetToDefaults bool `json:"reset_to_defaults"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/email-config", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { reset_to_defaults: true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Email templates reset to defaults.")
	return nil
}

// ── email templates test ─────────────────────────────────────────────

// validEmailAddress performs a minimal sanity check on an email address.
// It requires exactly one "@" with non-empty local and domain parts.
func validEmailAddress(s string) bool {
	parts := strings.SplitN(s, "@", 2)
	if len(parts) != 2 {
		return false
	}
	return parts[0] != "" && strings.Contains(parts[1], ".")
}

func cmdEmailTemplatesTest(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("email templates test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	kind := fs.String("kind", "", "Template kind to test (required).")
	to := fs.String("to", "", "Recipient email address (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *kind == "" {
		return fmt.Errorf("email templates test requires --kind=<kind>")
	}
	if *to == "" {
		return fmt.Errorf("email templates test requires --to=<email>")
	}
	if !validEmailAddress(*to) {
		return fmt.Errorf("email templates test: invalid email address %q", *to)
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
		"kind": *kind,
		"to":   *to,
	}
	var resp struct {
		Sent      bool   `json:"sent"`
		MessageID string `json:"message_id,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/email-config/test", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { sent: true, message_id: string }
		return printJSON(os.Stdout, resp)
	}
	if resp.Sent {
		fmt.Fprintf(os.Stdout, "Test email sent to %s", *to)
		if resp.MessageID != "" {
			fmt.Fprintf(os.Stdout, " (message_id: %s)", resp.MessageID)
		}
		fmt.Fprintln(os.Stdout)
	} else {
		fmt.Fprintln(os.Stdout, "Test email send failed (cloud returned sent=false).")
	}
	return nil
}

// ── email allowance ──────────────────────────────────────────────────

func cmdEmailAllowance(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("email allowance", flag.ContinueOnError)
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
		Allowance *EmailAllowance `json:"allowance"`
	}
	// Some cloud shapes return the fields at the top level rather than nested.
	// Try the nested form first; if allowance is nil, try the flat form.
	var flatResp EmailAllowance
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/email/allowance", nil, &resp); err != nil {
		return err
	}
	if resp.Allowance == nil {
		// Try flat decode (cloud may return { limit, used, reset_at } directly).
		if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/email/allowance", nil, &flatResp); err == nil && flatResp.Limit > 0 {
			resp.Allowance = &flatResp
		}
	}

	if g.json {
		// JSON shape: { allowance: { limit, used, reset_at } }
		return printJSON(os.Stdout, resp)
	}
	if resp.Allowance == nil {
		fmt.Fprintln(os.Stdout, "(email allowance data unavailable)")
		return nil
	}
	a := resp.Allowance
	fmt.Fprintf(os.Stdout, "limit:    %d\n", a.Limit)
	fmt.Fprintf(os.Stdout, "used:     %d\n", a.Used)
	fmt.Fprintf(os.Stdout, "reset_at: %s\n", a.ResetAt)
	return nil
}
