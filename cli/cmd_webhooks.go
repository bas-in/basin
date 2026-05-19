// cmd_webhooks — signed-webhook subscription lifecycle for a project.
//
//	basin webhooks list       [--project=<ref>]
//	basin webhooks create     --url=<u> --events=<csv> [--project=<ref>]
//	basin webhooks patch      <id> [--url=<u>] [--events=<csv>] [--enabled=true|false] [--project=<ref>]
//	basin webhooks test       <id> [--project=<ref>]
//	basin webhooks redeliver  <id> --delivery=<delivery-id> [--project=<ref>]
//	basin webhooks delete     <id> [--project=<ref>] [--yes]
//	basin webhooks deliveries <id> [--project=<ref>] [--limit=N]
//
// Backed by /v1/projects/{ref}/webhooks/* (migration 0024).
//
// Events CSV parsing: values are split on comma, each trimmed of
// whitespace, empty tokens dropped, and the resulting slice
// deduplicated (order-preserving, first-seen wins).
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

// Webhook is the read shape from GET /v1/projects/{ref}/webhooks.
// JSON shape: { id, url, events, secret_prefix, enabled, created_at }
type Webhook struct {
	ID           string   `json:"id,omitempty"`
	ProjectID    string   `json:"project_id,omitempty"`
	URL          string   `json:"url,omitempty"`
	Events       []string `json:"events,omitempty"`
	SecretPrefix string   `json:"secret_prefix,omitempty"`
	Enabled      bool     `json:"enabled"`
	CreatedAt    string   `json:"created_at,omitempty"`
}

// WebhookDelivery is one delivery attempt returned by
// GET /v1/projects/{ref}/webhooks/{id}/deliveries.
// JSON shape: { id, status_code, delivered_at, event, url }
type WebhookDelivery struct {
	ID          string `json:"id,omitempty"`
	StatusCode  int    `json:"status_code,omitempty"`
	DeliveredAt string `json:"delivered_at,omitempty"`
	Event       string `json:"event,omitempty"`
	URL         string `json:"url,omitempty"`
}

// parseEventsCSV splits a comma-separated events string into a
// deduplicated, whitespace-trimmed slice. Empty tokens are discarded.
// Order is preserved (first occurrence wins on duplicates).
func parseEventsCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, dup := seen[p]; dup {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdWebhooks(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdWebhooksList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdWebhooksList(g, args[1:])
	case "create":
		return cmdWebhooksCreate(g, args[1:])
	case "patch":
		return cmdWebhooksPatch(g, args[1:])
	case "test":
		return cmdWebhooksTest(g, args[1:])
	case "redeliver":
		return cmdWebhooksRedeliver(g, args[1:])
	case "delete":
		return cmdWebhooksDelete(g, args[1:])
	case "deliveries":
		return cmdWebhooksDeliveries(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("webhooks", "Manage webhook subscriptions for a project.", []string{
			"list                         [--project=<ref>]                     List webhooks.",
			"create --url=<u> --events=<csv> [--project=<ref>]                 Create a webhook (secret shown ONCE).",
			"patch  <id> [--url=<u>] [--events=<csv>] [--enabled=bool] [--project=<ref>]  Update a webhook.",
			"test   <id>                  [--project=<ref>]                     Send a test delivery.",
			"redeliver <id> --delivery=<id> [--project=<ref>]                  Redeliver a past delivery.",
			"delete <id>                  [--project=<ref>] [--yes]             Delete a webhook.",
			"deliveries <id> [--limit=N]  [--project=<ref>]                    List deliveries for a webhook.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for webhooks", args[0])
		return errSilent
	}
}

// ── webhooks list ─────────────────────────────────────────────────────

func cmdWebhooksList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks list", flag.ContinueOnError)
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
		Webhooks []*Webhook `json:"webhooks"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/webhooks", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { webhooks: [ Webhook ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Webhooks) == 0 {
		fmt.Fprintln(os.Stdout, "(no webhooks)")
		return nil
	}
	t := newTable(g, "ID", "URL", "EVENTS", "ENABLED", "CREATED")
	for _, wh := range resp.Webhooks {
		enabled := "false"
		if wh.Enabled {
			enabled = "true"
		}
		t.row(wh.ID, wh.URL, strings.Join(wh.Events, ","), enabled, wh.CreatedAt)
	}
	return t.flush()
}

// ── webhooks create ───────────────────────────────────────────────────

func cmdWebhooksCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	rawURL := fs.String("url", "", "Destination URL (required).")
	rawEvents := fs.String("events", "", "Comma-separated list of event types (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *rawURL == "" {
		return fmt.Errorf("webhooks create requires --url=<url>")
	}
	events := parseEventsCSV(*rawEvents)
	if len(events) == 0 {
		return fmt.Errorf("webhooks create requires --events=<csv> with at least one event type")
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
		"url":    *rawURL,
		"events": events,
	}
	var resp struct {
		Webhook *Webhook `json:"webhook,omitempty"`
		Secret  string   `json:"secret,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/webhooks", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { webhook: Webhook, secret: string }
		// secret is reveal-once; subsequent list calls only show secret_prefix.
		return printJSON(os.Stdout, resp)
	}
	if resp.Webhook != nil {
		fmt.Fprintf(os.Stdout, "Created webhook %s → %s\n", resp.Webhook.ID, resp.Webhook.URL)
	}
	if resp.Secret != "" {
		if !g.quiet {
			fmt.Fprintln(os.Stderr, "Store this signing secret now — it will NOT be shown again.")
		}
		fmt.Fprintf(os.Stdout, "secret: %s\n", resp.Secret)
	}
	return nil
}

// ── webhooks patch ────────────────────────────────────────────────────

func cmdWebhooksPatch(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks patch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	rawURL := fs.String("url", "", "New destination URL.")
	rawEvents := fs.String("events", "", "Replacement event list (comma-separated).")
	enabled := fs.String("enabled", "", "Enable or disable: true|false.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin webhooks patch <id> [--url=<u>] [--events=<csv>] [--enabled=true|false] [--project=<ref>]")
	}
	id := rest[0]

	// Build a partial body from whichever flags were set.
	body := make(map[string]any)
	if *rawURL != "" {
		body["url"] = *rawURL
	}
	if *rawEvents != "" {
		events := parseEventsCSV(*rawEvents)
		if len(events) == 0 {
			return fmt.Errorf("webhooks patch --events must contain at least one event type")
		}
		body["events"] = events
	}
	if *enabled != "" {
		switch strings.ToLower(*enabled) {
		case "true", "1", "yes":
			body["enabled"] = true
		case "false", "0", "no":
			body["enabled"] = false
		default:
			return fmt.Errorf("webhooks patch --enabled must be true or false, got %q", *enabled)
		}
	}
	if len(body) == 0 {
		return fmt.Errorf("webhooks patch: at least one of --url, --events, --enabled must be provided")
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
		Webhook *Webhook `json:"webhook,omitempty"`
	}
	if err := c.do(ctx, "PATCH", "/v1/projects/"+ref+"/webhooks/"+id, body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("webhook %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { webhook: Webhook }
		return printJSON(os.Stdout, resp)
	}
	if resp.Webhook != nil {
		enabled := "false"
		if resp.Webhook.Enabled {
			enabled = "true"
		}
		fmt.Fprintf(os.Stdout, "Updated webhook %s (url=%s, enabled=%s)\n",
			resp.Webhook.ID, resp.Webhook.URL, enabled)
	} else {
		fmt.Fprintf(os.Stdout, "Updated webhook %s.\n", id)
	}
	return nil
}

// ── webhooks test ─────────────────────────────────────────────────────

func cmdWebhooksTest(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks test", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin webhooks test <id> [--project=<ref>]")
	}
	id := rest[0]

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
		DeliveryID  string `json:"delivery_id,omitempty"`
		StatusCode  int    `json:"status_code,omitempty"`
		DeliveredAt string `json:"delivered_at,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/webhooks/"+id+"/test", nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("webhook %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { delivery_id: string, status_code: int, delivered_at: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Test delivery sent (delivery_id=%s, status=%d, at=%s)\n",
		resp.DeliveryID, resp.StatusCode, resp.DeliveredAt)
	return nil
}

// ── webhooks redeliver ────────────────────────────────────────────────

func cmdWebhooksRedeliver(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks redeliver", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	deliveryID := fs.String("delivery", "", "Delivery ID to redeliver (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin webhooks redeliver <id> --delivery=<delivery-id> [--project=<ref>]")
	}
	id := rest[0]
	if *deliveryID == "" {
		return fmt.Errorf("webhooks redeliver requires --delivery=<delivery-id>")
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
		DeliveryID string `json:"delivery_id,omitempty"`
		Status     string `json:"status,omitempty"`
	}
	path := "/v1/projects/" + ref + "/webhooks/" + id + "/redeliver/" + *deliveryID
	if err := c.do(ctx, "POST", path, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("webhook %q or delivery %q not found on project %q", id, *deliveryID, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { delivery_id: string, status: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Redelivered delivery %s (status=%s)\n", resp.DeliveryID, resp.Status)
	return nil
}

// ── webhooks delete ───────────────────────────────────────────────────

func cmdWebhooksDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin webhooks delete <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]

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
			return fmt.Errorf("confirmation required: pass --yes to delete webhook %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Delete webhook %q on project %q? This cannot be undone. [y/N] ", id, ref)
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
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/webhooks/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("webhook %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { deleted: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted webhook %s.\n", id)
	return nil
}

// ── webhooks deliveries ───────────────────────────────────────────────

func cmdWebhooksDeliveries(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("webhooks deliveries", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	limit := fs.Int("limit", 50, "Maximum number of deliveries to return.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin webhooks deliveries <id> [--project=<ref>] [--limit=N]")
	}
	id := rest[0]

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

	q := queryString(map[string]string{"limit": fmt.Sprintf("%d", *limit)})
	var resp struct {
		Deliveries []*WebhookDelivery `json:"deliveries"`
	}
	path := "/v1/projects/" + ref + "/webhooks/" + id + "/deliveries" + q
	if err := c.do(ctx, "GET", path, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("webhook %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { deliveries: [ WebhookDelivery ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Deliveries) == 0 {
		fmt.Fprintln(os.Stdout, "(no deliveries)")
		return nil
	}
	t := newTable(g, "ID", "EVENT", "STATUS", "DELIVERED_AT")
	for _, d := range resp.Deliveries {
		status := fmt.Sprintf("%d", d.StatusCode)
		if d.StatusCode == 0 {
			status = "—"
		}
		t.row(d.ID, d.Event, status, d.DeliveredAt)
	}
	return t.flush()
}
