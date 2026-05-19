// cmd_alerts — project-scoped alert rules and events.
//
//	basin alerts rules list        [--project=<ref>]
//	basin alerts rules create      --name=<n> --metric=<m> --threshold=<n> --comparator=<op> [--enabled=true] [--project=<ref>]
//	basin alerts rules get         <id> [--project=<ref>]
//	basin alerts rules patch       <id> [--name=<n>] [--threshold=<n>] [--enabled=true|false] [--project=<ref>]
//	basin alerts rules delete      <id> [--project=<ref>] [--yes]
//	basin alerts rules silence     <id> --until=<rfc3339> [--project=<ref>]
//	basin alerts rules unsilence   <id> [--project=<ref>]
//	basin alerts events list       [--project=<ref>] [--limit=N] [--since=<rfc3339>]
//
// Backed by /v1/projects/{ref}/alerts/* (migration 0028).
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// AlertRule is the read shape for a single alert rule.
// JSON shape: { id, name, metric, threshold, comparator, enabled, silenced_until }
type AlertRule struct {
	ID            string  `json:"id,omitempty"`
	Name          string  `json:"name,omitempty"`
	Metric        string  `json:"metric,omitempty"`
	Threshold     float64 `json:"threshold"`
	Comparator    string  `json:"comparator,omitempty"`
	Enabled       bool    `json:"enabled"`
	SilencedUntil *string `json:"silenced_until,omitempty"`
}

// AlertEvent is the read shape for a fired/resolved alert event.
// JSON shape: { id, rule_id, fired_at, resolved_at, value }
type AlertEvent struct {
	ID         string  `json:"id,omitempty"`
	RuleID     string  `json:"rule_id,omitempty"`
	FiredAt    string  `json:"fired_at,omitempty"`
	ResolvedAt *string `json:"resolved_at,omitempty"`
	Value      float64 `json:"value"`
}

// validComparators is the allowed set for --comparator.
var validComparators = map[string]bool{
	"gt": true, "lt": true, "gte": true, "lte": true, "eq": true,
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdAlerts(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("alerts", "Manage project alert rules and events.", []string{
			"rules list   [--project=<ref>]                                         List alert rules.",
			"rules create --name=<n> --metric=<m> --threshold=<n> --comparator=<op> Create an alert rule.",
			"rules get    <id> [--project=<ref>]                                    Get an alert rule.",
			"rules patch  <id> [...] [--project=<ref>]                              Update an alert rule.",
			"rules delete <id> [--project=<ref>] [--yes]                            Delete an alert rule.",
			"rules silence   <id> --until=<rfc3339> [--project=<ref>]               Silence a rule until a time.",
			"rules unsilence <id> [--project=<ref>]                                 Remove a silence.",
			"events list  [--project=<ref>] [--limit=N] [--since=<rfc3339>]         List fired alert events.",
		})
		return nil
	}
	switch args[0] {
	case "rules":
		return cmdAlertsRules(g, args[1:])
	case "events":
		return cmdAlertsEvents(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("alerts", "Manage project alert rules and events.", []string{
			"rules list   [--project=<ref>]                                         List alert rules.",
			"rules create --name=<n> --metric=<m> --threshold=<n> --comparator=<op> Create an alert rule.",
			"rules get    <id> [--project=<ref>]                                    Get an alert rule.",
			"rules patch  <id> [...] [--project=<ref>]                              Update an alert rule.",
			"rules delete <id> [--project=<ref>] [--yes]                            Delete an alert rule.",
			"rules silence   <id> --until=<rfc3339> [--project=<ref>]               Silence a rule until a time.",
			"rules unsilence <id> [--project=<ref>]                                 Remove a silence.",
			"events list  [--project=<ref>] [--limit=N] [--since=<rfc3339>]         List fired alert events.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for alerts", args[0])
		return errSilent
	}
}

// cmdAlertsRules dispatches alerts rules sub-subcommands.
func cmdAlertsRules(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdAlertsRulesList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdAlertsRulesList(g, args[1:])
	case "create":
		return cmdAlertsRulesCreate(g, args[1:])
	case "get":
		return cmdAlertsRulesGet(g, args[1:])
	case "patch":
		return cmdAlertsRulesPatch(g, args[1:])
	case "delete":
		return cmdAlertsRulesDelete(g, args[1:])
	case "silence":
		return cmdAlertsRulesSilence(g, args[1:])
	case "unsilence":
		return cmdAlertsRulesUnsilence(g, args[1:])
	default:
		printErr(g, "unknown subcommand %q for alerts rules", args[0])
		return errSilent
	}
}

// cmdAlertsEvents dispatches alerts events sub-subcommands.
func cmdAlertsEvents(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdAlertsEventsList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdAlertsEventsList(g, args[1:])
	default:
		printErr(g, "unknown subcommand %q for alerts events", args[0])
		return errSilent
	}
}

// ── alerts rules list ─────────────────────────────────────────────────

func cmdAlertsRulesList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules list", flag.ContinueOnError)
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
		Rules []*AlertRule `json:"rules"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/alerts/rules", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { rules: [ AlertRule ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Rules) == 0 {
		fmt.Fprintln(os.Stdout, "(no alert rules)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "METRIC", "COMPARATOR", "THRESHOLD", "ENABLED", "SILENCED_UNTIL")
	for _, r := range resp.Rules {
		silenced := "—"
		if r.SilencedUntil != nil && *r.SilencedUntil != "" {
			silenced = *r.SilencedUntil
		}
		enabled := "false"
		if r.Enabled {
			enabled = "true"
		}
		t.row(r.ID, r.Name, r.Metric, r.Comparator, fmt.Sprintf("%g", r.Threshold), enabled, silenced)
	}
	return t.flush()
}

// ── alerts rules create ───────────────────────────────────────────────

func cmdAlertsRulesCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	name := fs.String("name", "", "Rule name (required).")
	metric := fs.String("metric", "", "Metric key (required).")
	threshold := fs.Float64("threshold", 0, "Numeric threshold (required).")
	comparator := fs.String("comparator", "", "Comparator: gt|lt|gte|lte|eq (required).")
	enabled := fs.Bool("enabled", true, "Enable the rule immediately.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *name == "" {
		return fmt.Errorf("alerts rules create requires --name=<name>")
	}
	if *metric == "" {
		return fmt.Errorf("alerts rules create requires --metric=<metric>")
	}
	if *comparator == "" {
		return fmt.Errorf("alerts rules create requires --comparator=<gt|lt|gte|lte|eq>")
	}
	if !validComparators[strings.ToLower(*comparator)] {
		return fmt.Errorf("alerts rules create: invalid --comparator %q; must be one of gt, lt, gte, lte, eq", *comparator)
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
		"name":       *name,
		"metric":     *metric,
		"threshold":  *threshold,
		"comparator": strings.ToLower(*comparator),
		"enabled":    *enabled,
	}
	var resp struct {
		Rule *AlertRule `json:"rule"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/alerts/rules", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { rule: AlertRule }
		return printJSON(os.Stdout, resp)
	}
	if resp.Rule != nil {
		fmt.Fprintf(os.Stdout, "Created alert rule %s (%s %s %g)\n",
			resp.Rule.ID, resp.Rule.Metric, resp.Rule.Comparator, resp.Rule.Threshold)
	}
	return nil
}

// ── alerts rules get ──────────────────────────────────────────────────

func cmdAlertsRulesGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin alerts rules get <id> [--project=<ref>]")
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
		Rule *AlertRule `json:"rule"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/alerts/rules/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("alert rule %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { rule: AlertRule }
		return printJSON(os.Stdout, resp)
	}
	r := resp.Rule
	if r == nil {
		return fmt.Errorf("alert rule %q not found", id)
	}
	silenced := "—"
	if r.SilencedUntil != nil && *r.SilencedUntil != "" {
		silenced = *r.SilencedUntil
	}
	enabled := "false"
	if r.Enabled {
		enabled = "true"
	}
	fmt.Fprintf(os.Stdout, "id:             %s\n", r.ID)
	fmt.Fprintf(os.Stdout, "name:           %s\n", r.Name)
	fmt.Fprintf(os.Stdout, "metric:         %s\n", r.Metric)
	fmt.Fprintf(os.Stdout, "threshold:      %g\n", r.Threshold)
	fmt.Fprintf(os.Stdout, "comparator:     %s\n", r.Comparator)
	fmt.Fprintf(os.Stdout, "enabled:        %s\n", enabled)
	fmt.Fprintf(os.Stdout, "silenced_until: %s\n", silenced)
	return nil
}

// ── alerts rules patch ────────────────────────────────────────────────

func cmdAlertsRulesPatch(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules patch", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	name := fs.String("name", "", "New rule name.")
	threshold := fs.Float64("threshold", 0, "New threshold.")
	enabledStr := fs.String("enabled", "", "Enable or disable: true|false.")
	thresholdSet := false
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	// Detect whether --threshold was provided (can't distinguish 0 from unset otherwise).
	for _, a := range args {
		if strings.HasPrefix(a, "--threshold=") || a == "--threshold" {
			thresholdSet = true
		}
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin alerts rules patch <id> [--name=<n>] [--threshold=<n>] [--enabled=true|false] [--project=<ref>]")
	}
	id := rest[0]

	body := make(map[string]any)
	if *name != "" {
		body["name"] = *name
	}
	if thresholdSet {
		body["threshold"] = *threshold
	}
	if *enabledStr != "" {
		switch strings.ToLower(*enabledStr) {
		case "true", "1", "yes":
			body["enabled"] = true
		case "false", "0", "no":
			body["enabled"] = false
		default:
			return fmt.Errorf("alerts rules patch --enabled must be true or false, got %q", *enabledStr)
		}
	}
	if len(body) == 0 {
		return fmt.Errorf("alerts rules patch: at least one of --name, --threshold, --enabled must be provided")
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
		Rule *AlertRule `json:"rule"`
	}
	if err := c.do(ctx, "PATCH", "/v1/projects/"+ref+"/alerts/rules/"+id, body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("alert rule %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { rule: AlertRule }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Updated alert rule %s.\n", id)
	return nil
}

// ── alerts rules delete ───────────────────────────────────────────────

func cmdAlertsRulesDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin alerts rules delete <id> [--project=<ref>] [--yes]")
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
			return fmt.Errorf("confirmation required: pass --yes to delete alert rule %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Delete alert rule %q on project %q? This cannot be undone. [y/N] ", id, ref)
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
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/alerts/rules/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("alert rule %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { deleted: bool, id: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted alert rule %s.\n", id)
	return nil
}

// ── alerts rules silence ──────────────────────────────────────────────

func cmdAlertsRulesSilence(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules silence", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	until := fs.String("until", "", "Silence until this RFC3339 timestamp (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin alerts rules silence <id> --until=<rfc3339> [--project=<ref>]")
	}
	id := rest[0]
	if *until == "" {
		return fmt.Errorf("alerts rules silence requires --until=<rfc3339>")
	}
	// Validate RFC3339 format.
	if _, err := time.Parse(time.RFC3339, *until); err != nil {
		return fmt.Errorf("alerts rules silence --until: invalid RFC3339 timestamp %q: %w", *until, err)
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

	body := map[string]any{"until": *until}
	var resp struct {
		SilencedUntil string `json:"silenced_until,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/alerts/rules/"+id+"/silence", body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("alert rule %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { silenced_until: string }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Alert rule %s silenced until %s.\n", id, resp.SilencedUntil)
	return nil
}

// ── alerts rules unsilence ────────────────────────────────────────────

func cmdAlertsRulesUnsilence(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts rules unsilence", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin alerts rules unsilence <id> [--project=<ref>]")
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
		Silenced bool `json:"silenced"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/alerts/rules/"+id+"/unsilence", nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("alert rule %q not found on project %q", id, ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { silenced: false }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Alert rule %s unsilenced.\n", id)
	return nil
}

// ── alerts events list ────────────────────────────────────────────────

func cmdAlertsEventsList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("alerts events list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	limit := fs.Int("limit", 50, "Maximum number of events to return.")
	since := fs.String("since", "", "Return events fired at or after this RFC3339 timestamp.")
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
		Events []*AlertEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/alerts/events"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { events: [ AlertEvent ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no alert events)")
		return nil
	}
	t := newTable(g, "ID", "RULE_ID", "FIRED_AT", "RESOLVED_AT", "VALUE")
	for _, ev := range resp.Events {
		resolved := "—"
		if ev.ResolvedAt != nil && *ev.ResolvedAt != "" {
			resolved = *ev.ResolvedAt
		}
		t.row(ev.ID, ev.RuleID, ev.FiredAt, resolved, fmt.Sprintf("%g", ev.Value))
	}
	return t.flush()
}
