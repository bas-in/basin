// cmd_audit — org-scoped audit log.
//
//	basin audit list --org=<slug> [--since=<rfc3339>] [--until=<rfc3339>] [--limit=N] [--export=<dest>]
//
// Backed by GET /v1/orgs/{slug}/audit.
// --org is required: audit is org-scoped, not project-scoped.
// --export is accepted but deferred: the cloud's /audit-export/destinations
// surface is a larger sink-management area; the CLI stub prints an informational
// message and exits 0 without erroring.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// AuditEvent is the read shape for a single org audit event.
// JSON shape: { id, actor_id, action, resource_type, resource_id, occurred_at }
type AuditEvent struct {
	ID           string `json:"id,omitempty"`
	ActorID      string `json:"actor_id,omitempty"`
	Action       string `json:"action,omitempty"`
	ResourceType string `json:"resource_type,omitempty"`
	ResourceID   string `json:"resource_id,omitempty"`
	OccurredAt   string `json:"occurred_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdAudit(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdAuditList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdAuditList(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("audit", "List org-scoped audit log events.", []string{
			"list --org=<slug> [--since=<rfc3339>] [--until=<rfc3339>] [--limit=N] [--export=<dest>]",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for audit", args[0])
		return errSilent
	}
}

// ── audit list ────────────────────────────────────────────────────────

func cmdAuditList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("audit list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	org := fs.String("org", "", "Org slug (required).")
	since := fs.String("since", "", "Return events at or after this RFC3339 timestamp.")
	until := fs.String("until", "", "Return events at or before this RFC3339 timestamp.")
	limit := fs.Int("limit", 50, "Maximum number of events to return.")
	export := fs.String("export", "", "Export destination (deferred — not yet wired to the cloud).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}

	// --org is required; there is no working-project fallback for audit.
	if *org == "" {
		// fall through to globalFlags.orgSlug set by --org= at the top level
		if g.orgSlug != "" {
			*org = g.orgSlug
		}
	}
	if *org == "" {
		return fmt.Errorf("audit list requires --org=<slug> (audit is org-scoped; no project fallback)")
	}

	// --export is accepted but the cloud's sink-management surface is larger
	// than a single list command; defer it without erroring.
	if *export != "" {
		fmt.Fprintln(os.Stdout, "audit export deferred")
		return nil
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	q := queryString(map[string]string{
		"since": *since,
		"until": *until,
		"limit": fmt.Sprintf("%d", *limit),
	})
	var resp struct {
		Events []*AuditEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/orgs/"+*org+"/audit"+q, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 403 {
			return fmt.Errorf("access denied: you must be an org member to view the audit log for %q", *org)
		}
		return err
	}
	if g.json {
		// JSON shape: { events: [ AuditEvent ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no audit events)")
		return nil
	}
	t := newTable(g, "ID", "ACTOR", "ACTION", "RESOURCE_TYPE", "RESOURCE_ID", "OCCURRED_AT")
	for _, ev := range resp.Events {
		t.row(ev.ID, ev.ActorID, ev.Action, ev.ResourceType, ev.ResourceID, ev.OccurredAt)
	}
	return t.flush()
}
