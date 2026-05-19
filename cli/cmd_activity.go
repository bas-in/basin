// cmd_activity — project-scoped activity feed.
//
//	basin activity list [--project=<ref>] [--limit=N] [--since=<rfc3339>]
//
// Backed by GET /v1/projects/{ref}/activity (migration 0011).
// Project ref resolution: --project= flag, else loadWorkingProject(cwd).
// Error with hint at `basin link` if neither is set.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// ActivityEvent is the read shape for a single project activity event.
// JSON shape: { id, kind, actor_id, occurred_at, summary }
type ActivityEvent struct {
	ID         string `json:"id,omitempty"`
	Kind       string `json:"kind,omitempty"`
	ActorID    string `json:"actor_id,omitempty"`
	OccurredAt string `json:"occurred_at,omitempty"`
	Summary    string `json:"summary,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdActivity(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdActivityList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdActivityList(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("activity", "List project activity events.", []string{
			"list [--project=<ref>] [--limit=N] [--since=<rfc3339>]   List recent project activity.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for activity", args[0])
		return errSilent
	}
}

// ── activity list ─────────────────────────────────────────────────────

func cmdActivityList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("activity list", flag.ContinueOnError)
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
		Events []*ActivityEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/activity"+q, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 403 {
			return fmt.Errorf("access denied: insufficient permissions to view activity for project %q", ref)
		}
		return err
	}
	if g.json {
		// JSON shape: { events: [ ActivityEvent ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no activity)")
		return nil
	}
	t := newTable(g, "ID", "KIND", "ACTOR", "OCCURRED_AT", "SUMMARY")
	for _, ev := range resp.Events {
		t.row(ev.ID, ev.Kind, ev.ActorID, ev.OccurredAt, ev.Summary)
	}
	return t.flush()
}
