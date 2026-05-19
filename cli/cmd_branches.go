// cmd_branches — branch (preview environment) lifecycle.
//
//	basin branches list   [--project=<ref>]
//	basin branches create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]
//	basin branches get    <branch_ref> [--project=<ref>]
//	basin branches merge  <branch_ref> [--project=<ref>] [--yes]
//	basin branches delete <branch_ref> [--project=<ref>] [--yes]
//	basin branches events [--project=<ref>] [--follow]
//
// Branches are backed by the cloud's `/v1/projects/{ref}/branches/*`
// surface (migration 0011). Each branch is itself a Project row with
// parent_project_id set. Merge + delete are destructive: require
// confirmation unless --yes is passed.
//
// Project ref resolution order:
//  1. --project=<ref> flag
//  2. loadWorkingProject(cwd) — ./basin/config.toml
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"
)

// ── Wire shapes ──────────────────────────────────────────────────────

// Branch is the read shape from GET /v1/projects/{ref}/branches (list)
// and the embedded "branch" field in create/merge/delete responses.
// It mirrors the models.Project row that the cloud serialises for a
// branch project — the cloud returns the full Project JSON, not a
// reduced type.
type Branch struct {
	ID              string  `json:"id,omitempty"`
	OrgID           string  `json:"org_id,omitempty"`
	Slug            string  `json:"slug,omitempty"`
	Ref             string  `json:"ref,omitempty"`
	Name            string  `json:"name,omitempty"`
	Region          string  `json:"region,omitempty"`
	Status          string  `json:"status,omitempty"`
	ParentProjectID *string `json:"parent_project_id,omitempty"`
	BranchName      string  `json:"branch_name,omitempty"`
	BranchKind      string  `json:"branch_kind,omitempty"`
	MergedAt        *string `json:"merged_at,omitempty"`
	RetiredAt       *string `json:"retired_at,omitempty"`
	CreatedAt       string  `json:"created_at,omitempty"`
}

// BranchEvent mirrors handlers.branchEventResponse — the on-wire shape
// returned by GET /v1/projects/{ref}/branches/events and embedded in
// create/merge/delete response envelopes.
type BranchEvent struct {
	ID        string  `json:"id,omitempty"`
	ProjectID string  `json:"project_id,omitempty"`
	ParentID  *string `json:"parent_id,omitempty"`
	Kind      string  `json:"kind,omitempty"`
	Message   *string `json:"message,omitempty"`
	ActorID   *string `json:"actor_id,omitempty"`
	CreatedAt string  `json:"created_at,omitempty"`
}

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdBranches(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdBranchesList(g, nil)
	}
	switch args[0] {
	case "list":
		return cmdBranchesList(g, args[1:])
	case "create":
		return cmdBranchesCreate(g, args[1:])
	case "get":
		return cmdBranchesGet(g, args[1:])
	case "merge":
		return cmdBranchesMerge(g, args[1:])
	case "delete":
		return cmdBranchesDelete(g, args[1:])
	case "events":
		return cmdBranchesEvents(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("branches", "List / create / get / merge / delete branch preview environments.", []string{
			"list   [--project=<ref>]                               List branches for a project.",
			"create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]  Create a branch.",
			"get    <branch_ref> [--project=<ref>]                  Show one branch.",
			"merge  <branch_ref> [--project=<ref>] [--yes]          Merge a branch into its parent.",
			"delete <branch_ref> [--project=<ref>] [--yes]          Delete (retire) a branch.",
			"events [--project=<ref>] [--follow]                    List branch events; --follow polls every 5s.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for branches", args[0])
		return errSilent
	}
}

// resolveProjectRef returns the project ref from the --project flag or
// the working-project config file. Returns an error when neither is set.
func resolveProjectRef(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("could not determine cwd: %w", err)
	}
	wp, err := loadWorkingProject(cwd)
	if err != nil {
		return "", err
	}
	if wp == nil || wp.ProjectRef == "" {
		return "", fmt.Errorf("--project is required (or run `basin link` to bind this directory)")
	}
	return wp.ProjectRef, nil
}

// ── branches list ────────────────────────────────────────────────────

func cmdBranchesList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches list", flag.ContinueOnError)
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
		Branches []*Branch `json:"branches"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/branches", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { branches: [ Branch ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Branches) == 0 {
		fmt.Fprintln(os.Stdout, "(no branches)")
		return nil
	}
	t := newTable(g, "REF", "NAME", "KIND", "STATUS", "MERGED", "CREATED")
	for _, b := range resp.Branches {
		merged := "—"
		if b.MergedAt != nil && *b.MergedAt != "" {
			merged = *b.MergedAt
		}
		t.row(b.Ref, b.BranchName, b.BranchKind, b.Status, merged, b.CreatedAt)
	}
	return t.flush()
}

// ── branches create ──────────────────────────────────────────────────

func cmdBranchesCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	fromRef := fs.String("from", "", "Branch ref to create from (informational; engine cloning is v0.2).")
	kind := fs.String("kind", "branch", "Branch kind: 'branch' or 'preview'.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin branches create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]")
	}
	name := strings.Join(rest, " ")

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60_000_000_000) // 60s — engine provisioning
	defer cancel()

	// POST body matches CreateProjectBranchRequest on the cloud:
	// { "branch_name": string, "kind": string }
	// --from is stored as a comment in the event message; the engine
	// does not yet support data-copy from a sibling branch (v0.2 work).
	body := map[string]any{
		"branch_name": name,
		"kind":        *kind,
	}
	if *fromRef != "" {
		// Surface the intent in the request even though the cloud ignores
		// it today, so a future cloud upgrade can act on it without a CLI
		// change.
		body["from_ref"] = *fromRef
	}

	var resp struct {
		Branch       *Branch      `json:"branch"`
		CreatedEvent *BranchEvent `json:"created_event"`
		EngineOK     bool         `json:"engine_ok"`
		EngineMsg    string       `json:"engine_msg,omitempty"`
		Pgwire       *Pgwire      `json:"pgwire,omitempty"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/branches", body, &resp); err != nil {
		// Map 409 conflict to a user-friendly message.
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 409 {
			return fmt.Errorf("branch name conflict: a branch named %q already exists for this project", name)
		}
		return err
	}
	if g.json {
		// JSON shape: { branch: Branch, created_event: BranchEvent, engine_ok: bool, engine_msg?: string, pgwire?: Pgwire }
		return printJSON(os.Stdout, resp)
	}
	if resp.Branch != nil {
		fmt.Fprintf(os.Stdout, "Created branch %q (%s) — kind=%s\n",
			resp.Branch.BranchName, resp.Branch.Ref, resp.Branch.BranchKind)
	}
	if resp.Pgwire != nil && resp.Pgwire.ConnectionURL != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "Connection URL (shown ONCE — store it now):")
		fmt.Fprintln(os.Stdout, "  "+resp.Pgwire.ConnectionURL)
	}
	if resp.EngineMsg != "" {
		fmt.Fprintf(os.Stdout, "engine: %s\n", resp.EngineMsg)
	}
	return nil
}

// ── branches get ─────────────────────────────────────────────────────

// branches get resolves the branch via GET /v1/projects/{branch_ref}
// (a branch is just a project with parent_project_id set). The cloud
// does not have a dedicated GET /v1/projects/{ref}/branches/{branch_ref}
// endpoint; looking up by ref via the main projects route is equivalent.
func cmdBranchesGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Parent project ref (used for display context only).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin branches get <branch_ref> [--project=<ref>]")
	}
	branchRef := rest[0]
	_ = *project // accepted for consistency; not needed for the GET

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	// The cloud exposes branch metadata via the same /v1/projects/{ref}
	// endpoint used for parent projects — a branch IS a project row.
	var resp struct {
		Project *Branch `json:"project"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+branchRef, nil, &resp); err != nil {
		return err
	}
	b := resp.Project
	if b == nil {
		return fmt.Errorf("branch not found: %s", branchRef)
	}
	if g.json {
		// JSON shape: { project: Branch }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "ref:         %s\n", b.Ref)
	fmt.Fprintf(os.Stdout, "branch_name: %s\n", b.BranchName)
	fmt.Fprintf(os.Stdout, "kind:        %s\n", b.BranchKind)
	fmt.Fprintf(os.Stdout, "status:      %s\n", b.Status)
	fmt.Fprintf(os.Stdout, "region:      %s\n", b.Region)
	if b.ParentProjectID != nil {
		fmt.Fprintf(os.Stdout, "parent_id:   %s\n", *b.ParentProjectID)
	}
	if b.MergedAt != nil && *b.MergedAt != "" {
		fmt.Fprintf(os.Stdout, "merged_at:   %s\n", *b.MergedAt)
	}
	fmt.Fprintf(os.Stdout, "created_at:  %s\n", b.CreatedAt)
	return nil
}

// ── branches merge ───────────────────────────────────────────────────

func cmdBranchesMerge(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches merge", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Parent project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin branches merge <branch_ref> [--project=<ref>] [--yes]")
	}
	branchRef := rest[0]

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
			return fmt.Errorf("confirmation required: pass --yes to merge branch %q non-interactively under --json", branchRef)
		}
		fmt.Fprintf(os.Stderr, "Merge branch %q into project %q? This marks the branch as merged. [y/N] ", branchRef, ref)
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
		Branch *Branch      `json:"branch"`
		Event  *BranchEvent `json:"event"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/branches/"+branchRef+"/merge", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { branch: Branch, event: BranchEvent }
		return printJSON(os.Stdout, resp)
	}
	if resp.Branch != nil {
		fmt.Fprintf(os.Stdout, "Merged branch %q (%s).\n", resp.Branch.BranchName, resp.Branch.Ref)
	} else {
		fmt.Fprintf(os.Stdout, "Merged branch %s.\n", branchRef)
	}
	return nil
}

// ── branches delete ──────────────────────────────────────────────────

func cmdBranchesDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Parent project ref.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin branches delete <branch_ref> [--project=<ref>] [--yes]")
	}
	branchRef := rest[0]

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
			return fmt.Errorf("confirmation required: pass --yes to delete branch %q non-interactively under --json", branchRef)
		}
		fmt.Fprintf(os.Stderr, "Delete (retire) branch %q from project %q? This cannot be undone. [y/N] ", branchRef, ref)
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
		Retired bool         `json:"retired"`
		Event   *BranchEvent `json:"event"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/branches/"+branchRef, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { retired: bool, event: BranchEvent }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted branch %s.\n", branchRef)
	return nil
}

// ── branches events ──────────────────────────────────────────────────

func cmdBranchesEvents(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("branches events", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref.")
	follow := fs.Bool("follow", false, "Poll every 5s for new events.")
	limit := fs.Int("limit", 50, "Events to fetch per page.")
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

	if *follow {
		return followBranchEvents(c, ref, *limit, g)
	}
	return fetchBranchEventsOnce(c, ref, *limit, g)
}

func fetchBranchEventsOnce(c *Client, ref string, limit int, g *globalFlags) error {
	q := queryString(map[string]string{"limit": fmt.Sprintf("%d", limit)})
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Events []*BranchEvent `json:"events"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/branches/events"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { events: [ BranchEvent ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Events) == 0 {
		fmt.Fprintln(os.Stdout, "(no events)")
		return nil
	}
	t := newTable(g, "ID", "KIND", "PROJECT", "CREATED", "MESSAGE")
	for _, ev := range resp.Events {
		msg := ""
		if ev.Message != nil {
			msg = *ev.Message
		}
		t.row(ev.ID, ev.Kind, ev.ProjectID, ev.CreatedAt, msg)
	}
	return t.flush()
}

// followBranchEvents polls GET /v1/projects/{ref}/branches/events every
// 5s and prints new events (de-duplicated by ID). Ctrl-C stops the loop
// cleanly via signal.NotifyContext.
func followBranchEvents(c *Client, ref string, limit int, g *globalFlags) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	seen := make(map[string]struct{})
	printInfo(g, "Following branch events for project %s — Ctrl-C to stop.", ref)

	tk := time.NewTicker(5 * time.Second)
	defer tk.Stop()

	// Fetch immediately, then poll.
	if err := fetchAndPrintNewEvents(ctx, c, ref, limit, g, seen); err != nil && ctx.Err() == nil {
		printErr(g, "events poll failed: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tk.C:
			if err := fetchAndPrintNewEvents(ctx, c, ref, limit, g, seen); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// Non-fatal — log and keep polling.
				printErr(g, "events poll failed: %v", err)
			}
		}
	}
}

// fetchAndPrintNewEvents fetches one page of events and prints only
// those whose IDs haven't been seen before. Updates seen in-place.
func fetchAndPrintNewEvents(ctx context.Context, c *Client, ref string, limit int, g *globalFlags, seen map[string]struct{}) error {
	q := queryString(map[string]string{"limit": fmt.Sprintf("%d", limit)})
	reqCtx, cancel := context.WithTimeout(ctx, c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Events []*BranchEvent `json:"events"`
	}
	if err := c.do(reqCtx, "GET", "/v1/projects/"+ref+"/branches/events"+q, nil, &resp); err != nil {
		return err
	}
	for _, ev := range resp.Events {
		if _, ok := seen[ev.ID]; ok {
			continue
		}
		seen[ev.ID] = struct{}{}
		msg := ""
		if ev.Message != nil {
			msg = *ev.Message
		}
		if g.json {
			_ = printJSON(os.Stdout, ev)
		} else {
			fmt.Fprintf(os.Stdout, "%s  %-20s  %s  %s\n", ev.CreatedAt, ev.Kind, ev.ProjectID, msg)
		}
	}
	return nil
}
