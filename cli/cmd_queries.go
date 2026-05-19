// cmd_queries — saved query lifecycle + query history.
//
//	basin queries save   --name=<n> --sql=<s> [--description=<d>] [--public=<bool>] [--project=<ref>]
//	basin queries list   [--project=<ref>]
//	basin queries get    <id>
//	basin queries fork   <id> [--name=<n>] [--project=<ref>]
//	basin queries delete <id> [--project=<ref>] [--yes]
//	basin queries history [--project=<ref>] [--limit=N] [--clear --yes]
//
// Cloud routes:
//   - POST   /v1/projects/{ref}/sql/saved              — save
//   - GET    /v1/projects/{ref}/sql/saved              — list
//   - GET    /v1/sql/saved/{id}                        — get (top-level, not project-scoped)
//   - DELETE /v1/sql/saved/{id}                        — delete (top-level)
//   - POST   /v1/sql/saved/{id}/fork                   — fork (top-level)
//   - GET    /v1/projects/{ref}/sql/history            — history list
//   - DELETE /v1/projects/{ref}/sql/history            — history clear
//
// Note: get/delete/fork are mounted at the top-level /v1/sql/saved/{id}
// route (not under /v1/projects/{ref}/...) because saved queries carry
// the project_id in the row and the cloud enforces ownership/visibility
// inside the handler rather than via route gating.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
)

// SavedQuery is the wire shape for a saved query.
type SavedQuery struct {
	ID          string `json:"id,omitempty"`
	ProjectID   string `json:"project_id,omitempty"`
	Name        string `json:"name,omitempty"`
	SQL         string `json:"sql,omitempty"`
	Description string `json:"description,omitempty"`
	Public      bool   `json:"public,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
	OwnerID     string `json:"owner_id,omitempty"`
}

// QueryHistoryEntry is one row from the query history.
type QueryHistoryEntry struct {
	ID         string  `json:"id,omitempty"`
	ProjectID  string  `json:"project_id,omitempty"`
	SQL        string  `json:"sql,omitempty"`
	DurationMs float64 `json:"duration_ms,omitempty"`
	ExecutedAt string  `json:"executed_at,omitempty"`
	UserID     string  `json:"user_id,omitempty"`
}

// cmdQueries is the top-level dispatcher for `basin queries`.
func cmdQueries(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return cmdQueriesList(g, nil)
	}
	switch args[0] {
	case "save":
		return cmdQueriesSave(g, args[1:])
	case "list":
		return cmdQueriesList(g, args[1:])
	case "get":
		return cmdQueriesGet(g, args[1:])
	case "fork":
		return cmdQueriesFork(g, args[1:])
	case "delete":
		return cmdQueriesDelete(g, args[1:])
	case "history":
		return cmdQueriesHistory(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("queries", "Manage saved SQL queries and query history.", []string{
			"save    --name=<n> --sql=<s> [--description=<d>] [--public=bool] [--project=<ref>]  Save a query.",
			"list    [--project=<ref>]                                                            List saved queries.",
			"get     <id>                                                                         Get a saved query by ID.",
			"fork    <id> [--name=<n>] [--project=<ref>]                                         Fork a saved query.",
			"delete  <id> [--project=<ref>] [--yes]                                              Delete a saved query.",
			"history [--project=<ref>] [--limit=N] [--clear --yes]                               List or clear history.",
		})
		return nil
	default:
		printErr(g, "unknown subcommand %q for queries", args[0])
		return errSilent
	}
}

// ── queries save ─────────────────────────────────────────────────────

func cmdQueriesSave(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries save", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	name := fs.String("name", "", "Name for the saved query (required).")
	sql := fs.String("sql", "", "SQL text to save (required).")
	description := fs.String("description", "", "Optional description.")
	public := fs.Bool("public", false, "Make the query visible to all org members.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	if *sql == "" {
		return fmt.Errorf("--sql is required")
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
		"name":   *name,
		"sql":    *sql,
		"public": *public,
	}
	if *description != "" {
		body["description"] = *description
	}

	var resp struct {
		Query *SavedQuery `json:"query"`
	}
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/sql/saved", body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "query": { id, name, sql, description, public, created_at } }
		return printJSON(os.Stdout, resp)
	}
	if resp.Query != nil {
		fmt.Fprintf(os.Stdout, "Saved query %q (id=%s)\n", resp.Query.Name, resp.Query.ID)
	} else {
		fmt.Fprintln(os.Stdout, "Query saved.")
	}
	return nil
}

// ── queries list ─────────────────────────────────────────────────────

func cmdQueriesList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
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
		Queries []*SavedQuery `json:"queries"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/sql/saved", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "queries": [ SavedQuery ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.Queries) == 0 {
		fmt.Fprintln(os.Stdout, "(no saved queries)")
		return nil
	}
	t := newTable(g, "ID", "NAME", "PUBLIC", "CREATED")
	for _, q := range resp.Queries {
		pub := "false"
		if q.Public {
			pub = "true"
		}
		t.row(q.ID, q.Name, pub, q.CreatedAt)
	}
	return t.flush()
}

// ── queries get ──────────────────────────────────────────────────────
//
// Route: GET /v1/sql/saved/{id}  (top-level — see cloud router anchor sql-saved-by-id)

func cmdQueriesGet(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries get", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	// --project is accepted for flag consistency but not needed: the cloud
	// resolves ownership/visibility from the query's own project_id.
	project := fs.String("project", "", "Project ref (accepted for consistency; not sent to the server).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	_ = *project // accepted, not required
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin queries get <id> [--project=<ref>]")
	}
	id := rest[0]

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		Query *SavedQuery `json:"query"`
	}
	// Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
	if err := c.do(ctx, "GET", "/v1/sql/saved/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("query %q not found", id)
		}
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 403 {
			return fmt.Errorf("access denied: query %q is private or owned by another user", id)
		}
		return err
	}
	q := resp.Query
	if q == nil {
		return fmt.Errorf("query %q not found", id)
	}
	if g.json {
		// JSON shape: { "query": { id, name, sql, description, public, created_at } }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "id:          %s\n", q.ID)
	fmt.Fprintf(os.Stdout, "name:        %s\n", q.Name)
	fmt.Fprintf(os.Stdout, "public:      %v\n", q.Public)
	if q.Description != "" {
		fmt.Fprintf(os.Stdout, "description: %s\n", q.Description)
	}
	fmt.Fprintf(os.Stdout, "created_at:  %s\n", q.CreatedAt)
	if q.SQL != "" {
		fmt.Fprintf(os.Stdout, "\n-- SQL:\n%s\n", q.SQL)
	}
	return nil
}

// ── queries fork ─────────────────────────────────────────────────────
//
// Route: POST /v1/sql/saved/{id}/fork  (top-level)

func cmdQueriesFork(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries fork", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref for the forked query (optional).")
	name := fs.String("name", "", "Name for the forked query (optional; defaults to 'Fork of <original>').")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	_ = *project // passed through for awareness; not sent to the cloud today
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin queries fork <id> [--name=<n>] [--project=<ref>]")
	}
	id := rest[0]

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	body := map[string]any{}
	if *name != "" {
		body["name"] = *name
	}

	var resp struct {
		Query *SavedQuery `json:"query"`
	}
	// Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
	if err := c.do(ctx, "POST", "/v1/sql/saved/"+id+"/fork", body, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("query %q not found", id)
		}
		return err
	}
	if g.json {
		// JSON shape: { "query": { id, name, sql, description, public, created_at } }
		return printJSON(os.Stdout, resp)
	}
	if resp.Query != nil {
		fmt.Fprintf(os.Stdout, "Forked query %q → new id=%s name=%q\n", id, resp.Query.ID, resp.Query.Name)
	} else {
		fmt.Fprintf(os.Stdout, "Forked query %s.\n", id)
	}
	return nil
}

// ── queries delete ───────────────────────────────────────────────────
//
// Route: DELETE /v1/sql/saved/{id}  (top-level)

func cmdQueriesDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (accepted for consistency; not sent to the server).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	_ = *project
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin queries delete <id> [--project=<ref>] [--yes]")
	}
	id := rest[0]

	c, err := requireClient(g)
	if err != nil {
		return err
	}

	if !*yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to delete query %q non-interactively under --json", id)
		}
		fmt.Fprintf(os.Stderr, "Delete saved query %q? This cannot be undone. [y/N] ", id)
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
		ID      string `json:"id"`
	}
	// Top-level route (not project-scoped) — see cloud router anchor sql-saved-by-id.
	if err := c.do(ctx, "DELETE", "/v1/sql/saved/"+id, nil, &resp); err != nil {
		if ae, ok := AsAPIError(err); ok && ae.HTTPStatus == 404 {
			return fmt.Errorf("query %q not found", id)
		}
		return err
	}
	if g.json {
		// JSON shape: { "deleted": true, "id": "<query id>" }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted query %s.\n", id)
	return nil
}

// ── queries history ──────────────────────────────────────────────────

func cmdQueriesHistory(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("queries history", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	limit := fs.Int("limit", 50, "Maximum number of history entries to return.")
	clear := fs.Bool("clear", false, "Clear (delete) the query history.")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt for --clear.")
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

	if *clear {
		return cmdQueriesHistoryClear(g, c, ref, *yes)
	}
	return cmdQueriesHistoryList(g, c, ref, *limit)
}

func cmdQueriesHistoryList(g *globalFlags, c *Client, ref string, limit int) error {
	qs := queryString(map[string]string{"limit": fmt.Sprintf("%d", limit)})
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp struct {
		History []*QueryHistoryEntry `json:"history"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+ref+"/sql/history"+qs, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "history": [ QueryHistoryEntry ] }
		return printJSON(os.Stdout, resp)
	}
	if len(resp.History) == 0 {
		fmt.Fprintln(os.Stdout, "(no query history)")
		return nil
	}
	t := newTable(g, "ID", "SQL", "DURATION_MS", "EXECUTED_AT")
	for _, h := range resp.History {
		sqlSnippet := h.SQL
		if len(sqlSnippet) > 60 {
			sqlSnippet = sqlSnippet[:57] + "..."
		}
		t.row(h.ID, sqlSnippet, fmt.Sprintf("%.0f", h.DurationMs), h.ExecutedAt)
	}
	return t.flush()
}

func cmdQueriesHistoryClear(g *globalFlags, c *Client, ref string, yes bool) error {
	if !yes {
		if g.json {
			return fmt.Errorf("confirmation required: pass --yes to clear history non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Clear all query history for project %q? This cannot be undone. [y/N] ", ref)
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
		Cleared bool `json:"cleared"`
	}
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/sql/history", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "cleared": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintln(os.Stdout, "Query history cleared.")
	return nil
}
