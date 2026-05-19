// cmd_rows — row-level mutations for a project table.
//
//	basin rows insert <table> [--project=<ref>] [--json='<row>']
//	basin rows update <table> --pk=<val> --json='<patch>' [--project=<ref>]
//	basin rows delete <table> --where='<predicate-sql>' [--project=<ref>] [--yes]
//
// Routes (per-table family under /v1/projects/{ref}/tables/{name}):
//
//	POST   /v1/projects/{ref}/tables/{name}/rows           body { "row": {...} }
//	PATCH  /v1/projects/{ref}/tables/{name}/rows/{pk}      body { "patch": {...} }
//	DELETE /v1/projects/{ref}/tables/{name}/rows?where=<>
//
// Project ref resolution order:
//  1. --project=<ref> flag
//  2. loadWorkingProject(cwd) — ./basin/config.toml
//  3. Error with hint to run `basin link`
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// ── Dispatcher ───────────────────────────────────────────────────────

func cmdRows(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin rows (insert | update | delete) <table> ...")
	}
	switch args[0] {
	case "insert":
		return cmdRowsInsert(g, args[1:])
	case "update":
		return cmdRowsUpdate(g, args[1:])
	case "delete":
		return cmdRowsDelete(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("rows", "Insert / update / delete rows in a project table.", []string{
			"insert <table> [--project=<ref>] [--json='{...}']   Insert a row (or batch from stdin NDJSON).",
			"update <table> --pk=<val> --json='{...}' [--project=<ref>]  Update a row by primary-key value.",
			"delete <table> --where='<sql>' [--project=<ref>] [--yes]    Delete rows matching a predicate.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for rows", args[0])
	}
}

// ── rows insert ──────────────────────────────────────────────────────

// rowInsertResponse is the JSON shape returned by POST …/rows.
// JSON shape: { "inserted": N, "rows": [{...}] }
type rowInsertResponse struct {
	Inserted int              `json:"inserted"`
	Rows     []map[string]any `json:"rows"`
}

func cmdRowsInsert(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rows insert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	jsonFlag := fs.String("json", "", "JSON object for the row to insert. Omit to read NDJSON from stdin.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rows insert <table> [--project=<ref>] [--json='{...}']")
	}
	table := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	// Inline --json mode: single row insert.
	if *jsonFlag != "" {
		return rowsInsertOne(g, c, ref, table, *jsonFlag)
	}

	// Stdin NDJSON batch mode: one JSON object per line.
	return rowsInsertBatch(g, c, ref, table)
}

// rowsInsertOne POSTs a single row to the cloud and prints the result.
func rowsInsertOne(g *globalFlags, c *Client, ref, table, rowJSON string) error {
	var rowObj map[string]any
	if err := json.Unmarshal([]byte(rowJSON), &rowObj); err != nil {
		return fmt.Errorf("rows insert: invalid JSON for --json: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp rowInsertResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/tables/"+table+"/rows",
		map[string]any{"row": rowObj}, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "inserted": N, "rows": [{...returned...}] }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Inserted %d row(s) into %s.\n", resp.Inserted, table)
	return nil
}

// rowsInsertBatch reads NDJSON from stdin (one JSON object per line)
// and POSTs each line as a separate insert. Errors on any individual
// line are reported to stderr but do not abort the remaining lines.
// Returns a non-nil error only if zero lines were processed or if every
// line failed; partial success exits 0 (the caller sees "N inserted").
func rowsInsertBatch(g *globalFlags, c *Client, ref, table string) error {
	scanner := bufio.NewScanner(os.Stdin)
	var inserted int
	var lineNum int
	var lastErr error
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		lineNum++
		var rowObj map[string]any
		if err := json.Unmarshal([]byte(line), &rowObj); err != nil {
			printErr(g, "rows insert: line %d: invalid JSON: %v", lineNum, err)
			lastErr = err
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
		var resp rowInsertResponse
		if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/tables/"+table+"/rows",
			map[string]any{"row": rowObj}, &resp); err != nil {
			cancel()
			printErr(g, "rows insert: line %d: %v", lineNum, err)
			lastErr = err
			continue
		}
		cancel()
		inserted += resp.Inserted
		if !g.quiet {
			fmt.Fprintf(os.Stderr, "line %d: inserted %d row(s)\n", lineNum, resp.Inserted)
		}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return fmt.Errorf("rows insert: reading stdin: %w", err)
	}
	if lineNum == 0 {
		return fmt.Errorf("rows insert: no JSON lines found on stdin (pipe NDJSON or pass --json='{...}')")
	}
	if g.json {
		// JSON shape: { "inserted": N }
		return printJSON(os.Stdout, map[string]any{"inserted": inserted})
	}
	fmt.Fprintf(os.Stdout, "Inserted %d row(s) into %s.\n", inserted, table)
	// Return the last error so a fully-failed batch is non-zero, but a
	// partial success (some lines OK) exits 0 (lastErr stays nil when
	// all succeeded). Callers that need strict all-or-nothing should use
	// --json and inspect the inserted count.
	if inserted == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

// ── rows update ──────────────────────────────────────────────────────

// rowUpdateResponse is the JSON shape returned by PATCH …/rows/{pk}.
// JSON shape: { "row": {...} }
type rowUpdateResponse struct {
	Row map[string]any `json:"row"`
}

func cmdRowsUpdate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rows update", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	pk := fs.String("pk", "", "Primary-key value identifying the row to update (required).")
	jsonFlag := fs.String("json", "", "JSON patch object (required). Only the supplied fields are updated.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rows update <table> --pk=<val> --json='{...}' [--project=<ref>]")
	}
	table := rest[0]

	if *pk == "" {
		return fmt.Errorf("rows update: --pk is required")
	}
	if *jsonFlag == "" {
		return fmt.Errorf("rows update: --json is required")
	}

	var patchObj map[string]any
	if err := json.Unmarshal([]byte(*jsonFlag), &patchObj); err != nil {
		return fmt.Errorf("rows update: invalid JSON for --json: %w", err)
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
	var resp rowUpdateResponse
	if err := c.do(ctx, "PATCH",
		"/v1/projects/"+ref+"/tables/"+table+"/rows/"+*pk,
		map[string]any{"patch": patchObj}, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "row": {...} }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Updated row (pk=%s) in %s.\n", *pk, table)
	return nil
}

// ── rows delete ──────────────────────────────────────────────────────

// rowDeleteResponse is the JSON shape returned by DELETE …/rows?where=.
// JSON shape: { "deleted": N }
type rowDeleteResponse struct {
	Deleted int `json:"deleted"`
}

func cmdRowsDelete(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("rows delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	where := fs.String("where", "", "SQL predicate selecting rows to delete (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin rows delete <table> --where='<sql>' [--project=<ref>] [--yes]")
	}
	table := rest[0]

	if *where == "" {
		return fmt.Errorf("rows delete: --where is required (e.g. --where='id = 42')")
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
			return fmt.Errorf("confirmation required: pass --yes to delete rows non-interactively under --json")
		}
		fmt.Fprintf(os.Stderr, "Delete rows from %s.%s WHERE %s? This cannot be undone. [y/N] ", ref, table, *where)
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

	q := queryString(map[string]string{"where": *where})
	var resp rowDeleteResponse
	if err := c.do(ctx, "DELETE",
		"/v1/projects/"+ref+"/tables/"+table+"/rows"+q,
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "deleted": N }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Deleted %d row(s) from %s.\n", resp.Deleted, table)
	return nil
}
