// cmd_sql — `basin sql --project <ref> [-e "SELECT 1" | -f query.sql | <stdin>]`
//
// Sources of SQL (in priority order):
//
//	1. -e/--exec  inline string
//	2. -f/--file  file path
//	3. stdin (when piped)
//
// Empty input is rejected before we hit the network so a typo
// doesn't surface as an opaque 400 from the server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func cmdSQL(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("sql", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (required).")
	exec := fs.String("e", "", "Inline SQL (alias for --exec).")
	execL := fs.String("exec", "", "Inline SQL.")
	file := fs.String("f", "", "Read SQL from file (alias for --file).")
	fileL := fs.String("file", "", "Read SQL from file.")
	writes := fs.Bool("writes", false, "Permit DML/DDL (defaults to read-only).")
	helpFlag := fs.Bool("help", false, "Show help.")
	fs.Usage = func() {
		helpForCommand("sql", "Run a SQL query against a project's basin engine.", []string{
			"--project=<ref>      Project ref (required).",
			"-e/--exec '<sql>'    Inline query.",
			"-f/--file <path>     Read SQL from file.",
			"--writes             Permit DML/DDL (defaults to read-only).",
			"(if neither -e nor -f is set, SQL is read from stdin)",
		})
	}
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *helpFlag {
		fs.Usage()
		return nil
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}

	sqlBody := firstNonEmpty(*exec, *execL)
	if sqlBody == "" {
		// Try the file flag (long beats short when both set).
		path := firstNonEmpty(*fileL, *file)
		if path != "" {
			body, err := readMaybeFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", path, err)
			}
			sqlBody = body
		}
	}
	if sqlBody == "" {
		stdin, err := readAllStdin()
		if err != nil {
			return err
		}
		sqlBody = stdin
	}
	sqlBody = strings.TrimSpace(sqlBody)
	if sqlBody == "" {
		return fmt.Errorf("no SQL provided (use -e, -f, or pipe via stdin)")
	}

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	// Bigger timeout than the default — cold-path snapshots can take a
	// few seconds on shared regions.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	body := map[string]any{"sql": sqlBody, "writes_enabled": *writes}
	var res QueryResult
	if err := c.do(ctx, "POST", "/v1/projects/"+*project+"/sql/query", body, &res); err != nil {
		return err
	}
	if g.json {
		// JSON shape: QueryResult — { columns: [string], rows: [[any]],
		// rows_affected: int?, elapsed_ms: int }
		return printJSON(os.Stdout, res)
	}
	return renderRows(g, res.Columns, res.Rows, res.RowsAffected, res.ElapsedMs)
}

// renderRows is the table-output path shared by `basin sql` and
// `basin tables show-rows`. Empty result sets emit a tidy "(no rows)"
// instead of a blank table.
func renderRows(g *globalFlags, columns []string, rows [][]any, affected *int, elapsedMs int) error {
	if len(columns) == 0 && len(rows) == 0 {
		// Likely a DML — show the affected count if we have one.
		if affected != nil {
			fmt.Fprintf(os.Stdout, "%d row(s) affected (%dms)\n", *affected, elapsedMs)
			return nil
		}
		fmt.Fprintf(os.Stdout, "(no rows; %dms)\n", elapsedMs)
		return nil
	}
	headers := make([]string, len(columns))
	copy(headers, columns)
	if len(headers) == 0 && len(rows) > 0 {
		// Fallback: number the columns when the engine didn't ship a
		// header row.
		for i := range rows[0] {
			headers = append(headers, fmt.Sprintf("col%d", i+1))
		}
	}
	t := newTable(g, headers...)
	for _, r := range rows {
		cells := make([]any, len(headers))
		for i := range headers {
			if i < len(r) {
				cells[i] = stringifyCell(r[i])
			} else {
				cells[i] = ""
			}
		}
		t.row(cells...)
	}
	if err := t.flush(); err != nil {
		return err
	}
	if !g.quiet {
		fmt.Fprintf(os.Stderr, "(%d row(s) · %dms)\n", len(rows), elapsedMs)
	}
	return nil
}

// stringifyCell is the lossy "json-ish" cell renderer for the table
// view. Strings stay strings; everything else gets JSON-encoded.
func stringifyCell(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	case float64:
		// json.Unmarshal lands integers in float64; print them as ints
		// when the value is exactly an integer.
		if t == float64(int64(t)) {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	default:
		// Fall back to a compact JSON encoding for objects / arrays.
		return fmt.Sprintf("%v", t)
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
