// cmd_tables — table inspector and write-side management.
//
//	basin tables list        --project <ref>
//	basin tables describe    <name> --project <ref>
//	basin tables show-rows   <name> --project <ref> [--page N --page-size N]
//	basin tables create      <name> --column=<spec>... [--project <ref>]
//	basin tables alter       <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project <ref>]
//	basin tables drop        <name> [--yes] [--project <ref>]
//	basin tables import-csv  <name> [--file=<path>] [--header=true|false] [--project <ref>]
//	basin tables export-csv  <name> [--project <ref>]
//	basin tables columns add    <table> --column=<spec> [--project <ref>]
//	basin tables columns alter  <table> --name=<col> [--type=<t>] [--nullable=true|false] [--project <ref>]
//	basin tables columns drop   <table> --name=<col> [--yes] [--project <ref>]
//
// Column spec grammar (for --column / --add-column):
//
//	<name>:<type>[,nullable=<true|false>][,pk=<true|false>][,default=<expr>]
//
// Examples:
//
//	id:bigint,pk=true,nullable=false
//	email:text,nullable=false
//	created_at:timestamptz,default=now()
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
)

func cmdTables(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin tables (list | describe | show-rows | create | alter | drop | import-csv | export-csv | columns) ...")
	}
	switch args[0] {
	case "list":
		return cmdTablesList(g, args[1:])
	case "describe":
		return cmdTablesDescribe(g, args[1:])
	case "show-rows":
		return cmdTablesShowRows(g, args[1:])
	case "create":
		return cmdTablesCreate(g, args[1:])
	case "alter":
		return cmdTablesAlter(g, args[1:])
	case "drop":
		return cmdTablesDrop(g, args[1:])
	case "import-csv":
		return cmdTablesImportCSV(g, args[1:])
	case "export-csv":
		return cmdTablesExportCSV(g, args[1:])
	case "columns":
		return cmdTablesColumns(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("tables", "List / describe / manage tables and their columns.", []string{
			"list        --project <ref>                                 List tables.",
			"describe    <name> --project <ref>                          Show columns + RLS.",
			"show-rows   <name> --project <ref> [--page N --page-size N] Paginated rows.",
			"create      <name> --column=<spec>... [--project <ref>]     Create a table.",
			"alter       <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project <ref>]  Alter a table.",
			"drop        <name> [--yes] [--project <ref>]                Drop a table.",
			"import-csv  <name> [--file=<path>] [--header=true|false] [--project <ref>]  Import CSV.",
			"export-csv  <name> [--project <ref>]                        Export CSV to stdout.",
			"columns     add|alter|drop <table> ...                      Manage columns.",
		})
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for tables", args[0])
	}
}

func cmdTablesList(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables list", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Tables []TableInfo `json:"tables"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+*project+"/tables", nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { tables: [ TableInfo ] }
		return printJSON(os.Stdout, resp)
	}
	t := newTable(g, "NAME", "SCHEMA", "ROWS")
	for _, row := range resp.Tables {
		t.row(row.Name, row.Schema, row.Rows)
	}
	return t.flush()
}

func cmdTablesDescribe(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables describe", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables describe <name> --project <ref>")
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	name := rest[0]

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp struct {
		Table struct {
			Name    string   `json:"name"`
			Schema  string   `json:"schema"`
			Columns []Column `json:"columns"`
		} `json:"table"`
	}
	if err := c.do(ctx, "GET", "/v1/projects/"+*project+"/tables/"+name, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { table: { name: string, schema: string, columns: [ Column ] } }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Table: %s.%s\n", resp.Table.Schema, resp.Table.Name)
	t := newTable(g, "COLUMN", "TYPE", "NULL", "PK", "DEFAULT")
	for _, col := range resp.Table.Columns {
		def := ""
		if col.Default != nil {
			def = *col.Default
		}
		t.row(col.Name, col.Type, boolStr(col.Nullable), boolStr(col.PK), def)
	}
	return t.flush()
}

func cmdTablesShowRows(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables show-rows", flag.ContinueOnError)
	project := fs.String("project", "", "Project ref (required).")
	page := fs.Int("page", 1, "Page number (1-based).")
	pageSize := fs.Int("page-size", 50, "Rows per page (max 1000).")
	sortFlag := fs.String("sort", "", "Sort column (e.g. id:desc).")
	filter := fs.String("filter", "", "Filter expression (col=value, col~contains, etc).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables show-rows <name> --project <ref>")
	}
	if *project == "" {
		return fmt.Errorf("--project is required")
	}
	name := rest[0]

	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	q := queryString(map[string]string{
		"page":      strconv.Itoa(*page),
		"page_size": strconv.Itoa(*pageSize),
		"sort":      *sortFlag,
		"filter":    *filter,
	})
	var resp RowsPage
	if err := c.do(ctx, "GET", "/v1/projects/"+*project+"/tables/"+name+"/rows"+q, nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: RowsPage — { columns: [string], rows: [[any]],
		// total: int, page: int, page_size: int }
		return printJSON(os.Stdout, resp)
	}
	if err := renderRows(g, resp.Columns, resp.Rows, nil, 0); err != nil {
		return err
	}
	if !g.quiet {
		fmt.Fprintf(os.Stderr, "(page %d/%d · %d total)\n",
			resp.Page, divCeil(resp.Total, int64(resp.PageSize)), resp.Total)
	}
	return nil
}

func boolStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func divCeil(a, b int64) int64 {
	if b <= 0 {
		return 0
	}
	if a%b == 0 {
		return a / b
	}
	return a/b + 1
}

// ── columnSpec ────────────────────────────────────────────────────────────
//
// Grammar: <name>:<type>[,key=value]*
// Keys: nullable (true|false), pk (true|false), default (<expr>)
// The type passes through to the cloud as-is (text, bigint, vector(384), etc.).

type columnSpec struct {
	Name     string
	Type     string
	Nullable *bool
	PK       bool
	Default  *string
}

// parseColumnSpec parses one --column / --add-column flag value.
//
// Grammar: <name>:<type>[,<key>=<value>]*
// where keys are: nullable (true|false), pk (true|false), default (<expr>).
//
// The type string passes through to the cloud as-is (e.g. text, bigint,
// vector(384), character varying(255)). Commas inside the type (e.g. inside
// parentheses in a type modifier) are handled by scanning for the first comma
// that is followed by a recognised key name.
func parseColumnSpec(s string) (columnSpec, error) {
	if s == "" {
		return columnSpec{}, fmt.Errorf("column spec is empty")
	}
	// 1. Split on the first colon to separate name from the rest.
	colonIdx := strings.Index(s, ":")
	if colonIdx < 0 {
		return columnSpec{}, fmt.Errorf("column spec %q: missing ':' separator (want name:type[,key=val])", s)
	}
	name := strings.TrimSpace(s[:colonIdx])
	rest := s[colonIdx+1:]
	if name == "" {
		return columnSpec{}, fmt.Errorf("column spec %q: name is empty", s)
	}

	// 2. Find the type boundary: the first comma that is immediately followed
	// by a recognised key name. Commas that are part of the type string (e.g.
	// inside NUMERIC(10,2)) come before any key= token.
	knownKeys := []string{"nullable=", "pk=", "primary_key=", "default="}
	splitAt := -1
	for i, ch := range rest {
		if ch != ',' {
			continue
		}
		tail := rest[i+1:]
		tailLower := strings.ToLower(tail)
		for _, kk := range knownKeys {
			if strings.HasPrefix(tailLower, kk) {
				splitAt = i
				break
			}
		}
		if splitAt >= 0 {
			break
		}
	}

	var typePart, kvPart string
	if splitAt < 0 {
		typePart = rest // no keys at all — whole rest is the type
	} else {
		typePart = rest[:splitAt]
		kvPart = rest[splitAt+1:] // everything after the splitting comma
	}

	typ := strings.TrimSpace(typePart)
	if typ == "" {
		return columnSpec{}, fmt.Errorf("column spec %q: type is empty", s)
	}

	cs := columnSpec{Name: name, Type: typ}
	if kvPart == "" {
		return cs, nil
	}

	// 3. Parse key=value tokens from kvPart. The "default" key is special:
	// once we see it, the rest of the string (including any commas) is the
	// default expression. We therefore parse tokens left-to-right and stop
	// processing further commas once we hit default=.
	remaining := kvPart
	for remaining != "" {
		// Peek: is this token a "default=" prefix? If so, consume the rest.
		lowerRem := strings.ToLower(remaining)
		if strings.HasPrefix(lowerRem, "default=") {
			defVal := strings.TrimSpace(remaining[len("default="):])
			cs.Default = &defVal
			break // default consumes everything that follows
		}

		// Find the end of this token (next comma or end of string).
		commaIdx := -1
		for i, ch := range remaining {
			if ch == ',' {
				// Only split if the tail starts with a known key.
				tail := strings.ToLower(remaining[i+1:])
				for _, kk := range knownKeys {
					if strings.HasPrefix(tail, kk) {
						commaIdx = i
						break
					}
				}
				if commaIdx >= 0 {
					break
				}
			}
		}

		var token string
		if commaIdx < 0 {
			token = remaining
			remaining = ""
		} else {
			token = remaining[:commaIdx]
			remaining = remaining[commaIdx+1:]
		}

		eq := strings.Index(token, "=")
		if eq < 0 {
			return columnSpec{}, fmt.Errorf("column spec %q: malformed key/value %q (want key=value)", s, token)
		}
		k := strings.ToLower(strings.TrimSpace(token[:eq]))
		v := strings.TrimSpace(token[eq+1:])
		switch k {
		case "nullable":
			b, err := parseBoolVal(v)
			if err != nil {
				return columnSpec{}, fmt.Errorf("column spec %q: nullable: %w", s, err)
			}
			cs.Nullable = &b
		case "pk", "primary_key":
			b, err := parseBoolVal(v)
			if err != nil {
				return columnSpec{}, fmt.Errorf("column spec %q: pk: %w", s, err)
			}
			cs.PK = b
		default:
			return columnSpec{}, fmt.Errorf("column spec %q: unknown key %q (want nullable, pk, default)", s, k)
		}
	}
	return cs, nil
}

// parseBoolVal accepts "true", "false", "1", "0", "yes", "no".
func parseBoolVal(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "true", "1", "yes":
		return true, nil
	case "false", "0", "no":
		return false, nil
	}
	return false, fmt.Errorf("expected true/false, got %q", s)
}

// columnSpecToSQL renders a columnSpec into a SQL column definition fragment
// (for use inside CREATE TABLE or ALTER TABLE ADD COLUMN).
func columnSpecToSQL(cs columnSpec) string {
	var b strings.Builder
	b.WriteString(quoteIdent(cs.Name))
	b.WriteString(" ")
	b.WriteString(cs.Type)
	if cs.Nullable != nil && !*cs.Nullable {
		b.WriteString(" NOT NULL")
	}
	if cs.Default != nil {
		b.WriteString(" DEFAULT ")
		b.WriteString(*cs.Default)
	}
	return b.String()
}

// ── tables create ─────────────────────────────────────────────────────────

// tableCreateResponse is the JSON shape returned by POST …/tables.
// JSON shape: { "created": bool }
type tableCreateResponse struct {
	Created bool `json:"created"`
}

// columnSpecFlag is a repeatable string flag ([]string accumulator).
type columnSpecFlag []string

func (f *columnSpecFlag) String() string { return strings.Join(*f, ", ") }
func (f *columnSpecFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}

func cmdTablesCreate(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	var columns columnSpecFlag
	fs.Var(&columns, "column", "Column spec: name:type[,nullable=false][,pk=true][,default=<expr>]. Repeatable.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables create <name> --column=<spec>... [--project=<ref>]")
	}
	tableName := rest[0]

	if len(columns) == 0 {
		return fmt.Errorf("tables create: at least one --column=<spec> is required")
	}

	// Parse specs client-side so we can give clean error messages before
	// touching the wire.
	var specs []columnSpec
	for _, raw := range columns {
		cs, err := parseColumnSpec(raw)
		if err != nil {
			return fmt.Errorf("tables create: %w", err)
		}
		specs = append(specs, cs)
	}

	// Build the CREATE TABLE SQL.
	var colDefs []string
	var pkCols []string
	for _, cs := range specs {
		colDefs = append(colDefs, columnSpecToSQL(cs))
		if cs.PK {
			pkCols = append(pkCols, quoteIdent(cs.Name))
		}
	}
	if len(pkCols) > 0 {
		colDefs = append(colDefs, "PRIMARY KEY ("+strings.Join(pkCols, ", ")+")")
	}
	sql := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
		quoteIdent(tableName), strings.Join(colDefs, ",\n  "))

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
	var resp tableCreateResponse
	if err := c.do(ctx, "POST", "/v1/projects/"+ref+"/tables",
		map[string]any{"sql": sql}, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "created": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Created table %s.\n", tableName)
	return nil
}

// ── tables alter ──────────────────────────────────────────────────────────

// tableAlterResponse is the JSON shape returned by PATCH …/tables/{name}.
// JSON shape: { "altered": bool }
type tableAlterResponse struct {
	Altered bool `json:"altered"`
}

func cmdTablesAlter(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables alter", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	var addColumns columnSpecFlag
	var dropColumns columnSpecFlag
	fs.Var(&addColumns, "add-column", "Column spec to add: name:type[,nullable=false][,default=<expr>]. Repeatable.")
	fs.Var(&dropColumns, "drop-column", "Column name to drop. Repeatable.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables alter <name> [--add-column=<spec>]... [--drop-column=<col>]... [--project=<ref>]")
	}
	tableName := rest[0]

	if len(addColumns) == 0 && len(dropColumns) == 0 {
		return fmt.Errorf("tables alter: at least one --add-column or --drop-column is required")
	}

	// Parse add-column specs.
	var addSpecs []columnSpec
	for _, raw := range addColumns {
		cs, err := parseColumnSpec(raw)
		if err != nil {
			return fmt.Errorf("tables alter: %w", err)
		}
		addSpecs = append(addSpecs, cs)
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	// Build the ALTER TABLE SQL — one statement combining all changes.
	// Postgres supports multiple actions in a single ALTER TABLE.
	var actions []string
	for _, cs := range addSpecs {
		actions = append(actions, "ADD COLUMN "+columnSpecToSQL(cs))
	}
	for _, col := range dropColumns {
		actions = append(actions, "DROP COLUMN "+quoteIdent(strings.TrimSpace(col)))
	}
	sql := fmt.Sprintf("ALTER TABLE %s %s",
		quoteIdent(tableName), strings.Join(actions, ", "))

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp tableAlterResponse
	if err := c.do(ctx, "PATCH", "/v1/projects/"+ref+"/tables/"+tableName,
		map[string]any{"sql": sql}, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "altered": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Altered table %s.\n", tableName)
	return nil
}

// ── tables drop ───────────────────────────────────────────────────────────

// tableDropResponse is the JSON shape returned by DELETE …/tables/{name}.
// JSON shape: { "dropped": bool }
type tableDropResponse struct {
	Dropped bool `json:"dropped"`
}

func cmdTablesDrop(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables drop", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables drop <name> [--yes] [--project=<ref>]")
	}
	tableName := rest[0]

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
			return fmt.Errorf("confirmation required: pass --yes to drop table %q non-interactively under --json", tableName)
		}
		fmt.Fprintf(os.Stderr, "Drop table %q in project %q? This cannot be undone. [y/N] ", tableName, ref)
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

	// Cloud requires ?confirm_name=<exact table name> as a safety gate.
	q := queryString(map[string]string{"confirm_name": tableName})
	var resp tableDropResponse
	if err := c.do(ctx, "DELETE", "/v1/projects/"+ref+"/tables/"+tableName+q,
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "dropped": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Dropped table %s.\n", tableName)
	return nil
}

// ── tables import-csv ─────────────────────────────────────────────────────

// tableImportCSVResponse is the JSON shape returned by POST …/import-csv.
// JSON shape: { "rows_imported": N, "rows_rejected": N, "rejections": [...], "elapsed_ms": N }
type tableImportCSVResponse struct {
	RowsImported int      `json:"rows_imported"`
	RowsRejected int      `json:"rows_rejected"`
	Rejections   []string `json:"rejections,omitempty"`
	ElapsedMs    int      `json:"elapsed_ms,omitempty"`
}

func cmdTablesImportCSV(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables import-csv", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	file := fs.String("file", "", "Path to CSV file (default: stdin).")
	header := fs.String("header", "true", "Whether the CSV has a header row (true|false).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables import-csv <name> [--file=<path>] [--header=true|false] [--project=<ref>]")
	}
	tableName := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	// Open the CSV source — either a named file or stdin.
	var csvSrc io.Reader
	if *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			return fmt.Errorf("tables import-csv: open %q: %w", *file, err)
		}
		defer f.Close()
		csvSrc = f
	} else {
		csvSrc = os.Stdin
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()

	var resp tableImportCSVResponse
	if err := postMultipartCSV(ctx, c, "/v1/projects/"+ref+"/tables/"+tableName+"/import-csv",
		csvSrc, *header, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "rows_imported": N, "rows_rejected": N, "rejections": [...], "elapsed_ms": N }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Imported %d row(s) into %s", resp.RowsImported, tableName)
	if resp.RowsRejected > 0 {
		fmt.Fprintf(os.Stdout, " (%d rejected)", resp.RowsRejected)
	}
	fmt.Fprintln(os.Stdout, ".")
	return nil
}

// postMultipartCSV sends a CSV body as multipart/form-data to the cloud's
// import endpoint. The cloud expects a "file" form field + optional "header"
// field. We build a multipart body in memory via pipe so the caller's
// io.Reader is streamed without buffering the entire file.
//
// This is a local helper so Client's contract stays unchanged (no new method
// on *Client — see task hard constraints).
func postMultipartCSV(ctx context.Context, c *Client, path string, body io.Reader, header string, out any) error {
	pr, pw := io.Pipe()

	mw := multipart.NewWriter(pw)
	go func() {
		// Write the "header" field first.
		_ = mw.WriteField("header", header)
		// Stream the CSV into the "file" field.
		fw, err := mw.CreateFormFile("file", "data.csv")
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_, err = io.Copy(fw, body)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = mw.Close()
		_ = pw.Close()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, pr)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if c.UserAgent != "" {
		req.Header.Set("User-Agent", c.UserAgent)
	}

	res, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		if out == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			return nil
		}
		rawBody, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		// Try the cloud's wrapped {"data":{...}} envelope first.
		var wrappedRaw struct {
			Data  json.RawMessage `json:"data"`
			Error json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(rawBody, &wrappedRaw); err == nil &&
			len(wrappedRaw.Data) > 0 && string(wrappedRaw.Data) != "null" {
			return json.Unmarshal(wrappedRaw.Data, out)
		}
		return json.Unmarshal(rawBody, out)
	}

	b, _ := io.ReadAll(res.Body)
	ae := &APIError{HTTPStatus: res.StatusCode}
	if len(b) > 0 {
		var nested struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(b, &nested); err == nil &&
			(nested.Error.Code != "" || nested.Error.Message != "") {
			ae.Code = nested.Error.Code
			ae.Message = nested.Error.Message
			return ae
		}
		ae.Message = strings.TrimSpace(string(b))
	}
	return ae
}

// ── tables export-csv ─────────────────────────────────────────────────────

func cmdTablesExportCSV(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables export-csv", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables export-csv <name> [--project=<ref>]")
	}
	tableName := rest[0]

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	// Use a context with no per-request deadline — large tables may
	// stream for a while. (httpStream client has no Timeout set.)
	ctx := context.Background()
	res, err := c.stream(ctx, "/v1/projects/"+ref+"/tables/"+tableName+"/export-csv")
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// Stream the CSV directly to stdout — no buffering.
	if _, err := io.Copy(os.Stdout, res.Body); err != nil {
		return fmt.Errorf("tables export-csv: streaming response: %w", err)
	}
	return nil
}

// ── tables columns ────────────────────────────────────────────────────────

func cmdTablesColumns(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin tables columns (add | alter | drop) <table> ...")
	}
	switch args[0] {
	case "add":
		return cmdTablesColumnsAdd(g, args[1:])
	case "alter":
		return cmdTablesColumnsAlter(g, args[1:])
	case "drop":
		return cmdTablesColumnsDrop(g, args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q for tables columns", args[0])
	}
}

// ── tables columns add ────────────────────────────────────────────────────

// columnAddResponse is the JSON shape returned by POST …/columns.
// JSON shape: { "added": bool }
type columnAddResponse struct {
	Added bool `json:"added"`
}

func cmdTablesColumnsAdd(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables columns add", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	colSpec := fs.String("column", "", "Column spec: name:type[,nullable=false][,default=<expr>] (required).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables columns add <table> --column=<spec> [--project=<ref>]")
	}
	tableName := rest[0]

	if *colSpec == "" {
		return fmt.Errorf("tables columns add: --column=<spec> is required")
	}
	cs, err := parseColumnSpec(*colSpec)
	if err != nil {
		return fmt.Errorf("tables columns add: %w", err)
	}

	ref, err := resolveProjectRef(*project)
	if err != nil {
		return err
	}
	c, err := requireClient(g)
	if err != nil {
		return err
	}

	body := map[string]any{
		"name": cs.Name,
		"type": cs.Type,
	}
	if cs.Nullable != nil {
		body["nullable"] = *cs.Nullable
	}
	if cs.Default != nil {
		body["default"] = *cs.Default
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.HTTP.Timeout)
	defer cancel()
	var resp columnAddResponse
	if err := c.do(ctx, "POST",
		"/v1/projects/"+ref+"/tables/"+tableName+"/columns",
		body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "added": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Added column %s to table %s.\n", cs.Name, tableName)
	return nil
}

// ── tables columns alter ──────────────────────────────────────────────────

// columnAlterResponse is the JSON shape returned by PATCH …/columns/{name}.
// JSON shape: { "altered": bool }
type columnAlterResponse struct {
	Altered bool `json:"altered"`
}

func cmdTablesColumnsAlter(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables columns alter", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	colName := fs.String("name", "", "Column name to alter (required).")
	colType := fs.String("type", "", "New type for the column (optional).")
	nullableFlag := fs.String("nullable", "", "Set nullability: true or false (optional).")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables columns alter <table> --name=<col> [--type=<t>] [--nullable=true|false] [--project=<ref>]")
	}
	tableName := rest[0]

	if *colName == "" {
		return fmt.Errorf("tables columns alter: --name is required")
	}
	if *colType == "" && *nullableFlag == "" {
		return fmt.Errorf("tables columns alter: at least one of --type or --nullable is required")
	}

	body := map[string]any{}
	if *colType != "" {
		body["type"] = *colType
	}
	if *nullableFlag != "" {
		b, err := parseBoolVal(*nullableFlag)
		if err != nil {
			return fmt.Errorf("tables columns alter: --nullable: %w", err)
		}
		body["nullable"] = b
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
	var resp columnAlterResponse
	if err := c.do(ctx, "PATCH",
		"/v1/projects/"+ref+"/tables/"+tableName+"/columns/"+*colName,
		body, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "altered": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Altered column %s on table %s.\n", *colName, tableName)
	return nil
}

// ── tables columns drop ───────────────────────────────────────────────────

// columnDropResponse is the JSON shape returned by DELETE …/columns/{name}.
// JSON shape: { "dropped": bool }
type columnDropResponse struct {
	Dropped bool `json:"dropped"`
}

func cmdTablesColumnsDrop(g *globalFlags, args []string) error {
	fs := flag.NewFlagSet("tables columns drop", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	project := fs.String("project", "", "Project ref (or auto-detected from ./basin/config.toml).")
	colName := fs.String("name", "", "Column name to drop (required).")
	yes := fs.Bool("yes", false, "Skip the confirmation prompt.")
	if err := fs.Parse(args); err != nil {
		return errSilent
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: basin tables columns drop <table> --name=<col> [--yes] [--project=<ref>]")
	}
	tableName := rest[0]

	if *colName == "" {
		return fmt.Errorf("tables columns drop: --name is required")
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
			return fmt.Errorf("confirmation required: pass --yes to drop column %q non-interactively under --json", *colName)
		}
		fmt.Fprintf(os.Stderr, "Drop column %q from table %s? This cannot be undone. [y/N] ", *colName, tableName)
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
	var resp columnDropResponse
	if err := c.do(ctx, "DELETE",
		"/v1/projects/"+ref+"/tables/"+tableName+"/columns/"+*colName,
		nil, &resp); err != nil {
		return err
	}
	if g.json {
		// JSON shape: { "dropped": true }
		return printJSON(os.Stdout, resp)
	}
	fmt.Fprintf(os.Stdout, "Dropped column %s from table %s.\n", *colName, tableName)
	return nil
}

