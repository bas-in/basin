// cmd_tables — read-only table inspector.
//
//	basin tables list      --project <ref>
//	basin tables describe  <name> --project <ref>
//	basin tables show-rows <name> --project <ref> [--page N --page-size N]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
)

func cmdTables(g *globalFlags, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: basin tables (list | describe | show-rows) ...")
	}
	switch args[0] {
	case "list":
		return cmdTablesList(g, args[1:])
	case "describe":
		return cmdTablesDescribe(g, args[1:])
	case "show-rows":
		return cmdTablesShowRows(g, args[1:])
	case "--help", "-h", "help":
		helpForCommand("tables", "List / describe tables; show paginated rows.", []string{
			"list      --project <ref>                                 List tables.",
			"describe  <name> --project <ref>                          Show columns + RLS.",
			"show-rows <name> --project <ref> [--page N --page-size N] Paginated rows.",
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
