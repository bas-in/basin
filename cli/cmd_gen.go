// cmd_gen — `basin gen <subcommand>` dispatcher.
//
// Currently implements:
//
//	basin gen types <typescript|go|python> [--project=<ref>] [--schema=public]
//	                                       [--output=<path>] [--package=<name>]
//	                                       [--watch]
//
// Future subcommands (gen migrations, etc.) will be added here as the
// platform surface grows. The dispatcher structure is intentionally
// future-extensible: add a case to cmdGen's switch and a new handler
// function.
//
// Implementation note (decisions.md 2026-05-19): gen types is CLI-
// assembled over information_schema via the existing
// POST /v1/projects/{ref}/sql/query endpoint. No dedicated /v1/types
// endpoint is needed or planned.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"
)

// ── Dispatcher ────────────────────────────────────────────────────────────────

func cmdGen(g *globalFlags, args []string) error {
	if len(args) == 0 {
		helpForCommand("gen", "Code-generation utilities.", []string{
			"types <typescript|go|python>   Generate typed database interfaces.",
		})
		return nil
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "types":
		return cmdGenTypes(g, rest)
	case "--help", "-h", "help":
		helpForCommand("gen", "Code-generation utilities.", []string{
			"types <typescript|go|python>   Generate typed database interfaces.",
		})
		return nil
	default:
		return fmt.Errorf("gen: unknown subcommand %q (available: types)", sub)
	}
}

// ── gen types ─────────────────────────────────────────────────────────────────

// genSQLQueryBody mirrors cmd_sql.go's request body shape exactly.
// The endpoint is POST /v1/projects/{ref}/sql/query; the body carries
// the SQL as "sql" and a "writes_enabled" flag (false for read-only).
// We do NOT use parameterised $1 placeholders here because the cloud's
// sql/query endpoint does not expose a params array — the schema name
// is embedded directly via string formatting (safe: it is a short
// identifier validated by flag parsing before we get here).
type genSQLQueryBody struct {
	SQL           string `json:"sql"`
	WritesEnabled bool   `json:"writes_enabled"`
}

// schemaColumn is one row from the information_schema.columns result set.
// The query returns exactly these five columns in this order; we index
// by position (rows are [][]any) rather than a named decode.
type schemaColumn struct {
	TableName   string
	ColumnName  string
	DataType    string
	IsNullable  string // "YES" or "NO"
	OrdinalPosition int
}

// cmdGenTypes is the public entry point called by the dispatcher.
// It writes generated source to os.Stdout (or to --output=<path>) and
// informational/JSON metadata to os.Stderr.
func cmdGenTypes(g *globalFlags, args []string) error {
	return genTypes(g, args, os.Stdout, os.Stderr)
}

// genTypes is the testable inner implementation. Tests inject their own
// io.Writer values to capture output without touching os.Stdout or os.Stderr
// (which would race with parallel test goroutines).
//
//   - out    — destination for the generated source code (os.Stdout in production)
//   - errOut — destination for progress/JSON metadata (os.Stderr in production)
func genTypes(g *globalFlags, args []string, out io.Writer, errOut io.Writer) error {
	// ── flag parsing ──────────────────────────────────────────────────
	// We use a minimal manual parser (matching the repo's stdlib-only
	// convention) instead of flag.FlagSet so we can pull the positional
	// <lang> arg naturally.
	var (
		project    string
		schema     = "public"
		outputPath string
		pkgName    = "database"
		watch      bool
	)
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--help" || a == "-h":
			genTypesUsage()
			return nil
		case strings.HasPrefix(a, "--project="):
			project = strings.TrimPrefix(a, "--project=")
		case a == "--project":
			if i+1 >= len(args) {
				return fmt.Errorf("gen types: --project requires a value")
			}
			i++
			project = args[i]
		case strings.HasPrefix(a, "--schema="):
			schema = strings.TrimPrefix(a, "--schema=")
		case a == "--schema":
			if i+1 >= len(args) {
				return fmt.Errorf("gen types: --schema requires a value")
			}
			i++
			schema = args[i]
		case strings.HasPrefix(a, "--output="):
			outputPath = strings.TrimPrefix(a, "--output=")
		case a == "--output":
			if i+1 >= len(args) {
				return fmt.Errorf("gen types: --output requires a value")
			}
			i++
			outputPath = args[i]
		case strings.HasPrefix(a, "--package="):
			pkgName = strings.TrimPrefix(a, "--package=")
		case a == "--package":
			if i+1 >= len(args) {
				return fmt.Errorf("gen types: --package requires a value")
			}
			i++
			pkgName = args[i]
		case a == "--watch":
			watch = true
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("gen types: unknown flag %q", a)
		default:
			positional = append(positional, a)
		}
	}

	if len(positional) == 0 {
		genTypesUsage()
		return fmt.Errorf("gen types: <lang> is required (typescript|go|python)")
	}
	langStr := strings.ToLower(positional[0])

	var lang LangTarget
	switch langStr {
	case "typescript", "ts":
		lang = LangTypeScript
	case "go":
		lang = LangGo
	case "python", "py":
		lang = LangPython
	default:
		return fmt.Errorf("gen types: unknown language %q (want typescript, go, or python)", positional[0])
	}

	// --watch stub — full implementation deferred to a later batch.
	if watch {
		fmt.Fprintln(errOut, "basin: watch not yet implemented")
		return nil
	}

	// ── project ref resolution ─────────────────────────────────────────
	if project == "" {
		// Fall back to working-project context (./basin/config.toml).
		cwd, cwdErr := os.Getwd()
		if cwdErr == nil {
			if wp, werr := loadWorkingProject(cwd); werr == nil && wp != nil {
				project = wp.ProjectRef
			}
		}
	}
	if project == "" {
		return fmt.Errorf("gen types: --project is required (or link a project with `basin link`)")
	}

	// ── schema name validation ─────────────────────────────────────────
	// Only allow alphanumeric + underscore to prevent SQL injection via
	// the schema name (it is embedded directly in the query string).
	if !isIdentifier(schema) {
		return fmt.Errorf("gen types: invalid schema name %q (must be alphanumeric + underscore)", schema)
	}

	// ── HTTP query ────────────────────────────────────────────────────
	c, err := requireClient(g)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Query information_schema.columns for all user tables in the schema.
	// The schema name is embedded directly (validated above as a safe
	// identifier — alphanumeric + underscore only).
	sqlQuery := fmt.Sprintf(
		"SELECT table_name, column_name, data_type, is_nullable, ordinal_position "+
			"FROM information_schema.columns "+
			"WHERE table_schema = '%s' "+
			"ORDER BY table_name, ordinal_position",
		schema,
	)
	reqBody := genSQLQueryBody{SQL: sqlQuery, WritesEnabled: false}
	var res QueryResult
	if err := c.do(ctx, "POST", "/v1/projects/"+project+"/sql/query", reqBody, &res); err != nil {
		return fmt.Errorf("gen types: query failed: %w", err)
	}

	// ── parse result rows ─────────────────────────────────────────────
	// Columns: table_name, column_name, data_type, is_nullable, ordinal_position
	tables, tableOrder, totalCols, err := parseSchemaColumns(res)
	if err != nil {
		return fmt.Errorf("gen types: parse result: %w", err)
	}

	// ── emit source ───────────────────────────────────────────────────
	var sb strings.Builder
	switch lang {
	case LangTypeScript:
		emitTypeScript(&sb, tables, tableOrder)
	case LangGo:
		emitGo(&sb, tables, tableOrder, pkgName)
	case LangPython:
		emitPython(&sb, tables, tableOrder)
	}
	src := sb.String()

	// Determine output destination.
	if outputPath != "" {
		if werr := os.WriteFile(outputPath, []byte(src), 0o644); werr != nil {
			return fmt.Errorf("gen types: write %s: %w", outputPath, werr)
		}
	} else {
		if _, werr := io.WriteString(out, src); werr != nil {
			return fmt.Errorf("gen types: write output: %w", werr)
		}
	}

	// ── optional JSON metadata (always to stderr) ─────────────────────
	outDisplay := outputPath
	if outDisplay == "" {
		outDisplay = "-"
	}
	if g.json {
		// JSON shape: { "lang": string, "tables": int, "columns": int, "output_path": string }
		return printJSON(errOut, map[string]any{
			"lang":        langStr,
			"tables":      len(tableOrder),
			"columns":     totalCols,
			"output_path": outDisplay,
		})
	}
	if !g.quiet {
		fmt.Fprintf(errOut, "generated %d table(s), %d column(s) → %s\n",
			len(tableOrder), totalCols, outDisplay)
	}
	return nil
}

func genTypesUsage() {
	helpForCommand("gen types", "Generate typed database interfaces from information_schema.", []string{
		"<lang>              Target language: typescript, go, python (required).",
		"--project=<ref>     Project ref (required if not linked).",
		"--schema=public     Schema name to introspect (default: public).",
		"--output=<path>     Write to file instead of stdout.",
		"--package=<name>    Go package name (default: database, Go only).",
		"--watch             Re-emit on every migration push (stub — coming soon).",
	})
}

// ── Parsing ───────────────────────────────────────────────────────────────────

// tableColumns holds the ordered columns for one table.
type tableColumns struct {
	Columns []schemaColumn
}

// parseSchemaColumns walks the QueryResult rows and groups them by table_name.
// Returns: map[table]columns, ordered table names, total column count, error.
func parseSchemaColumns(res QueryResult) (map[string]*tableColumns, []string, int, error) {
	tables := make(map[string]*tableColumns)
	var tableOrder []string
	totalCols := 0

	for _, row := range res.Rows {
		if len(row) < 5 {
			continue
		}
		col := schemaColumn{
			TableName:  stringVal(row[0]),
			ColumnName: stringVal(row[1]),
			DataType:   strings.ToLower(stringVal(row[2])),
			IsNullable: stringVal(row[3]),
		}
		if col.TableName == "" || col.ColumnName == "" {
			continue
		}
		if _, exists := tables[col.TableName]; !exists {
			tables[col.TableName] = &tableColumns{}
			tableOrder = append(tableOrder, col.TableName)
		}
		tables[col.TableName].Columns = append(tables[col.TableName].Columns, col)
		totalCols++
	}
	sort.Strings(tableOrder)
	return tables, tableOrder, totalCols, nil
}

// stringVal coerces an any (from JSON unmarshal) to string.
func stringVal(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// isIdentifier returns true when s consists only of letters, digits, or underscores.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

// ── TypeScript emitter ────────────────────────────────────────────────────────

func emitTypeScript(w *strings.Builder, tables map[string]*tableColumns, order []string) {
	w.WriteString("// Code generated by basin gen types — DO NOT EDIT.\n\n")

	// Per-table interface declarations first.
	for _, tbl := range order {
		tc := tables[tbl]
		ifaceName := "Table_" + toPascalCase(tbl)
		w.WriteString("export interface " + ifaceName + " {\n")
		for _, col := range tc.Columns {
			nullable := strings.EqualFold(col.IsNullable, "YES")
			mt, ok := MapType(PgType{Name: col.DataType}, LangTypeScript)
			var tsType string
			if ok {
				if nullable {
					tsType = mt.Nullable
				} else {
					tsType = mt.Type
				}
			} else {
				// Unknown type — safe fallback with a warning comment.
				w.WriteString("  // WARNING: unknown pg type " + col.DataType + "\n")
				if nullable {
					tsType = "unknown | null"
				} else {
					tsType = "unknown"
				}
			}
			w.WriteString("  " + col.ColumnName + ": " + tsType + ";\n")
		}
		w.WriteString("}\n\n")
	}

	// Database wrapper following the supabase-shape convention.
	w.WriteString("export interface Database {\n")
	w.WriteString("  Tables: {\n")
	for _, tbl := range order {
		ifaceName := "Table_" + toPascalCase(tbl)
		w.WriteString("    " + tbl + ": " + ifaceName + ";\n")
	}
	w.WriteString("  };\n")
	w.WriteString("}\n")
}

// ── Go emitter ────────────────────────────────────────────────────────────────

func emitGo(w *strings.Builder, tables map[string]*tableColumns, order []string, pkgName string) {
	w.WriteString("// Code generated by basin gen types — DO NOT EDIT.\n\n")
	w.WriteString("package " + pkgName + "\n\n")

	// Collect imports from all mapped types.
	imports := make(map[string]struct{})
	for _, tbl := range order {
		for _, col := range tables[tbl].Columns {
			mt, ok := MapType(PgType{Name: col.DataType}, LangGo)
			if ok && mt.Import != "" {
				imports[mt.Import] = struct{}{}
			}
		}
	}
	if len(imports) > 0 {
		importList := make([]string, 0, len(imports))
		for imp := range imports {
			importList = append(importList, imp)
		}
		sort.Strings(importList)
		w.WriteString("import (\n")
		for _, imp := range importList {
			w.WriteString("\t\"" + imp + "\"\n")
		}
		w.WriteString(")\n\n")
	}

	// One struct per table with json and db tags.
	for _, tbl := range order {
		tc := tables[tbl]
		structName := toPascalCase(tbl)
		w.WriteString("type " + structName + " struct {\n")
		for _, col := range tc.Columns {
			nullable := strings.EqualFold(col.IsNullable, "YES")
			mt, ok := MapType(PgType{Name: col.DataType}, LangGo)
			var goType string
			if ok {
				if nullable {
					goType = mt.Nullable
				} else {
					goType = mt.Type
				}
			} else {
				// Unknown type — fallback with a warning comment.
				w.WriteString("\t// WARNING: unknown pg type " + col.DataType + "\n")
				goType = "interface{}"
			}
			fieldName := toPascalCase(col.ColumnName)
			w.WriteString("\t" + fieldName + " " + goType +
				" `json:\"" + col.ColumnName + "\" db:\"" + col.ColumnName + "\"`\n")
		}
		w.WriteString("}\n\n")
	}
}

// ── Python emitter ────────────────────────────────────────────────────────────

func emitPython(w *strings.Builder, tables map[string]*tableColumns, order []string) {
	w.WriteString("# Code generated by basin gen types — DO NOT EDIT.\n\n")

	// Collect imports from all mapped types.
	imports := make(map[string]struct{})
	imports["from pydantic import BaseModel"] = struct{}{}

	for _, tbl := range order {
		for _, col := range tables[tbl].Columns {
			mt, ok := MapType(PgType{Name: col.DataType}, LangPython)
			if ok && mt.Import != "" {
				// Imports may be multi-line
				// (e.g. "from decimal import Decimal\nfrom typing import Optional").
				for _, line := range strings.Split(mt.Import, "\n") {
					line = strings.TrimSpace(line)
					if line != "" {
						imports[line] = struct{}{}
					}
				}
			}
		}
	}

	importList := make([]string, 0, len(imports))
	for imp := range imports {
		importList = append(importList, imp)
	}
	sort.Strings(importList)
	for _, imp := range importList {
		w.WriteString(imp + "\n")
	}
	w.WriteString("\n")

	// One class per table.
	for _, tbl := range order {
		tc := tables[tbl]
		className := toPascalCase(tbl)
		w.WriteString("class " + className + "(BaseModel):\n")
		if len(tc.Columns) == 0 {
			w.WriteString("    pass\n")
		}
		for _, col := range tc.Columns {
			nullable := strings.EqualFold(col.IsNullable, "YES")
			mt, ok := MapType(PgType{Name: col.DataType}, LangPython)
			var pyType string
			if ok {
				if nullable {
					pyType = mt.Nullable
				} else {
					pyType = mt.Type
				}
			} else {
				// Unknown type — safe fallback.
				w.WriteString("    # WARNING: unknown pg type " + col.DataType + "\n")
				if nullable {
					pyType = "Optional[None]"
				} else {
					pyType = "None"
				}
			}
			w.WriteString("    " + col.ColumnName + ": " + pyType + "\n")
		}
		w.WriteString("\n")
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// toPascalCase converts snake_case or mixed identifiers to PascalCase.
// "user_profile" → "UserProfile", "orders" → "Orders".
func toPascalCase(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	upper := true
	for _, r := range s {
		if r == '_' || r == '-' || r == ' ' {
			upper = true
			continue
		}
		if upper {
			b.WriteRune(unicode.ToUpper(r))
			upper = false
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}
