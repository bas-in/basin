// cmd_gen_test — test coverage for `basin gen types`.
//
// The stub httptest.Server returns a known information_schema.columns payload
// (2 tables, 9 columns total, mix of nullable + non-nullable types including
// jsonb, uuid, timestamptz, vector, numeric, and text). Snapshot tests compare
// emitter output against testdata/expected.{ts,go,py} fixtures. Error-path
// tests cover unknown lang, missing project ref, server error (500), and empty
// schema (no tables). The --output=<path> test asserts the file is written and
// its content matches the stdout case. The JSON metadata test locks the shape.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── Stub server ───────────────────────────────────────────────────────────────

// stubSchemaRows is the canonical test payload: 2 tables, 9 columns total.
//
// Table "orders" (4 columns):
//
//	id       uuid      NOT NULL
//	user_id  uuid      YES (nullable)
//	total    numeric   NOT NULL
//	metadata jsonb     YES (nullable)
//
// Table "users" (5 columns):
//
//	id          uuid                     NOT NULL
//	email       text                     NOT NULL
//	created_at  timestamp with time zone YES (nullable)
//	embedding   vector                   YES (nullable)
//	score       integer                  NOT NULL
var stubSchemaRows = [][]any{
	{"orders", "id", "uuid", "NO", 1},
	{"orders", "user_id", "uuid", "YES", 2},
	{"orders", "total", "numeric", "NO", 3},
	{"orders", "metadata", "jsonb", "YES", 4},
	{"users", "id", "uuid", "NO", 1},
	{"users", "email", "text", "NO", 2},
	{"users", "created_at", "timestamp with time zone", "YES", 3},
	{"users", "embedding", "vector", "YES", 4},
	{"users", "score", "integer", "NO", 5},
}

// buildSchemaServer returns an httptest.Server that handles the sql/query
// endpoint and returns stubSchemaRows wrapped in the basin-cloud envelope.
func buildSchemaServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.Contains(r.URL.Path, "/sql/query") {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		payload := map[string]any{
			"data": map[string]any{
				"columns": []string{
					"table_name", "column_name", "data_type", "is_nullable", "ordinal_position",
				},
				"rows":       stubSchemaRows,
				"elapsed_ms": 5,
			},
			"error": nil,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
}

// build500Server returns a server that always responds HTTP 500.
func build500Server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    "internal_error",
				"message": "something went wrong",
			},
		})
	}))
}

// buildEmptySchemaServer returns a server that returns an empty rows set.
func buildEmptySchemaServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload := map[string]any{
			"data": map[string]any{
				"columns":    []string{"table_name", "column_name", "data_type", "is_nullable", "ordinal_position"},
				"rows":       [][]any{},
				"elapsed_ms": 1,
			},
			"error": nil,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
}

// gForServer builds a *globalFlags pointed at the stub server.
func gForServer(srv *httptest.Server) *globalFlags {
	return &globalFlags{
		apiURL: srv.URL,
		token:  "test-token",
	}
}

// runGen calls genTypes (the testable inner function) with strings.Builder
// values for both stdout and stderr so tests never touch os.Stdout/os.Stderr
// (which are globals that race when parallel tests manipulate them).
func runGen(t *testing.T, g *globalFlags, args []string) (string, error) {
	t.Helper()
	var out, errOut strings.Builder
	err := genTypes(g, args, &out, &errOut)
	return out.String(), err
}

// runGenCaptureBoth returns both stdout (generated source) and stderr (metadata).
func runGenCaptureBoth(t *testing.T, g *globalFlags, args []string) (string, string, error) {
	t.Helper()
	var out, errOut strings.Builder
	err := genTypes(g, args, &out, &errOut)
	return out.String(), errOut.String(), err
}

// ── Snapshot tests ────────────────────────────────────────────────────────────

func TestGenTypesTypeScript(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"typescript", "--project=test-ref"})
	if err != nil {
		t.Fatalf("cmdGenTypes typescript: %v", err)
	}

	expected, ferr := os.ReadFile(filepath.Join("testdata", "expected.ts"))
	if ferr != nil {
		t.Fatalf("read testdata/expected.ts: %v", ferr)
	}
	if got != string(expected) {
		t.Errorf("TypeScript output mismatch.\nGOT:\n%s\nWANT:\n%s", got, string(expected))
	}
}

func TestGenTypesGo(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"go", "--project=test-ref"})
	if err != nil {
		t.Fatalf("cmdGenTypes go: %v", err)
	}

	expected, ferr := os.ReadFile(filepath.Join("testdata", "expected.go"))
	if ferr != nil {
		t.Fatalf("read testdata/expected.go: %v", ferr)
	}
	if got != string(expected) {
		t.Errorf("Go output mismatch.\nGOT:\n%s\nWANT:\n%s", got, string(expected))
	}
}

func TestGenTypesPython(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"python", "--project=test-ref"})
	if err != nil {
		t.Fatalf("cmdGenTypes python: %v", err)
	}

	expected, ferr := os.ReadFile(filepath.Join("testdata", "expected.py"))
	if ferr != nil {
		t.Fatalf("read testdata/expected.py: %v", ferr)
	}
	if got != string(expected) {
		t.Errorf("Python output mismatch.\nGOT:\n%s\nWANT:\n%s", got, string(expected))
	}
}

// ── Error paths ───────────────────────────────────────────────────────────────

func TestGenTypesUnknownLang(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	_, err := runGen(t, gForServer(srv), []string{"ruby", "--project=test-ref"})
	if err == nil {
		t.Fatal("expected error for unknown language, got nil")
	}
	if !strings.Contains(err.Error(), "unknown language") {
		t.Errorf("expected 'unknown language' in error, got: %v", err)
	}
}

func TestGenTypesMissingProject(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	// Run in a temp dir with no basin/config.toml so project fallback fails.
	// We don't actually chdir (that would be a global side-effect with race);
	// instead we just don't pass --project and expect the error.
	_, err := runGen(t, gForServer(srv), []string{"typescript"})
	if err == nil {
		t.Fatal("expected error for missing project ref, got nil")
	}
	if !strings.Contains(err.Error(), "--project is required") {
		t.Errorf("expected '--project is required' in error, got: %v", err)
	}
}

func TestGenTypesServerError500(t *testing.T) {
	t.Parallel()
	srv := build500Server(t)
	defer srv.Close()

	_, err := runGen(t, gForServer(srv), []string{"typescript", "--project=test-ref"})
	if err == nil {
		t.Fatal("expected error on 500 response, got nil")
	}
	if !strings.Contains(err.Error(), "query failed") {
		t.Errorf("expected 'query failed' in error, got: %v", err)
	}
}

func TestGenTypesEmptySchema(t *testing.T) {
	t.Parallel()
	srv := buildEmptySchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"typescript", "--project=test-ref"})
	if err != nil {
		t.Fatalf("unexpected error on empty schema: %v", err)
	}
	// Should still produce a valid minimal header + empty Database block.
	if !strings.Contains(got, "Code generated by basin gen types") {
		t.Errorf("expected generated header in output, got: %q", got)
	}
	if !strings.Contains(got, "export interface Database") {
		t.Errorf("expected Database interface in output, got: %q", got)
	}
	// No table interfaces (empty schema).
	if strings.Contains(got, "export interface Table_") {
		t.Errorf("expected no table interfaces for empty schema, got: %q", got)
	}
}

// ── --output=<path> writes to file ───────────────────────────────────────────

func TestGenTypesOutputToFile(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	tmp := t.TempDir()
	outPath := filepath.Join(tmp, "database.ts")

	g := gForServer(srv)
	_, err := runGen(t, g, []string{"typescript", "--project=test-ref", "--output=" + outPath})
	if err != nil {
		t.Fatalf("gen types --output: %v", err)
	}

	// File must exist.
	data, ferr := os.ReadFile(outPath)
	if ferr != nil {
		t.Fatalf("output file not created: %v", ferr)
	}

	// Content must match the stdout (writer) case.
	stdoutGot, serr := runGen(t, g, []string{"typescript", "--project=test-ref"})
	if serr != nil {
		t.Fatalf("stdout run: %v", serr)
	}
	if string(data) != stdoutGot {
		t.Errorf("file content differs from writer output.\nFILE:\n%s\nWRITER:\n%s", string(data), stdoutGot)
	}
}

// ── JSON metadata shape ───────────────────────────────────────────────────────

func TestGenTypesJSONMetadata(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	g := gForServer(srv)
	g.json = true
	// genTypes writes JSON metadata to errOut (not os.Stderr); we capture it
	// via runGenCaptureBoth without touching any global file descriptors.
	_, stderrStr, runErr := runGenCaptureBoth(t, g, []string{"typescript", "--project=test-ref"})
	if runErr != nil {
		t.Fatalf("genTypes --json: %v", runErr)
	}

	var meta map[string]any
	if jerr := json.Unmarshal([]byte(stderrStr), &meta); jerr != nil {
		t.Fatalf("JSON metadata parse: %v\nraw: %q", jerr, stderrStr)
	}
	// Shape: lang, tables, columns, output_path.
	if meta["lang"] != "typescript" {
		t.Errorf("lang = %v, want typescript", meta["lang"])
	}
	if got := int(meta["tables"].(float64)); got != 2 {
		t.Errorf("tables = %d, want 2", got)
	}
	if got := int(meta["columns"].(float64)); got != 9 {
		t.Errorf("columns = %d, want 9", got)
	}
	if meta["output_path"] != "-" {
		t.Errorf("output_path = %v, want '-'", meta["output_path"])
	}
}

// ── --watch stub ──────────────────────────────────────────────────────────────

func TestGenTypesWatchStub(t *testing.T) {
	t.Parallel()
	// --watch is a stub — should return nil without querying the server.
	g := &globalFlags{token: "test-token", apiURL: "http://127.0.0.1:0"}
	_, err := runGen(t, g, []string{"typescript", "--project=test-ref", "--watch"})
	if err != nil {
		t.Fatalf("--watch should not error (stub), got: %v", err)
	}
}

// ── Dispatcher ────────────────────────────────────────────────────────────────

func TestGenDispatcherUnknownSubcommand(t *testing.T) {
	t.Parallel()
	err := cmdGen(&globalFlags{}, []string{"migrations"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
	if !strings.Contains(err.Error(), "unknown subcommand") {
		t.Errorf("expected 'unknown subcommand', got: %v", err)
	}
}

func TestGenDispatcherNoArgs(t *testing.T) {
	t.Parallel()
	// No args prints help and returns nil.
	if err := cmdGen(&globalFlags{}, []string{}); err != nil {
		t.Fatalf("cmdGen with no args should return nil, got: %v", err)
	}
}

// ── toPascalCase helper ───────────────────────────────────────────────────────

func TestToPascalCase(t *testing.T) {
	t.Parallel()
	cases := []struct{ in, want string }{
		{"users", "Users"},
		{"user_profile", "UserProfile"},
		{"order_line_item", "OrderLineItem"},
		{"id", "Id"},
		{"", ""},
		{"already_pascal", "AlreadyPascal"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := toPascalCase(tc.in)
			if got != tc.want {
				t.Errorf("toPascalCase(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ── isIdentifier helper ───────────────────────────────────────────────────────

func TestIsIdentifier(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"public", true},
		{"my_schema", true},
		{"schema123", true},
		{"", false},
		{"bad-schema", false},
		{"bad schema", false},
		{"drop table", false},
		{"'; DROP TABLE", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(fmt.Sprintf("%q", tc.in), func(t *testing.T) {
			t.Parallel()
			got := isIdentifier(tc.in)
			if got != tc.want {
				t.Errorf("isIdentifier(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// ── parseSchemaColumns ────────────────────────────────────────────────────────

func TestParseSchemaColumns(t *testing.T) {
	t.Parallel()
	res := QueryResult{
		Columns: []string{"table_name", "column_name", "data_type", "is_nullable", "ordinal_position"},
		Rows:    stubSchemaRows,
	}
	tables, order, total, err := parseSchemaColumns(res)
	if err != nil {
		t.Fatalf("parseSchemaColumns: %v", err)
	}
	if len(order) != 2 {
		t.Errorf("table count = %d, want 2", len(order))
	}
	if total != 9 {
		t.Errorf("total columns = %d, want 9", total)
	}
	if order[0] != "orders" || order[1] != "users" {
		t.Errorf("table order = %v, want [orders users]", order)
	}
	if len(tables["orders"].Columns) != 4 {
		t.Errorf("orders columns = %d, want 4", len(tables["orders"].Columns))
	}
	if len(tables["users"].Columns) != 5 {
		t.Errorf("users columns = %d, want 5", len(tables["users"].Columns))
	}
}

// ── TypeScript emit shape ─────────────────────────────────────────────────────

func TestEmitTypeScriptShape(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"typescript", "--project=test-ref"})
	if err != nil {
		t.Fatalf("typescript emit: %v", err)
	}

	checks := []string{
		"// Code generated by basin gen types — DO NOT EDIT.",
		"export interface Table_Orders",
		"export interface Table_Users",
		"export interface Database",
		"Tables:",
		"id: string;",                             // uuid NOT NULL
		"user_id: string | null;",                 // uuid nullable
		"total: string;",                           // numeric NOT NULL
		"metadata: Record<string, unknown> | null;", // jsonb nullable
		"created_at: string | null;",              // timestamptz nullable
		"embedding: number[] | null;",             // vector nullable
		"score: number;",                          // integer NOT NULL
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Errorf("TypeScript output missing %q\nFull output:\n%s", check, got)
		}
	}
}

// ── Go emit shape ─────────────────────────────────────────────────────────────

func TestEmitGoShape(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"go", "--project=test-ref"})
	if err != nil {
		t.Fatalf("go emit: %v", err)
	}

	checks := []string{
		"// Code generated by basin gen types — DO NOT EDIT.",
		"package database",
		`"encoding/json"`,
		`"time"`,
		"type Orders struct",
		"type Users struct",
		"Id [16]byte",            // uuid NOT NULL
		"UserId *[16]byte",       // uuid nullable
		"Total string",           // numeric NOT NULL
		"Metadata json.RawMessage", // jsonb (nullable == non-null for []byte)
		"Email string",           // text NOT NULL
		"CreatedAt *time.Time",   // timestamptz nullable
		"Embedding []float32",    // vector nullable (slice is reference)
		"Score int32",            // integer NOT NULL
		`json:"id"`,
		`db:"id"`,
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Errorf("Go output missing %q\nFull output:\n%s", check, got)
		}
	}
}

// ── Python emit shape ─────────────────────────────────────────────────────────

func TestEmitPythonShape(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"python", "--project=test-ref"})
	if err != nil {
		t.Fatalf("python emit: %v", err)
	}

	checks := []string{
		"# Code generated by basin gen types — DO NOT EDIT.",
		"from pydantic import BaseModel",
		"from uuid import UUID",
		"class Orders(BaseModel):",
		"class Users(BaseModel):",
		"id: UUID",                       // uuid NOT NULL
		"user_id: Optional[UUID]",        // uuid nullable
		"email: str",                     // text NOT NULL
		"created_at: Optional[datetime]", // timestamptz nullable
		"embedding: Optional[list[float]]", // vector nullable
		"score: int",                     // integer NOT NULL
	}
	for _, check := range checks {
		if !strings.Contains(got, check) {
			t.Errorf("Python output missing %q\nFull output:\n%s", check, got)
		}
	}
}

// ── --package flag ────────────────────────────────────────────────────────────

func TestGenTypesGoPackageFlag(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	got, err := runGen(t, gForServer(srv), []string{"go", "--project=test-ref", "--package=mydb"})
	if err != nil {
		t.Fatalf("go --package=mydb: %v", err)
	}
	if !strings.Contains(got, "package mydb") {
		t.Errorf("expected 'package mydb', got:\n%s", got)
	}
}

// ── unknown flag ──────────────────────────────────────────────────────────────

func TestGenTypesUnknownFlag(t *testing.T) {
	t.Parallel()
	g := &globalFlags{token: "tok", apiURL: "http://127.0.0.1:0"}
	_, err := runGen(t, g, []string{"typescript", "--project=ref", "--no-such-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag")
	}
}

// ── language aliases ──────────────────────────────────────────────────────────

func TestGenTypesLangAlias(t *testing.T) {
	t.Parallel()
	srv := buildSchemaServer(t)
	defer srv.Close()

	// "ts" is an alias for "typescript".
	got, err := runGen(t, gForServer(srv), []string{"ts", "--project=test-ref"})
	if err != nil {
		t.Fatalf("lang alias 'ts': %v", err)
	}
	if !strings.Contains(got, "export interface Database") {
		t.Errorf("'ts' alias did not emit TypeScript output")
	}

	// "py" is an alias for "python".
	got2, err2 := runGen(t, gForServer(srv), []string{"py", "--project=test-ref"})
	if err2 != nil {
		t.Fatalf("lang alias 'py': %v", err2)
	}
	if !strings.Contains(got2, "class Orders(BaseModel)") {
		t.Errorf("'py' alias did not emit Python output")
	}
}
