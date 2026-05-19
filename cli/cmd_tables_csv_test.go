// cmd_tables_csv_test — coverage for the write-side tables sub-handlers:
// create, alter, drop, import-csv, export-csv, columns add/alter/drop.
//
// Tests drive cmdTables() directly (not run()) so they stay green even
// before the top-level dispatch table is extended. Confirmation prompts
// are bypassed via --yes or piped stdin strings (pipeStdin is declared in
// cmd_backups_test.go, same package).
//
// Flag ordering convention: flags come BEFORE the positional table name,
// matching how flag.FlagSet stops at the first non-flag argument and
// leaves it in fs.Args() for the handler to consume.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────

// tablesServer creates an httptest.Server and returns it.
func tablesServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// setTablesEnv wires the test server URL and a fake token.
func setTablesEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

// tFlag returns globalFlags pointed at srv.
func tFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// tFlagJSON returns globalFlags with --json mode on.
func tFlagJSON(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
}

// stubTablesJSON is a convenience wrapper matching stubJSON from branches.
func stubTablesJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// ── parseColumnSpec unit tests ─────────────────────────────────────────────

func TestParseColumnSpecSimple(t *testing.T) {
	cs, err := parseColumnSpec("email:text")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cs.Name != "email" {
		t.Errorf("name: got %q, want %q", cs.Name, "email")
	}
	if cs.Type != "text" {
		t.Errorf("type: got %q, want %q", cs.Type, "text")
	}
	if cs.Nullable != nil {
		t.Errorf("nullable should be nil (unset)")
	}
	if cs.PK {
		t.Errorf("pk should be false")
	}
}

func TestParseColumnSpecWithPKAndNullable(t *testing.T) {
	cs, err := parseColumnSpec("id:bigint,pk=true,nullable=false")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cs.Name != "id" || cs.Type != "bigint" {
		t.Errorf("name/type wrong: %q %q", cs.Name, cs.Type)
	}
	if !cs.PK {
		t.Errorf("pk should be true")
	}
	if cs.Nullable == nil || *cs.Nullable {
		t.Errorf("nullable should be false")
	}
}

func TestParseColumnSpecWithDefault(t *testing.T) {
	cs, err := parseColumnSpec("created_at:timestamptz,default=now()")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cs.Default == nil || *cs.Default != "now()" {
		t.Errorf("default: got %v, want now()", cs.Default)
	}
}

func TestParseColumnSpecEmpty(t *testing.T) {
	_, err := parseColumnSpec("")
	if err == nil {
		t.Fatal("expected error for empty spec")
	}
}

func TestParseColumnSpecMissingColon(t *testing.T) {
	_, err := parseColumnSpec("nocohere")
	if err == nil {
		t.Fatal("expected error for missing colon")
	}
}

func TestParseColumnSpecEmptyName(t *testing.T) {
	_, err := parseColumnSpec(":text")
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestParseColumnSpecEmptyType(t *testing.T) {
	_, err := parseColumnSpec("col:")
	if err == nil {
		t.Fatal("expected error for empty type")
	}
}

// ── tables create ─────────────────────────────────────────────────────────

func TestTablesCreateHappyPath(t *testing.T) {
	var gotSQL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotSQL, _ = body["sql"].(string)
		w.WriteHeader(http.StatusCreated)
		stubTablesJSON(w, map[string]any{"created": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	// Flags come before the positional table name so flag.FlagSet parses them.
	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"create",
		"--project=proj1",
		"--column=id:bigint,pk=true,nullable=false",
		"--column=email:text,nullable=false",
		"--column=created_at:timestamptz,default=now()",
		"users",
	})
	out := read()
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !strings.Contains(out, "Created") {
		t.Errorf("expected 'Created' in output, got %q", out)
	}
	if !strings.Contains(gotSQL, "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE in SQL, got %q", gotSQL)
	}
	if !strings.Contains(gotSQL, "PRIMARY KEY") {
		t.Errorf("expected PRIMARY KEY in SQL, got %q", gotSQL)
	}
}

func TestTablesCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubTablesJSON(w, map[string]any{"created": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"create", "--project=proj1", "--column=id:bigint", "items",
	})
	out := read()
	if err != nil {
		t.Fatalf("create --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON output: %v\nbody=%s", err, out)
	}
	if result["created"] != true {
		t.Errorf("JSON shape: expected created=true, got %v", result)
	}
}

func TestTablesCreateMissingColumn(t *testing.T) {
	g := &globalFlags{apiURL: "http://unused", token: "tok"}
	// No --column flag at all.
	err := cmdTables(g, []string{"create", "--project=p1", "foo"})
	if err == nil || !strings.Contains(err.Error(), "--column") {
		t.Fatalf("expected error about --column, got %v", err)
	}
}

func TestTablesCreateMalformedColumnSpec(t *testing.T) {
	g := &globalFlags{apiURL: "http://unused", token: "tok"}
	err := cmdTables(g, []string{"create", "--project=p1", "--column=badspec", "foo"})
	if err == nil {
		t.Fatal("expected error for malformed column spec")
	}
}

// ── tables alter ──────────────────────────────────────────────────────────

func TestTablesAlterAddOnly(t *testing.T) {
	var gotSQL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotSQL, _ = body["sql"].(string)
		stubTablesJSON(w, map[string]any{"altered": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"alter", "--project=proj1", "--add-column=bio:text", "users",
	})
	out := read()
	if err != nil {
		t.Fatalf("alter: %v", err)
	}
	if !strings.Contains(out, "Altered") {
		t.Errorf("expected 'Altered' in output, got %q", out)
	}
	if !strings.Contains(gotSQL, "ADD COLUMN") {
		t.Errorf("expected ADD COLUMN in SQL, got %q", gotSQL)
	}
}

func TestTablesAlterDropOnly(t *testing.T) {
	var gotSQL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotSQL, _ = body["sql"].(string)
		stubTablesJSON(w, map[string]any{"altered": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	err := cmdTables(tFlag(srv), []string{
		"alter", "--project=proj1", "--drop-column=old_field", "users",
	})
	if err != nil {
		t.Fatalf("alter drop-only: %v", err)
	}
	if !strings.Contains(gotSQL, "DROP COLUMN") {
		t.Errorf("expected DROP COLUMN in SQL, got %q", gotSQL)
	}
}

func TestTablesAlterBoth(t *testing.T) {
	var gotSQL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotSQL, _ = body["sql"].(string)
		stubTablesJSON(w, map[string]any{"altered": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	err := cmdTables(tFlag(srv), []string{
		"alter", "--project=proj1",
		"--add-column=score:int",
		"--drop-column=old_col",
		"users",
	})
	if err != nil {
		t.Fatalf("alter both: %v", err)
	}
	if !strings.Contains(gotSQL, "ADD COLUMN") || !strings.Contains(gotSQL, "DROP COLUMN") {
		t.Errorf("SQL should contain both ADD and DROP, got %q", gotSQL)
	}
}

// ── tables drop ───────────────────────────────────────────────────────────

func TestTablesDropWithYes(t *testing.T) {
	var gotConfirmName string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		gotConfirmName = r.URL.Query().Get("confirm_name")
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"drop", "--project=proj1", "--yes", "orders",
	})
	out := read()
	if err != nil {
		t.Fatalf("drop: %v", err)
	}
	if !strings.Contains(out, "Dropped") {
		t.Errorf("expected 'Dropped' in output, got %q", out)
	}
	if gotConfirmName != "orders" {
		t.Errorf("confirm_name: got %q, want %q", gotConfirmName, "orders")
	}
}

func TestTablesDropDeclined(t *testing.T) {
	mux := http.NewServeMux()
	called := false
	mux.HandleFunc("/v1/projects/proj1/tables/orders", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	restore := pipeStdin("n")
	defer restore()

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{"drop", "--project=proj1", "orders"})
	out := read()
	if err != nil {
		t.Fatalf("drop declined: unexpected error: %v", err)
	}
	if called {
		t.Error("DELETE should not have been called after declining")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output, got %q", out)
	}
}

func TestTablesDropJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/orders", func(w http.ResponseWriter, r *http.Request) {
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"drop", "--project=proj1", "--yes", "orders",
	})
	out := read()
	if err != nil {
		t.Fatalf("drop --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\nbody=%s", err, out)
	}
	if result["dropped"] != true {
		t.Errorf("JSON shape: expected dropped=true, got %v", result)
	}
}

// ── tables import-csv ─────────────────────────────────────────────────────

func TestTablesImportCSVFromStdin(t *testing.T) {
	var gotContentType string
	var gotFileContent string
	var gotHeaderField string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/import-csv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		gotContentType = r.Header.Get("Content-Type")

		// Parse multipart to validate form fields.
		mediaType, params, _ := mime.ParseMediaType(gotContentType)
		if mediaType != "multipart/form-data" {
			t.Errorf("Content-Type not multipart/form-data, got %q", mediaType)
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			b, _ := io.ReadAll(p)
			switch p.FormName() {
			case "file":
				gotFileContent = string(b)
			case "header":
				gotHeaderField = string(b)
			}
		}

		stubTablesJSON(w, map[string]any{
			"rows_imported": 2,
			"rows_rejected": 0,
		})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	// Write CSV to a pipe and set it as stdin.
	csvData := "name,age\nAlice,30\nBob,25\n"
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprint(pw, csvData)
	_ = pw.Close()
	defer func() { os.Stdin = prevStdin }()

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"import-csv", "--project=proj1", "users",
	})
	out := read()
	if err != nil {
		t.Fatalf("import-csv: %v", err)
	}
	if !strings.Contains(out, "Imported") {
		t.Errorf("expected 'Imported' in output, got %q", out)
	}
	if !strings.Contains(gotContentType, "multipart/form-data") {
		t.Errorf("Content-Type should be multipart/form-data, got %q", gotContentType)
	}
	if gotHeaderField != "true" {
		t.Errorf("header form field: got %q, want %q", gotHeaderField, "true")
	}
	if gotFileContent != csvData {
		t.Errorf("file content mismatch:\ngot  %q\nwant %q", gotFileContent, csvData)
	}
}

func TestTablesImportCSVFromFile(t *testing.T) {
	csvContent := "id,name\n1,Widget\n2,Gadget\n"
	tmp := t.TempDir()
	csvPath := filepath.Join(tmp, "data.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0o644); err != nil {
		t.Fatal(err)
	}

	var gotFileBytes []byte
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/products/import-csv", func(w http.ResponseWriter, r *http.Request) {
		mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mediaType != "multipart/form-data" {
			t.Errorf("expected multipart/form-data, got %q", mediaType)
		}
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			if p.FormName() == "file" {
				gotFileBytes, _ = io.ReadAll(p)
			}
		}
		stubTablesJSON(w, map[string]any{"rows_imported": 2, "rows_rejected": 0})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	err := cmdTables(tFlag(srv), []string{
		"import-csv", "--project=proj1", "--file=" + csvPath, "--header=true", "products",
	})
	if err != nil {
		t.Fatalf("import-csv --file: %v", err)
	}
	if string(gotFileBytes) != csvContent {
		t.Errorf("file content mismatch:\ngot  %q\nwant %q", gotFileBytes, csvContent)
	}
}

func TestTablesImportCSVJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/t1/import-csv", func(w http.ResponseWriter, r *http.Request) {
		// Drain the body so the client doesn't get a broken pipe.
		_, _ = io.Copy(io.Discard, r.Body)
		stubTablesJSON(w, map[string]any{
			"rows_imported": 5,
			"rows_rejected": 1,
			"rejections":    []string{"row 3: bad value"},
		})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	csvContent := "a,b\n1,2\n"
	tmp := t.TempDir()
	csvPath := filepath.Join(tmp, "data.csv")
	_ = os.WriteFile(csvPath, []byte(csvContent), 0o644)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"import-csv", "--project=proj1", "--file=" + csvPath, "t1",
	})
	out := read()
	if err != nil {
		t.Fatalf("import-csv --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\nbody=%s", err, out)
	}
	if result["rows_imported"] == nil {
		t.Errorf("JSON shape missing rows_imported, got %v", result)
	}
}

// ── tables export-csv ─────────────────────────────────────────────────────

func TestTablesExportCSVStreamsToStdout(t *testing.T) {
	csvBody := "id,name\n1,Alice\n2,Bob\n"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/export-csv", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		_, _ = fmt.Fprint(w, csvBody)
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"export-csv", "--project=proj1", "users",
	})
	out := read()
	if err != nil {
		t.Fatalf("export-csv: %v", err)
	}
	if out != csvBody {
		t.Errorf("stdout CSV mismatch:\ngot  %q\nwant %q", out, csvBody)
	}
}

// ── tables columns add ────────────────────────────────────────────────────

func TestTablesColumnsAddHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/columns", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubTablesJSON(w, map[string]any{"added": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"columns", "add",
		"--project=proj1",
		"--column=bio:text,nullable=true",
		"users",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns add: %v", err)
	}
	if !strings.Contains(out, "Added") {
		t.Errorf("expected 'Added' in output, got %q", out)
	}
	if gotBody["name"] != "bio" {
		t.Errorf("body name: got %v, want bio", gotBody["name"])
	}
	if gotBody["type"] != "text" {
		t.Errorf("body type: got %v, want text", gotBody["type"])
	}
}

func TestTablesColumnsAddJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/t1/columns", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubTablesJSON(w, map[string]any{"added": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"columns", "add", "--project=proj1", "--column=x:int", "t1",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns add --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\nbody=%s", err, out)
	}
	if result["added"] != true {
		t.Errorf("JSON shape: expected added=true, got %v", result)
	}
}

// ── tables columns alter ──────────────────────────────────────────────────

func TestTablesColumnsAlterHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/columns/email", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubTablesJSON(w, map[string]any{"altered": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"columns", "alter",
		"--project=proj1",
		"--name=email",
		"--nullable=false",
		"users",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns alter: %v", err)
	}
	if !strings.Contains(out, "Altered") {
		t.Errorf("expected 'Altered' in output, got %q", out)
	}
	if gotBody["nullable"] != false {
		t.Errorf("body nullable: got %v, want false", gotBody["nullable"])
	}
}

func TestTablesColumnsAlterJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/t1/columns/col1", func(w http.ResponseWriter, r *http.Request) {
		stubTablesJSON(w, map[string]any{"altered": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"columns", "alter",
		"--project=proj1",
		"--name=col1",
		"--type=bigint",
		"t1",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns alter --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\nbody=%s", err, out)
	}
	if result["altered"] != true {
		t.Errorf("JSON shape: expected altered=true, got %v", result)
	}
}

// ── tables columns drop ───────────────────────────────────────────────────

func TestTablesColumnsDropWithYes(t *testing.T) {
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/columns/bio", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"columns", "drop",
		"--project=proj1",
		"--name=bio",
		"--yes",
		"users",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns drop: %v", err)
	}
	if !called {
		t.Error("DELETE endpoint not called")
	}
	if !strings.Contains(out, "Dropped") {
		t.Errorf("expected 'Dropped' in output, got %q", out)
	}
}

func TestTablesColumnsDropDeclined(t *testing.T) {
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/columns/bio", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	restore := pipeStdin("n")
	defer restore()

	read := captureStdout(t)
	err := cmdTables(tFlag(srv), []string{
		"columns", "drop", "--project=proj1", "--name=bio", "users",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns drop declined: unexpected error: %v", err)
	}
	if called {
		t.Error("DELETE should not be called after declining")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted', got %q", out)
	}
}

func TestTablesColumnsDropJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/t1/columns/x", func(w http.ResponseWriter, r *http.Request) {
		stubTablesJSON(w, map[string]any{"dropped": true})
	})
	srv := tablesServer(t, mux)
	setTablesEnv(t, srv)

	read := captureStdout(t)
	err := cmdTables(tFlagJSON(srv), []string{
		"columns", "drop",
		"--project=proj1",
		"--name=x",
		"--yes",
		"t1",
	})
	out := read()
	if err != nil {
		t.Fatalf("columns drop --json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("parse JSON: %v\nbody=%s", err, out)
	}
	if result["dropped"] != true {
		t.Errorf("JSON shape: expected dropped=true, got %v", result)
	}
}
