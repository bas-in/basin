// cmd_migrations_test — coverage for the migrations subcommand.
//
// All cloud-hitting tests use an httptest.Server stub. The stub asserts
// on request method and path. The "new" sub-command is pure local (no
// network) and uses a temp directory.
//
// Tests call cmdMigrations() / cmdMigrationsList() / etc. directly rather
// than run() because "migrations" may not be registered in commands() in
// the dispatcher session's main.go at this point. This matches the pattern
// used in cmd_branches_test.go and other cmd_*_test.go files.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

// migStubJSON writes v as a JSON response to w with a 200 status.
func migStubJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// migStubError writes a cloud-style error envelope.
func migStubError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg},
	})
}

// migServer builds an httptest.Server from a mux and registers cleanup.
func migServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// migGlobal returns a globalFlags pointed at the test server.
func migGlobal(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "tok-test"}
}

// ── migrations new ────────────────────────────────────────────────────

func TestMigrationsNewSlugNormalization(t *testing.T) {
	cases := []struct {
		input    string
		wantSlug string
	}{
		{"add users table", "add_users_table"},
		{"Add Users Table", "add_users_table"},
		{"hello world", "hello_world"},
		{"foo--bar", "foo_bar"},
		{"  spaces  ", "spaces"},
		{"special!@#chars", "special_chars"},
		{"__leading trailing__", "leading_trailing"},
		{"123 numeric start", "123_numeric_start"},
		{"café résumé", "caf_r_sum"},     // non-ASCII collapsed to _
		{"a___b", "a_b"},                 // runs collapsed
	}
	for _, tc := range cases {
		got := toSlug(tc.input)
		if got != tc.wantSlug {
			t.Errorf("toSlug(%q) = %q, want %q", tc.input, got, tc.wantSlug)
		}
	}
}

func TestMigrationsNewFileWritten(t *testing.T) {
	dir := chdirTemp(t)
	withTempConfigDir(t)

	g := &globalFlags{}
	read := captureStdout(t)
	err := cmdMigrationsNew(g, []string{"add users table"})
	out := read()

	if err != nil {
		t.Fatalf("migrations new: %v", err)
	}
	// stdout should contain the path
	path := strings.TrimSpace(out)
	if !filepath.IsAbs(path) {
		t.Errorf("expected absolute path in stdout, got: %q", path)
	}
	if !strings.HasSuffix(path, "_add_users_table.sql") {
		t.Errorf("expected filename to end with _add_users_table.sql, got: %q", path)
	}

	// File should exist and be inside <dir>/basin/migrations/.
	// Use EvalSymlinks to handle macOS /var → /private/var indirection.
	realDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		realDir = dir
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		realPath = path
	}
	migrDir := filepath.Join(realDir, "basin", "migrations")
	if !strings.HasPrefix(realPath, migrDir) {
		t.Errorf("expected path under %s, got %q", migrDir, realPath)
	}

	content, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read migration file: %v", readErr)
	}
	body := string(content)
	if !strings.Contains(body, "-- migration: add users table") {
		t.Errorf("missing migration header, got: %q", body)
	}
	if !strings.Contains(body, "-- created:") {
		t.Errorf("missing created header, got: %q", body)
	}
}

func TestMigrationsNewCollisionBump(t *testing.T) {
	dir := chdirTemp(t)
	withTempConfigDir(t)

	migrDir := filepath.Join(dir, "basin", "migrations")
	if err := os.MkdirAll(migrDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	g := &globalFlags{}

	// Create first file.
	read1 := captureStdout(t)
	if err := cmdMigrationsNew(g, []string{"bump test"}); err != nil {
		t.Fatalf("first new: %v", err)
	}
	path1 := strings.TrimSpace(read1())

	// Create second file in same second (the implementation bumps ts by 1s
	// on collision). We force a collision by directly creating a file with
	// the next expected timestamp.
	//
	// Simpler approach: just call cmdMigrationsNew twice and assert the
	// two paths are different (collision bump guarantees uniqueness).
	read2 := captureStdout(t)
	if err := cmdMigrationsNew(g, []string{"bump test"}); err != nil {
		t.Fatalf("second new: %v", err)
	}
	path2 := strings.TrimSpace(read2())

	if path1 == path2 {
		t.Errorf("expected different paths for collision bump, both got: %q", path1)
	}
	// Both should still end in the same slug.
	if !strings.HasSuffix(path1, "_bump_test.sql") {
		t.Errorf("path1 slug wrong: %q", path1)
	}
	if !strings.HasSuffix(path2, "_bump_test.sql") {
		t.Errorf("path2 slug wrong: %q", path2)
	}
}

func TestMigrationsNewJSONOutput(t *testing.T) {
	chdirTemp(t)
	withTempConfigDir(t)

	g := &globalFlags{json: true}
	read := captureStdout(t)
	if err := cmdMigrationsNew(g, []string{"json output test"}); err != nil {
		t.Fatalf("migrations new --json: %v", err)
	}
	out := read()

	var got struct {
		Path      string `json:"path"`
		Name      string `json:"name"`
		Slug      string `json:"slug"`
		Timestamp int64  `json:"timestamp"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode --json output: %v\nbody=%s", err, out)
	}
	if got.Path == "" {
		t.Errorf("path is empty")
	}
	if got.Name != "json output test" {
		t.Errorf("name: got %q, want %q", got.Name, "json output test")
	}
	if got.Slug != "json_output_test" {
		t.Errorf("slug: got %q, want %q", got.Slug, "json_output_test")
	}
	if got.Timestamp == 0 {
		t.Errorf("timestamp should be non-zero")
	}
}

func TestMigrationsNewMissingName(t *testing.T) {
	g := &globalFlags{}
	err := cmdMigrationsNew(g, []string{})
	if err == nil {
		t.Fatal("expected error for missing name")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage error, got: %v", err)
	}
}

// ── migrations list (new /v1/ path) ──────────────────────────────────

func TestMigrationsListV1Path(t *testing.T) {
	var gotMethod, gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		migStubJSON(w, map[string]any{
			"migrations": []any{
				map[string]any{
					"id":      "m1",
					"at":      "2026-01-01T00:00:00Z",
					"op":      "CREATE_TABLE",
					"table":   "users",
					"summary": "create users table",
				},
			},
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsList(g, []string{"--project=proj-abc"})
	out := read()

	if err != nil {
		t.Fatalf("migrations list: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("expected GET, got %s", gotMethod)
	}
	if gotPath != "/admin/v1/projects/proj-abc/migrations" {
		t.Errorf("expected /admin/v1/projects/proj-abc/migrations, got %s", gotPath)
	}
	if !strings.Contains(out, "m1") {
		t.Errorf("expected migration id in output, got: %q", out)
	}
}

func TestMigrationsListV1PathJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations", func(w http.ResponseWriter, r *http.Request) {
		migStubJSON(w, map[string]any{
			"migrations": []any{
				map[string]any{"id": "m1", "op": "CREATE_TABLE"},
			},
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	g.json = true
	read := captureStdout(t)
	err := cmdMigrationsList(g, []string{"--project=proj-abc"})
	out := read()

	if err != nil {
		t.Fatalf("migrations list --json: %v", err)
	}
	var got struct {
		Migrations []struct {
			ID string `json:"id"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, out)
	}
	if len(got.Migrations) != 1 || got.Migrations[0].ID != "m1" {
		t.Errorf("unexpected migrations: %+v", got.Migrations)
	}
}

// ── migrations apply (new /v1/ path) ─────────────────────────────────

func TestMigrationsApplyV1Path(t *testing.T) {
	var gotMethod, gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m1/rollback", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		migStubJSON(w, map[string]any{"applied": true})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsApply(g, []string{"--project=proj-abc", "m1"})
	out := read()

	if err != nil {
		t.Fatalf("migrations apply: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("expected POST, got %s", gotMethod)
	}
	if gotPath != "/admin/v1/projects/proj-abc/migrations/m1/rollback" {
		t.Errorf("expected /v1/.../m1/rollback, got %s", gotPath)
	}
	if !strings.Contains(out, "m1") {
		t.Errorf("expected migration id in output, got: %q", out)
	}
}

// ── migrations get ────────────────────────────────────────────────────

func TestMigrationsGet200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m42", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		migStubJSON(w, map[string]any{
			"id":         "m42",
			"status":     "applied",
			"applied_at": "2026-03-01T12:00:00Z",
			"sql":        "CREATE TABLE foo (id bigint primary key);",
			"op":         "CREATE_TABLE",
			"table":      "foo",
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsGet(g, []string{"--project=proj-abc", "m42"})
	out := read()

	if err != nil {
		t.Fatalf("migrations get: %v", err)
	}
	if !strings.Contains(out, "m42") {
		t.Errorf("expected id in output, got: %q", out)
	}
	if !strings.Contains(out, "applied") {
		t.Errorf("expected status in output, got: %q", out)
	}
	if !strings.Contains(out, "CREATE TABLE foo") {
		t.Errorf("expected SQL in output, got: %q", out)
	}
}

func TestMigrationsGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/notexist", func(w http.ResponseWriter, r *http.Request) {
		migStubError(w, http.StatusNotFound, "not_found", "migration not found")
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	err := cmdMigrationsGet(g, []string{"--project=proj-abc", "notexist"})
	if err == nil {
		t.Fatal("expected error for 404")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected 404, got %d", ae.HTTPStatus)
	}
}

func TestMigrationsGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m7", func(w http.ResponseWriter, r *http.Request) {
		migStubJSON(w, map[string]any{
			"id":         "m7",
			"status":     "applied",
			"applied_at": "2026-04-01T00:00:00Z",
			"sql":        "SELECT 1;",
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	g.json = true
	read := captureStdout(t)
	err := cmdMigrationsGet(g, []string{"--project=proj-abc", "m7"})
	out := read()

	if err != nil {
		t.Fatalf("migrations get --json: %v", err)
	}
	var got MigrationDetail
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, out)
	}
	if got.ID != "m7" {
		t.Errorf("id: got %q, want m7", got.ID)
	}
	if got.Status != "applied" {
		t.Errorf("status: got %q, want applied", got.Status)
	}
}

func TestMigrationsGetMissingID(t *testing.T) {
	withTempConfigDir(t)
	mux := http.NewServeMux()
	srv := migServer(t, mux)
	g := migGlobal(srv)
	err := cmdMigrationsGet(g, []string{"--project=proj-abc"})
	if err == nil {
		t.Fatal("expected error for missing id")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage error, got: %v", err)
	}
}

// ── migrations diff ───────────────────────────────────────────────────

func TestMigrationsDiffNoDiff(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		migStubJSON(w, map[string]any{})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsDiff(g, []string{"--project=proj-abc"})
	out := read()

	if err != nil {
		t.Fatalf("migrations diff: %v", err)
	}
	if !strings.Contains(out, "(no diff)") {
		t.Errorf("expected '(no diff)', got: %q", out)
	}
}

func TestMigrationsDiffRealDiff(t *testing.T) {
	diffText := "--- a/schema.sql\n+++ b/schema.sql\n@@ -0,0 +1 @@\n+CREATE TABLE bar (id bigint);\n"
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		migStubJSON(w, map[string]any{
			"diff": diffText,
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsDiff(g, []string{"--project=proj-abc"})
	out := read()

	if err != nil {
		t.Fatalf("migrations diff: %v", err)
	}
	if !strings.Contains(out, "CREATE TABLE bar") {
		t.Errorf("expected diff content in output, got: %q", out)
	}
}

func TestMigrationsDiffErrorMapping(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		migStubError(w, http.StatusInternalServerError, "diff_error", "engine unavailable")
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	err := cmdMigrationsDiff(g, []string{"--project=proj-abc"})
	if err == nil {
		t.Fatal("expected error from server error")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", ae.HTTPStatus)
	}
}

func TestMigrationsDiffJSONPassthrough(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		migStubJSON(w, map[string]any{
			"diff":    "--- a\n+++ b\n",
			"changes": []any{"add bar table"},
		})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	g.json = true
	read := captureStdout(t)
	err := cmdMigrationsDiff(g, []string{"--project=proj-abc"})
	out := read()

	if err != nil {
		t.Fatalf("migrations diff --json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, out)
	}
	if _, ok := got["diff"]; !ok {
		t.Errorf("expected 'diff' key in JSON output")
	}
}

// ── migrations rollback ───────────────────────────────────────────────

func TestMigrationsRollbackConfirmed(t *testing.T) {
	var gotMethod, gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m99/rollback", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		migStubJSON(w, map[string]any{"rolled_back": true, "migration_id": "m99"})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	read := captureStdout(t)
	// Use --yes to skip the prompt.
	err := cmdMigrationsRollback(g, []string{"--project=proj-abc", "--yes", "m99"})
	out := read()

	if err != nil {
		t.Fatalf("migrations rollback: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("expected POST, got %s", gotMethod)
	}
	if gotPath != "/admin/v1/projects/proj-abc/migrations/m99/rollback" {
		t.Errorf("expected /v1/.../m99/rollback, got %s", gotPath)
	}
	if !strings.Contains(out, "m99") {
		t.Errorf("expected migration id in output, got: %q", out)
	}
}

func TestMigrationsRollbackDeclined(t *testing.T) {
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m99/rollback", func(w http.ResponseWriter, r *http.Request) {
		called = true
		migStubJSON(w, map[string]any{"rolled_back": true})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	// Pipe "n" into stdin to decline.
	pr, pw, _ := os.Pipe()
	prev := os.Stdin
	os.Stdin = pr
	_, _ = pw.WriteString("n\n")
	_ = pw.Close()
	defer func() { os.Stdin = prev }()

	g := migGlobal(srv)
	read := captureStdout(t)
	err := cmdMigrationsRollback(g, []string{"--project=proj-abc", "m99"})
	out := read()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if called {
		t.Error("expected no HTTP call when rollback is declined")
	}
	if !strings.Contains(out, "aborted") {
		t.Errorf("expected 'aborted' in output, got: %q", out)
	}
}

func TestMigrationsRollback409(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m99/rollback", func(w http.ResponseWriter, r *http.Request) {
		migStubError(w, http.StatusConflict, "mid_flight", "migration in progress")
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	g := migGlobal(srv)
	// Use --yes to skip prompt.
	err := cmdMigrationsRollback(g, []string{"--project=proj-abc", "--yes", "m99"})
	if err == nil {
		t.Fatal("expected error for 409")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != http.StatusConflict {
		t.Errorf("expected 409, got %d", ae.HTTPStatus)
	}
	if ae.Code != "mid_flight" {
		t.Errorf("expected code 'mid_flight', got %q", ae.Code)
	}
}

func TestMigrationsRollbackJSONSkipsPrompt(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj-abc/migrations/m55/rollback", func(w http.ResponseWriter, r *http.Request) {
		migStubJSON(w, map[string]any{"rolled_back": true, "migration_id": "m55"})
	})
	srv := migServer(t, mux)
	withTempConfigDir(t)

	// --json skips the prompt; stdin is untouched.
	g := migGlobal(srv)
	g.json = true
	read := captureStdout(t)
	err := cmdMigrationsRollback(g, []string{"--project=proj-abc", "m55"})
	out := read()

	if err != nil {
		t.Fatalf("migrations rollback --json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode: %v\nbody=%s", err, out)
	}
	if v, _ := got["migration_id"].(string); v != "m55" {
		t.Errorf("expected migration_id=m55, got: %v", got["migration_id"])
	}
}

// ── project ref resolution ────────────────────────────────────────────

func TestMigrationsListResolvesProjectFromConfig(t *testing.T) {
	dir := chdirTemp(t)
	withTempConfigDir(t)

	// Write a linked project config.
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "linked-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/linked-proj/migrations", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		migStubJSON(w, map[string]any{"migrations": []any{}})
	})
	srv := migServer(t, mux)

	g := migGlobal(srv)
	read := captureStdout(t)
	// No --project flag; should auto-resolve from config.toml.
	err := cmdMigrationsList(g, []string{})
	_ = read()

	if err != nil {
		t.Fatalf("migrations list (auto-resolve): %v", err)
	}
	if gotPath != "/admin/v1/projects/linked-proj/migrations" {
		t.Errorf("expected /admin/v1/projects/linked-proj/migrations, got %s", gotPath)
	}
}

func TestMigrationsListNoProjectError(t *testing.T) {
	// No --project and no config.toml.
	dir := chdirTemp(t)
	withTempConfigDir(t)
	_ = dir

	mux := http.NewServeMux()
	srv := migServer(t, mux)
	g := migGlobal(srv)
	err := cmdMigrationsList(g, []string{})
	if err == nil {
		t.Fatal("expected error when no project is set")
	}
	// Should mention basin link or --project.
	if !strings.Contains(err.Error(), "basin link") && !strings.Contains(err.Error(), "--project") {
		t.Errorf("expected error to mention 'basin link' or '--project', got: %v", err)
	}
}
