// integration_test — drive the CLI dispatch path against an in-memory
// httptest.Server that mocks the four v1 endpoints the CLI actually
// hits during a typical session.
//
// We don't compile the binary in subprocess form here: instead we
// invoke `run([]string{...})` directly, which is the same entrypoint
// `main()` uses. Tests reset os.Stdout / os.Stderr around each call
// so we can capture the rendered output and assert against it.
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// mockServer bundles the four endpoints into one handler.
func mockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/v1/user", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user": map[string]any{
				"id":    "01H...",
				"email": "pc@example.com",
				"name":  "pc",
			},
		})
	})
	mux.HandleFunc("/v1/orgs", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"orgs": []map[string]any{
				{"id": "o1", "slug": "acme", "name": "Acme", "plan": "free"},
			},
		})
	})
	mux.HandleFunc("/v1/orgs/acme/projects", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"projects": []map[string]any{
				{"id": "p1", "ref": "abcd", "name": "demo", "region": "jnb", "status": "active"},
			},
		})
	})
	mux.HandleFunc("/v1/projects/abcd/sql/query", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(QueryResult{
			Columns:   []string{"n"},
			Rows:      [][]any{{float64(1)}},
			ElapsedMs: 2,
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// captureStdout swaps os.Stdout with an os.Pipe and returns a func
// that restores stdout and reads everything that was written.
//
// Also registers a t.Cleanup that restores os.Stdout unconditionally
// so a test that fatals (or otherwise skips the read() call) doesn't
// leave os.Stdout pointing at a closed pipe — which would surface as
// "write |1: broken pipe" in the next test that touches stdout.
func captureStdout(t *testing.T) (restore func() string) {
	t.Helper()
	pr, pw, _ := os.Pipe()
	prev := os.Stdout
	os.Stdout = pw
	restored := false
	t.Cleanup(func() {
		if !restored {
			_ = pw.Close()
			os.Stdout = prev
		}
	})
	return func() string {
		if restored {
			return ""
		}
		restored = true
		_ = pw.Close()
		b, _ := io.ReadAll(pr)
		os.Stdout = prev
		return string(b)
	}
}

func TestRunWhoamiJSON(t *testing.T) {
	withTempConfigDir(t)
	srv := mockServer(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
	read := captureStdout(t)
	if err := run([]string{"--json", "whoami"}); err != nil {
		t.Fatalf("run whoami: %v", err)
	}
	out := read()
	var got struct {
		User *User  `json:"user"`
		Orgs []*Org `json:"orgs"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode whoami: %v\nbody=%s", err, out)
	}
	if got.User == nil || got.User.Email != "pc@example.com" {
		t.Errorf("user: %+v", got.User)
	}
	if len(got.Orgs) != 1 || got.Orgs[0].Slug != "acme" {
		t.Errorf("orgs: %+v", got.Orgs)
	}
}

func TestRunProjectsListJSON(t *testing.T) {
	withTempConfigDir(t)
	srv := mockServer(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
	read := captureStdout(t)
	if err := run([]string{"--json", "projects", "list", "--org=acme"}); err != nil {
		t.Fatalf("projects list: %v", err)
	}
	out := read()
	var got struct {
		Projects []*Project `json:"projects"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode projects: %v\nbody=%s", err, out)
	}
	if len(got.Projects) != 1 || got.Projects[0].Ref != "abcd" {
		t.Errorf("projects: %+v", got.Projects)
	}
}

func TestRunSQLInlineJSON(t *testing.T) {
	withTempConfigDir(t)
	srv := mockServer(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
	read := captureStdout(t)
	if err := run([]string{"--json", "sql", "--project=abcd", "-e", "SELECT 1"}); err != nil {
		t.Fatalf("sql: %v", err)
	}
	out := read()
	var got QueryResult
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode sql: %v\nbody=%s", err, out)
	}
	if len(got.Rows) != 1 {
		t.Errorf("rows: %+v", got.Rows)
	}
}

// TestVersionNoNetwork confirms `basin --help` and `basin version`
// run without any HTTP setup. This is the "must run --help cleanly
// with zero network access" non-negotiable.
func TestVersionAndHelpNoNetwork(t *testing.T) {
	withTempConfigDir(t)
	t.Setenv("BASIN_TOKEN", "")
	t.Setenv("BASIN_API", "http://127.0.0.1:1")
	read := captureStdout(t)
	if err := run([]string{"version"}); err != nil {
		t.Fatalf("version: %v", err)
	}
	out := read()
	if !strings.Contains(out, "basin ") {
		t.Errorf("version output missing prefix: %q", out)
	}
	read = captureStdout(t)
	if err := run([]string{"--help"}); err != nil {
		t.Fatalf("--help: %v", err)
	}
	if !strings.Contains(read(), "USAGE") {
		t.Errorf("help output missing USAGE")
	}
}
