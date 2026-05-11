// cmd_sql_test — verify the input source ladder for `basin sql`:
// inline (-e) wins over file (-f); empty input is rejected; stdin
// pipe is honoured when neither flag is set.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// stubSQLServer captures the SQL the client posted and returns a
// canned QueryResult.
func stubSQLServer(t *testing.T, capturedSQL *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/projects/p1/sql/query" {
			t.Errorf("path: got %s", r.URL.Path)
		}
		var body struct {
			SQL string `json:"sql"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if capturedSQL != nil {
			*capturedSQL = body.SQL
		}
		_ = json.NewEncoder(w).Encode(QueryResult{
			Columns:   []string{"x"},
			Rows:      [][]any{{float64(1)}},
			ElapsedMs: 1,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestSQLInlineExec(t *testing.T) {
	var got string
	srv := stubSQLServer(t, &got)
	withTempConfigDir(t)
	g := &globalFlags{apiURL: srv.URL, token: "tok"}
	if err := cmdSQL(g, []string{"--project=p1", "-e", "SELECT 1"}); err != nil {
		t.Fatalf("cmdSQL: %v", err)
	}
	if got != "SELECT 1" {
		t.Errorf("captured SQL: got %q", got)
	}
}

func TestSQLFileSource(t *testing.T) {
	var got string
	srv := stubSQLServer(t, &got)
	withTempConfigDir(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "q.sql")
	if err := os.WriteFile(path, []byte("SELECT 42"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	g := &globalFlags{apiURL: srv.URL, token: "tok"}
	if err := cmdSQL(g, []string{"--project=p1", "-f", path}); err != nil {
		t.Fatalf("cmdSQL: %v", err)
	}
	if got != "SELECT 42" {
		t.Errorf("captured SQL: got %q", got)
	}
}

func TestSQLStdinPipe(t *testing.T) {
	var got string
	srv := stubSQLServer(t, &got)
	withTempConfigDir(t)

	// Replace stdin with a pipe carrying the query body so the cmd
	// reads from it; restore afterwards. We use a real pipe here
	// because readAllStdin checks ModeCharDevice and would skip a
	// regular file.
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	if _, err := io.WriteString(pw, "SELECT 7"); err != nil {
		t.Fatalf("write pipe: %v", err)
	}
	pw.Close()
	prev := os.Stdin
	os.Stdin = pr
	t.Cleanup(func() { os.Stdin = prev })

	g := &globalFlags{apiURL: srv.URL, token: "tok"}
	if err := cmdSQL(g, []string{"--project=p1"}); err != nil {
		t.Fatalf("cmdSQL: %v", err)
	}
	if got != "SELECT 7" {
		t.Errorf("captured SQL: got %q", got)
	}
}

func TestSQLRejectsEmptyInput(t *testing.T) {
	withTempConfigDir(t)
	// No -e, no -f, no stdin pipe. We can't easily simulate "no pipe"
	// (stdin in tests already isn't a tty) — but we can pipe an
	// explicitly-empty payload and confirm we error.
	pr, pw, _ := os.Pipe()
	pw.Close() // empty
	prev := os.Stdin
	os.Stdin = pr
	t.Cleanup(func() { os.Stdin = prev })
	g := &globalFlags{apiURL: "http://127.0.0.1:1", token: "tok"}
	err := cmdSQL(g, []string{"--project=p1"})
	if err == nil {
		t.Fatalf("expected error on empty SQL, got nil")
	}
}

// TestSQLPriorityInlineBeatsFile sanity-checks the priority ladder:
// when -e is set we never read -f.
func TestSQLPriorityInlineBeatsFile(t *testing.T) {
	var got string
	srv := stubSQLServer(t, &got)
	withTempConfigDir(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "q.sql")
	_ = os.WriteFile(path, []byte("FROM_FILE"), 0o644)
	g := &globalFlags{apiURL: srv.URL, token: "tok"}
	if err := cmdSQL(g, []string{"--project=p1", "-e", "INLINE", "-f", path}); err != nil {
		t.Fatalf("cmdSQL: %v", err)
	}
	if got != "INLINE" {
		t.Errorf("expected INLINE, got %q", got)
	}
}

// TestRequireProjectFlag confirms the validation gate.
func TestRequireProjectFlag(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{apiURL: "http://127.0.0.1:1", token: "tok"}
	if err := cmdSQL(g, []string{"-e", "SELECT 1"}); err == nil {
		t.Errorf("expected error without --project")
	}
}

// keep imports referenced even when individual tests are commented.
var _ = bytes.NewBuffer
var _ = context.Background
