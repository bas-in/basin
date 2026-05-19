// cmd_queries_test — coverage for the queries subcommand.
//
// Each test drives an httptest.Server stub that asserts on method,
// path, and (where applicable) request body + query-string.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and config_test.go — all in package main.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────

func queriesSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setQueriesEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gQueries(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleSavedQuery(id, name string) map[string]any {
	return map[string]any{
		"id":          id,
		"project_id":  "proj1",
		"name":        name,
		"sql":         "SELECT 1",
		"description": "test query",
		"public":      false,
		"created_at":  "2026-01-01T00:00:00Z",
	}
}

func sampleHistoryEntry(id string) map[string]any {
	return map[string]any{
		"id":          id,
		"project_id":  "proj1",
		"sql":         "SELECT now()",
		"duration_ms": 12.5,
		"executed_at": "2026-01-01T00:00:00Z",
	}
}

// ── queries save — happy path ─────────────────────────────────────────

func TestQueriesSaveHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/saved", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"query": sampleSavedQuery("q1", "My Query")})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"save", "--project=proj1", "--name=My Query", "--sql=SELECT 1", "--description=test"})
	out := read()
	if err != nil {
		t.Fatalf("queries save: %v", err)
	}
	if gotBody["name"] != "My Query" {
		t.Errorf("name in body: got %v, want 'My Query'", gotBody["name"])
	}
	if gotBody["sql"] != "SELECT 1" {
		t.Errorf("sql in body: got %v", gotBody["sql"])
	}
	if !strings.Contains(out, "My Query") {
		t.Errorf("expected query name in output: %q", out)
	}
}

// ── queries save — missing --name ─────────────────────────────────────

func TestQueriesSaveMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	err := cmdQueries(gQueries(srv), []string{"save", "--project=proj1", "--sql=SELECT 1"})
	if err == nil {
		t.Fatal("expected error when --name is missing")
	}
	if !strings.Contains(err.Error(), "--name") {
		t.Errorf("expected '--name' in error: %v", err)
	}
}

// ── queries save — --json shape lock ─────────────────────────────────

func TestQueriesSaveJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/saved", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"query": sampleSavedQuery("q1", "My Query")})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"save", "--project=proj1", "--name=My Query", "--sql=SELECT 1"})
	out := read()
	if err != nil {
		t.Fatalf("queries save --json: %v", err)
	}
	var got struct {
		Query *SavedQuery `json:"query"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode queries save JSON: %v\nbody=%s", err, out)
	}
	if got.Query == nil || got.Query.ID != "q1" {
		t.Errorf("query.id: got %+v", got.Query)
	}
}

// ── queries list — happy path ─────────────────────────────────────────

func TestQueriesListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/saved", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"queries": []any{
				sampleSavedQuery("q1", "Alpha"),
				sampleSavedQuery("q2", "Beta"),
			},
		})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("queries list: %v", err)
	}
	if !strings.Contains(out, "Alpha") || !strings.Contains(out, "Beta") {
		t.Errorf("expected query names in output: %q", out)
	}
}

// ── queries list — --json shape lock ─────────────────────────────────

func TestQueriesListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/saved", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"queries": []any{sampleSavedQuery("q1", "Alpha")},
		})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("queries list --json: %v", err)
	}
	var got struct {
		Queries []*SavedQuery `json:"queries"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode queries list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Queries) != 1 || got.Queries[0].ID != "q1" {
		t.Errorf("unexpected queries: %+v", got.Queries)
	}
}

// ── queries get — happy path via top-level route ──────────────────────

func TestQueriesGetHappyPath(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/q1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		stubJSON(w, map[string]any{"query": sampleSavedQuery("q1", "Alpha")})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"get", "q1"})
	out := read()
	if err != nil {
		t.Fatalf("queries get: %v", err)
	}
	if gotPath != "/v1/sql/saved/q1" {
		t.Errorf("expected top-level path /v1/sql/saved/q1, got %q", gotPath)
	}
	if !strings.Contains(out, "Alpha") {
		t.Errorf("expected query name in output: %q", out)
	}
}

// ── queries get — 404 error mapping ──────────────────────────────────

func TestQueriesGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "query not found")
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	err := cmdQueries(gQueries(srv), []string{"get", "missing"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

// ── queries get — 403 error mapping ──────────────────────────────────

func TestQueriesGet403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/priv", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "access denied")
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	err := cmdQueries(gQueries(srv), []string{"get", "priv"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if !strings.Contains(err.Error(), "private") {
		t.Errorf("expected 'private' in error: %v", err)
	}
}

// ── queries fork — happy path ─────────────────────────────────────────

func TestQueriesForkHappyPath(t *testing.T) {
	var gotBody map[string]any
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/q1/fork", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"query": sampleSavedQuery("q2", "Fork of Alpha")})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"fork", "--name=Fork of Alpha", "q1"})
	out := read()
	if err != nil {
		t.Fatalf("queries fork: %v", err)
	}
	if gotPath != "/v1/sql/saved/q1/fork" {
		t.Errorf("expected /v1/sql/saved/q1/fork, got %q", gotPath)
	}
	if gotBody["name"] != "Fork of Alpha" {
		t.Errorf("name in fork body: got %v", gotBody["name"])
	}
	if !strings.Contains(out, "q2") {
		t.Errorf("expected new query id in output: %q", out)
	}
}

// ── queries delete — with --yes ───────────────────────────────────────

func TestQueriesDeleteWithYes(t *testing.T) {
	var called bool
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/q1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		gotPath = r.URL.Path
		stubJSON(w, map[string]any{"deleted": true, "id": "q1"})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"delete", "--yes", "q1"})
	out := read()
	if err != nil {
		t.Fatalf("queries delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if gotPath != "/v1/sql/saved/q1" {
		t.Errorf("expected top-level delete path, got %q", gotPath)
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

// ── queries delete — declined ─────────────────────────────────────────

func TestQueriesDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/q1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"delete", "--project=proj1", "q1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("queries delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

// ── queries delete — --json requires --yes ────────────────────────────

func TestQueriesDeleteJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"delete", "--project=proj1", "q1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

// ── queries delete — --json shape lock ───────────────────────────────

func TestQueriesDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/sql/saved/q1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true, "id": "q1"})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"delete", "--yes", "q1"})
	out := read()
	if err != nil {
		t.Fatalf("queries delete --json: %v", err)
	}
	var got struct {
		Deleted bool   `json:"deleted"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Deleted {
		t.Errorf("deleted: expected true, got false")
	}
	if got.ID != "q1" {
		t.Errorf("id: got %q, want q1", got.ID)
	}
}

// ── queries history — list happy path ────────────────────────────────

func TestQueriesHistoryList(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		stubJSON(w, map[string]any{
			"history": []any{
				sampleHistoryEntry("h1"),
				sampleHistoryEntry("h2"),
			},
		})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"history", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("queries history list: %v", err)
	}
	if gotPath != "/v1/projects/proj1/sql/history" {
		t.Errorf("unexpected path: %q", gotPath)
	}
	if !strings.Contains(out, "h1") {
		t.Errorf("expected history entry id in output: %q", out)
	}
}

// ── queries history -- --json shape lock ─────────────────────────────

func TestQueriesHistoryJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/history", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"history": []any{sampleHistoryEntry("h1")},
		})
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"history", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("queries history --json: %v", err)
	}
	var got struct {
		History []*QueryHistoryEntry `json:"history"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode history JSON: %v\nbody=%s", err, out)
	}
	if len(got.History) != 1 || got.History[0].ID != "h1" {
		t.Errorf("unexpected history: %+v", got.History)
	}
}

// ── queries history --clear requires --yes ────────────────────────────

func TestQueriesHistoryClearRequiresYes(t *testing.T) {
	var deleteCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/history", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCalled = true
		}
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	// --clear without --yes: confirmation prompt should be shown;
	// pipe "n" to decline.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"history", "--project=proj1", "--clear"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("history clear declined: unexpected error: %v", err)
	}
	if deleteCalled {
		t.Error("DELETE should not be called when confirmation declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

// ── queries history --clear --yes happy path ─────────────────────────

func TestQueriesHistoryClearWithYes(t *testing.T) {
	var gotMethod string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/history", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		if r.Method == http.MethodDelete {
			stubJSON(w, map[string]any{"cleared": true})
		}
	})
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	read := captureStdout(t)
	err := cmdQueries(gQueries(srv), []string{"history", "--project=proj1", "--clear", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("history clear --yes: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("expected DELETE, got %s", gotMethod)
	}
	if !strings.Contains(out, "cleared") {
		t.Errorf("expected 'cleared' in output: %q", out)
	}
}

// ── queries history --clear --json requires --yes ─────────────────────

func TestQueriesHistoryClearJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdQueries(g, []string{"history", "--project=proj1", "--clear"})
	if err == nil {
		t.Fatal("expected error when --json + --clear without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

// ── queries — unknown subcommand ──────────────────────────────────────

func TestQueriesUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := queriesSrv(t, mux)
	setQueriesEnv(t, srv)

	err := cmdQueries(gQueries(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

// ── queries help ──────────────────────────────────────────────────────

func TestQueriesHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdQueries(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("queries help: %v", err)
	}
}
