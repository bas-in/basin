// cmd_rows_test — coverage for the rows subcommand.
//
// Tests invoke cmdRows() directly (not run()) because "rows" is
// registered in commands() by the dispatcher session AFTER this file is
// committed — calling cmdRows directly keeps the test suite green in the
// interim and matches the pattern used by cmd_branches_test.go.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────

func rowsServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setRowsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func rowsGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// insertResp builds a canonical insert response envelope.
func insertResp(n int) map[string]any {
	rows := make([]any, n)
	for i := 0; i < n; i++ {
		rows[i] = map[string]any{"id": i + 1, "name": fmt.Sprintf("row%d", i+1)}
	}
	return map[string]any{"inserted": n, "rows": rows}
}

// updateResp builds a canonical update response envelope.
func updateResp() map[string]any {
	return map[string]any{"row": map[string]any{"id": 1, "name": "updated"}}
}

// deleteResp builds a canonical delete response envelope.
func deleteResp(n int) map[string]any {
	return map[string]any{"deleted": n}
}

// ── rows insert — single --json ───────────────────────────────────────

func TestRowsInsertSingleJSON(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, insertResp(1))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", `--json={"name":"Alice"}`, "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows insert: %v", err)
	}
	if gotBody["row"] == nil {
		t.Errorf("body should contain 'row' key, got: %v", gotBody)
	}
	if !strings.Contains(out, "Inserted") {
		t.Errorf("expected 'Inserted' in output: %q", out)
	}
}

func TestRowsInsertJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, insertResp(1))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRows(g, []string{"insert", "--project=proj1", `--json={"name":"Alice"}`, "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows insert --json: %v", err)
	}
	var got rowInsertResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode insert JSON: %v\nbody=%s", err, out)
	}
	if got.Inserted != 1 {
		t.Errorf("inserted: got %d, want 1", got.Inserted)
	}
	if len(got.Rows) != 1 {
		t.Errorf("rows count: got %d, want 1", len(got.Rows))
	}
}

func TestRowsInsertInvalidJSON(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", "--json=notjson", "users"})
	if err == nil {
		t.Fatal("expected error for invalid --json")
	}
	if !strings.Contains(err.Error(), "invalid JSON") {
		t.Errorf("expected 'invalid JSON' in error: %v", err)
	}
}

func TestRowsInsert404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Table not found.")
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", `--json={"name":"Alice"}`, "users"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestRowsInsert403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", `--json={"name":"Alice"}`, "users"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── rows insert — stdin NDJSON batch ─────────────────────────────────

func TestRowsInsertStdinBatch(t *testing.T) {
	var callCount atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/orders/rows", func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, insertResp(1))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	// Feed 3 NDJSON lines via stdin.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, `{"item":"a"}`)
	_, _ = fmt.Fprintln(pw, `{"item":"b"}`)
	_, _ = fmt.Fprintln(pw, `{"item":"c"}`)
	_ = pw.Close()

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", "orders"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("rows insert batch: %v", err)
	}
	if n := callCount.Load(); n != 3 {
		t.Errorf("expected 3 POST calls, got %d", n)
	}
	if !strings.Contains(out, "3") {
		t.Errorf("expected '3' in batch output: %q", out)
	}
}

func TestRowsInsertStdinBatchMidStreamError(t *testing.T) {
	var callCount atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/orders/rows", func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 2 {
			// Second call returns 422.
			stubError(w, http.StatusUnprocessableEntity, "validation_error", "bad value")
			return
		}
		stubJSON(w, insertResp(1))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, `{"item":"ok"}`)
	_, _ = fmt.Fprintln(pw, `{"item":"bad"}`)
	_, _ = fmt.Fprintln(pw, `{"item":"ok2"}`)
	_ = pw.Close()

	// Capture stderr to check for per-line error messages.
	prErr, pwErr, _ := os.Pipe()
	prevStderr := os.Stderr
	os.Stderr = pwErr

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1", "orders"})
	out := read()

	_ = pwErr.Close()
	errOut, _ := io.ReadAll(prErr)
	os.Stderr = prevStderr
	os.Stdin = prevStdin

	// Partial success: 2 rows inserted, 1 failed. err should be nil
	// because at least one insert succeeded.
	if err != nil {
		t.Fatalf("rows insert mid-stream error: unexpected err=%v", err)
	}
	if !strings.Contains(out, "2") {
		t.Errorf("expected '2' inserted in output: %q", out)
	}
	if !strings.Contains(string(errOut), "line 2") {
		t.Errorf("expected 'line 2' error in stderr: %q", string(errOut))
	}
}

func TestRowsInsertMissingTable(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error for missing table name")
	}
}

// ── rows update ───────────────────────────────────────────────────────

func TestRowsUpdateHappyPath(t *testing.T) {
	var gotPath string
	var gotBody map[string]any
	mux := http.NewServeMux()
	// net/http ServeMux doesn't support path params; register the full path.
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows/42", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, updateResp())
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"update", "--project=proj1", "--pk=42", `--json={"name":"Bob"}`, "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows update: %v", err)
	}
	if !strings.HasSuffix(gotPath, "/rows/42") {
		t.Errorf("expected path ending /rows/42, got: %s", gotPath)
	}
	if gotBody["patch"] == nil {
		t.Errorf("body should contain 'patch' key, got: %v", gotBody)
	}
	if !strings.Contains(out, "Updated") {
		t.Errorf("expected 'Updated' in output: %q", out)
	}
}

func TestRowsUpdateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows/1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, updateResp())
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRows(g, []string{"update", "--project=proj1", "--pk=1", `--json={"name":"Bob"}`, "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows update --json: %v", err)
	}
	var got rowUpdateResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode update JSON: %v\nbody=%s", err, out)
	}
	if got.Row == nil {
		t.Errorf("row field missing in update response JSON")
	}
}

func TestRowsUpdateMissingPK(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"update", "--project=proj1", `--json={"name":"Bob"}`, "users"})
	if err == nil {
		t.Fatal("expected error for missing --pk")
	}
	if !strings.Contains(err.Error(), "--pk is required") {
		t.Errorf("expected '--pk is required' in error: %v", err)
	}
}

func TestRowsUpdateMissingJSON(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"update", "--project=proj1", "--pk=1", "users"})
	if err == nil {
		t.Fatal("expected error for missing --json")
	}
	if !strings.Contains(err.Error(), "--json is required") {
		t.Errorf("expected '--json is required' in error: %v", err)
	}
}

func TestRowsUpdate422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows/99", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "constraint violation")
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"update", "--project=proj1", "--pk=99", `--json={"name":"X"}`, "users"})
	if err == nil {
		t.Fatal("expected error on 422")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── rows delete ───────────────────────────────────────────────────────

func TestRowsDeleteConfirmed(t *testing.T) {
	var called bool
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		gotQuery = r.URL.RawQuery
		called = true
		stubJSON(w, deleteResp(3))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"delete", "--project=proj1", "--where=age > 30", "--yes", "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(gotQuery, "where=") {
		t.Errorf("expected 'where' in query string: %q", gotQuery)
	}
	if !strings.Contains(out, "Deleted") || !strings.Contains(out, "3") {
		t.Errorf("expected 'Deleted 3' in output: %q", out)
	}
}

func TestRowsDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, deleteResp(0))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"delete", "--project=proj1", "--where=id=1", "users"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("rows delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestRowsDeleteJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/users/rows", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, deleteResp(5))
	})
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRows(g, []string{"delete", "--project=proj1", "--where=age>18", "--yes", "users"})
	out := read()
	if err != nil {
		t.Fatalf("rows delete --json: %v", err)
	}
	var got rowDeleteResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if got.Deleted != 5 {
		t.Errorf("deleted: got %d, want 5", got.Deleted)
	}
}

func TestRowsDeleteMissingWhere(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"delete", "--project=proj1", "--yes", "users"})
	if err == nil {
		t.Fatal("expected error for missing --where")
	}
	if !strings.Contains(err.Error(), "--where is required") {
		t.Errorf("expected '--where is required' in error: %v", err)
	}
}

func TestRowsDeleteJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	setRowsEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRows(g, []string{"delete", "--project=proj1", "--where=id=1", "users"})
	if err == nil {
		t.Fatal("expected error under --json without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

// ── misc ──────────────────────────────────────────────────────────────

func TestRowsUnknownSubcommand(t *testing.T) {
	g := &globalFlags{}
	err := cmdRows(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestRowsHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdRows(g, []string{"help"})
	if err != nil {
		t.Fatalf("rows help: %v", err)
	}
}

func TestRowsNoProjectResolution(t *testing.T) {
	mux := http.NewServeMux()
	srv := rowsServer(t, mux)
	// withTempConfigDir ensures no config.toml exists.
	setRowsEnv(t, srv)

	// No --project flag and no basin/config.toml → resolveProjectRef fails.
	g := rowsGFlag(srv)
	err := cmdRows(g, []string{"insert", `--json={"x":1}`, "users"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
