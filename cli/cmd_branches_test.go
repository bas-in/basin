// cmd_branches_test — exhaustive coverage for the branches subcommand.
//
// All tests use an httptest.Server stub. The stub asserts on request
// method and path. Confirmation prompts are bypassed via --yes or
// piped stdin strings.
//
// Tests invoke cmdBranches() directly (rather than run()) because
// "branches" is registered in commands() by the dispatcher session
// AFTER this file is committed — calling cmdBranches directly keeps
// the test suite green in the interim and matches how other cmd_*_test
// files test their handlers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ── test helpers ──────────────────────────────────────────────────────

// branchServer builds a test server wired with the branches endpoints.
func branchServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// setBranchEnv wires the test server and a fake token into the environment.
func setBranchEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

// gFlag returns a globalFlags aimed at the test server.
func gFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// sampleBranch returns a minimal Branch payload for use in stubs.
func sampleBranch(ref, name string) map[string]any {
	parentID := "parent-id"
	return map[string]any{
		"id":                "b-" + ref,
		"org_id":            "org1",
		"slug":              "proj-" + name,
		"ref":               ref,
		"name":              "Demo · " + name,
		"region":            "jnb",
		"status":            "active",
		"parent_project_id": parentID,
		"branch_name":       name,
		"branch_kind":       "branch",
		"created_at":        "2026-01-01T00:00:00Z",
	}
}

// sampleEvent returns a minimal BranchEvent payload for use in stubs.
func sampleEvent(id, kind, projectID string) map[string]any {
	msg := kind + " event"
	return map[string]any{
		"id":         id,
		"project_id": projectID,
		"kind":       kind,
		"message":    msg,
		"created_at": "2026-01-01T00:00:00Z",
	}
}

// stubJSON encodes v as JSON to w with a 200 status.
func stubJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// stubError encodes a cloud-style error envelope.
func stubError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg},
	})
}

// captureStderr captures stderr during fn.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	pr, pw, _ := os.Pipe()
	prev := os.Stderr
	os.Stderr = pw
	fn()
	_ = pw.Close()
	b, _ := io.ReadAll(pr)
	os.Stderr = prev
	return string(b)
}

// ── branches list ─────────────────────────────────────────────────────

func TestBranchesListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"branches": []any{}})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches list empty: %v", err)
	}
	if !strings.Contains(out, "(no branches)") {
		t.Errorf("expected '(no branches)', got: %q", out)
	}
}

func TestBranchesListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"branches": []any{
				sampleBranch("br1", "feat-a"),
				sampleBranch("br2", "feat-b"),
			},
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches list: %v", err)
	}
	if !strings.Contains(out, "feat-a") || !strings.Contains(out, "feat-b") {
		t.Errorf("branch names missing from output: %q", out)
	}
	if !strings.Contains(out, "br1") || !strings.Contains(out, "br2") {
		t.Errorf("branch refs missing from output: %q", out)
	}
}

func TestBranchesListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"branches": []any{sampleBranch("br1", "feat-a")},
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches list --json: %v", err)
	}
	var got struct {
		Branches []*Branch `json:"branches"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode branches list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Branches) != 1 {
		t.Fatalf("expected 1 branch, got %d", len(got.Branches))
	}
	if got.Branches[0].Ref != "br1" {
		t.Errorf("branch ref: got %q, want %q", got.Branches[0].Ref, "br1")
	}
	if got.Branches[0].BranchName != "feat-a" {
		t.Errorf("branch_name: got %q, want %q", got.Branches[0].BranchName, "feat-a")
	}
}

// ── branches create ───────────────────────────────────────────────────

func TestBranchesCreateDefaults(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"branch":        sampleBranch("br-new", "my-feature"),
			"created_event": sampleEvent("ev1", "created", "br-new"),
			"engine_ok":     true,
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"create", "--project=proj1", "my-feature"})
	out := read()
	if err != nil {
		t.Fatalf("branches create: %v", err)
	}
	if gotBody["branch_name"] != "my-feature" {
		t.Errorf("branch_name in body: got %v, want 'my-feature'", gotBody["branch_name"])
	}
	if gotBody["kind"] != "branch" {
		t.Errorf("kind in body: got %v, want 'branch'", gotBody["kind"])
	}
	if !strings.Contains(out, "my-feature") {
		t.Errorf("branch name missing from output: %q", out)
	}
}

func TestBranchesCreateWithFrom(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"branch":        sampleBranch("br-new", "feat-from"),
			"created_event": sampleEvent("ev1", "created", "br-new"),
			"engine_ok":     true,
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"create", "--project=proj1", "--from=br-parent", "--kind=preview", "feat-from"})
	_ = read()
	if err != nil {
		t.Fatalf("branches create --from: %v", err)
	}
	if gotBody["from_ref"] != "br-parent" {
		t.Errorf("from_ref in body: got %v, want 'br-parent'", gotBody["from_ref"])
	}
	if gotBody["kind"] != "preview" {
		t.Errorf("kind in body: got %v, want 'preview'", gotBody["kind"])
	}
}

func TestBranchesCreateNameConflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "bad_request", "A branch with that name already exists for this project.")
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"create", "--project=proj1", "existing-branch"})
	if err == nil {
		t.Fatal("expected error on 409 conflict, got nil")
	}
	if !strings.Contains(err.Error(), "branch name conflict") {
		t.Errorf("expected 'branch name conflict' in error, got: %v", err)
	}
}

func TestBranchesCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"branch":        sampleBranch("br-json", "json-feature"),
			"created_event": sampleEvent("ev1", "created", "br-json"),
			"engine_ok":     false,
			"engine_msg":    "engine_unconfigured",
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"create", "--project=proj1", "json-feature"})
	out := read()
	if err != nil {
		t.Fatalf("branches create --json: %v", err)
	}
	var got struct {
		Branch    *Branch      `json:"branch"`
		EngineOK  bool         `json:"engine_ok"`
		EngineMsg string       `json:"engine_msg"`
		Event     *BranchEvent `json:"created_event"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode branches create JSON: %v\nbody=%s", err, out)
	}
	if got.Branch == nil || got.Branch.Ref != "br-json" {
		t.Errorf("branch in JSON: %+v", got.Branch)
	}
	if got.EngineMsg != "engine_unconfigured" {
		t.Errorf("engine_msg: got %q, want 'engine_unconfigured'", got.EngineMsg)
	}
}

// ── branches get ──────────────────────────────────────────────────────

func TestBranchesGet200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/br1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"project": sampleBranch("br1", "feat-a"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"get", "br1", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches get: %v", err)
	}
	if !strings.Contains(out, "br1") {
		t.Errorf("ref missing from output: %q", out)
	}
	if !strings.Contains(out, "feat-a") {
		t.Errorf("branch_name missing from output: %q", out)
	}
}

func TestBranchesGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/br-missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Project not found.")
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"get", "br-missing"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
}

func TestBranchesGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/br1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"project": sampleBranch("br1", "feat-a"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"get", "br1"})
	out := read()
	if err != nil {
		t.Fatalf("branches get --json: %v", err)
	}
	var got struct {
		Project *Branch `json:"project"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode branches get JSON: %v\nbody=%s", err, out)
	}
	if got.Project == nil || got.Project.BranchName != "feat-a" {
		t.Errorf("project.branch_name: %+v", got.Project)
	}
}

// ── branches merge ────────────────────────────────────────────────────

func TestBranchesMergeConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1/merge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{
			"branch": sampleBranch("br1", "feat-a"),
			"event":  sampleEvent("ev-m", "merged", "br1"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"merge", "--project=proj1", "--yes", "br1"})
	out := read()
	if err != nil {
		t.Fatalf("branches merge: %v", err)
	}
	if !called {
		t.Error("merge endpoint was not called")
	}
	if !strings.Contains(out, "Merged") {
		t.Errorf("expected 'Merged' in output: %q", out)
	}
}

func TestBranchesMergeDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1/merge", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"branch": sampleBranch("br1", "feat-a"), "event": sampleEvent("ev1", "merged", "br1")})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// Pipe "n" to simulate the user declining.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"merge", "--project=proj1", "br1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("branches merge declined: unexpected error: %v", err)
	}
	if called {
		t.Error("merge endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestBranchesMergeJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// Under --json, missing --yes should return an error.
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"merge", "--project=proj1", "br1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestBranchesMergeConflictResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1/merge", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusBadRequest, "bad_request", "Branch is already merged.")
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"merge", "--project=proj1", "--yes", "br1"})
	if err == nil {
		t.Fatal("expected error for already-merged branch, got nil")
	}
	// Verify the error maps through the APIError type.
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusBadRequest {
		t.Errorf("expected APIError 400, got: %v", err)
	}
}

func TestBranchesMergeJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1/merge", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"branch": sampleBranch("br1", "feat-a"),
			"event":  sampleEvent("ev-m", "merged", "br1"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"merge", "--project=proj1", "--yes", "br1"})
	out := read()
	if err != nil {
		t.Fatalf("branches merge --json: %v", err)
	}
	var got struct {
		Branch *Branch      `json:"branch"`
		Event  *BranchEvent `json:"event"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode merge JSON: %v\nbody=%s", err, out)
	}
	if got.Branch == nil || got.Branch.Ref != "br1" {
		t.Errorf("branch in merge response: %+v", got.Branch)
	}
	if got.Event == nil || got.Event.Kind != "merged" {
		t.Errorf("event in merge response: %+v", got.Event)
	}
}

// ── branches delete ───────────────────────────────────────────────────

func TestBranchesDeleteConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{
			"retired": true,
			"event":   sampleEvent("ev-r", "retired", "br1"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"delete", "--project=proj1", "--yes", "br1"})
	out := read()
	if err != nil {
		t.Fatalf("branches delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

func TestBranchesDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, map[string]any{"retired": true, "event": sampleEvent("ev1", "retired", "br1")})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// Pipe "n" to simulate the user declining.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"delete", "--project=proj1", "br1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("branches delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestBranchesDelete404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br-missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Branch not found for this project.")
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"delete", "--project=proj1", "--yes", "br-missing"})
	if err == nil {
		t.Fatal("expected error on 404 delete, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestBranchesDeleteJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/br1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"retired": true,
			"event":   sampleEvent("ev-r", "retired", "br1"),
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"delete", "--project=proj1", "--yes", "br1"})
	out := read()
	if err != nil {
		t.Fatalf("branches delete --json: %v", err)
	}
	var got struct {
		Retired bool         `json:"retired"`
		Event   *BranchEvent `json:"event"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Retired {
		t.Errorf("retired: got false, want true")
	}
	if got.Event == nil || got.Event.Kind != "retired" {
		t.Errorf("event.kind: %+v", got.Event)
	}
}

// ── branches events ───────────────────────────────────────────────────

func TestBranchesEventsSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"events": []any{
				sampleEvent("ev1", "created", "br1"),
				sampleEvent("ev2", "ready", "br1"),
			},
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches events: %v", err)
	}
	if !strings.Contains(out, "created") || !strings.Contains(out, "ready") {
		t.Errorf("event kinds missing from output: %q", out)
	}
}

func TestBranchesEventsSnapshotJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{sampleEvent("ev1", "created", "br1")},
		})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBranches(g, []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches events --json: %v", err)
	}
	var got struct {
		Events []*BranchEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode events JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].Kind != "created" {
		t.Errorf("events: %+v", got.Events)
	}
}

func TestBranchesEventsEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdBranches(g, []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches events empty: %v", err)
	}
	if !strings.Contains(out, "(no events)") {
		t.Errorf("expected '(no events)', got: %q", out)
	}
}

// TestBranchesEventsFollow runs the follow mode and interrupts it after
// the first page is fetched. Verifies that the initial immediate fetch
// prints events and that the loop exits cleanly on Interrupt.
func TestBranchesEventsFollow(t *testing.T) {
	var callCount atomic.Int32

	page1 := map[string]any{
		"events": []any{
			sampleEvent("ev1", "created", "br1"),
			sampleEvent("ev2", "ready", "br1"),
		},
	}
	page2 := map[string]any{
		"events": []any{
			sampleEvent("ev1", "created", "br1"), // already seen — dedup'd
			sampleEvent("ev2", "ready", "br1"),   // already seen — dedup'd
			sampleEvent("ev3", "merged", "br1"),  // new
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n == 1 {
			stubJSON(w, page1)
		} else {
			stubJSON(w, page2)
		}
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// Capture stdout so we can inspect what was printed.
	pr, pw, _ := os.Pipe()
	prevStdout := os.Stdout
	os.Stdout = pw

	done := make(chan error, 1)
	go func() {
		g := gFlag(srv)
		done <- cmdBranches(g, []string{"events", "--project=proj1", "--follow"})
	}()

	// Give the goroutine time for the initial fetch, then interrupt.
	time.Sleep(200 * time.Millisecond)
	proc, _ := os.FindProcess(os.Getpid())
	_ = proc.Signal(os.Interrupt)

	err := <-done
	_ = pw.Close()
	var buf strings.Builder
	_, _ = io.Copy(&buf, pr)
	os.Stdout = prevStdout

	// Follow mode exits cleanly on Interrupt (nil error).
	if err != nil {
		t.Fatalf("branches events --follow: unexpected error: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "created") {
		t.Errorf("expected 'created' event in follow output: %q", out)
	}
}

// TestBranchesEventsFollowDedup verifies that follow mode doesn't
// re-print events whose IDs have already been seen. Calls
// fetchAndPrintNewEvents directly to avoid the ticker delay.
func TestBranchesEventsFollowDedup(t *testing.T) {
	seen := make(map[string]struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{
				sampleEvent("ev1", "created", "br1"),
				sampleEvent("ev2", "ready", "br1"),
			},
		})
	})
	srv := branchServer(t, mux)
	withTempConfigDir(t)

	c := NewClient(srv.URL, "tok")
	g := &globalFlags{}

	ctx := context.Background()

	read := captureStdout(t)
	// First call — both events are new.
	err := fetchAndPrintNewEvents(ctx, c, "proj1", 50, g, seen)
	out1 := read()
	if err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	if len(seen) != 2 {
		t.Errorf("seen after first fetch: %d, want 2", len(seen))
	}
	if !strings.Contains(out1, "created") {
		t.Errorf("first fetch output missing 'created': %q", out1)
	}

	read = captureStdout(t)
	// Second call — same events returned; nothing new should print.
	err = fetchAndPrintNewEvents(ctx, c, "proj1", 50, g, seen)
	out2 := read()
	if err != nil {
		t.Fatalf("second fetch: %v", err)
	}
	if out2 != "" {
		t.Errorf("expected no output on second fetch (dedup), got: %q", out2)
	}
}

// ── arg-parse error paths ─────────────────────────────────────────────

func TestBranchesCreateMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"create", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error for missing branch name")
	}
}

func TestBranchesGetMissingRef(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"get"})
	if err == nil {
		t.Fatal("expected error for missing branch ref in get")
	}
}

func TestBranchesMergeMissingRef(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"merge", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing branch ref in merge")
	}
}

func TestBranchesDeleteMissingRef(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"delete", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing branch ref in delete")
	}
}

// TestBranchesNoProjectFlag verifies that a missing --project flag and
// no working-project config returns a clear error.
func TestBranchesNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// withTempConfigDir means no config.toml exists → resolveProjectRef fails.
	g := gFlag(srv)
	err := cmdBranches(g, []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}

func TestBranchesUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	g := gFlag(srv)
	err := cmdBranches(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

// TestBranchesHelp verifies the help subcommand returns nil.
func TestBranchesHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdBranches(g, []string{"help"})
	if err != nil {
		t.Fatalf("branches help: %v", err)
	}
}

// ── quiet flag ────────────────────────────────────────────────────────

func TestBranchesEventsQuiet(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/branches/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{sampleEvent("ev1", "created", "br1")}})
	})
	srv := branchServer(t, mux)
	setBranchEnv(t, srv)

	// --quiet suppresses info messages but data rows still print.
	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", quiet: true}
	err := cmdBranches(g, []string{"events", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("branches events --quiet: %v", err)
	}
	if !strings.Contains(out, "created") {
		t.Errorf("data row suppressed under --quiet: %q", out)
	}
}
