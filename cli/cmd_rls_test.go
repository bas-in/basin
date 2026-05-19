// cmd_rls_test — coverage for the rls subcommand.
//
// Tests invoke cmdRLS() directly (not run()) because "rls" is
// registered in commands() by the dispatcher session AFTER this file is
// committed — calling cmdRLS directly keeps the test suite green in the
// interim and matches the pattern used by cmd_branches_test.go.
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

func rlsServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setRLSEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func rlsGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func samplePolicy(name, cmd string) map[string]any {
	return map[string]any{
		"name":       name,
		"command":    cmd,
		"using":      "auth.uid() = user_id",
		"with_check": "auth.uid() = user_id",
		"roles":      []string{"authenticated"},
	}
}

// ── rls enable ────────────────────────────────────────────────────────

func TestRLSEnableHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/rls/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"enabled": true, "table": "posts"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"enable", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls enable: %v", err)
	}
	if !called {
		t.Error("enable endpoint was not called")
	}
	if !strings.Contains(out, "enabled") || !strings.Contains(out, "posts") {
		t.Errorf("expected 'enabled' and 'posts' in output: %q", out)
	}
}

func TestRLSEnableJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/rls/enable", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"enabled": true, "table": "posts"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{"enable", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls enable --json: %v", err)
	}
	var got RLSStatus
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode rls enable JSON: %v\nbody=%s", err, out)
	}
	if !got.Enabled {
		t.Errorf("enabled: got false, want true")
	}
	if got.Table != "posts" {
		t.Errorf("table: got %q, want 'posts'", got.Table)
	}
}

func TestRLSEnableMissingTable(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"enable", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error for missing table name")
	}
}

func TestRLSEnable403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/rls/enable", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"enable", "--project=proj1", "posts"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── rls disable ───────────────────────────────────────────────────────

func TestRLSDisableHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/rls/disable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"enabled": false, "table": "posts"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"disable", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls disable: %v", err)
	}
	if !called {
		t.Error("disable endpoint was not called")
	}
	if !strings.Contains(out, "disabled") || !strings.Contains(out, "posts") {
		t.Errorf("expected 'disabled' and 'posts' in output: %q", out)
	}
}

func TestRLSDisableJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/rls/disable", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"enabled": false, "table": "posts"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{"disable", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls disable --json: %v", err)
	}
	var got RLSStatus
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode rls disable JSON: %v\nbody=%s", err, out)
	}
	if got.Enabled {
		t.Errorf("enabled: got true, want false")
	}
}

// ── rls policies list ─────────────────────────────────────────────────

func TestRLSPoliciesListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"policies": []any{
				samplePolicy("read_own", "select"),
				samplePolicy("write_own", "insert"),
			},
		})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "list", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls policies list: %v", err)
	}
	if !strings.Contains(out, "read_own") || !strings.Contains(out, "write_own") {
		t.Errorf("expected policy names in output: %q", out)
	}
}

func TestRLSPoliciesListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"policies": []any{}})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "list", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls policies list empty: %v", err)
	}
	if !strings.Contains(out, "(no policies)") {
		t.Errorf("expected '(no policies)', got: %q", out)
	}
}

func TestRLSPoliciesListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"policies": []any{samplePolicy("read_own", "select")},
		})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{"policies", "list", "--project=proj1", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls policies list --json: %v", err)
	}
	var got struct {
		Policies []*Policy `json:"policies"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode policies list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(got.Policies))
	}
	if got.Policies[0].Name != "read_own" {
		t.Errorf("policy name: got %q, want 'read_own'", got.Policies[0].Name)
	}
	if got.Policies[0].Command != "select" {
		t.Errorf("command: got %q, want 'select'", got.Policies[0].Command)
	}
}

func TestRLSPoliciesList404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/ghost/policies", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Table not found.")
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "list", "--project=proj1", "ghost"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── rls policies create ───────────────────────────────────────────────

func TestRLSPoliciesCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"policy": samplePolicy("read_own", "select")})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=read_own", "--command=select",
		"--using=auth.uid()=user_id",
		"--roles=authenticated",
		"posts",
	})
	out := read()
	if err != nil {
		t.Fatalf("rls policies create: %v", err)
	}
	if gotBody["name"] != "read_own" {
		t.Errorf("body name: got %v, want 'read_own'", gotBody["name"])
	}
	if gotBody["command"] != "select" {
		t.Errorf("body command: got %v, want 'select'", gotBody["command"])
	}
	if !strings.Contains(out, "read_own") {
		t.Errorf("expected 'read_own' in output: %q", out)
	}
}

func TestRLSPoliciesCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"policy": samplePolicy("write_own", "insert")})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=write_own", "--command=insert",
		"--using=true", "posts",
	})
	out := read()
	if err != nil {
		t.Fatalf("rls policies create --json: %v", err)
	}
	var got policyCreateResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode policies create JSON: %v\nbody=%s", err, out)
	}
	if got.Policy == nil || got.Policy.Name != "write_own" {
		t.Errorf("policy in response: %+v", got.Policy)
	}
}

func TestRLSPoliciesCreateMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--command=select", "--using=true", "posts",
	})
	if err == nil {
		t.Fatal("expected error for missing --name")
	}
	if !strings.Contains(err.Error(), "--name is required") {
		t.Errorf("expected '--name is required' in error: %v", err)
	}
}

func TestRLSPoliciesCreateMissingCommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=p1", "--using=true", "posts",
	})
	if err == nil {
		t.Fatal("expected error for missing --command")
	}
	if !strings.Contains(err.Error(), "--command is required") {
		t.Errorf("expected '--command is required' in error: %v", err)
	}
}

func TestRLSPoliciesCreateInvalidCommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=p1", "--command=truncate", "--using=true", "posts",
	})
	if err == nil {
		t.Fatal("expected error for invalid --command")
	}
	if !strings.Contains(err.Error(), "--command must be one of") {
		t.Errorf("expected '--command must be one of' in error: %v", err)
	}
}

func TestRLSPoliciesCreateMissingUsing(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=p1", "--command=select", "posts",
	})
	if err == nil {
		t.Fatal("expected error for missing --using")
	}
	if !strings.Contains(err.Error(), "--using is required") {
		t.Errorf("expected '--using is required' in error: %v", err)
	}
}

func TestRLSPoliciesCreate422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "policy already exists")
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{
		"policies", "create", "--project=proj1",
		"--name=dup", "--command=all", "--using=true", "posts",
	})
	if err == nil {
		t.Fatal("expected error on 422")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── rls policies drop ─────────────────────────────────────────────────

func TestRLSPoliciesDropConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies/read_own", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"dropped": true, "name": "read_own"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "drop", "--project=proj1", "--name=read_own", "--yes", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls policies drop: %v", err)
	}
	if !called {
		t.Error("drop endpoint was not called")
	}
	if !strings.Contains(out, "Dropped") || !strings.Contains(out, "read_own") {
		t.Errorf("expected 'Dropped' and 'read_own' in output: %q", out)
	}
}

func TestRLSPoliciesDropDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies/read_own", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, map[string]any{"dropped": true, "name": "read_own"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "drop", "--project=proj1", "--name=read_own", "posts"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("rls policies drop declined: unexpected error: %v", err)
	}
	if called {
		t.Error("drop endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestRLSPoliciesDropJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/tables/posts/policies/read_own", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"dropped": true, "name": "read_own"})
	})
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{"policies", "drop", "--project=proj1", "--name=read_own", "--yes", "posts"})
	out := read()
	if err != nil {
		t.Fatalf("rls policies drop --json: %v", err)
	}
	var got policyDropResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode policies drop JSON: %v\nbody=%s", err, out)
	}
	if !got.Dropped {
		t.Errorf("dropped: got false, want true")
	}
	if got.Name != "read_own" {
		t.Errorf("name: got %q, want 'read_own'", got.Name)
	}
}

func TestRLSPoliciesDropMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"policies", "drop", "--project=proj1", "--yes", "posts"})
	if err == nil {
		t.Fatal("expected error for missing --name")
	}
	if !strings.Contains(err.Error(), "--name is required") {
		t.Errorf("expected '--name is required' in error: %v", err)
	}
}

func TestRLSPoliciesDropJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	setRLSEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdRLS(g, []string{"policies", "drop", "--project=proj1", "--name=read_own", "posts"})
	if err == nil {
		t.Fatal("expected error under --json without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

// ── misc ──────────────────────────────────────────────────────────────

func TestRLSUnknownSubcommand(t *testing.T) {
	g := &globalFlags{}
	err := cmdRLS(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestRLSPoliciesUnknownSubcommand(t *testing.T) {
	g := &globalFlags{}
	err := cmdRLS(g, []string{"policies", "frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown policies subcommand")
	}
}

func TestRLSHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdRLS(g, []string{"help"})
	if err != nil {
		t.Fatalf("rls help: %v", err)
	}
}

func TestRLSNoProjectResolution(t *testing.T) {
	mux := http.NewServeMux()
	srv := rlsServer(t, mux)
	// withTempConfigDir ensures no config.toml exists.
	setRLSEnv(t, srv)

	// No --project flag and no basin/config.toml → resolveProjectRef fails.
	g := rlsGFlag(srv)
	err := cmdRLS(g, []string{"enable", "posts"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
