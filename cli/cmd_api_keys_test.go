// cmd_api_keys_test — coverage for the api-keys subcommand.
//
// Every test uses an httptest.Server stub. Stubs assert on request
// method and path. Confirmation prompts are bypassed via --yes or
// piped stdin. The test helpers stubJSON and stubError defined in
// cmd_branches_test.go are reused across files in package main.
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

// ── test server helpers ───────────────────────────────────────────────

func apiKeySrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setAPIKeyEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gAPIKey(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleKey(id, name, scope, prefix string) map[string]any {
	return map[string]any{
		"id":         id,
		"name":       name,
		"scope":      scope,
		"prefix":     prefix,
		"masked_key": prefix + "••••••••",
		"created_at": "2026-01-01T00:00:00Z",
	}
}

// ── api-keys list ─────────────────────────────────────────────────────

func TestAPIKeysListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"keys": []any{}})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys list empty: %v", err)
	}
	if !strings.Contains(out, "(no api keys)") {
		t.Errorf("expected '(no api keys)', got: %q", out)
	}
}

func TestAPIKeysListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"keys": []any{
				sampleKey("k1", "my-key", "write", "bsp_"),
				sampleKey("k2", "read-key", "read", "bsp_"),
			},
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys list: %v", err)
	}
	if !strings.Contains(out, "my-key") || !strings.Contains(out, "read-key") {
		t.Errorf("key names missing from output: %q", out)
	}
	// Masked key should appear, not a bare secret.
	if !strings.Contains(out, "••••••••") {
		t.Errorf("masked bullet chars missing from output: %q", out)
	}
}

func TestAPIKeysListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"keys": []any{sampleKey("k1", "my-key", "write", "bsp_")},
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAPIKeys(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys list --json: %v", err)
	}
	var got struct {
		Keys []*ProjectAPIKey `json:"keys"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode api-keys list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(got.Keys))
	}
	if got.Keys[0].ID != "k1" {
		t.Errorf("key id: got %q, want %q", got.Keys[0].ID, "k1")
	}
}

// ── api-keys create ───────────────────────────────────────────────────

func TestAPIKeysCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"key":      sampleKey("k-new", "deploy-key", "write", "bsp_"),
			"full_key": "bsp_supersecrettoken1234",
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"create", "--project=proj1", "--name=deploy-key", "--scope=write"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys create: %v", err)
	}
	if gotBody["name"] != "deploy-key" {
		t.Errorf("name in body: got %v, want 'deploy-key'", gotBody["name"])
	}
	if gotBody["scope"] != "write" {
		t.Errorf("scope in body: got %v, want 'write'", gotBody["scope"])
	}
	// Full key must appear in create output.
	if !strings.Contains(out, "bsp_supersecrettoken1234") {
		t.Errorf("full key missing from create output: %q", out)
	}
}

func TestAPIKeysCreateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"key":      sampleKey("k-json", "ci-key", "read", "bsp_"),
			"full_key": "bsp_fulljsontoken5678",
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAPIKeys(g, []string{"create", "--project=proj1", "--name=ci-key"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys create --json: %v", err)
	}
	var got CreateAPIKeyResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode api-keys create JSON: %v\nbody=%s", err, out)
	}
	if got.FullKey != "bsp_fulljsontoken5678" {
		t.Errorf("full_key: got %q, want 'bsp_fulljsontoken5678'", got.FullKey)
	}
	if got.Key == nil || got.Key.ID != "k-json" {
		t.Errorf("key.id in response: %+v", got.Key)
	}
}

func TestAPIKeysCreateMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"create", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when --name is missing")
	}
	if !strings.Contains(err.Error(), "--name") {
		t.Errorf("error should mention --name, got: %v", err)
	}
}

// ── api-keys rotate ───────────────────────────────────────────────────

func TestAPIKeysRotateWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{
			"key":      sampleKey("k1", "my-key", "write", "bsp_"),
			"full_key": "bsp_newrotatedtoken9999",
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"rotate", "--project=proj1", "--yes", "k1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys rotate: %v", err)
	}
	if !called {
		t.Error("rotate endpoint was not called")
	}
	// New secret must appear in output.
	if !strings.Contains(out, "bsp_newrotatedtoken9999") {
		t.Errorf("rotated key missing from output: %q", out)
	}
}

func TestAPIKeysRotateDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1/rotate", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"key": sampleKey("k1", "my-key", "write", "bsp_"), "full_key": "bsp_new"})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	// Pipe "n" to simulate decline.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"rotate", "--project=proj1", "k1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("api-keys rotate declined: unexpected error: %v", err)
	}
	if called {
		t.Error("rotate endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestAPIKeysRotateJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAPIKeys(g, []string{"rotate", "--project=proj1", "k1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestAPIKeysRotateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1/rotate", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"key":      sampleKey("k1", "my-key", "write", "bsp_"),
			"full_key": "bsp_rotatedjsontoken",
		})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAPIKeys(g, []string{"rotate", "--project=proj1", "--yes", "k1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys rotate --json: %v", err)
	}
	var got RotateAPIKeyResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode rotate JSON: %v\nbody=%s", err, out)
	}
	if got.FullKey != "bsp_rotatedjsontoken" {
		t.Errorf("full_key: got %q", got.FullKey)
	}
}

func TestAPIKeysRotateMissingID(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"rotate", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing key id")
	}
}

// ── api-keys revoke ───────────────────────────────────────────────────

func TestAPIKeysRevokeWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"revoked": true, "id": "k1"})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"revoke", "--project=proj1", "--yes", "k1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys revoke: %v", err)
	}
	if !called {
		t.Error("revoke endpoint was not called")
	}
	if !strings.Contains(out, "Revoked") {
		t.Errorf("expected 'Revoked' in output: %q", out)
	}
}

func TestAPIKeysRevokeDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdAPIKeys(gAPIKey(srv), []string{"revoke", "--project=proj1", "k1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("api-keys revoke declined: unexpected error: %v", err)
	}
	if called {
		t.Error("revoke endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestAPIKeysRevoke404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "API key not found.")
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"revoke", "--project=proj1", "--yes", "missing"})
	if err == nil {
		t.Fatal("expected error on 404 revoke, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

func TestAPIKeysRevoke403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"revoke", "--project=proj1", "--yes", "k1"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

func TestAPIKeysRevokeJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/api-keys/k1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"revoked": true, "id": "k1"})
	})
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAPIKeys(g, []string{"revoke", "--project=proj1", "--yes", "k1"})
	out := read()
	if err != nil {
		t.Fatalf("api-keys revoke --json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode revoke JSON: %v\nbody=%s", err, out)
	}
	if got["revoked"] != true {
		t.Errorf("revoked: expected true, got %v", got["revoked"])
	}
}

func TestAPIKeysRevokeMissingID(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"revoke", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing key id")
	}
}

// ── arg-parse / dispatcher ────────────────────────────────────────────

func TestAPIKeysUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	err := cmdAPIKeys(gAPIKey(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestAPIKeysHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdAPIKeys(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("api-keys help: %v", err)
	}
}

func TestAPIKeysMaskingConvention(t *testing.T) {
	// Verify maskAPIKey returns the masked_key field when present and
	// falls back to prefix + bullets when it is absent.
	k := &ProjectAPIKey{ID: "k1", Prefix: "bsp_", MaskedKey: "bsp_••••••••abcd"}
	if got := maskAPIKey(k); got != "bsp_••••••••abcd" {
		t.Errorf("maskAPIKey with MaskedKey: got %q, want %q", got, "bsp_••••••••abcd")
	}

	k2 := &ProjectAPIKey{ID: "k2", Prefix: "bsp_"}
	if got := maskAPIKey(k2); !strings.HasPrefix(got, "bsp_") || !strings.Contains(got, "••••••••") {
		t.Errorf("maskAPIKey fallback: got %q", got)
	}

	k3 := &ProjectAPIKey{ID: "k3"}
	if got := maskAPIKey(k3); got == "" {
		t.Errorf("maskAPIKey empty: got empty string")
	}
}

func TestAPIKeysNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := apiKeySrv(t, mux)
	setAPIKeyEnv(t, srv)

	// No --project flag and no working project config → error with hint.
	err := cmdAPIKeys(gAPIKey(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
