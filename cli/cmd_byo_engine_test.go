// cmd_byo_engine_test — coverage for `basin byo engine` subcommands.
//
// shared-key is verified to NEVER appear in any printed output.
// Helper functions live in cmd_branches_test.go and config_test.go.
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

// ── test helpers ──────────────────────────────────────────────────────

func byoEngineSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setByoEngineEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gByoEngine(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// ── byo engine get ────────────────────────────────────────────────────

func TestBYOEngineGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"engine": map[string]any{
				"url":           "https://engine.example.com",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine get: %v", err)
	}
	if !strings.Contains(out, "engine.example.com") {
		t.Errorf("expected URL in output: %q", out)
	}
}

func TestBYOEngineGetNull(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"engine": nil})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine get null: %v", err)
	}
	if !strings.Contains(out, "(no BYO engine configured)") {
		t.Errorf("expected '(no BYO engine configured)', got: %q", out)
	}
}

func TestBYOEngineGetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"engine": map[string]any{
				"url":           "https://engine.example.com",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"engine", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine get --json: %v", err)
	}
	var got struct {
		Engine *BYOEngineConfig `json:"engine"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode byo engine get JSON: %v\nbody=%s", err, out)
	}
	if got.Engine == nil || got.Engine.URL != "https://engine.example.com" {
		t.Errorf("unexpected engine in JSON: %+v", got.Engine)
	}
}

func TestBYOEngineGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 404 {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── byo engine put — validation ───────────────────────────────────────

func TestBYOEnginePutMissingURL(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "put",
		"--project=proj1", "--shared-key=k1"})
	if err == nil {
		t.Fatal("expected error when --url is missing")
	}
	if !strings.Contains(err.Error(), "--url") {
		t.Errorf("error should mention --url: %v", err)
	}
}

func TestBYOEnginePutMissingSharedKey(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "put",
		"--project=proj1", "--url=https://engine.example.com"})
	if err == nil {
		t.Fatal("expected error when --shared-key is missing")
	}
	if !strings.Contains(err.Error(), "--shared-key") {
		t.Errorf("error should mention --shared-key: %v", err)
	}
}

// ── byo engine put — shared-key masking ──────────────────────────────

func TestBYOEnginePutSharedKeyNeverEchoed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"engine": map[string]any{
				"url":           "https://engine.example.com",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	const sharedKey = "ULTRA_SECRET_SHARED_KEY_NEVER_ECHO"
	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "put",
		"--project=proj1", "--url=https://engine.example.com",
		"--shared-key=" + sharedKey, "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine put: %v", err)
	}
	if strings.Contains(out, sharedKey) {
		t.Errorf("shared-key must never appear in stdout: %q", out)
	}
}

func TestBYOEnginePutHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"engine": map[string]any{
				"url":           "https://engine.example.com",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "put",
		"--project=proj1", "--url=https://engine.example.com",
		"--shared-key=mysecret", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine put: %v", err)
	}
	if gotBody["url"] != "https://engine.example.com" {
		t.Errorf("url in body: got %v", gotBody["url"])
	}
	if gotBody["shared_key"] != "mysecret" {
		t.Errorf("shared_key in body: got %v", gotBody["shared_key"])
	}
	if !strings.Contains(out, "configured") {
		t.Errorf("expected 'configured' in output: %q", out)
	}
}

func TestBYOEnginePutJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"engine": map[string]any{
				"url":           "https://engine.example.com",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"engine", "put",
		"--project=proj1", "--url=https://engine.example.com",
		"--shared-key=mysecret", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine put --json: %v", err)
	}
	var got struct {
		Engine *BYOEngineConfig `json:"engine"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode put JSON: %v\nbody=%s", err, out)
	}
	if got.Engine == nil {
		t.Error("expected engine in JSON response")
	}
	// shared_key must NOT appear in response JSON.
	if strings.Contains(out, "mysecret") {
		t.Errorf("shared_key must never appear in JSON output: %q", out)
	}
}

func TestBYOEnginePut403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "put",
		"--project=proj1", "--url=https://engine.example.com",
		"--shared-key=sk", "--yes"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 403 {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── byo engine probe ──────────────────────────────────────────────────

func TestBYOEngineProbeOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine/probe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"ok": true, "latency_ms": 7.3})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine probe: %v", err)
	}
	if !strings.Contains(out, "OK") {
		t.Errorf("expected 'OK' in output: %q", out)
	}
}

func TestBYOEngineProbeFailure(t *testing.T) {
	mux := http.NewServeMux()
	errMsg := "dial tcp: connection refused"
	mux.HandleFunc("/v1/projects/proj1/byo-engine/probe", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"ok": false, "latency_ms": 0, "error": errMsg})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine probe failure: %v", err)
	}
	if !strings.Contains(out, "FAILED") {
		t.Errorf("expected 'FAILED' in output: %q", out)
	}
	if !strings.Contains(out, "connection refused") {
		t.Errorf("expected error message in output: %q", out)
	}
}

func TestBYOEngineProbeJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine/probe", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"ok": true, "latency_ms": 3.0})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"engine", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine probe --json: %v", err)
	}
	var got struct {
		OK        bool    `json:"ok"`
		LatencyMs float64 `json:"latency_ms"`
		Error     *string `json:"error"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode probe JSON: %v\nbody=%s", err, out)
	}
	if !got.OK {
		t.Errorf("ok: expected true, got false")
	}
}

// ── byo engine delete ─────────────────────────────────────────────────

func TestBYOEngineDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"removed": true})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "removed") {
		t.Errorf("expected 'removed' in output: %q", out)
	}
}

func TestBYOEngineDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"removed": true})
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"engine", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo engine delete --json: %v", err)
	}
	var got struct {
		Removed bool `json:"removed"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Removed {
		t.Errorf("removed: expected true, got false")
	}
}

func TestBYOEngineDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdBYO(gByoEngine(srv), []string{"engine", "delete", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("byo engine delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestBYOEngineDelete422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/byo-engine", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "engine is active")
	})
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "delete", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error on 422")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 422 {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestBYOEngineUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoEngineSrv(t, mux)
	setByoEngineEnv(t, srv)

	err := cmdBYO(gByoEngine(srv), []string{"engine", "frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown byo engine subcommand")
	}
}

func TestBYOEngineHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdBYOEngine(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("byo engine help: %v", err)
	}
}
