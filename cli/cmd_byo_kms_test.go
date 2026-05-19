// cmd_byo_kms_test — coverage for `basin byo kms` subcommands.
//
// Includes file-read, stdin read, and malformed-JSON paths for
// --auth-json. The auth blob is verified to NEVER appear in output.
// Helper functions live in cmd_branches_test.go and config_test.go.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

func byoKMSSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setByoKMSEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gByoKMS(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleKMS(provider, keyID string) map[string]any {
	return map[string]any{
		"provider":      provider,
		"key_id":        keyID,
		"configured_at": "2026-05-01T00:00:00Z",
	}
}

// ── byo kms get ───────────────────────────────────────────────────────

func TestBYOKMSGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"kms": sampleKMS("aws", "arn:aws:kms:us-east-1:123:key/abc")})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms get: %v", err)
	}
	if !strings.Contains(out, "aws") || !strings.Contains(out, "arn:aws") {
		t.Errorf("expected provider/key_id in output: %q", out)
	}
}

func TestBYOKMSGetNull(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"kms": nil})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms get null: %v", err)
	}
	if !strings.Contains(out, "(no BYO KMS configured)") {
		t.Errorf("expected '(no BYO KMS configured)', got: %q", out)
	}
}

func TestBYOKMSGetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"kms": sampleKMS("gcp", "projects/p/keys/k")})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms get --json: %v", err)
	}
	var got struct {
		KMS *BYOKMSConfig `json:"kms"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode byo kms get JSON: %v\nbody=%s", err, out)
	}
	if got.KMS == nil || got.KMS.Provider != "gcp" {
		t.Errorf("unexpected kms in JSON: %+v", got.KMS)
	}
}

// ── byo kms put — validation ──────────────────────────────────────────

func TestBYOKMSPutMissingProvider(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "put", "--project=proj1", "--key-id=k1"})
	if err == nil {
		t.Fatal("expected error when --provider is missing")
	}
	if !strings.Contains(err.Error(), "--provider") {
		t.Errorf("error should mention --provider: %v", err)
	}
}

func TestBYOKMSPutInvalidProvider(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "put",
		"--project=proj1", "--provider=vault", "--key-id=k1"})
	if err == nil {
		t.Fatal("expected error for invalid KMS provider")
	}
	if !strings.Contains(err.Error(), "vault") {
		t.Errorf("error should mention the invalid provider: %v", err)
	}
}

func TestBYOKMSPutMissingKeyID(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "put",
		"--project=proj1", "--provider=aws"})
	if err == nil {
		t.Fatal("expected error when --key-id is missing")
	}
	if !strings.Contains(err.Error(), "--key-id") {
		t.Errorf("error should mention --key-id: %v", err)
	}
}

// ── byo kms put — auth-json file ──────────────────────────────────────

func TestBYOKMSPutAuthJSONFileSuccess(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"kms": sampleKMS("gcp", "projects/p/keys/k")})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	// Write a temporary credentials file.
	dir := t.TempDir()
	credFile := filepath.Join(dir, "creds.json")
	creds := `{"type":"service_account","project_id":"myproj","private_key":"secret"}`
	if err := os.WriteFile(credFile, []byte(creds), 0600); err != nil {
		t.Fatalf("write creds file: %v", err)
	}

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "put",
		"--project=proj1", "--provider=gcp", "--key-id=projects/p/keys/k",
		"--auth-json=" + credFile, "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms put with auth-json: %v", err)
	}
	// Auth blob embedded in request body.
	if _, ok := gotBody["auth"]; !ok {
		t.Error("expected 'auth' field in request body")
	}
	// Auth blob NEVER echoed in stdout.
	if strings.Contains(out, "secret") || strings.Contains(out, "private_key") {
		t.Errorf("auth blob must never appear in stdout: %q", out)
	}
}

func TestBYOKMSPutAuthJSONFileNotFound(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "put",
		"--project=proj1", "--provider=aws", "--key-id=k1",
		"--auth-json=/nonexistent/path/creds.json", "--yes"})
	if err == nil {
		t.Fatal("expected error for nonexistent auth-json file")
	}
	if !strings.Contains(err.Error(), "nonexistent") && !strings.Contains(err.Error(), "auth JSON") {
		t.Errorf("error should mention the file/auth: %v", err)
	}
}

func TestBYOKMSPutAuthJSONMalformed(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	dir := t.TempDir()
	badFile := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badFile, []byte(`{not valid json`), 0600); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	err := cmdBYO(gByoKMS(srv), []string{"kms", "put",
		"--project=proj1", "--provider=aws", "--key-id=k1",
		"--auth-json=" + badFile, "--yes"})
	if err == nil {
		t.Fatal("expected error for malformed auth-json")
	}
	if !strings.Contains(err.Error(), "JSON") && !strings.Contains(err.Error(), "json") {
		t.Errorf("error should mention JSON: %v", err)
	}
}

// ── byo kms put — auth blob never printed ────────────────────────────

func TestBYOKMSPutAuthBlobNeverPrinted(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"kms": sampleKMS("aws", "arn:aws:kms:us-east-1:123:key/abc")})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	dir := t.TempDir()
	credFile := filepath.Join(dir, "creds.json")
	const secretToken = "TOP_SECRET_AUTH_BLOB_2026"
	if err := os.WriteFile(credFile, []byte(fmt.Sprintf(`{"access_token":"%s"}`, secretToken)), 0600); err != nil {
		t.Fatalf("write creds file: %v", err)
	}

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "put",
		"--project=proj1", "--provider=aws", "--key-id=k1",
		"--auth-json=" + credFile, "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms put: %v", err)
	}
	if strings.Contains(out, secretToken) {
		t.Errorf("auth blob secret must NEVER appear in stdout: %q", out)
	}
}

// ── byo kms rotate ────────────────────────────────────────────────────

func TestBYOKMSRotateHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"rotated": true, "new_key_version": "v2"})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms rotate: %v", err)
	}
	if !strings.Contains(out, "v2") {
		t.Errorf("expected new key version in output: %q", out)
	}
}

func TestBYOKMSRotateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/rotate", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"rotated": true, "new_key_version": "v3"})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "rotate", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms rotate --json: %v", err)
	}
	var got struct {
		Rotated       bool   `json:"rotated"`
		NewKeyVersion string `json:"new_key_version"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode rotate JSON: %v\nbody=%s", err, out)
	}
	if !got.Rotated || got.NewKeyVersion != "v3" {
		t.Errorf("unexpected rotate response: %+v", got)
	}
}

// ── byo kms verify ────────────────────────────────────────────────────

func TestBYOKMSVerifyOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"ok": true, "latency_ms": 5.0})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "verify", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms verify: %v", err)
	}
	if !strings.Contains(out, "OK") {
		t.Errorf("expected 'OK' in output: %q", out)
	}
}

// ── byo kms audit ────────────────────────────────────────────────────

func TestBYOKMSAuditHappyPath(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/audit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{
			"events": []any{
				map[string]any{
					"id": "ev1", "action": "encrypt",
					"at": "2026-05-01T00:00:00Z",
					"actor_id": "usr1", "key_version": "v1",
				},
			},
		})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "audit",
		"--project=proj1", "--limit=25", "--since=2026-01-01T00:00:00Z"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms audit: %v", err)
	}
	if !strings.Contains(out, "ev1") || !strings.Contains(out, "encrypt") {
		t.Errorf("expected audit event in output: %q", out)
	}
	// Query params must be forwarded.
	if !strings.Contains(gotQuery, "limit=25") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
	if !strings.Contains(gotQuery, "since=") {
		t.Errorf("since not passed as query param: got %q", gotQuery)
	}
}

func TestBYOKMSAuditEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/audit", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "audit", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms audit empty: %v", err)
	}
	if !strings.Contains(out, "(no KMS audit events)") {
		t.Errorf("expected '(no KMS audit events)', got: %q", out)
	}
}

func TestBYOKMSAuditJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/audit", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{
				map[string]any{
					"id": "ev2", "action": "decrypt",
					"at": "2026-05-02T00:00:00Z",
					"actor_id": "usr2", "key_version": "v2",
				},
			},
		})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "audit", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms audit --json: %v", err)
	}
	var got struct {
		Events []*KMSAuditEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode audit JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "ev2" {
		t.Errorf("unexpected events in JSON: %+v", got.Events)
	}
}

// ── byo kms delete ────────────────────────────────────────────────────

func TestBYOKMSDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "deleted") {
		t.Errorf("expected 'deleted' in output: %q", out)
	}
}

func TestBYOKMSDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms delete --json: %v", err)
	}
	var got struct {
		Deleted bool `json:"deleted"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Deleted {
		t.Errorf("deleted: expected true, got false")
	}
}

func TestBYOKMSDelete403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "owner role required")
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "delete", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 403 {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── byo kms cache-stats ───────────────────────────────────────────────

func TestBYOKMSCacheStatsHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/cache-stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"hits":     100,
			"misses":   10,
			"size":     50,
			"max_size": 1000,
		})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoKMS(srv), []string{"kms", "cache-stats", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms cache-stats: %v", err)
	}
	if !strings.Contains(out, "hits") || !strings.Contains(out, "misses") {
		t.Errorf("expected hits/misses in output: %q", out)
	}
}

func TestBYOKMSCacheStatsJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/encryption/cache-stats", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"hits": 200, "misses": 20, "size": 80, "max_size": 500,
		})
	})
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"kms", "cache-stats", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo kms cache-stats --json: %v", err)
	}
	var got struct {
		Hits    int `json:"hits"`
		Misses  int `json:"misses"`
		Size    int `json:"size"`
		MaxSize int `json:"max_size"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode cache-stats JSON: %v\nbody=%s", err, out)
	}
	if got.Hits != 200 || got.Misses != 20 {
		t.Errorf("unexpected cache-stats: %+v", got)
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestBYOKMSUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoKMSSrv(t, mux)
	setByoKMSEnv(t, srv)

	err := cmdBYO(gByoKMS(srv), []string{"kms", "frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown byo kms subcommand")
	}
}

func TestBYOKMSHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdBYOKMS(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("byo kms help: %v", err)
	}
}
