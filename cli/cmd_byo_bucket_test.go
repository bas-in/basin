// cmd_byo_bucket_test — coverage for `basin byo bucket` subcommands.
//
// Each test drives an httptest.Server stub that asserts on request
// method, path, body, and query string. Validation errors are tested
// before any HTTP call. Helper functions (stubJSON, stubError,
// captureStdout, withTempConfigDir) live in cmd_branches_test.go
// and config_test.go.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

func byoBucketSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setByoBucketEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gByoBucket(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// ── byo bucket get ────────────────────────────────────────────────────

func TestBYOBucketGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"storage": map[string]any{
				"provider":     "s3",
				"bucket":       "my-bucket",
				"region":       "us-east-1",
				"configured_at": "2026-05-01T00:00:00Z",
			},
		})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket get: %v", err)
	}
	if !strings.Contains(out, "s3") || !strings.Contains(out, "my-bucket") {
		t.Errorf("expected provider/bucket in output: %q", out)
	}
}

func TestBYOBucketGetNull(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"storage": nil})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket get null: %v", err)
	}
	if !strings.Contains(out, "(no BYO bucket configured)") {
		t.Errorf("expected '(no BYO bucket configured)', got: %q", out)
	}
}

func TestBYOBucketGetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"storage": map[string]any{
				"provider": "r2",
				"bucket":   "r2-bucket",
				"region":   "auto",
			},
		})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"bucket", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket get --json: %v", err)
	}
	var got struct {
		Storage *BYOStorageConfig `json:"storage"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode byo bucket get JSON: %v\nbody=%s", err, out)
	}
	if got.Storage == nil || got.Storage.Provider != "r2" {
		t.Errorf("unexpected storage in JSON: %+v", got.Storage)
	}
}

func TestBYOBucketGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 404 {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── byo bucket put — validation ───────────────────────────────────────

func TestBYOBucketPutMissingProvider(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--bucket=b", "--region=us-east-1",
		"--access-key=ak", "--secret-key=sk"})
	if err == nil {
		t.Fatal("expected error when --provider is missing")
	}
	if !strings.Contains(err.Error(), "--provider") {
		t.Errorf("error should mention --provider: %v", err)
	}
}

func TestBYOBucketPutInvalidProvider(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=gcs", "--bucket=b",
		"--region=us-east-1", "--access-key=ak", "--secret-key=sk"})
	if err == nil {
		t.Fatal("expected error for invalid provider")
	}
	if !strings.Contains(err.Error(), "gcs") {
		t.Errorf("error should mention the invalid provider: %v", err)
	}
}

func TestBYOBucketPutMissingBucket(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=s3", "--region=us-east-1",
		"--access-key=ak", "--secret-key=sk"})
	if err == nil {
		t.Fatal("expected error when --bucket is missing")
	}
	if !strings.Contains(err.Error(), "--bucket") {
		t.Errorf("error should mention --bucket: %v", err)
	}
}

func TestBYOBucketPutMissingSecretKey(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=s3", "--bucket=b",
		"--region=us-east-1", "--access-key=ak"})
	if err == nil {
		t.Fatal("expected error when --secret-key is missing")
	}
	if !strings.Contains(err.Error(), "--secret-key") {
		t.Errorf("error should mention --secret-key: %v", err)
	}
}

// ── byo bucket put — secret masking ──────────────────────────────────

func TestBYOBucketPutSecretNeverEchoed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"storage": map[string]any{
				"provider": "s3",
				"bucket":   "my-bucket",
				"region":   "us-east-1",
			},
		})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	const secret = "SUPER_SECRET_KEY_NEVER_ECHO_ME"
	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=s3", "--bucket=my-bucket",
		"--region=us-east-1", "--access-key=AKID", "--secret-key=" + secret, "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket put: %v", err)
	}
	if strings.Contains(out, secret) {
		t.Errorf("secret key must never appear in stdout: %q", out)
	}
}

func TestBYOBucketPutHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"storage": map[string]any{
				"provider": "s3",
				"bucket":   "my-bucket",
				"region":   "us-east-1",
			},
		})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=s3", "--bucket=my-bucket",
		"--region=us-east-1", "--access-key=AKID", "--secret-key=secret", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket put: %v", err)
	}
	if gotBody["provider"] != "s3" {
		t.Errorf("provider in body: got %v", gotBody["provider"])
	}
	if gotBody["bucket"] != "my-bucket" {
		t.Errorf("bucket in body: got %v", gotBody["bucket"])
	}
	if !strings.Contains(out, "configured") {
		t.Errorf("expected 'configured' in output: %q", out)
	}
}

func TestBYOBucketPut403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "put",
		"--project=proj1", "--provider=s3", "--bucket=b",
		"--region=us-east-1", "--access-key=ak", "--secret-key=sk", "--yes"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 403 {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── byo bucket probe ──────────────────────────────────────────────────

func TestBYOBucketProbeOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage/probe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"ok": true, "latency_ms": 12.5})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket probe: %v", err)
	}
	if !strings.Contains(out, "OK") {
		t.Errorf("expected 'OK' in output: %q", out)
	}
}

func TestBYOBucketProbeFailure(t *testing.T) {
	mux := http.NewServeMux()
	errMsg := "connection refused"
	mux.HandleFunc("/v1/projects/proj1/storage/probe", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"ok": false, "latency_ms": 0, "error": errMsg})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket probe failure: %v", err)
	}
	if !strings.Contains(out, "FAILED") || !strings.Contains(out, errMsg) {
		t.Errorf("expected FAILED + error message in output: %q", out)
	}
}

func TestBYOBucketProbeJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage/probe", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"ok": true, "latency_ms": 8.0})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"bucket", "probe", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket probe --json: %v", err)
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

// ── byo bucket delete ─────────────────────────────────────────────────

func TestBYOBucketDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"revoked": true})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	err := cmdBYO(gByoBucket(srv), []string{"bucket", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "removed") {
		t.Errorf("expected 'removed' in output: %q", out)
	}
}

func TestBYOBucketDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"revoked": true})
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdBYO(g, []string{"bucket", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("byo bucket delete --json: %v", err)
	}
	var got struct {
		Revoked bool `json:"revoked"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Revoked {
		t.Errorf("revoked: expected true, got false")
	}
}

func TestBYOBucketDelete422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/storage", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "storage is in use")
	})
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "delete", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error on 422")
	}
	ae, ok := AsAPIError(err)
	if !ok || ae.HTTPStatus != 422 {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestBYOUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestBYOBucketUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := byoBucketSrv(t, mux)
	setByoBucketEnv(t, srv)

	err := cmdBYO(gByoBucket(srv), []string{"bucket", "frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown byo bucket subcommand")
	}
}

func TestBYOHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdBYO(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("byo help: %v", err)
	}
}
