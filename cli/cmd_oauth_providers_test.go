// cmd_oauth_providers_test — coverage for the oauth-providers subcommand.
//
// Tests drive an httptest.Server stub and assert on method + path + body.
// Validation errors are tested before any HTTP call.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and integration_test.go.
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

func oauthSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setOAuthEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gOAuth(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleOAuthProvider(provider string, enabled bool, prefix string) map[string]any {
	return map[string]any{
		"provider":            provider,
		"enabled":             enabled,
		"client_id_prefix":   prefix,
		"client_secret_masked": "***",
	}
}

// ── validation: oauth-providers set ──────────────────────────────────

func TestOAuthProvidersSetMissingClientID(t *testing.T) {
	mux := http.NewServeMux()
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	// provider comes AFTER flags (standard flag-then-positional pattern)
	err := cmdOAuthProviders(gOAuth(srv), []string{"set", "--client-secret=secret", "--project=proj1", "google"})
	if err == nil {
		t.Fatal("expected error when --client-id is missing")
	}
	if !strings.Contains(err.Error(), "--client-id") {
		t.Errorf("error should mention --client-id: %v", err)
	}
}

func TestOAuthProvidersSetMissingClientSecret(t *testing.T) {
	mux := http.NewServeMux()
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	// provider comes AFTER flags (standard flag-then-positional pattern)
	err := cmdOAuthProviders(gOAuth(srv), []string{"set", "--client-id=my-id", "--project=proj1", "google"})
	if err == nil {
		t.Fatal("expected error when --client-secret is missing")
	}
	if !strings.Contains(err.Error(), "--client-secret") {
		t.Errorf("error should mention --client-secret: %v", err)
	}
}

func TestOAuthProvidersSetMissingProvider(t *testing.T) {
	mux := http.NewServeMux()
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	// No positional provider arg → should get usage error.
	err := cmdOAuthProviders(gOAuth(srv), []string{"set", "--client-id=my-id", "--client-secret=secret", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when provider is missing")
	}
}

// ── oauth-providers list ──────────────────────────────────────────────

func TestOAuthProvidersListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"providers": []any{
				sampleOAuthProvider("google", true, "GOOG"),
				sampleOAuthProvider("github", false, "GH"),
			},
		})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers list: %v", err)
	}
	if !strings.Contains(out, "google") || !strings.Contains(out, "github") {
		t.Errorf("provider names missing from output: %q", out)
	}
}

func TestOAuthProvidersListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"providers": []any{}})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers list empty: %v", err)
	}
	if !strings.Contains(out, "(no OAuth providers") {
		t.Errorf("expected '(no OAuth providers' in output: %q", out)
	}
}

func TestOAuthProvidersListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"providers": []any{sampleOAuthProvider("google", true, "GOOG")},
		})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOAuthProviders(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers list --json: %v", err)
	}
	var got struct {
		Providers []*OAuthProvider `json:"providers"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode providers list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Providers) != 1 || got.Providers[0].Provider != "google" {
		t.Errorf("unexpected providers in JSON: %+v", got.Providers)
	}
}

// ── oauth-providers get ───────────────────────────────────────────────

func TestOAuthProvidersGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/google", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"provider": sampleOAuthProvider("google", true, "GOOG")})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"get", "--project=proj1", "google"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers get: %v", err)
	}
	if !strings.Contains(out, "google") {
		t.Errorf("provider name missing from output: %q", out)
	}
}

func TestOAuthProvidersGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/unknown", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "provider not found")
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	err := cmdOAuthProviders(gOAuth(srv), []string{"get", "--project=proj1", "unknown"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

// ── oauth-providers set ───────────────────────────────────────────────

func TestOAuthProvidersSetHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/google", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		// Cloud masks the secret in the response.
		stubJSON(w, map[string]any{"provider": sampleOAuthProvider("google", true, "my-")})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"set",
		"--project=proj1", "--client-id=my-id", "--client-secret=supersecret", "--enabled=true", "google"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers set: %v", err)
	}
	if gotBody["client_id"] != "my-id" {
		t.Errorf("client_id in body: got %v", gotBody["client_id"])
	}
	if gotBody["client_secret"] != "supersecret" {
		t.Errorf("client_secret in body: got %v", gotBody["client_secret"])
	}
	if !strings.Contains(out, "secret stored") {
		t.Errorf("expected 'secret stored' in output: %q", out)
	}
}

func TestOAuthProvidersSetSecretNotEchoedInOutput(t *testing.T) {
	// Verify the raw secret never appears in stdout even if the cloud
	// (erroneously) echoes it back in the response.
	rawSecret := "ultra-sensitive-secret-xyz"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/github", func(w http.ResponseWriter, r *http.Request) {
		// Simulate a buggy cloud that echoes the secret in a field.
		stubJSON(w, map[string]any{
			"provider": map[string]any{
				"provider":             "github",
				"enabled":              true,
				"client_id_prefix":     rawSecret, // intentionally wrong — cloud bug sim
				"client_secret_masked": rawSecret,  // intentionally wrong
			},
		})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOAuthProviders(g, []string{"set",
		"--project=proj1",
		"--client-id=gh-id",
		"--client-secret=" + rawSecret,
		"github"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers set secret mask: %v", err)
	}
	// The raw secret must not appear in the JSON output.
	if strings.Contains(out, rawSecret) {
		t.Errorf("raw client_secret leaked into JSON output: %q", out)
	}
}

func TestOAuthProvidersSetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/google", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"provider": sampleOAuthProvider("google", true, "my-")})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOAuthProviders(g, []string{"set",
		"--project=proj1", "--client-id=my-id", "--client-secret=s3cr3t", "google"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers set --json: %v", err)
	}
	var got struct {
		Provider *OAuthProvider `json:"provider"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode set JSON: %v\nbody=%s", err, out)
	}
	if got.Provider == nil || got.Provider.Provider != "google" {
		t.Errorf("provider in JSON: got %+v", got.Provider)
	}
}

// ── oauth-providers delete ────────────────────────────────────────────

func TestOAuthProvidersDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/github", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true, "provider": "github"})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"delete", "--project=proj1", "--yes", "github"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

func TestOAuthProvidersDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/github", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdOAuthProviders(gOAuth(srv), []string{"delete", "--project=proj1", "github"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("oauth-providers delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestOAuthProvidersDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/github", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true, "provider": "github"})
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOAuthProviders(g, []string{"delete", "--project=proj1", "--yes", "github"})
	out := read()
	if err != nil {
		t.Fatalf("oauth-providers delete --json: %v", err)
	}
	var got struct {
		Deleted  bool   `json:"deleted"`
		Provider string `json:"provider"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Deleted {
		t.Errorf("deleted: expected true, got false")
	}
	if got.Provider != "github" {
		t.Errorf("provider: got %q, want github", got.Provider)
	}
}

func TestOAuthProvidersDelete422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/oauth-providers/google", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "cannot delete last provider")
	})
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	err := cmdOAuthProviders(gOAuth(srv), []string{"delete", "--project=proj1", "--yes", "google"})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

func TestOAuthProvidersNoProject(t *testing.T) {
	mux := http.NewServeMux()
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	err := cmdOAuthProviders(gOAuth(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}

func TestOAuthProvidersUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := oauthSrv(t, mux)
	setOAuthEnv(t, srv)

	err := cmdOAuthProviders(gOAuth(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}
