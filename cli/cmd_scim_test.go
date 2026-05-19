// cmd_scim_test — coverage for `basin scim` subcommands.
//
// Secret-bearing responses (tokens create) are verified to print the
// plaintext secret with a "store now" warning. Subsequent list calls
// MUST NOT include the secret — only prefix + metadata fields.
//
// The SCIM tokens list endpoint may 404 on older clouds; tests verify
// the graceful empty-list fallback.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────

func scimSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func gSCIM(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleSCIMConfig() map[string]any {
	return map[string]any{
		"enabled":       true,
		"base_url":      "https://api.basin.run/scim/v2/orgs/acme",
		"schemas_url":   "https://api.basin.run/scim/v2/orgs/acme/Schemas",
		"configured_at": "2026-05-01T00:00:00Z",
	}
}

func sampleSCIMToken(id, prefix string) map[string]any {
	return map[string]any{
		"id":         id,
		"name":       "Okta provisioning",
		"prefix":     prefix,
		"created_at": "2026-05-01T00:00:00Z",
	}
}

// ── scim config get ───────────────────────────────────────────────────

func TestSCIMConfigGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"scim": sampleSCIMConfig()})
	})
	srv := scimSrv(t, mux)

	read := captureStdout(t)
	if err := cmdSCIM(gSCIM(srv), []string{"config", "get", "--org=acme"}); err != nil {
		t.Fatalf("scim config get: %v", err)
	}
	out := read()
	if !strings.Contains(out, "scim/v2/orgs/acme") {
		t.Errorf("expected base_url in output: %q", out)
	}
}

func TestSCIMConfigGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/config", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"scim": sampleSCIMConfig()})
	})
	srv := scimSrv(t, mux)

	g := gSCIM(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdSCIM(g, []string{"config", "get", "--org=acme"}); err != nil {
		t.Fatalf("scim config get --json: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	scim, ok := result["scim"]
	if !ok {
		t.Fatalf("JSON missing 'scim' key: %s", out)
	}
	scimMap, _ := scim.(map[string]any)
	if _, ok := scimMap["base_url"]; !ok {
		t.Errorf("'scim' missing base_url: %v", scimMap)
	}
}

func TestSCIMConfigGetMissingOrg(t *testing.T) {
	err := cmdSCIM(&globalFlags{token: "tok"}, []string{"config", "get"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("expected --org mention in error: %v", err)
	}
}

// ── scim tokens list ──────────────────────────────────────────────────

func TestSCIMTokensListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"tokens": []any{sampleSCIMToken("tok1", "bso_scim_abc")},
		})
	})
	srv := scimSrv(t, mux)

	read := captureStdout(t)
	if err := cmdSCIM(gSCIM(srv), []string{"tokens", "list", "--org=acme"}); err != nil {
		t.Fatalf("scim tokens list: %v", err)
	}
	out := read()
	if !strings.Contains(out, "tok1") {
		t.Errorf("expected token id in output: %q", out)
	}
	// Secret must NOT appear in list output.
	if strings.Contains(out, "secret") {
		t.Errorf("secret must not appear in list output: %q", out)
	}
}

func TestSCIMTokensListFallback404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens", func(w http.ResponseWriter, r *http.Request) {
		// Older cloud: list endpoint not implemented.
		w.WriteHeader(http.StatusNotFound)
	})
	srv := scimSrv(t, mux)

	read := captureStdout(t)
	// Should NOT error — graceful fallback.
	if err := cmdSCIM(gSCIM(srv), []string{"tokens", "list", "--org=acme"}); err != nil {
		t.Fatalf("expected graceful 404 handling, got: %v", err)
	}
	out := read()
	if !strings.Contains(out, "not available") && !strings.Contains(out, "no SCIM") {
		t.Errorf("expected fallback message in output: %q", out)
	}
}

func TestSCIMTokensListMissingOrg(t *testing.T) {
	err := cmdSCIM(&globalFlags{token: "tok"}, []string{"tokens", "list"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

// ── scim tokens create ────────────────────────────────────────────────

func TestSCIMTokensCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		stubJSON(w, map[string]any{
			"token":  sampleSCIMToken("tok1", "bso_scim_abc"),
			"secret": "bso_scim_supersecret123",
		})
	})
	srv := scimSrv(t, mux)

	read := captureStdout(t)
	if err := cmdSCIM(gSCIM(srv), []string{"tokens", "create", "--org=acme", "--name=Okta"}); err != nil {
		t.Fatalf("scim tokens create: %v", err)
	}
	out := read()
	// Secret MUST be shown on creation.
	if !strings.Contains(out, "bso_scim_supersecret123") {
		t.Errorf("expected secret in create output: %q", out)
	}
	// Store-now warning must be present.
	if !strings.Contains(strings.ToLower(out), "store") {
		t.Errorf("expected 'store now' warning in output: %q", out)
	}
	if gotBody["name"] != "Okta" {
		t.Errorf("expected name=Okta in body: %v", gotBody)
	}
}

func TestSCIMTokensCreateSecretNotInList(t *testing.T) {
	// After creating a token, a subsequent list call must not reveal the secret.
	secret := "bso_scim_supersecret123"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			stubJSON(w, map[string]any{
				"token":  sampleSCIMToken("tok1", "bso_scim_abc"),
				"secret": secret,
			})
		case http.MethodGet:
			// List response must NOT contain the secret.
			stubJSON(w, map[string]any{
				"tokens": []any{sampleSCIMToken("tok1", "bso_scim_abc")},
			})
		}
	})
	srv := scimSrv(t, mux)

	g := gSCIM(srv)
	g.json = true

	// Create step.
	read := captureStdout(t)
	if err := cmdSCIM(g, []string{"tokens", "create", "--org=acme", "--name=Okta"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	createOut := read()
	if !strings.Contains(createOut, secret) {
		t.Fatalf("secret must appear in create output: %q", createOut)
	}

	// List step — secret must NOT appear.
	read = captureStdout(t)
	if err := cmdSCIM(g, []string{"tokens", "list", "--org=acme"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	listOut := read()
	if strings.Contains(listOut, secret) {
		t.Errorf("secret must NOT appear in list output: %q", listOut)
	}
}

func TestSCIMTokensCreateMissingOrg(t *testing.T) {
	err := cmdSCIM(&globalFlags{token: "tok"}, []string{"tokens", "create", "--name=Okta"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestSCIMTokensCreateMissingName(t *testing.T) {
	err := cmdSCIM(&globalFlags{token: "tok"}, []string{"tokens", "create", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing --name")
	}
}

// ── scim tokens revoke ────────────────────────────────────────────────

func TestSCIMTokensRevokeHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens/tok1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"revoked": true, "id": "tok1"})
	})
	srv := scimSrv(t, mux)

	g := gSCIM(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdSCIM(g, []string{"tokens", "revoke", "--org=acme", "--yes", "tok1"}); err != nil {
		t.Fatalf("scim tokens revoke: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["revoked"] != true {
		t.Errorf("expected revoked=true: %v", result)
	}
}

func TestSCIMTokensRevokeMissingOrg(t *testing.T) {
	err := cmdSCIM(&globalFlags{token: "tok"}, []string{"tokens", "revoke", "tok1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestSCIMTokensRevoke404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/scim/tokens/notfound", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		stubJSON(w, map[string]any{"error": map[string]any{"code": "not_found", "message": "token not found"}})
	})
	srv := scimSrv(t, mux)

	err := cmdSCIM(gSCIM(srv), []string{"tokens", "revoke", "--org=acme", "--yes", "notfound"})
	if err == nil {
		t.Fatal("expected error for 404")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if ae.HTTPStatus != 404 {
		t.Errorf("expected HTTP 404, got %d", ae.HTTPStatus)
	}
}
