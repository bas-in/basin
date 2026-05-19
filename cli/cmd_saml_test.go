// cmd_saml_test — coverage for `basin saml` subcommands.
//
// All tests use an httptest.Server stub. Confirmation prompts are
// bypassed via --yes. Missing --org produces a typed error before
// any HTTP call is made.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────

func samlSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func gSAML(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleSAML() map[string]any {
	return map[string]any{
		"idp_metadata_url": "https://idp.example.com/metadata",
		"idp_entity_id":    "https://idp.example.com/entity",
		"sp_metadata_url":  "https://api.basin.run/saml/sp/orgs/acme/metadata",
		"enabled":          false,
		"configured_at":    "2026-05-01T00:00:00Z",
	}
}

// ── saml get ──────────────────────────────────────────────────────────

func TestSAMLGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"saml": sampleSAML()})
	})
	srv := samlSrv(t, mux)

	read := captureStdout(t)
	err := cmdSAML(gSAML(srv), []string{"get", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("saml get: %v", err)
	}
	if !strings.Contains(out, "idp.example.com") {
		t.Errorf("expected idp_metadata_url in output: %q", out)
	}
}

func TestSAMLGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"saml": sampleSAML()})
	})
	srv := samlSrv(t, mux)

	g := gSAML(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdSAML(g, []string{"get", "--org=acme"}); err != nil {
		t.Fatalf("saml get --json: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	saml, ok := result["saml"]
	if !ok {
		t.Fatalf("JSON missing 'saml' key: %s", out)
	}
	samlMap, _ := saml.(map[string]any)
	if _, ok := samlMap["idp_metadata_url"]; !ok {
		t.Errorf("'saml' missing idp_metadata_url: %v", samlMap)
	}
}

func TestSAMLGetMissingOrg(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"get"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("expected --org mention in error: %v", err)
	}
}

func TestSAMLGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		stubJSON(w, map[string]any{"error": map[string]any{"code": "not_found", "message": "org not found"}})
	})
	srv := samlSrv(t, mux)

	err := cmdSAML(gSAML(srv), []string{"get", "--org=acme"})
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

// ── saml put ──────────────────────────────────────────────────────────

func TestSAMLPutHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		stubJSON(w, map[string]any{"saml": sampleSAML()})
	})
	srv := samlSrv(t, mux)

	read := captureStdout(t)
	err := cmdSAML(gSAML(srv), []string{"put", "--org=acme",
		"--metadata-url=https://idp.example.com/metadata", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("saml put: %v", err)
	}
	if gotBody["idp_metadata_url"] != "https://idp.example.com/metadata" {
		t.Errorf("expected metadata URL in body: %v", gotBody)
	}
	if !strings.Contains(out, "configured") {
		t.Errorf("expected 'configured' in output: %q", out)
	}
}

func TestSAMLPutMissingOrg(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"put", "--metadata-url=https://x.com/meta"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestSAMLPutMissingMetadataURL(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"put", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing --metadata-url")
	}
}

// ── saml test ─────────────────────────────────────────────────────────

func TestSAMLTestOK(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"ok": true, "error": nil, "tested_at": "2026-05-01T12:00:00Z"})
	})
	srv := samlSrv(t, mux)

	read := captureStdout(t)
	if err := cmdSAML(gSAML(srv), []string{"test", "--org=acme"}); err != nil {
		t.Fatalf("saml test: %v", err)
	}
	out := read()
	if !strings.Contains(out, "OK") {
		t.Errorf("expected OK in output: %q", out)
	}
}

func TestSAMLTestMissingOrg(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"test"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

// ── saml enable ───────────────────────────────────────────────────────

func TestSAMLEnableHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml/enable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"enabled": true})
	})
	srv := samlSrv(t, mux)

	g := gSAML(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdSAML(g, []string{"enable", "--org=acme", "--yes"}); err != nil {
		t.Fatalf("saml enable: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if result["enabled"] != true {
		t.Errorf("expected enabled=true: %v", result)
	}
}

func TestSAMLEnableMissingOrg(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"enable", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestSAMLEnable403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml/enable", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		stubJSON(w, map[string]any{"error": map[string]any{"code": "forbidden", "message": "owner role required"}})
	})
	srv := samlSrv(t, mux)

	err := cmdSAML(gSAML(srv), []string{"enable", "--org=acme", "--yes"})
	if err == nil {
		t.Fatal("expected error for 403")
	}
	ae, ok := AsAPIError(err)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if ae.HTTPStatus != 403 {
		t.Errorf("expected HTTP 403, got %d", ae.HTTPStatus)
	}
}

// ── saml disable ──────────────────────────────────────────────────────

func TestSAMLDisableHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/saml/disable", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"enabled": false})
	})
	srv := samlSrv(t, mux)

	g := gSAML(srv)
	g.json = true
	read := captureStdout(t)
	if err := cmdSAML(g, []string{"disable", "--org=acme", "--yes"}); err != nil {
		t.Fatalf("saml disable: %v", err)
	}
	out := read()
	var result map[string]any
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	// JSON shape: { "enabled": false }
	if enabled, ok := result["enabled"].(bool); !ok || enabled {
		t.Errorf("expected enabled=false: %v", result)
	}
}

func TestSAMLDisableMissingOrg(t *testing.T) {
	err := cmdSAML(&globalFlags{token: "tok"}, []string{"disable", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}
