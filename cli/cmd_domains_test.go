// cmd_domains_test — coverage for the domains subcommand.
//
// Each test drives an httptest.Server stub that asserts on method + path.
// Confirmation prompts are bypassed via --yes or piped stdin strings.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and integration_test.go / config_test.go
// respectively — all in package main.
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

func domainSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setDomainEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gDomain(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleDomain(id, hostname, status, certStatus string) map[string]any {
	return map[string]any{
		"id":          id,
		"hostname":    hostname,
		"status":      status,
		"cert_status": certStatus,
		"created_at":  "2026-01-01T00:00:00Z",
	}
}

// ── domains list ──────────────────────────────────────────────────────

func TestDomainsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"domains": []any{}})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("domains list empty: %v", err)
	}
	if !strings.Contains(out, "(no custom domains)") {
		t.Errorf("expected '(no custom domains)', got: %q", out)
	}
}

func TestDomainsListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"domains": []any{
				sampleDomain("d1", "app.example.com", "active", "issued"),
				sampleDomain("d2", "api.example.com", "pending", "pending"),
			},
		})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("domains list: %v", err)
	}
	if !strings.Contains(out, "app.example.com") || !strings.Contains(out, "api.example.com") {
		t.Errorf("hostnames missing from output: %q", out)
	}
	if !strings.Contains(out, "issued") {
		t.Errorf("cert status missing from output: %q", out)
	}
}

func TestDomainsListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"domains": []any{sampleDomain("d1", "app.example.com", "active", "issued")},
		})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("domains list --json: %v", err)
	}
	var got struct {
		Domains []*CustomDomain `json:"domains"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode domains list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Domains) != 1 || got.Domains[0].ID != "d1" {
		t.Errorf("unexpected domains in JSON: %+v", got.Domains)
	}
}

// ── domains add ───────────────────────────────────────────────────────

func TestDomainsAddHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"domain": sampleDomain("d-new", "app.example.com", "pending", "none"),
			"dns_records": []any{
				map[string]any{"type": "CNAME", "name": "_basin.app.example.com", "value": "verify.basin.run", "ttl": 3600},
			},
		})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"add", "--project=proj1", "app.example.com"})
	out := read()
	if err != nil {
		t.Fatalf("domains add: %v", err)
	}
	if gotBody["hostname"] != "app.example.com" {
		t.Errorf("hostname in body: got %v, want 'app.example.com'", gotBody["hostname"])
	}
	if !strings.Contains(out, "app.example.com") {
		t.Errorf("hostname missing from output: %q", out)
	}
	if !strings.Contains(out, "CNAME") {
		t.Errorf("DNS record type missing from output: %q", out)
	}
}

func TestDomainsAddJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"domain":      sampleDomain("d-new", "app.example.com", "pending", "none"),
			"dns_records": []any{},
		})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"add", "--project=proj1", "app.example.com"})
	out := read()
	if err != nil {
		t.Fatalf("domains add --json: %v", err)
	}
	var got AddDomainResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode domains add JSON: %v\nbody=%s", err, out)
	}
	if got.Domain == nil || got.Domain.ID != "d-new" {
		t.Errorf("domain.id: got %+v", got.Domain)
	}
}

func TestDomainsAdd409Conflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "domain_conflict", "hostname already registered")
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	err := cmdDomains(gDomain(srv), []string{"add", "--project=proj1", "dup.example.com"})
	if err == nil {
		t.Fatal("expected error on 409, got nil")
	}
	if !strings.Contains(err.Error(), "dup.example.com") {
		t.Errorf("expected hostname in error: %v", err)
	}
}

func TestDomainsAddMissingHostname(t *testing.T) {
	mux := http.NewServeMux()
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	err := cmdDomains(gDomain(srv), []string{"add", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when hostname missing")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage hint in error: %v", err)
	}
}

// ── domains verify ────────────────────────────────────────────────────

func TestDomainsVerifyHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1/verify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"verified": true, "next_check_at": ""})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"verify", "--project=proj1", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains verify: %v", err)
	}
	if !called {
		t.Error("verify endpoint was not called")
	}
	if !strings.Contains(out, "verified") {
		t.Errorf("expected 'verified' in output: %q", out)
	}
}

func TestDomainsVerify404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/missing/verify", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "domain not found")
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	err := cmdDomains(gDomain(srv), []string{"verify", "--project=proj1", "missing"})
	if err == nil {
		t.Fatal("expected error on 404 verify, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

func TestDomainsVerifyJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1/verify", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"verified": false, "next_check_at": "2026-01-02T00:00:00Z"})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"verify", "--project=proj1", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains verify --json: %v", err)
	}
	var got struct {
		Verified    bool   `json:"verified"`
		NextCheckAt string `json:"next_check_at"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode verify JSON: %v\nbody=%s", err, out)
	}
	if got.Verified {
		t.Errorf("expected verified=false, got true")
	}
	if got.NextCheckAt != "2026-01-02T00:00:00Z" {
		t.Errorf("next_check_at: got %q", got.NextCheckAt)
	}
}

// ── domains cert ──────────────────────────────────────────────────────

func TestDomainsCertHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1/cert", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"cert_status": "pending", "issued": false})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"cert", "--project=proj1", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains cert: %v", err)
	}
	if !called {
		t.Error("cert endpoint was not called")
	}
	if !strings.Contains(out, "pending") {
		t.Errorf("expected 'pending' in output: %q", out)
	}
}

func TestDomainsCertJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1/cert", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"cert_status": "pending", "issued": false})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"cert", "--project=proj1", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains cert --json: %v", err)
	}
	var got struct {
		CertStatus string `json:"cert_status"`
		Issued     bool   `json:"issued"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode cert JSON: %v\nbody=%s", err, out)
	}
	if got.CertStatus != "pending" {
		t.Errorf("cert_status: got %q, want pending", got.CertStatus)
	}
}

// ── domains remove ────────────────────────────────────────────────────

func TestDomainsRemoveWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"removed": true, "id": "d1"})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"remove", "--project=proj1", "--yes", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains remove: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "Removed") {
		t.Errorf("expected 'Removed' in output: %q", out)
	}
}

func TestDomainsRemoveDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdDomains(gDomain(srv), []string{"remove", "--project=proj1", "d1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("domains remove declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestDomainsRemove404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "domain not found")
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	err := cmdDomains(gDomain(srv), []string{"remove", "--project=proj1", "--yes", "missing"})
	if err == nil {
		t.Fatal("expected error on 404 remove, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

func TestDomainsRemoveJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"remove", "--project=proj1", "d1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestDomainsRemoveJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/custom-domains/d1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"removed": true, "id": "d1"})
	})
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDomains(g, []string{"remove", "--project=proj1", "--yes", "d1"})
	out := read()
	if err != nil {
		t.Fatalf("domains remove --json: %v", err)
	}
	var got struct {
		Removed bool   `json:"removed"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode remove JSON: %v\nbody=%s", err, out)
	}
	if !got.Removed {
		t.Errorf("removed: expected true, got false")
	}
	if got.ID != "d1" {
		t.Errorf("id: got %q, want d1", got.ID)
	}
}

// ── dispatcher / arg-parse ────────────────────────────────────────────

func TestDomainsUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	err := cmdDomains(gDomain(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestDomainsHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdDomains(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("domains help: %v", err)
	}
}

func TestDomainsNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := domainSrv(t, mux)
	setDomainEnv(t, srv)

	// No --project flag and no working project config → error with hint.
	err := cmdDomains(gDomain(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
