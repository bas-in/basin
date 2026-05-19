// cmd_orgs_test — coverage for the orgs subcommand write paths
// (create, update, delete, branding) plus existing read paths.
//
// Tests use httptest.Server stubs and drive cmdOrgs() directly.
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

func orgsServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setOrgsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func orgsGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleOrg(slug, name, plan string) map[string]any {
	return map[string]any{
		"id":   "org-" + slug,
		"slug": slug,
		"name": name,
		"plan": plan,
	}
}

// ── orgs create ───────────────────────────────────────────────────────

func TestOrgsCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"org": sampleOrg("acme", "Acme Corp", "free")})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"create", "--slug=acme", "--name=Acme Corp", "--plan=free"})
	out := read()
	if err != nil {
		t.Fatalf("orgs create: %v", err)
	}
	if gotBody["slug"] != "acme" {
		t.Errorf("slug in body: got %v, want 'acme'", gotBody["slug"])
	}
	if gotBody["name"] != "Acme Corp" {
		t.Errorf("name in body: got %v, want 'Acme Corp'", gotBody["name"])
	}
	if !strings.Contains(out, "acme") {
		t.Errorf("org slug missing from output: %q", out)
	}
}

func TestOrgsCreateMissingSlug(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"create", "--name=Acme Corp"})
	if err == nil {
		t.Fatal("expected error for missing --slug, got nil")
	}
	if !strings.Contains(err.Error(), "--slug") {
		t.Errorf("expected '--slug' in error, got: %v", err)
	}
}

func TestOrgsCreateMissingName(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"create", "--slug=acme"})
	if err == nil {
		t.Fatal("expected error for missing --name, got nil")
	}
	if !strings.Contains(err.Error(), "--name") {
		t.Errorf("expected '--name' in error, got: %v", err)
	}
}

func TestOrgsCreate409Conflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "conflict", "Org slug already taken.")
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"create", "--slug=acme", "--name=Acme"})
	if err == nil {
		t.Fatal("expected error on 409, got nil")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Errorf("expected 'conflict' in error: %v", err)
	}
}

func TestOrgsCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{"org": sampleOrg("acme", "Acme Corp", "pro")})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"create", "--slug=acme", "--name=Acme Corp", "--plan=pro"})
	out := read()
	if err != nil {
		t.Fatalf("orgs create --json: %v", err)
	}
	var got struct {
		Org *Org `json:"org"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode orgs create JSON: %v\nbody=%s", err, out)
	}
	if got.Org == nil || got.Org.Slug != "acme" {
		t.Errorf("org.slug: %+v", got.Org)
	}
	if got.Org.Plan != "pro" {
		t.Errorf("org.plan: got %q, want 'pro'", got.Org.Plan)
	}
}

// ── orgs update ───────────────────────────────────────────────────────

func TestOrgsUpdateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"org": sampleOrg("acme", "Acme Renamed", "free")})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"update", "--name=Acme Renamed", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs update: %v", err)
	}
	if gotBody["name"] != "Acme Renamed" {
		t.Errorf("name in body: got %v, want 'Acme Renamed'", gotBody["name"])
	}
	if !strings.Contains(out, "acme") {
		t.Errorf("org slug missing from output: %q", out)
	}
}

func TestOrgsUpdateMissingSlug(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"update", "--name=New Name"})
	if err == nil {
		t.Fatal("expected error for missing slug arg, got nil")
	}
}

func TestOrgsUpdateNothingToUpdate(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"update", "acme"})
	if err == nil {
		t.Fatal("expected error for missing update fields, got nil")
	}
	if !strings.Contains(err.Error(), "nothing to update") {
		t.Errorf("expected 'nothing to update' in error: %v", err)
	}
}

func TestOrgsUpdateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"org": sampleOrg("acme", "Acme Updated", "pro")})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"update", "--plan=pro", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs update --json: %v", err)
	}
	var got struct {
		Org *Org `json:"org"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode orgs update JSON: %v\nbody=%s", err, out)
	}
	if got.Org == nil || got.Org.Plan != "pro" {
		t.Errorf("org.plan: %+v", got.Org)
	}
}

// ── orgs delete ───────────────────────────────────────────────────────

func TestOrgsDeleteConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true, "slug": "acme"})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"delete", "--yes", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint not called")
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

func TestOrgsDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, map[string]any{"deleted": true, "slug": "acme"})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"delete", "acme"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("orgs delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestOrgsDeleteJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"delete", "acme"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestOrgsDeleteJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true, "slug": "acme"})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"delete", "--yes", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs delete --json: %v", err)
	}
	var got struct {
		Deleted bool   `json:"deleted"`
		Slug    string `json:"slug"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode orgs delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Deleted {
		t.Errorf("deleted: got false, want true")
	}
	if got.Slug != "acme" {
		t.Errorf("slug: got %q, want 'acme'", got.Slug)
	}
}

// ── orgs branding get ─────────────────────────────────────────────────

func TestOrgsBrandingGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/branding", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"branding": map[string]any{
				"logo_url":        "https://example.com/logo.png",
				"primary_color":   "#1a2b3c",
				"secondary_color": "#4d5e6f",
			},
		})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"branding", "get", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs branding get: %v", err)
	}
	if !strings.Contains(out, "#1a2b3c") {
		t.Errorf("primary_color missing from output: %q", out)
	}
}

func TestOrgsBrandingGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/branding", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"branding": map[string]any{
				"logo_url":      "https://example.com/logo.png",
				"primary_color": "#1a2b3c",
			},
		})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"branding", "get", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs branding get --json: %v", err)
	}
	var got struct {
		Branding *OrgBranding `json:"branding"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode branding get JSON: %v\nbody=%s", err, out)
	}
	if got.Branding == nil || got.Branding.PrimaryColor != "#1a2b3c" {
		t.Errorf("branding.primary_color: %+v", got.Branding)
	}
}

// ── orgs branding put ─────────────────────────────────────────────────

func TestOrgsBrandingPut(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/branding", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"branding": map[string]any{
				"logo_url":      "https://example.com/logo.png",
				"primary_color": "#1a2b3c",
			},
		})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"branding", "put",
		"--logo-url=https://example.com/logo.png",
		"--primary-color=#1a2b3c",
		"acme",
	})
	out := read()
	if err != nil {
		t.Fatalf("orgs branding put: %v", err)
	}
	if gotBody["logo_url"] != "https://example.com/logo.png" {
		t.Errorf("logo_url in body: got %v", gotBody["logo_url"])
	}
	if !strings.Contains(out, "Updated branding") {
		t.Errorf("expected 'Updated branding' in output: %q", out)
	}
}

// ── orgs branding delete ──────────────────────────────────────────────

func TestOrgsBrandingDelete(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/branding", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true})
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	read := captureStdout(t)
	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"branding", "delete", "acme"})
	out := read()
	if err != nil {
		t.Fatalf("orgs branding delete: %v", err)
	}
	if !called {
		t.Error("branding delete endpoint not called")
	}
	if !strings.Contains(out, "Deleted branding") {
		t.Errorf("expected 'Deleted branding' in output: %q", out)
	}
}

// ── error paths ───────────────────────────────────────────────────────

func TestOrgsBrandingMissingSlug(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"branding", "get"})
	if err == nil {
		t.Fatal("expected error for missing slug in branding get, got nil")
	}
}

func TestOrgsDeleteMissingSlug(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdOrgs(g, []string{"delete"})
	if err == nil {
		t.Fatal("expected error for missing slug in delete, got nil")
	}
}

func TestOrgsDelete404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/ghost", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Org not found.")
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"delete", "--yes", "ghost"})
	if err == nil {
		t.Fatal("expected error on 404 delete, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestOrgsUpdate403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := orgsServer(t, mux)
	setOrgsEnv(t, srv)

	g := orgsGFlag(srv)
	err := cmdOrgs(g, []string{"update", "--name=New Name", "acme"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

func TestOrgsUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdOrgs(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestOrgsHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdOrgs(g, []string{"help"})
	if err != nil {
		t.Fatalf("orgs help: %v", err)
	}
}
