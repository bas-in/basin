// cmd_extensions_test — coverage for the extensions subcommand.
//
// Tests drive an httptest.Server stub and assert on method + path.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and integration_test.go.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

func extensionsSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setExtensionsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gExtensions(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleExtension(name, version string, installed bool) map[string]any {
	return map[string]any{
		"name":        name,
		"version":     version,
		"installed":   installed,
		"description": "description of " + name,
	}
}

// ── extensions list ───────────────────────────────────────────────────

func TestExtensionsListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"extensions": []any{
				sampleExtension("pgvector", "0.7.0", true),
				sampleExtension("basin-cron", "1.0.0", false),
			},
		})
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	read := captureStdout(t)
	err := cmdExtensions(gExtensions(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("extensions list: %v", err)
	}
	if !strings.Contains(out, "pgvector") || !strings.Contains(out, "basin-cron") {
		t.Errorf("extension names missing from output: %q", out)
	}
}

func TestExtensionsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"extensions": []any{}})
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	read := captureStdout(t)
	err := cmdExtensions(gExtensions(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("extensions list empty: %v", err)
	}
	if !strings.Contains(out, "(no extensions)") {
		t.Errorf("expected '(no extensions)', got: %q", out)
	}
}

func TestExtensionsListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"extensions": []any{
				sampleExtension("pgvector", "0.7.0", true),
			},
		})
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdExtensions(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("extensions list --json: %v", err)
	}
	var got struct {
		Extensions []*Extension `json:"extensions"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode extensions JSON: %v\nbody=%s", err, out)
	}
	if len(got.Extensions) != 1 || got.Extensions[0].Name != "pgvector" {
		t.Errorf("unexpected extensions in JSON: %+v", got.Extensions)
	}
}

func TestExtensionsListCatalogFallback(t *testing.T) {
	// Simulate an older cloud that 404s on /catalog but serves /extensions root.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "route not found")
	})
	mux.HandleFunc("/v1/projects/proj1/extensions", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"extensions": []any{
				sampleExtension("basin-net", "2.0.0", false),
			},
		})
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	read := captureStdout(t)
	err := cmdExtensions(gExtensions(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("extensions list fallback: %v", err)
	}
	if !strings.Contains(out, "basin-net") {
		t.Errorf("extension from fallback route missing: %q", out)
	}
}

func TestExtensionsListCatalog403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	err := cmdExtensions(gExtensions(srv), []string{"list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

func TestExtensionsDispatchExplicitList(t *testing.T) {
	// Explicit "list" subcommand with --project flag.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/extensions/catalog", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"extensions": []any{}})
	})
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	err := cmdExtensions(gExtensions(srv), []string{"list", "--project=proj1"})
	if err != nil {
		t.Fatalf("extensions list via explicit dispatch: %v", err)
	}
}

func TestExtensionsUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	err := cmdExtensions(gExtensions(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestExtensionsHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdExtensions(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("extensions help: %v", err)
	}
}

func TestExtensionsNoProject(t *testing.T) {
	mux := http.NewServeMux()
	srv := extensionsSrv(t, mux)
	setExtensionsEnv(t, srv)

	err := cmdExtensions(gExtensions(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
