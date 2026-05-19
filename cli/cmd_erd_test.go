// cmd_erd_test — coverage for the erd subcommand.
//
// Each test drives an httptest.Server stub that asserts on method,
// path, and the ?format= query parameter. The stub returns the
// appropriate content-type for svg/dot/json responses.
// Shared helpers come from cmd_branches_test.go and config_test.go.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────

func erdSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setERDEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gERD(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func erdMux(t *testing.T) (*http.ServeMux, *string) {
	t.Helper()
	mux := http.NewServeMux()
	var gotFormat string
	mux.HandleFunc("/v1/projects/proj1/erd", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotFormat = r.URL.Query().Get("format")
		switch gotFormat {
		case "json":
			w.Header().Set("Content-Type", "application/json")
			stubJSON(w, map[string]any{
				"tables":        []any{map[string]any{"name": "users", "schema": "public"}},
				"relationships": []any{},
			})
		case "svg":
			w.Header().Set("Content-Type", "image/svg+xml")
			_, _ = w.Write([]byte("<svg xmlns='http://www.w3.org/2000/svg'><g/></svg>"))
		case "dot":
			w.Header().Set("Content-Type", "text/vnd.graphviz")
			_, _ = w.Write([]byte("digraph G { users; }"))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	return mux, &gotFormat
}

// ── erd export -- format=svg ──────────────────────────────────────────

func TestERDExportSVG(t *testing.T) {
	mux, gotFormat := erdMux(t)
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	read := captureStdout(t)
	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=svg"})
	out := read()
	if err != nil {
		t.Fatalf("erd export svg: %v", err)
	}
	if *gotFormat != "svg" {
		t.Errorf("expected format=svg in query, got %q", *gotFormat)
	}
	if !strings.Contains(out, "<svg") {
		t.Errorf("expected SVG content in output: %q", out)
	}
}

// ── erd export -- format=dot ──────────────────────────────────────────

func TestERDExportDOT(t *testing.T) {
	mux, gotFormat := erdMux(t)
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	read := captureStdout(t)
	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=dot"})
	out := read()
	if err != nil {
		t.Fatalf("erd export dot: %v", err)
	}
	if *gotFormat != "dot" {
		t.Errorf("expected format=dot in query, got %q", *gotFormat)
	}
	if !strings.Contains(out, "digraph") {
		t.Errorf("expected DOT content in output: %q", out)
	}
}

// ── erd export -- format=json --json shape lock ───────────────────────

func TestERDExportJSONShape(t *testing.T) {
	mux, gotFormat := erdMux(t)
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdERD(g, []string{"export", "--project=proj1", "--format=json"})
	out := read()
	if err != nil {
		t.Fatalf("erd export json: %v", err)
	}
	if *gotFormat != "json" {
		t.Errorf("expected format=json in query, got %q", *gotFormat)
	}
	var got ERDResponse
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode erd JSON: %v\nbody=%s", err, out)
	}
	if len(got.Tables) != 1 || got.Tables[0].Name != "users" {
		t.Errorf("unexpected tables in ERD JSON: %+v", got.Tables)
	}
}

// ── erd export -- format=json raw passthrough (without --json) ────────

func TestERDExportJSONRaw(t *testing.T) {
	mux, _ := erdMux(t)
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	read := captureStdout(t)
	// Without --json flag: raw body passthrough.
	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=json"})
	out := read()
	if err != nil {
		t.Fatalf("erd export json raw: %v", err)
	}
	if !strings.Contains(out, "users") {
		t.Errorf("expected table name 'users' in raw output: %q", out)
	}
}

// ── erd export -- output to file ──────────────────────────────────────

func TestERDExportToFile(t *testing.T) {
	mux, _ := erdMux(t)
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "erd.svg")

	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=svg", "--output=" + outPath})
	if err != nil {
		t.Fatalf("erd export to file: %v", err)
	}
	b, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(b), "<svg") {
		t.Errorf("expected SVG in output file: %q", string(b))
	}
}

// ── erd export -- invalid format ─────────────────────────────────────

func TestERDExportInvalidFormat(t *testing.T) {
	mux := http.NewServeMux()
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=pdf"})
	if err == nil {
		t.Fatal("expected error for invalid format")
	}
	if !strings.Contains(err.Error(), "--format") {
		t.Errorf("expected '--format' in error: %v", err)
	}
}

// ── erd export -- missing project ────────────────────────────────────

func TestERDExportMissingProject(t *testing.T) {
	mux := http.NewServeMux()
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	err := cmdERD(gERD(srv), []string{"export"})
	if err == nil {
		t.Fatal("expected error when --project missing and no working project")
	}
}

// ── erd export -- 404 error mapping ──────────────────────────────────

func TestERDExport404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/erd", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := erdSrv(t, mux)
	setERDEnv(t, srv)

	err := cmdERD(gERD(srv), []string{"export", "--project=proj1", "--format=svg"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if ae, ok := AsAPIError(err); ok {
		if ae.HTTPStatus != 404 {
			t.Errorf("expected HTTP 404, got %d", ae.HTTPStatus)
		}
	}
}

// ── erd -- no subcommand ──────────────────────────────────────────────

func TestERDNoSubcommand(t *testing.T) {
	withTempConfigDir(t)
	err := cmdERD(&globalFlags{}, []string{})
	if err == nil {
		t.Fatal("expected error when no subcommand given")
	}
}

// ── erd -- unknown subcommand ─────────────────────────────────────────

func TestERDUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	err := cmdERD(&globalFlags{}, []string{"render"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

// ── erd help ──────────────────────────────────────────────────────────

func TestERDHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdERD(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("erd help: %v", err)
	}
}
