// cmd_metrics_test — coverage for the metrics subcommand.
//
// Each test drives an httptest.Server stub that asserts on method,
// path, and query-string. Shared helpers (stubJSON, stubError,
// captureStdout, withTempConfigDir) come from cmd_branches_test.go
// and config_test.go — all in package main.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── helpers ──────────────────────────────────────────────────────────

func metricsSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setMetricsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gMetrics(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleMetrics() []any {
	return []any{
		map[string]any{"name": "queries_per_sec", "value": 42.5, "unit": "qps", "timestamp": "2026-01-01T00:00:00Z"},
		map[string]any{"name": "cache_hit_ratio", "value": 0.97, "unit": "ratio", "timestamp": "2026-01-01T00:00:00Z"},
	}
}

// ── project metrics — happy path ──────────────────────────────────────

func TestMetricsProjectHappyPath(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.RequestURI()
		stubJSON(w, map[string]any{"metrics": sampleMetrics()})
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	read := captureStdout(t)
	err := cmdMetrics(gMetrics(srv), []string{"--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("metrics project: %v", err)
	}
	if !strings.Contains(out, "queries_per_sec") {
		t.Errorf("metric name missing from output: %q", out)
	}
	if !strings.Contains(gotPath, "/v1/projects/proj1/metrics") {
		t.Errorf("unexpected path: %s", gotPath)
	}
}

// ── project metrics — range and metric query-string passthrough ────────

func TestMetricsProjectQueryStringPassthrough(t *testing.T) {
	var gotRaw string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		gotRaw = r.URL.RawQuery
		stubJSON(w, map[string]any{"metrics": []any{}})
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	_ = captureStdout(t)
	_ = cmdMetrics(gMetrics(srv), []string{"--project=proj1", "--range=7d", "--metric=cache_hit_ratio"})
	if !strings.Contains(gotRaw, "range=7d") {
		t.Errorf("expected range=7d in query string, got: %q", gotRaw)
	}
	if !strings.Contains(gotRaw, "metric=cache_hit_ratio") {
		t.Errorf("expected metric= in query string, got: %q", gotRaw)
	}
}

// ── project metrics — --json shape lock ───────────────────────────────

func TestMetricsProjectJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"metrics": sampleMetrics()})
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMetrics(g, []string{"--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("metrics project --json: %v", err)
	}
	var got struct {
		Metrics []Metric `json:"metrics"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode metrics JSON: %v\nbody=%s", err, out)
	}
	if len(got.Metrics) != 2 {
		t.Errorf("expected 2 metrics, got %d", len(got.Metrics))
	}
	if got.Metrics[0].Name != "queries_per_sec" {
		t.Errorf("first metric name: got %q, want queries_per_sec", got.Metrics[0].Name)
	}
}

// ── org metrics — happy path ──────────────────────────────────────────

func TestMetricsOrgHappyPath(t *testing.T) {
	var gotPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		gotPath = r.URL.Path
		stubJSON(w, map[string]any{"metrics": sampleMetrics()})
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	read := captureStdout(t)
	err := cmdMetrics(gMetrics(srv), []string{"--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("metrics org: %v", err)
	}
	if gotPath != "/v1/orgs/acme/metrics" {
		t.Errorf("expected /v1/orgs/acme/metrics, got %q", gotPath)
	}
	if !strings.Contains(out, "queries_per_sec") {
		t.Errorf("metric name missing from output: %q", out)
	}
}

// ── metrics empty result ──────────────────────────────────────────────

func TestMetricsEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"metrics": []any{}})
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	read := captureStdout(t)
	err := cmdMetrics(gMetrics(srv), []string{"--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("metrics empty: %v", err)
	}
	if !strings.Contains(out, "(no metrics)") {
		t.Errorf("expected '(no metrics)', got: %q", out)
	}
}

// ── validation: neither --project nor --org ───────────────────────────

func TestMetricsMissingProjectAndOrg(t *testing.T) {
	mux := http.NewServeMux()
	srv := metricsSrv(t, mux)
	// Set up env with no working project.
	setMetricsEnv(t, srv)

	err := cmdMetrics(gMetrics(srv), []string{})
	if err == nil {
		t.Fatal("expected error when neither --project nor --org set")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Errorf("expected 'required' in error: %v", err)
	}
}

// ── validation: both --project and --org set ──────────────────────────

func TestMetricsBothProjectAndOrg(t *testing.T) {
	mux := http.NewServeMux()
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	err := cmdMetrics(gMetrics(srv), []string{"--project=proj1", "--org=acme"})
	if err == nil {
		t.Fatal("expected error when both --project and --org are set")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected 'mutually exclusive' in error: %v", err)
	}
}

// ── 403 error mapping ─────────────────────────────────────────────────

func TestMetrics403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "access denied")
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	err := cmdMetrics(gMetrics(srv), []string{"--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if !strings.Contains(err.Error(), "access denied") {
		t.Errorf("expected 'access denied' in error: %v", err)
	}
}

// ── 404 error passthrough ─────────────────────────────────────────────

func TestMetrics404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/metrics", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := metricsSrv(t, mux)
	setMetricsEnv(t, srv)

	err := cmdMetrics(gMetrics(srv), []string{"--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404")
	}
	if ae, ok := AsAPIError(err); ok {
		if ae.HTTPStatus != 404 {
			t.Errorf("expected HTTP 404, got %d", ae.HTTPStatus)
		}
	}
}

// ── help does not error ───────────────────────────────────────────────

func TestMetricsHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdMetrics(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("metrics help: %v", err)
	}
}
