// cmd_alerts_test — coverage for the alerts subcommand.
//
// Each test drives an httptest.Server stub that asserts on method + path +
// query string + body. Validation errors are tested before any HTTP call.
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

func alertsSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setAlertsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gAlerts(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleAlertRule(id, name, metric, comparator string, threshold float64, enabled bool) map[string]any {
	return map[string]any{
		"id":         id,
		"name":       name,
		"metric":     metric,
		"threshold":  threshold,
		"comparator": comparator,
		"enabled":    enabled,
	}
}

func sampleAlertEvent(id, ruleID, firedAt string, value float64) map[string]any {
	return map[string]any{
		"id":       id,
		"rule_id":  ruleID,
		"fired_at": firedAt,
		"value":    value,
	}
}

// ── validation: alerts rules create ──────────────────────────────────

func TestAlertsRulesCreateMissingName(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "create",
		"--project=proj1", "--metric=cpu", "--threshold=90", "--comparator=gt"})
	if err == nil {
		t.Fatal("expected error when --name is missing")
	}
	if !strings.Contains(err.Error(), "--name") {
		t.Errorf("error should mention --name: %v", err)
	}
}

func TestAlertsRulesCreateMissingMetric(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "create",
		"--project=proj1", "--name=high-cpu", "--threshold=90", "--comparator=gt"})
	if err == nil {
		t.Fatal("expected error when --metric is missing")
	}
	if !strings.Contains(err.Error(), "--metric") {
		t.Errorf("error should mention --metric: %v", err)
	}
}

func TestAlertsRulesCreateMissingThresholdComparator(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "create",
		"--project=proj1", "--name=high-cpu", "--metric=cpu", "--threshold=90"})
	if err == nil {
		t.Fatal("expected error when --comparator is missing")
	}
	if !strings.Contains(err.Error(), "--comparator") {
		t.Errorf("error should mention --comparator: %v", err)
	}
}

func TestAlertsRulesCreateInvalidComparator(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "create",
		"--project=proj1", "--name=high-cpu", "--metric=cpu", "--threshold=90", "--comparator=neq"})
	if err == nil {
		t.Fatal("expected error for invalid comparator")
	}
	if !strings.Contains(err.Error(), "neq") || !strings.Contains(err.Error(), "comparator") {
		t.Errorf("error should mention the invalid comparator: %v", err)
	}
}

// ── validation: alerts rules silence ─────────────────────────────────

func TestAlertsRulesSilenceMissingUntil(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "silence", "--project=proj1", "rule-1"})
	if err == nil {
		t.Fatal("expected error when --until is missing")
	}
	if !strings.Contains(err.Error(), "--until") {
		t.Errorf("error should mention --until: %v", err)
	}
}

func TestAlertsRulesSilenceInvalidRFC3339(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "silence",
		"--project=proj1", "--until=not-a-timestamp", "rule-1"})
	if err == nil {
		t.Fatal("expected error for invalid RFC3339 timestamp")
	}
	if !strings.Contains(err.Error(), "RFC3339") && !strings.Contains(err.Error(), "invalid") {
		t.Errorf("error should mention RFC3339/invalid: %v", err)
	}
}

// ── alerts rules list ─────────────────────────────────────────────────

func TestAlertsRulesListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"rules": []any{
				sampleAlertRule("r1", "high-cpu", "cpu_pct", "gt", 90, true),
				sampleAlertRule("r2", "low-disk", "disk_free", "lt", 10, false),
			},
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules list: %v", err)
	}
	if !strings.Contains(out, "r1") || !strings.Contains(out, "r2") {
		t.Errorf("rule ids missing from output: %q", out)
	}
}

func TestAlertsRulesListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"rules": []any{}})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules list empty: %v", err)
	}
	if !strings.Contains(out, "(no alert rules)") {
		t.Errorf("expected '(no alert rules)', got: %q", out)
	}
}

func TestAlertsRulesListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"rules": []any{sampleAlertRule("r1", "high-cpu", "cpu_pct", "gt", 90, true)},
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAlerts(g, []string{"rules", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules list --json: %v", err)
	}
	var got struct {
		Rules []*AlertRule `json:"rules"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode rules list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Rules) != 1 || got.Rules[0].ID != "r1" {
		t.Errorf("unexpected rules in JSON: %+v", got.Rules)
	}
}

// ── alerts rules create ───────────────────────────────────────────────

func TestAlertsRulesCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"rule": sampleAlertRule("r-new", "high-cpu", "cpu_pct", "gt", 90, true),
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "create",
		"--project=proj1", "--name=high-cpu", "--metric=cpu_pct",
		"--threshold=90", "--comparator=gt"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules create: %v", err)
	}
	if gotBody["comparator"] != "gt" {
		t.Errorf("comparator in body: got %v", gotBody["comparator"])
	}
	if !strings.Contains(out, "r-new") {
		t.Errorf("rule id missing from output: %q", out)
	}
}

func TestAlertsRulesCreateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"rule": sampleAlertRule("r-json", "low-mem", "mem_free", "lte", 512, true),
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAlerts(g, []string{"rules", "create",
		"--project=proj1", "--name=low-mem", "--metric=mem_free",
		"--threshold=512", "--comparator=lte"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules create --json: %v", err)
	}
	var got struct {
		Rule *AlertRule `json:"rule"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode create JSON: %v\nbody=%s", err, out)
	}
	if got.Rule == nil || got.Rule.ID != "r-json" {
		t.Errorf("rule.id: got %+v", got.Rule)
	}
}

// ── alerts rules get ──────────────────────────────────────────────────

func TestAlertsRulesGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"rule": sampleAlertRule("r1", "high-cpu", "cpu_pct", "gt", 90, true)})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "get", "--project=proj1", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules get: %v", err)
	}
	if !strings.Contains(out, "r1") || !strings.Contains(out, "cpu_pct") {
		t.Errorf("expected rule details in output: %q", out)
	}
}

func TestAlertsRulesGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "rule not found")
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "get", "--project=proj1", "missing"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

// ── alerts rules patch ────────────────────────────────────────────────

func TestAlertsRulesPatchHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"rule": sampleAlertRule("r1", "renamed", "cpu_pct", "gt", 90, false)})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "patch",
		"--project=proj1", "--name=renamed", "--enabled=false", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules patch: %v", err)
	}
	if gotBody["name"] != "renamed" {
		t.Errorf("name in patch body: got %v", gotBody["name"])
	}
	if gotBody["enabled"] != false {
		t.Errorf("enabled in patch body: got %v", gotBody["enabled"])
	}
	if !strings.Contains(out, "Updated") {
		t.Errorf("expected 'Updated' in output: %q", out)
	}
}

func TestAlertsRulesPatchNoFields(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "patch", "--project=proj1", "r1"})
	if err == nil {
		t.Fatal("expected error when no patch fields provided")
	}
}

func TestAlertsRulesPatch403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"rules", "patch",
		"--project=proj1", "--name=rename", "r1"})
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── alerts rules delete ───────────────────────────────────────────────

func TestAlertsRulesDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true, "id": "r1"})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "delete", "--project=proj1", "--yes", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

func TestAlertsRulesDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true, "id": "r1"})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAlerts(g, []string{"rules", "delete", "--project=proj1", "--yes", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules delete --json: %v", err)
	}
	var got struct {
		Deleted bool   `json:"deleted"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.Deleted {
		t.Errorf("deleted: expected true, got false")
	}
	if got.ID != "r1" {
		t.Errorf("id: got %q, want r1", got.ID)
	}
}

func TestAlertsRulesDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "delete", "--project=proj1", "r1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("alerts rules delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

// ── alerts rules silence ──────────────────────────────────────────────

func TestAlertsRulesSilenceHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1/silence", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"silenced_until": "2026-12-31T00:00:00Z"})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "silence",
		"--project=proj1", "--until=2026-12-31T00:00:00Z", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules silence: %v", err)
	}
	if gotBody["until"] != "2026-12-31T00:00:00Z" {
		t.Errorf("until in body: got %v", gotBody["until"])
	}
	if !strings.Contains(out, "silenced") {
		t.Errorf("expected 'silenced' in output: %q", out)
	}
}

func TestAlertsRulesSilenceJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1/silence", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"silenced_until": "2026-12-31T00:00:00Z"})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAlerts(g, []string{"rules", "silence",
		"--project=proj1", "--until=2026-12-31T00:00:00Z", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules silence --json: %v", err)
	}
	var got struct {
		SilencedUntil string `json:"silenced_until"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode silence JSON: %v\nbody=%s", err, out)
	}
	if got.SilencedUntil != "2026-12-31T00:00:00Z" {
		t.Errorf("silenced_until: got %q", got.SilencedUntil)
	}
}

// ── alerts rules unsilence ────────────────────────────────────────────

func TestAlertsRulesUnsilenceHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/rules/r1/unsilence", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"silenced": false})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"rules", "unsilence", "--project=proj1", "r1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts rules unsilence: %v", err)
	}
	if !called {
		t.Error("unsilence endpoint was not called")
	}
	if !strings.Contains(out, "unsilenced") {
		t.Errorf("expected 'unsilenced' in output: %q", out)
	}
}

// ── alerts events list ────────────────────────────────────────────────

func TestAlertsEventsListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"events": []any{
				sampleAlertEvent("e1", "r1", "2026-05-01T12:00:00Z", 95.5),
				sampleAlertEvent("e2", "r2", "2026-05-02T12:00:00Z", 5.1),
			},
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"events", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts events list: %v", err)
	}
	if !strings.Contains(out, "e1") || !strings.Contains(out, "e2") {
		t.Errorf("event ids missing from output: %q", out)
	}
}

func TestAlertsEventsListQueryPassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/events", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"events", "list",
		"--project=proj1", "--limit=10", "--since=2026-01-01T00:00:00Z"})
	_ = read()
	if err != nil {
		t.Fatalf("alerts events list query passthrough: %v", err)
	}
	if !strings.Contains(gotQuery, "limit=10") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
	if !strings.Contains(gotQuery, "since=") {
		t.Errorf("since not passed as query param: got %q", gotQuery)
	}
}

func TestAlertsEventsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	err := cmdAlerts(gAlerts(srv), []string{"events", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts events list empty: %v", err)
	}
	if !strings.Contains(out, "(no alert events)") {
		t.Errorf("expected '(no alert events)', got: %q", out)
	}
}

func TestAlertsEventsListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/events", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{sampleAlertEvent("e1", "r1", "2026-05-01T12:00:00Z", 95.5)},
		})
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAlerts(g, []string{"events", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("alerts events list --json: %v", err)
	}
	var got struct {
		Events []*AlertEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode events list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "e1" {
		t.Errorf("unexpected events in JSON: %+v", got.Events)
	}
}

func TestAlertsEventsListJSON422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/alerts/events", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "invalid since value")
	})
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"events", "list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestAlertsUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	err := cmdAlerts(gAlerts(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestAlertsHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdAlerts(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("alerts help: %v", err)
	}
}

func TestAlertsNoProjectFlagForRulesList(t *testing.T) {
	mux := http.NewServeMux()
	srv := alertsSrv(t, mux)
	setAlertsEnv(t, srv)

	// No --project flag and no working project config → error with hint.
	err := cmdAlerts(gAlerts(srv), []string{"rules", "list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
