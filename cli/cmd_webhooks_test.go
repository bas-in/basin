// cmd_webhooks_test — coverage for the webhooks subcommand.
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

func webhookSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setWebhookEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gWebhook(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleWebhook(id, url string, events []string, enabled bool) map[string]any {
	return map[string]any{
		"id":            id,
		"url":           url,
		"events":        events,
		"secret_prefix": "whsec_",
		"enabled":       enabled,
		"created_at":    "2026-01-01T00:00:00Z",
	}
}

func sampleDelivery(id string, statusCode int) map[string]any {
	return map[string]any{
		"id":           id,
		"status_code":  statusCode,
		"delivered_at": "2026-01-01T00:00:00Z",
		"event":        "project.created",
	}
}

// ── parseEventsCSV unit tests ─────────────────────────────────────────

func TestParseEventsCSVBasic(t *testing.T) {
	got := parseEventsCSV("a,b,c")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("basic CSV: got %v", got)
	}
}

func TestParseEventsCSVTrimsWhitespace(t *testing.T) {
	got := parseEventsCSV("  foo , bar  , baz ")
	want := []string{"foo", "bar", "baz"}
	if len(got) != len(want) {
		t.Fatalf("trim whitespace: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("trim[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseEventsCSVDeduplication(t *testing.T) {
	got := parseEventsCSV("a,b,a,c,b")
	if len(got) != 3 {
		t.Errorf("dedup: expected 3 unique events, got %d: %v", len(got), got)
	}
}

func TestParseEventsCSVEmptyTokensDropped(t *testing.T) {
	got := parseEventsCSV(",foo,,bar,")
	if len(got) != 2 || got[0] != "foo" || got[1] != "bar" {
		t.Errorf("empty tokens: got %v", got)
	}
}

func TestParseEventsCSVEmpty(t *testing.T) {
	got := parseEventsCSV("")
	if len(got) != 0 {
		t.Errorf("empty string: expected no events, got %v", got)
	}
}

// ── webhooks list ─────────────────────────────────────────────────────

func TestWebhooksListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{"webhooks": []any{}})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks list empty: %v", err)
	}
	if !strings.Contains(out, "(no webhooks)") {
		t.Errorf("expected '(no webhooks)', got: %q", out)
	}
}

func TestWebhooksListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"webhooks": []any{
				sampleWebhook("wh1", "https://example.com/hook", []string{"project.created"}, true),
				sampleWebhook("wh2", "https://other.com/hook", []string{"table.altered"}, false),
			},
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks list: %v", err)
	}
	if !strings.Contains(out, "example.com") || !strings.Contains(out, "other.com") {
		t.Errorf("webhook URLs missing from output: %q", out)
	}
}

func TestWebhooksListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"webhooks": []any{sampleWebhook("wh1", "https://example.com/hook", []string{"project.created"}, true)},
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks list --json: %v", err)
	}
	var got struct {
		Webhooks []*Webhook `json:"webhooks"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode webhooks list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Webhooks) != 1 || got.Webhooks[0].ID != "wh1" {
		t.Errorf("unexpected webhooks in JSON: %+v", got.Webhooks)
	}
}

// ── webhooks create ───────────────────────────────────────────────────

func TestWebhooksCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"webhook": sampleWebhook("wh-new", "https://example.com/hook", []string{"project.created"}, true),
			"secret":  "whsec_supersecret1234",
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{
		"create", "--project=proj1",
		"--url=https://example.com/hook",
		"--events=project.created,table.altered",
	})
	out := read()
	if err != nil {
		t.Fatalf("webhooks create: %v", err)
	}
	if gotBody["url"] != "https://example.com/hook" {
		t.Errorf("url in body: got %v", gotBody["url"])
	}
	// The secret must appear in the output.
	if !strings.Contains(out, "whsec_supersecret1234") {
		t.Errorf("secret missing from create output: %q", out)
	}
}

func TestWebhooksCreateJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"webhook": sampleWebhook("wh-json", "https://example.com/hook", []string{"project.created"}, true),
			"secret":  "whsec_jsontest",
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{
		"create", "--project=proj1",
		"--url=https://example.com/hook",
		"--events=project.created",
	})
	out := read()
	if err != nil {
		t.Fatalf("webhooks create --json: %v", err)
	}
	var got struct {
		Webhook *Webhook `json:"webhook"`
		Secret  string   `json:"secret"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode create JSON: %v\nbody=%s", err, out)
	}
	if got.Secret != "whsec_jsontest" {
		t.Errorf("secret: got %q, want whsec_jsontest", got.Secret)
	}
	if got.Webhook == nil || got.Webhook.ID != "wh-json" {
		t.Errorf("webhook.id: got %+v", got.Webhook)
	}
}

func TestWebhooksCreateMissingURL(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"create", "--project=proj1", "--events=project.created"})
	if err == nil {
		t.Fatal("expected error when --url is missing")
	}
	if !strings.Contains(err.Error(), "--url") {
		t.Errorf("error should mention --url: %v", err)
	}
}

func TestWebhooksCreateMissingEvents(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"create", "--project=proj1", "--url=https://example.com/hook"})
	if err == nil {
		t.Fatal("expected error when --events is missing")
	}
	if !strings.Contains(err.Error(), "--events") {
		t.Errorf("error should mention --events: %v", err)
	}
}

func TestWebhooksCreateEventsOnlyWhitespace(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	// comma-only CSV parses to empty → should fail before HTTP.
	err := cmdWebhooks(gWebhook(srv), []string{
		"create", "--project=proj1",
		"--url=https://example.com/hook",
		"--events=,,,",
	})
	if err == nil {
		t.Fatal("expected error for empty events CSV")
	}
	if !strings.Contains(err.Error(), "--events") {
		t.Errorf("error should mention --events: %v", err)
	}
}

// ── webhooks patch ────────────────────────────────────────────────────

func TestWebhooksPatchHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"webhook": sampleWebhook("wh1", "https://new.example.com/hook", []string{"project.created"}, false),
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{
		"patch", "--project=proj1",
		"--url=https://new.example.com/hook",
		"--enabled=false",
		"wh1",
	})
	out := read()
	if err != nil {
		t.Fatalf("webhooks patch: %v", err)
	}
	if gotBody["url"] != "https://new.example.com/hook" {
		t.Errorf("url in patch body: got %v", gotBody["url"])
	}
	if gotBody["enabled"] != false {
		t.Errorf("enabled in patch body: got %v, want false", gotBody["enabled"])
	}
	if !strings.Contains(out, "Updated") {
		t.Errorf("expected 'Updated' in output: %q", out)
	}
}

func TestWebhooksPatchNoFields(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"patch", "--project=proj1", "wh1"})
	if err == nil {
		t.Fatal("expected error when no patch fields provided")
	}
}

func TestWebhooksPatch404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/missing", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "webhook not found")
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"patch", "--project=proj1", "--url=https://x.com/h", "missing"})
	if err == nil {
		t.Fatal("expected error on 404 patch, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error: %v", err)
	}
}

// ── webhooks test ─────────────────────────────────────────────────────

func TestWebhooksTestHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{
			"delivery_id":  "del-001",
			"status_code":  200,
			"delivered_at": "2026-01-01T00:00:00Z",
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"test", "--project=proj1", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks test: %v", err)
	}
	if !called {
		t.Error("test endpoint was not called")
	}
	if !strings.Contains(out, "del-001") {
		t.Errorf("delivery_id missing from output: %q", out)
	}
}

func TestWebhooksTestJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/test", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"delivery_id":  "del-json",
			"status_code":  200,
			"delivered_at": "2026-01-01T00:00:00Z",
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"test", "--project=proj1", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks test --json: %v", err)
	}
	var got struct {
		DeliveryID  string `json:"delivery_id"`
		StatusCode  int    `json:"status_code"`
		DeliveredAt string `json:"delivered_at"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode test JSON: %v\nbody=%s", err, out)
	}
	if got.DeliveryID != "del-json" {
		t.Errorf("delivery_id: got %q", got.DeliveryID)
	}
	if got.StatusCode != 200 {
		t.Errorf("status_code: got %d", got.StatusCode)
	}
}

// ── webhooks redeliver ────────────────────────────────────────────────

func TestWebhooksRedeliverHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/redeliver/del-001", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"delivery_id": "del-001", "status": "queued"})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"redeliver", "--project=proj1", "--delivery=del-001", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks redeliver: %v", err)
	}
	if !called {
		t.Error("redeliver endpoint was not called")
	}
	if !strings.Contains(out, "del-001") {
		t.Errorf("delivery_id missing from output: %q", out)
	}
}

func TestWebhooksRedeliverMissingDelivery(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"redeliver", "--project=proj1", "wh1"})
	if err == nil {
		t.Fatal("expected error when --delivery is missing")
	}
	if !strings.Contains(err.Error(), "--delivery") {
		t.Errorf("error should mention --delivery: %v", err)
	}
}

func TestWebhooksRedeliverJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/redeliver/del-001", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"delivery_id": "del-001", "status": "queued"})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"redeliver", "--project=proj1", "--delivery=del-001", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks redeliver --json: %v", err)
	}
	var got struct {
		DeliveryID string `json:"delivery_id"`
		Status     string `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode redeliver JSON: %v\nbody=%s", err, out)
	}
	if got.Status != "queued" {
		t.Errorf("status: got %q, want queued", got.Status)
	}
}

// ── webhooks delete ───────────────────────────────────────────────────

func TestWebhooksDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"deleted": true, "id": "wh1"})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"delete", "--project=proj1", "--yes", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "Deleted") {
		t.Errorf("expected 'Deleted' in output: %q", out)
	}
}

func TestWebhooksDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"delete", "--project=proj1", "wh1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("webhooks delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestWebhooksDeleteJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"delete", "--project=proj1", "wh1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestWebhooksDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deleted": true, "id": "wh1"})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"delete", "--project=proj1", "--yes", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks delete --json: %v", err)
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
	if got.ID != "wh1" {
		t.Errorf("id: got %q, want wh1", got.ID)
	}
}

// ── webhooks deliveries ───────────────────────────────────────────────

func TestWebhooksDeliveriesHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/deliveries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"deliveries": []any{
				sampleDelivery("del-001", 200),
				sampleDelivery("del-002", 500),
			},
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"deliveries", "--project=proj1", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks deliveries: %v", err)
	}
	if !strings.Contains(out, "del-001") || !strings.Contains(out, "del-002") {
		t.Errorf("delivery ids missing from output: %q", out)
	}
}

func TestWebhooksDeliveriesLimitPassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/deliveries", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"deliveries": []any{}})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"deliveries", "--project=proj1", "--limit=25", "wh1"})
	_ = read()
	if err != nil {
		t.Fatalf("webhooks deliveries --limit: %v", err)
	}
	if !strings.Contains(gotQuery, "limit=25") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
}

func TestWebhooksDeliveriesEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/deliveries", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"deliveries": []any{}})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	err := cmdWebhooks(gWebhook(srv), []string{"deliveries", "--project=proj1", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks deliveries empty: %v", err)
	}
	if !strings.Contains(out, "(no deliveries)") {
		t.Errorf("expected '(no deliveries)', got: %q", out)
	}
}

func TestWebhooksDeliveriesJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/deliveries", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"deliveries": []any{sampleDelivery("del-001", 200)},
		})
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdWebhooks(g, []string{"deliveries", "--project=proj1", "wh1"})
	out := read()
	if err != nil {
		t.Fatalf("webhooks deliveries --json: %v", err)
	}
	var got struct {
		Deliveries []*WebhookDelivery `json:"deliveries"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode deliveries JSON: %v\nbody=%s", err, out)
	}
	if len(got.Deliveries) != 1 || got.Deliveries[0].ID != "del-001" {
		t.Errorf("unexpected deliveries: %+v", got.Deliveries)
	}
}

func TestWebhooksDeliveries422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/webhooks/wh1/deliveries", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "invalid limit value")
	})
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"deliveries", "--project=proj1", "wh1"})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── dispatcher / arg-parse ────────────────────────────────────────────

func TestWebhooksUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	err := cmdWebhooks(gWebhook(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestWebhooksHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdWebhooks(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("webhooks help: %v", err)
	}
}

func TestWebhooksNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := webhookSrv(t, mux)
	setWebhookEnv(t, srv)

	// No --project flag and no working project config → error with hint.
	err := cmdWebhooks(gWebhook(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}
