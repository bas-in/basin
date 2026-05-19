// cmd_email_test — coverage for the email subcommand.
//
// Tests drive an httptest.Server stub and assert on method + path + body.
// Validation errors are tested before any HTTP call.
// Shared helpers (stubJSON, stubError, captureStdout, withTempConfigDir)
// come from cmd_branches_test.go and integration_test.go.
//
// Cloud route note: the email template surface lives at /email-config
// (not /email/templates). The CLI surfaces it as `email templates ...`
// for usability while the underlying path is /email-config.
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

func emailSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setEmailEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gEmail(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// ── validation: email templates test ─────────────────────────────────

func TestEmailTemplatesTestMissingKind(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "test",
		"--project=proj1", "--to=user@example.com"})
	if err == nil {
		t.Fatal("expected error when --kind is missing")
	}
	if !strings.Contains(err.Error(), "--kind") {
		t.Errorf("error should mention --kind: %v", err)
	}
}

func TestEmailTemplatesTestInvalidTo(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "test",
		"--project=proj1", "--kind=confirmation", "--to=not-an-email"})
	if err == nil {
		t.Fatal("expected error for invalid --to address")
	}
	if !strings.Contains(err.Error(), "invalid email address") && !strings.Contains(err.Error(), "not-an-email") {
		t.Errorf("error should mention invalid email: %v", err)
	}
}

func TestEmailTemplatesTestMissingTo(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "test",
		"--project=proj1", "--kind=confirmation"})
	if err == nil {
		t.Fatal("expected error when --to is missing")
	}
	if !strings.Contains(err.Error(), "--to") {
		t.Errorf("error should mention --to: %v", err)
	}
}

// ── email templates get ───────────────────────────────────────────────

func TestEmailTemplatesGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"templates": map[string]any{
				"confirmation": map[string]any{"subject": "Confirm your email", "body": "<p>Click here</p>"},
				"recovery":     map[string]any{"subject": "Reset your password", "body": "<p>Reset</p>"},
			},
		})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"templates", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("email templates get: %v", err)
	}
	if !strings.Contains(out, "confirmation") {
		t.Errorf("expected 'confirmation' in output: %q", out)
	}
}

func TestEmailTemplatesGetJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"templates": map[string]any{
				"confirmation": map[string]any{"subject": "Confirm", "body": "body"},
			},
		})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEmail(g, []string{"templates", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("email templates get --json: %v", err)
	}
	var got struct {
		Templates map[string]any `json:"templates"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode templates JSON: %v\nbody=%s", err, out)
	}
	if _, ok := got.Templates["confirmation"]; !ok {
		t.Errorf("confirmation template missing from JSON: %v", got.Templates)
	}
}

func TestEmailTemplatesGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── email templates put ───────────────────────────────────────────────

func TestEmailTemplatesPutHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"templates": map[string]any{
				"confirmation": map[string]any{"subject": "New subject", "body": "New body"},
			},
		})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"templates", "put",
		"--project=proj1", "--kind=confirmation", "--subject=New subject", "--body=New body"})
	out := read()
	if err != nil {
		t.Fatalf("email templates put: %v", err)
	}
	if !strings.Contains(out, "Updated") {
		t.Errorf("expected 'Updated' in output: %q", out)
	}
	// Verify body structure: { templates: { confirmation: { subject, body } } }
	if templates, ok := gotBody["templates"].(map[string]any); !ok {
		t.Errorf("expected 'templates' key in body: %v", gotBody)
	} else if conf, ok := templates["confirmation"].(map[string]any); !ok {
		t.Errorf("expected 'confirmation' key in templates: %v", templates)
	} else if conf["subject"] != "New subject" {
		t.Errorf("subject in body: got %v", conf["subject"])
	}
}

func TestEmailTemplatesPutMissingKind(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "put",
		"--project=proj1", "--subject=Subject", "--body=Body"})
	if err == nil {
		t.Fatal("expected error when --kind is missing")
	}
	if !strings.Contains(err.Error(), "--kind") {
		t.Errorf("error should mention --kind: %v", err)
	}
}

// ── email templates delete ────────────────────────────────────────────

func TestEmailTemplatesDeleteWithYes(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"reset_to_defaults": true})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"templates", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("email templates delete: %v", err)
	}
	if !called {
		t.Error("delete endpoint was not called")
	}
	if !strings.Contains(out, "reset") {
		t.Errorf("expected 'reset' in output: %q", out)
	}
}

func TestEmailTemplatesDeleteDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"templates", "delete", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("email templates delete declined: unexpected error: %v", err)
	}
	if called {
		t.Error("delete endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestEmailTemplatesDeleteJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"reset_to_defaults": true})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEmail(g, []string{"templates", "delete", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("email templates delete --json: %v", err)
	}
	var got struct {
		ResetToDefaults bool `json:"reset_to_defaults"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode delete JSON: %v\nbody=%s", err, out)
	}
	if !got.ResetToDefaults {
		t.Errorf("reset_to_defaults: expected true, got false")
	}
}

// ── email templates test ──────────────────────────────────────────────

func TestEmailTemplatesTestHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{"sent": true, "message_id": "msg-123"})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"templates", "test",
		"--project=proj1", "--kind=confirmation", "--to=user@example.com"})
	out := read()
	if err != nil {
		t.Fatalf("email templates test: %v", err)
	}
	if gotBody["kind"] != "confirmation" {
		t.Errorf("kind in body: got %v", gotBody["kind"])
	}
	if gotBody["to"] != "user@example.com" {
		t.Errorf("to in body: got %v", gotBody["to"])
	}
	if !strings.Contains(out, "user@example.com") {
		t.Errorf("expected 'user@example.com' in output: %q", out)
	}
}

func TestEmailTemplatesTestJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config/test", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"sent": true, "message_id": "msg-456"})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEmail(g, []string{"templates", "test",
		"--project=proj1", "--kind=recovery", "--to=admin@example.com"})
	out := read()
	if err != nil {
		t.Fatalf("email templates test --json: %v", err)
	}
	var got struct {
		Sent      bool   `json:"sent"`
		MessageID string `json:"message_id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode test JSON: %v\nbody=%s", err, out)
	}
	if !got.Sent {
		t.Errorf("sent: expected true, got false")
	}
	if got.MessageID != "msg-456" {
		t.Errorf("message_id: got %q, want msg-456", got.MessageID)
	}
}

func TestEmailTemplatesTest403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email-config/test", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "not allowed")
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"templates", "test",
		"--project=proj1", "--kind=confirmation", "--to=user@example.com"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── email allowance ───────────────────────────────────────────────────

func TestEmailAllowanceHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email/allowance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"allowance": map[string]any{
				"limit":    1000,
				"used":     42,
				"reset_at": "2026-06-01T00:00:00Z",
			},
		})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	err := cmdEmail(gEmail(srv), []string{"allowance", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("email allowance: %v", err)
	}
	if !strings.Contains(out, "1000") || !strings.Contains(out, "42") {
		t.Errorf("expected limit and used in output: %q", out)
	}
}

func TestEmailAllowanceJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email/allowance", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"allowance": map[string]any{
				"limit":    500,
				"used":     100,
				"reset_at": "2026-07-01T00:00:00Z",
			},
		})
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdEmail(g, []string{"allowance", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("email allowance --json: %v", err)
	}
	var got struct {
		Allowance *EmailAllowance `json:"allowance"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode allowance JSON: %v\nbody=%s", err, out)
	}
	if got.Allowance == nil || got.Allowance.Limit != 500 {
		t.Errorf("allowance.limit: got %+v", got.Allowance)
	}
	if got.Allowance.Used != 100 {
		t.Errorf("allowance.used: got %d, want 100", got.Allowance.Used)
	}
}

func TestEmailAllowance404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/email/allowance", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"allowance", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestEmailNoProject(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"allowance"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}

func TestEmailUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := emailSrv(t, mux)
	setEmailEnv(t, srv)

	err := cmdEmail(gEmail(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestEmailHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdEmail(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("email help: %v", err)
	}
}
