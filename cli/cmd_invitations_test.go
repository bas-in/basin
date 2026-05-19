// cmd_invitations_test — exhaustive coverage for the invitations subcommand.
//
// Tests use httptest.Server stubs and drive cmdInvitations() directly.
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

func invitationsServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setInvitationsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func invitationsGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// ── invitations list ──────────────────────────────────────────────────

func TestInvitationsListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"invitations": []any{
				sampleInvitation("inv-1", "alice@example.com", "admin"),
				sampleInvitation("inv-2", "bob@example.com", "member"),
			},
		})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("invitations list: %v", err)
	}
	if !strings.Contains(out, "alice@example.com") || !strings.Contains(out, "bob@example.com") {
		t.Errorf("invitation emails missing from output: %q", out)
	}
	if !strings.Contains(out, "inv-1") || !strings.Contains(out, "inv-2") {
		t.Errorf("invitation IDs missing from output: %q", out)
	}
}

func TestInvitationsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"invitations": []any{}})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("invitations list empty: %v", err)
	}
	if !strings.Contains(out, "(no invitations)") {
		t.Errorf("expected '(no invitations)', got: %q", out)
	}
}

func TestInvitationsListMissingOrg(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdInvitations(g, []string{"list"})
	if err == nil {
		t.Fatal("expected error for missing --org, got nil")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("expected '--org' in error: %v", err)
	}
}

func TestInvitationsListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"invitations": []any{
				sampleInvitation("inv-1", "alice@example.com", "admin"),
			},
		})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdInvitations(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("invitations list --json: %v", err)
	}
	var got struct {
		Invitations []*Invitation `json:"invitations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode invitations list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Invitations) != 1 {
		t.Fatalf("expected 1 invitation, got %d", len(got.Invitations))
	}
	if got.Invitations[0].Email != "alice@example.com" {
		t.Errorf("invitation.email: got %q, want 'alice@example.com'", got.Invitations[0].Email)
	}
	if got.Invitations[0].Status != "pending" {
		t.Errorf("invitation.status: got %q, want 'pending'", got.Invitations[0].Status)
	}
}

func TestInvitationsListPendingVsExpired(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"invitations": []any{
				map[string]any{
					"id": "inv-p", "email": "pending@example.com",
					"role": "member", "expires_at": "2026-06-01T00:00:00Z",
					"status": "pending",
				},
				map[string]any{
					"id": "inv-e", "email": "expired@example.com",
					"role": "member", "expires_at": "2025-01-01T00:00:00Z",
					"status": "expired",
				},
			},
		})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("invitations list pending+expired: %v", err)
	}
	if !strings.Contains(out, "pending") || !strings.Contains(out, "expired") {
		t.Errorf("status values missing from output: %q", out)
	}
}

func TestInvitationsList403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"list", "--org=acme"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── invitations resend ────────────────────────────────────────────────

func TestInvitationsResendHappyPath(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/inv-1/resend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"resent": true, "id": "inv-1"})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"resend", "--org=acme", "inv-1"})
	out := read()
	if err != nil {
		t.Fatalf("invitations resend: %v", err)
	}
	if !called {
		t.Error("resend endpoint not called")
	}
	if !strings.Contains(out, "Resent") {
		t.Errorf("expected 'Resent' in output: %q", out)
	}
}

func TestInvitationsResendMissingID(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdInvitations(g, []string{"resend", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing invitation ID, got nil")
	}
}

func TestInvitationsResendMissingOrg(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdInvitations(g, []string{"resend", "inv-1"})
	if err == nil {
		t.Fatal("expected error for missing --org, got nil")
	}
}

func TestInvitationsResendJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/inv-1/resend", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"resent": true, "id": "inv-1"})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdInvitations(g, []string{"resend", "--org=acme", "inv-1"})
	out := read()
	if err != nil {
		t.Fatalf("invitations resend --json: %v", err)
	}
	var got struct {
		Resent bool   `json:"resent"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode resend JSON: %v\nbody=%s", err, out)
	}
	if !got.Resent {
		t.Errorf("resent: got false, want true")
	}
	if got.ID != "inv-1" {
		t.Errorf("id: got %q, want 'inv-1'", got.ID)
	}
}

func TestInvitationsResend404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/ghost/resend", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Invitation not found.")
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"resend", "--org=acme", "ghost"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── invitations revoke ────────────────────────────────────────────────

func TestInvitationsRevokeConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/inv-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"revoked": true, "id": "inv-1"})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"revoke", "--org=acme", "--yes", "inv-1"})
	out := read()
	if err != nil {
		t.Fatalf("invitations revoke: %v", err)
	}
	if !called {
		t.Error("revoke endpoint not called")
	}
	if !strings.Contains(out, "Revoked") {
		t.Errorf("expected 'Revoked' in output: %q", out)
	}
}

func TestInvitationsRevokeDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/inv-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, map[string]any{"revoked": true, "id": "inv-1"})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"revoke", "--org=acme", "inv-1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("invitations revoke declined: unexpected error: %v", err)
	}
	if called {
		t.Error("revoke endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestInvitationsRevokeJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdInvitations(g, []string{"revoke", "--org=acme", "inv-1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

func TestInvitationsRevokeJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/inv-1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"revoked": true, "id": "inv-1"})
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdInvitations(g, []string{"revoke", "--org=acme", "--yes", "inv-1"})
	out := read()
	if err != nil {
		t.Fatalf("invitations revoke --json: %v", err)
	}
	var got struct {
		Revoked bool   `json:"revoked"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode revoke JSON: %v\nbody=%s", err, out)
	}
	if !got.Revoked {
		t.Errorf("revoked: got false, want true")
	}
	if got.ID != "inv-1" {
		t.Errorf("id: got %q, want 'inv-1'", got.ID)
	}
}

func TestInvitationsRevokeMissingID(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdInvitations(g, []string{"revoke", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing invitation ID in revoke, got nil")
	}
}

func TestInvitationsRevoke404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/invitations/ghost", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Invitation not found.")
	})
	srv := invitationsServer(t, mux)
	setInvitationsEnv(t, srv)

	g := invitationsGFlag(srv)
	err := cmdInvitations(g, []string{"revoke", "--org=acme", "--yes", "ghost"})
	if err == nil {
		t.Fatal("expected error on 404 revoke, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestInvitationsUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdInvitations(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestInvitationsHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdInvitations(g, []string{"help"})
	if err != nil {
		t.Fatalf("invitations help: %v", err)
	}
}
