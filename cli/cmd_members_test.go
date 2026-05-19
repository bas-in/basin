// cmd_members_test — exhaustive coverage for the members subcommand.
//
// Tests use httptest.Server stubs and drive cmdMembers() directly.
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

func membersServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setMembersEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func membersGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleMember(userID, email, role string) map[string]any {
	return map[string]any{
		"user_id":   userID,
		"email":     email,
		"role":      role,
		"joined_at": "2026-01-01T00:00:00Z",
	}
}

func sampleInvitation(id, email, role string) map[string]any {
	return map[string]any{
		"id":         id,
		"email":      email,
		"role":       role,
		"expires_at": "2026-06-01T00:00:00Z",
		"status":     "pending",
	}
}

// ── members list ──────────────────────────────────────────────────────

func TestMembersListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"members": []any{
				sampleMember("user-1", "alice@example.com", "admin"),
				sampleMember("user-2", "bob@example.com", "member"),
			},
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("members list: %v", err)
	}
	if !strings.Contains(out, "alice@example.com") || !strings.Contains(out, "bob@example.com") {
		t.Errorf("member emails missing from output: %q", out)
	}
	if !strings.Contains(out, "admin") || !strings.Contains(out, "member") {
		t.Errorf("roles missing from output: %q", out)
	}
}

func TestMembersListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"members": []any{}})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("members list empty: %v", err)
	}
	if !strings.Contains(out, "(no members)") {
		t.Errorf("expected '(no members)', got: %q", out)
	}
}

func TestMembersListMissingOrg(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdMembers(g, []string{"list"})
	if err == nil {
		t.Fatal("expected error for missing --org, got nil")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("expected '--org' in error: %v", err)
	}
}

func TestMembersListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"members": []any{sampleMember("user-1", "alice@example.com", "admin")},
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMembers(g, []string{"list", "--org=acme"})
	out := read()
	if err != nil {
		t.Fatalf("members list --json: %v", err)
	}
	var got struct {
		Members []*Member `json:"members"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode members list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(got.Members))
	}
	if got.Members[0].Email != "alice@example.com" {
		t.Errorf("member.email: got %q, want 'alice@example.com'", got.Members[0].Email)
	}
}

func TestMembersList403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "Insufficient permissions.")
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"list", "--org=acme"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusForbidden {
		t.Errorf("expected APIError 403, got: %v", err)
	}
}

// ── members invite ────────────────────────────────────────────────────

func TestMembersInviteHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/invite", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"invitation": sampleInvitation("inv-1", "charlie@example.com", "member"),
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"invite", "--org=acme", "--email=charlie@example.com", "--role=member"})
	out := read()
	if err != nil {
		t.Fatalf("members invite: %v", err)
	}
	if gotBody["email"] != "charlie@example.com" {
		t.Errorf("email in body: got %v", gotBody["email"])
	}
	if gotBody["role"] != "member" {
		t.Errorf("role in body: got %v, want 'member'", gotBody["role"])
	}
	if !strings.Contains(out, "charlie@example.com") {
		t.Errorf("email missing from output: %q", out)
	}
}

func TestMembersInviteMissingEmail(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdMembers(g, []string{"invite", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing --email, got nil")
	}
	if !strings.Contains(err.Error(), "--email") {
		t.Errorf("expected '--email' in error: %v", err)
	}
}

func TestMembersInvite409Conflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/invite", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "conflict", "Member already exists.")
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"invite", "--org=acme", "--email=alice@example.com"})
	if err == nil {
		t.Fatal("expected error on 409, got nil")
	}
	if !strings.Contains(err.Error(), "already") {
		t.Errorf("expected conflict message in error: %v", err)
	}
}

func TestMembersInviteJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/invite", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"invitation": sampleInvitation("inv-1", "charlie@example.com", "admin"),
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMembers(g, []string{"invite", "--org=acme", "--email=charlie@example.com", "--role=admin"})
	out := read()
	if err != nil {
		t.Fatalf("members invite --json: %v", err)
	}
	var got struct {
		Invitation *Invitation `json:"invitation"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode invite JSON: %v\nbody=%s", err, out)
	}
	if got.Invitation == nil || got.Invitation.Email != "charlie@example.com" {
		t.Errorf("invitation.email: %+v", got.Invitation)
	}
	if got.Invitation.Role != "admin" {
		t.Errorf("invitation.role: got %q, want 'admin'", got.Invitation.Role)
	}
}

// ── members remove ────────────────────────────────────────────────────

func TestMembersRemoveConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/user-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"removed": true, "user_id": "user-1"})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"remove", "--org=acme", "--user-id=user-1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("members remove: %v", err)
	}
	if !called {
		t.Error("remove endpoint not called")
	}
	if !strings.Contains(out, "Removed") {
		t.Errorf("expected 'Removed' in output: %q", out)
	}
}

func TestMembersRemoveDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/user-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			called = true
		}
		stubJSON(w, map[string]any{"removed": true, "user_id": "user-1"})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"remove", "--org=acme", "--user-id=user-1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("members remove declined: unexpected error: %v", err)
	}
	if called {
		t.Error("remove endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestMembersRemoveJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/user-1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"removed": true, "user_id": "user-1"})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMembers(g, []string{"remove", "--org=acme", "--user-id=user-1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("members remove --json: %v", err)
	}
	var got struct {
		Removed bool   `json:"removed"`
		UserID  string `json:"user_id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode remove JSON: %v\nbody=%s", err, out)
	}
	if !got.Removed {
		t.Errorf("removed: got false, want true")
	}
	if got.UserID != "user-1" {
		t.Errorf("user_id: got %q, want 'user-1'", got.UserID)
	}
}

func TestMembersRemoveMissingUserID(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	err := cmdMembers(g, []string{"remove", "--org=acme"})
	if err == nil {
		t.Fatal("expected error for missing --user-id, got nil")
	}
}

func TestMembersRemoveJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMembers(g, []string{"remove", "--org=acme", "--user-id=user-1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required' in error: %v", err)
	}
}

// ── members role ──────────────────────────────────────────────────────

func TestMembersRoleHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/user-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"member": map[string]any{"user_id": "user-1", "role": "admin"},
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"role", "--org=acme", "--user-id=user-1", "--role=admin"})
	out := read()
	if err != nil {
		t.Fatalf("members role: %v", err)
	}
	if gotBody["role"] != "admin" {
		t.Errorf("role in body: got %v, want 'admin'", gotBody["role"])
	}
	if !strings.Contains(out, "admin") {
		t.Errorf("role missing from output: %q", out)
	}
}

func TestMembersRoleMissingFlags(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "tok"}
	// Missing --role
	err := cmdMembers(g, []string{"role", "--org=acme", "--user-id=user-1"})
	if err == nil {
		t.Fatal("expected error for missing --role, got nil")
	}
}

func TestMembersRoleJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/user-1", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"member": map[string]any{"user_id": "user-1", "role": "owner"},
		})
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdMembers(g, []string{"role", "--org=acme", "--user-id=user-1", "--role=owner"})
	out := read()
	if err != nil {
		t.Fatalf("members role --json: %v", err)
	}
	var got struct {
		Member *Member `json:"member"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode role JSON: %v\nbody=%s", err, out)
	}
	if got.Member == nil || got.Member.Role != "owner" {
		t.Errorf("member.role: %+v", got.Member)
	}
}

func TestMembersRole404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/acme/members/ghost", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Member not found.")
	})
	srv := membersServer(t, mux)
	setMembersEnv(t, srv)

	g := membersGFlag(srv)
	err := cmdMembers(g, []string{"role", "--org=acme", "--user-id=ghost", "--role=member"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestMembersUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdMembers(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestMembersHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdMembers(g, []string{"help"})
	if err != nil {
		t.Fatalf("members help: %v", err)
	}
}
