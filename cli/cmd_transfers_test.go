// cmd_transfers_test — coverage for Tier 22 transfer surfaces.
//
// Covers:
//   - projects transfers list / create / cancel
//   - orgs ownership-transfers list / create / cancel / accept / decline
//   - orgs incoming-project-transfers list / accept / decline
//
// All tests use an httptest.Server stub. Shared helpers (stubJSON,
// stubError, captureStdout, withTempConfigDir, gFlag) come from
// cmd_branches_test.go and integration_test.go.
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

// ── helpers ──────────────────────────────────────────────────────────

func sampleProjectTransfer(id, fromOrg, toOrg, status string) map[string]any {
	return map[string]any{
		"id":            id,
		"from_org_slug": fromOrg,
		"to_org_slug":   toOrg,
		"status":        status,
		"created_at":    "2026-01-01T00:00:00Z",
	}
}

func sampleOwnershipTransfer(id, toUserID, status string) map[string]any {
	return map[string]any{
		"id":         id,
		"to_user_id": toUserID,
		"status":     status,
		"created_at": "2026-01-01T00:00:00Z",
	}
}

func sampleIncomingProjectTransfer(id, fromOrg, toOrg, projRef, status string) map[string]any {
	return map[string]any{
		"id":            id,
		"from_org_slug": fromOrg,
		"to_org_slug":   toOrg,
		"project_ref":   projRef,
		"status":        status,
		"created_at":    "2026-01-01T00:00:00Z",
	}
}

func transferServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setTransferEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

// ═══════════════════════════════════════════════════════════════════
// projects transfers list
// ═══════════════════════════════════════════════════════════════════

func TestProjectsTransfersList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"transfers": []any{
				sampleProjectTransfer("xfr-1", "org-a", "org-b", "pending"),
			},
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers list: %v", err)
	}
	if !strings.Contains(out, "xfr-1") {
		t.Errorf("transfer ID missing from output: %q", out)
	}
	if !strings.Contains(out, "org-b") {
		t.Errorf("to_org_slug missing from output: %q", out)
	}
}

func TestProjectsTransfersListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"transfers": []any{}})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers list empty: %v", err)
	}
	if !strings.Contains(out, "(no transfers)") {
		t.Errorf("expected '(no transfers)', got: %q", out)
	}
}

func TestProjectsTransfersListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"transfers": []any{sampleProjectTransfer("xfr-1", "org-a", "org-b", "pending")},
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdProjects(g, []string{"transfers", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers list --json: %v", err)
	}
	var got struct {
		Transfers []*ProjectTransfer `json:"transfers"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if len(got.Transfers) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(got.Transfers))
	}
	if got.Transfers[0].ID != "xfr-1" {
		t.Errorf("id: got %q, want %q", got.Transfers[0].ID, "xfr-1")
	}
	if got.Transfers[0].ToOrgSlug != "org-b" {
		t.Errorf("to_org_slug: got %q, want %q", got.Transfers[0].ToOrgSlug, "org-b")
	}
}

func TestProjectsTransfersList404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, 404, "not_found", "project not found")
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 in error, got: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════
// projects transfers create
// ═══════════════════════════════════════════════════════════════════

func TestProjectsTransfersCreateMissingToOrg(t *testing.T) {
	mux := http.NewServeMux()
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "create", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error when --to-org is missing, got nil")
	}
	if !strings.Contains(err.Error(), "--to-org") {
		t.Errorf("error should mention --to-org: %v", err)
	}
}

func TestProjectsTransfersCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"transfer": sampleProjectTransfer("xfr-2", "org-a", "org-b", "pending"),
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "create", "--project=proj1", "--to-org=org-b", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers create: %v", err)
	}
	if gotBody["to_org_slug"] != "org-b" {
		t.Errorf("body to_org_slug: got %v, want org-b", gotBody["to_org_slug"])
	}
	if !strings.Contains(out, "Transfer initiated") {
		t.Errorf("expected 'Transfer initiated' in output: %q", out)
	}
}

func TestProjectsTransfersCreateJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdProjects(g, []string{"transfers", "create", "--project=proj1", "--to-org=org-b"})
	if err == nil {
		t.Fatal("expected error when --json without --yes, got nil")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Errorf("error should mention --yes: %v", err)
	}
}

func TestProjectsTransfersCreateDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"transfer": sampleProjectTransfer("xfr-2", "org-a", "org-b", "pending")})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "create", "--project=proj1", "--to-org=org-b"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("unexpected error on decline: %v", err)
	}
	if called {
		t.Error("POST should not be called when user declines")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

// ═══════════════════════════════════════════════════════════════════
// projects transfers cancel
// ═══════════════════════════════════════════════════════════════════

func TestProjectsTransfersCancel(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers/xfr-1/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"cancelled": true, "id": "xfr-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdProjects(g, []string{"transfers", "cancel", "--project=proj1", "--yes", "xfr-1"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers cancel: %v", err)
	}
	if !called {
		t.Error("cancel endpoint was not called")
	}
	if !strings.Contains(out, "Cancelled") {
		t.Errorf("expected 'Cancelled' in output: %q", out)
	}
}

func TestProjectsTransfersCancelJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/transfers/xfr-1/cancel", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"cancelled": true, "id": "xfr-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdProjects(g, []string{"transfers", "cancel", "--project=proj1", "--yes", "xfr-1"})
	out := read()
	if err != nil {
		t.Fatalf("projects transfers cancel --json: %v", err)
	}
	var got struct {
		Cancelled bool   `json:"cancelled"`
		ID        string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if !got.Cancelled {
		t.Error("expected cancelled=true")
	}
	if got.ID != "xfr-1" {
		t.Errorf("id: got %q, want %q", got.ID, "xfr-1")
	}
}

// ═══════════════════════════════════════════════════════════════════
// orgs ownership-transfers list
// ═══════════════════════════════════════════════════════════════════

func TestOrgsOwnershipTransfersList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"transfers": []any{
				sampleOwnershipTransfer("own-1", "user-abc", "pending"),
			},
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"ownership-transfers", "list", "my-org"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers list: %v", err)
	}
	if !strings.Contains(out, "own-1") {
		t.Errorf("transfer ID missing from output: %q", out)
	}
	if !strings.Contains(out, "user-abc") {
		t.Errorf("to_user_id missing from output: %q", out)
	}
}

// ═══════════════════════════════════════════════════════════════════
// orgs ownership-transfers create
// ═══════════════════════════════════════════════════════════════════

func TestOrgsOwnershipTransfersCreateMissingToUserID(t *testing.T) {
	mux := http.NewServeMux()
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := gFlag(srv)
	// --yes before positional arg so flag.Parse captures it; --to-user-id intentionally absent.
	err := cmdOrgs(g, []string{"ownership-transfers", "create", "--yes", "my-org"})
	if err == nil {
		t.Fatal("expected error when --to-user-id is missing, got nil")
	}
	if !strings.Contains(err.Error(), "--to-user-id") {
		t.Errorf("error should mention --to-user-id: %v", err)
	}
}

func TestOrgsOwnershipTransfersCreateHappyPath(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"transfer": sampleOwnershipTransfer("own-2", "user-xyz", "pending"),
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	// flags before positional arg so flag.Parse captures them.
	err := cmdOrgs(g, []string{"ownership-transfers", "create", "--to-user-id=user-xyz", "--yes", "my-org"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers create: %v", err)
	}
	if gotBody["to_user_id"] != "user-xyz" {
		t.Errorf("body to_user_id: got %v, want user-xyz", gotBody["to_user_id"])
	}
	if !strings.Contains(out, "Ownership transfer initiated") {
		t.Errorf("expected 'Ownership transfer initiated' in output: %q", out)
	}
}

func TestOrgsOwnershipTransfersCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"transfer": sampleOwnershipTransfer("own-2", "user-xyz", "pending"),
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"ownership-transfers", "create", "--to-user-id=user-xyz", "--yes", "my-org"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers create --json: %v", err)
	}
	var got struct {
		Transfer *OrgOwnershipTransfer `json:"transfer"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if got.Transfer == nil {
		t.Fatal("transfer is nil in JSON output")
	}
	if got.Transfer.ToUserID != "user-xyz" {
		t.Errorf("to_user_id: got %q, want user-xyz", got.Transfer.ToUserID)
	}
}

// ═══════════════════════════════════════════════════════════════════
// orgs ownership-transfers cancel
// ═══════════════════════════════════════════════════════════════════

func TestOrgsOwnershipTransfersCancel(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers/own-1/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"cancelled": true, "id": "own-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	// --yes before positional args so flag.Parse captures it.
	err := cmdOrgs(g, []string{"ownership-transfers", "cancel", "--yes", "my-org", "own-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers cancel: %v", err)
	}
	if !called {
		t.Error("cancel endpoint was not called")
	}
	if !strings.Contains(out, "Cancelled") {
		t.Errorf("expected 'Cancelled' in output: %q", out)
	}
}

// ═══════════════════════════════════════════════════════════════════
// orgs ownership-transfers accept / decline
// ═══════════════════════════════════════════════════════════════════

func TestOrgsOwnershipTransfersAccept(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers/own-1/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"accepted": true, "id": "own-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"ownership-transfers", "accept", "my-org", "own-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers accept: %v", err)
	}
	if !called {
		t.Error("accept endpoint was not called")
	}
	if !strings.Contains(out, "Accepted") {
		t.Errorf("expected 'Accepted' in output: %q", out)
	}
}

func TestOrgsOwnershipTransfersAccept409(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers/own-1/accept", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, 409, "already_accepted", "transfer already accepted")
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := gFlag(srv)
	err := cmdOrgs(g, []string{"ownership-transfers", "accept", "my-org", "own-1"})
	if err == nil {
		t.Fatal("expected error on 409, got nil")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected 409 in error, got: %v", err)
	}
}

func TestOrgsOwnershipTransfersDecline(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers/own-1/decline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"declined": true, "id": "own-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"ownership-transfers", "decline", "my-org", "own-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers decline: %v", err)
	}
	if !called {
		t.Error("decline endpoint was not called")
	}
	if !strings.Contains(out, "Declined") {
		t.Errorf("expected 'Declined' in output: %q", out)
	}
}

func TestOrgsOwnershipTransfersDeclineJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/ownership-transfers/own-1/decline", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"declined": true, "id": "own-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"ownership-transfers", "decline", "my-org", "own-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs ownership-transfers decline --json: %v", err)
	}
	var got struct {
		Declined bool   `json:"declined"`
		ID       string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if !got.Declined {
		t.Error("expected declined=true")
	}
	if got.ID != "own-1" {
		t.Errorf("id: got %q, want own-1", got.ID)
	}
}

// ═══════════════════════════════════════════════════════════════════
// orgs incoming-project-transfers list / accept / decline
// ═══════════════════════════════════════════════════════════════════

func TestOrgsIncomingProjectTransfersList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/incoming-project-transfers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"transfers": []any{
				sampleIncomingProjectTransfer("ipt-1", "org-a", "my-org", "proj-ref-1", "pending"),
			},
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"incoming-project-transfers", "list", "my-org"})
	out := read()
	if err != nil {
		t.Fatalf("orgs incoming-project-transfers list: %v", err)
	}
	if !strings.Contains(out, "ipt-1") {
		t.Errorf("transfer ID missing from output: %q", out)
	}
	if !strings.Contains(out, "org-a") {
		t.Errorf("from_org_slug missing from output: %q", out)
	}
}

func TestOrgsIncomingProjectTransfersListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/incoming-project-transfers", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"transfers": []any{
				sampleIncomingProjectTransfer("ipt-1", "org-a", "my-org", "proj-ref-1", "pending"),
			},
		})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdOrgs(g, []string{"incoming-project-transfers", "list", "my-org"})
	out := read()
	if err != nil {
		t.Fatalf("orgs incoming-project-transfers list --json: %v", err)
	}
	var got struct {
		Transfers []*IncomingProjectTransfer `json:"transfers"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if len(got.Transfers) != 1 {
		t.Fatalf("expected 1 transfer, got %d", len(got.Transfers))
	}
	if got.Transfers[0].ProjectRef != "proj-ref-1" {
		t.Errorf("project_ref: got %q, want proj-ref-1", got.Transfers[0].ProjectRef)
	}
}

func TestOrgsIncomingProjectTransfersAccept(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/incoming-project-transfers/ipt-1/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"accepted": true, "id": "ipt-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"incoming-project-transfers", "accept", "my-org", "ipt-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs incoming-project-transfers accept: %v", err)
	}
	if !called {
		t.Error("accept endpoint was not called")
	}
	if !strings.Contains(out, "Accepted") {
		t.Errorf("expected 'Accepted' in output: %q", out)
	}
}

func TestOrgsIncomingProjectTransfersDecline(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/incoming-project-transfers/ipt-1/decline", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"declined": true, "id": "ipt-1"})
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	read := captureStdout(t)
	g := gFlag(srv)
	err := cmdOrgs(g, []string{"incoming-project-transfers", "decline", "my-org", "ipt-1"})
	out := read()
	if err != nil {
		t.Fatalf("orgs incoming-project-transfers decline: %v", err)
	}
	if !called {
		t.Error("decline endpoint was not called")
	}
	if !strings.Contains(out, "Declined") {
		t.Errorf("expected 'Declined' in output: %q", out)
	}
}

func TestOrgsIncomingProjectTransfersList403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/incoming-project-transfers", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, 403, "forbidden", "insufficient role")
	})
	srv := transferServer(t, mux)
	setTransferEnv(t, srv)

	g := gFlag(srv)
	err := cmdOrgs(g, []string{"incoming-project-transfers", "list", "my-org"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("expected 403 in error, got: %v", err)
	}
}
