// cmd_audit_test — coverage for the audit subcommand.
//
// Each test drives an httptest.Server stub that asserts on method + path +
// query string. The org flag is required; tests confirm the typed error
// when it's absent. Shared helpers (stubJSON, stubError, captureStdout,
// withTempConfigDir) come from cmd_branches_test.go and integration_test.go.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────

func auditSrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setAuditEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gAudit(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleAuditEvent(id, actorID, action, resourceType, resourceID, occurredAt string) map[string]any {
	return map[string]any{
		"id":            id,
		"actor_id":      actorID,
		"action":        action,
		"resource_type": resourceType,
		"resource_id":   resourceID,
		"occurred_at":   occurredAt,
	}
}

// ── validation: --org required ────────────────────────────────────────

func TestAuditListRequiresOrg(t *testing.T) {
	mux := http.NewServeMux()
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	err := cmdAudit(gAudit(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --org is missing")
	}
	if !strings.Contains(err.Error(), "--org") {
		t.Errorf("error should mention --org: %v", err)
	}
}

// ── audit list happy path ─────────────────────────────────────────────

func TestAuditListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"events": []any{
				sampleAuditEvent("ev1", "user-1", "project.create", "project", "proj-1", "2026-05-01T10:00:00Z"),
				sampleAuditEvent("ev2", "user-2", "member.invite", "member", "user-3", "2026-05-02T11:00:00Z"),
			},
		})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org"})
	out := read()
	if err != nil {
		t.Fatalf("audit list: %v", err)
	}
	if !strings.Contains(out, "ev1") || !strings.Contains(out, "ev2") {
		t.Errorf("event ids missing from output: %q", out)
	}
	if !strings.Contains(out, "project.create") {
		t.Errorf("action missing from output: %q", out)
	}
}

func TestAuditListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org"})
	out := read()
	if err != nil {
		t.Fatalf("audit list empty: %v", err)
	}
	if !strings.Contains(out, "(no audit events)") {
		t.Errorf("expected '(no audit events)', got: %q", out)
	}
}

// ── query-string passthrough ──────────────────────────────────────────

func TestAuditListSincePassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org", "--since=2026-01-01T00:00:00Z"})
	_ = read()
	if err != nil {
		t.Fatalf("audit list --since: %v", err)
	}
	if !strings.Contains(gotQuery, "since=") {
		t.Errorf("since not passed as query param: got %q", gotQuery)
	}
}

func TestAuditListUntilPassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org", "--until=2026-06-01T00:00:00Z"})
	_ = read()
	if err != nil {
		t.Fatalf("audit list --until: %v", err)
	}
	if !strings.Contains(gotQuery, "until=") {
		t.Errorf("until not passed as query param: got %q", gotQuery)
	}
}

func TestAuditListLimitPassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org", "--limit=25"})
	_ = read()
	if err != nil {
		t.Fatalf("audit list --limit: %v", err)
	}
	if !strings.Contains(gotQuery, "limit=25") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
}

// ── --json shape lock ─────────────────────────────────────────────────

func TestAuditListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{
				sampleAuditEvent("ev1", "user-1", "project.create", "project", "proj-1", "2026-05-01T10:00:00Z"),
			},
		})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdAudit(g, []string{"list", "--org=my-org"})
	out := read()
	if err != nil {
		t.Fatalf("audit list --json: %v", err)
	}
	var got struct {
		Events []*AuditEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode audit list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "ev1" {
		t.Errorf("unexpected events in JSON: %+v", got.Events)
	}
	if got.Events[0].Action != "project.create" {
		t.Errorf("action: got %q", got.Events[0].Action)
	}
}

// ── error mapping ─────────────────────────────────────────────────────

func TestAuditList403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "not an org member")
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if !strings.Contains(err.Error(), "access denied") && !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("expected access denied error, got: %v", err)
	}
}

func TestAuditList404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/nonexistent/audit", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "org not found")
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	err := cmdAudit(gAudit(srv), []string{"list", "--org=nonexistent"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── export deferred ───────────────────────────────────────────────────

func TestAuditListExportDeferred(t *testing.T) {
	// The --export flag is accepted but deferred. The handler should
	// print "audit export deferred" and return nil (no HTTP call).
	mux := http.NewServeMux()
	var httpCalled bool
	mux.HandleFunc("/v1/orgs/my-org/audit", func(w http.ResponseWriter, r *http.Request) {
		httpCalled = true
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	read := captureStdout(t)
	err := cmdAudit(gAudit(srv), []string{"list", "--org=my-org", "--export=s3://my-bucket/audit"})
	out := read()
	if err != nil {
		t.Fatalf("audit list --export deferred: %v", err)
	}
	if httpCalled {
		t.Error("expected no HTTP call when --export is set (deferred)")
	}
	if !strings.Contains(out, "audit export deferred") {
		t.Errorf("expected 'audit export deferred' in output: %q", out)
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestAuditUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	err := cmdAudit(gAudit(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestAuditHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdAudit(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("audit help: %v", err)
	}
}

// ── globalFlags.orgSlug fallback ──────────────────────────────────────

func TestAuditListOrgFromGlobalFlags(t *testing.T) {
	var calledPath string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/orgs/global-org/audit", func(w http.ResponseWriter, r *http.Request) {
		calledPath = r.URL.Path
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := auditSrv(t, mux)
	setAuditEnv(t, srv)

	// orgSlug set via globalFlags (simulates --org= parsed at top level).
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", orgSlug: "global-org"}
	read := captureStdout(t)
	err := cmdAudit(g, []string{"list"})
	_ = read()
	if err != nil {
		t.Fatalf("audit list with global orgSlug: %v", err)
	}
	if calledPath != "/v1/orgs/global-org/audit" {
		t.Errorf("expected path /v1/orgs/global-org/audit, got %q", calledPath)
	}
}
