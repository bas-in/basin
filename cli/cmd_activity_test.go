// cmd_activity_test — coverage for the activity subcommand.
//
// Each test drives an httptest.Server stub that asserts on method + path +
// query string. Project ref resolution falls through to loadWorkingProject(cwd)
// when --project is absent. Shared helpers (stubJSON, stubError, captureStdout,
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

func activitySrv(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func setActivityEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

func gActivity(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

func sampleActivityEvent(id, kind, actorID, occurredAt, summary string) map[string]any {
	return map[string]any{
		"id":          id,
		"kind":        kind,
		"actor_id":    actorID,
		"occurred_at": occurredAt,
		"summary":     summary,
	}
}

// ── activity list happy path ──────────────────────────────────────────

func TestActivityListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"events": []any{
				sampleActivityEvent("act1", "migration.apply", "user-1", "2026-05-01T10:00:00Z", "Applied migration v1"),
				sampleActivityEvent("act2", "secret.create", "user-2", "2026-05-02T11:00:00Z", "Created secret DB_URL"),
			},
		})
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	read := captureStdout(t)
	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("activity list: %v", err)
	}
	if !strings.Contains(out, "act1") || !strings.Contains(out, "act2") {
		t.Errorf("event ids missing from output: %q", out)
	}
	if !strings.Contains(out, "migration.apply") {
		t.Errorf("kind missing from output: %q", out)
	}
}

func TestActivityListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	read := captureStdout(t)
	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("activity list empty: %v", err)
	}
	if !strings.Contains(out, "(no activity)") {
		t.Errorf("expected '(no activity)', got: %q", out)
	}
}

// ── query-string passthrough ──────────────────────────────────────────

func TestActivityListLimitPassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	read := captureStdout(t)
	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1", "--limit=20"})
	_ = read()
	if err != nil {
		t.Fatalf("activity list --limit: %v", err)
	}
	if !strings.Contains(gotQuery, "limit=20") {
		t.Errorf("limit not passed as query param: got %q", gotQuery)
	}
}

func TestActivityListSincePassthrough(t *testing.T) {
	var gotQuery string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		stubJSON(w, map[string]any{"events": []any{}})
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	read := captureStdout(t)
	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1", "--since=2026-01-01T00:00:00Z"})
	_ = read()
	if err != nil {
		t.Fatalf("activity list --since: %v", err)
	}
	if !strings.Contains(gotQuery, "since=") {
		t.Errorf("since not passed as query param: got %q", gotQuery)
	}
}

// ── --json shape lock ─────────────────────────────────────────────────

func TestActivityListJSON(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"events": []any{
				sampleActivityEvent("act1", "migration.apply", "user-1", "2026-05-01T10:00:00Z", "Applied migration v1"),
			},
		})
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdActivity(g, []string{"list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("activity list --json: %v", err)
	}
	var got struct {
		Events []*ActivityEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode activity list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "act1" {
		t.Errorf("unexpected events in JSON: %+v", got.Events)
	}
	if got.Events[0].Kind != "migration.apply" {
		t.Errorf("kind: got %q", got.Events[0].Kind)
	}
	if got.Events[0].Summary != "Applied migration v1" {
		t.Errorf("summary: got %q", got.Events[0].Summary)
	}
}

// ── error mapping ─────────────────────────────────────────────────────

func TestActivityList403(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusForbidden, "forbidden", "insufficient permissions")
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 403, got nil")
	}
	if !strings.Contains(err.Error(), "access denied") && !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("expected access denied error, got: %v", err)
	}
}

func TestActivityList404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "project not found")
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestActivityList422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/activity", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "validation_error", "invalid since value")
	})
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	err := cmdActivity(gActivity(srv), []string{"list", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

// ── project ref resolution ────────────────────────────────────────────

func TestActivityListNoProjectFlag(t *testing.T) {
	mux := http.NewServeMux()
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	// No --project flag and no working project config → error with hint.
	err := cmdActivity(gActivity(srv), []string{"list"})
	if err == nil {
		t.Fatal("expected error when --project is missing and no working project")
	}
}

// ── dispatcher ────────────────────────────────────────────────────────

func TestActivityUnknownSubcommand(t *testing.T) {
	mux := http.NewServeMux()
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	err := cmdActivity(gActivity(srv), []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestActivityHelp(t *testing.T) {
	withTempConfigDir(t)
	err := cmdActivity(&globalFlags{}, []string{"help"})
	if err != nil {
		t.Fatalf("activity help: %v", err)
	}
}

func TestActivityNoArgsDefaultsList(t *testing.T) {
	// Calling with no args should invoke the list subcommand.
	// With no project ref → error (no HTTP call expected).
	mux := http.NewServeMux()
	srv := activitySrv(t, mux)
	setActivityEnv(t, srv)

	err := cmdActivity(gActivity(srv), []string{})
	// We expect an error because there's no --project and no working project.
	if err == nil {
		t.Fatal("expected error (no project) when calling cmdActivity with no args")
	}
}
