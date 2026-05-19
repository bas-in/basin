// cmd_backups_test — exhaustive coverage for the backups subcommand.
//
// All tests use an httptest.Server stub that asserts method + path + body.
// Confirmation prompts are driven via piped stdin ("y" / "n") or bypassed
// via --yes. JSON shape lock tests unmarshal the output and assert fields.
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

// ── test helpers (local to this file) ────────────────────────────────────

// backupsServer starts an httptest.Server using the provided mux and
// registers a cleanup hook to close it.
func backupsServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// setBackupsEnv wires the test server and a fake token into the process env.
func setBackupsEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

// bgFlag returns a globalFlags aimed at the test server.
func bgFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// bgFlagJSON returns a globalFlags with --json.
func bgFlagJSON(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
}

// sampleBackupPolicy returns a canonical BackupPolicy for stubs.
func sampleBackupPolicy() map[string]any {
	return map[string]any{
		"retention_days": float64(7),
		"schedule":       "0 2 * * *",
		"enabled":        true,
	}
}

// sampleBackupSnapshot returns a canonical BackupSnapshot for stubs.
func sampleBackupSnapshot(id, label string) map[string]any {
	return map[string]any{
		"id":         id,
		"label":      label,
		"size_bytes": float64(1024 * 1024),
		"created_at": "2026-01-01T00:00:00Z",
	}
}

// sampleRestoreJob returns a minimal RestoreJob for stubs.
func sampleRestoreJob(id, status, snapshotID string) map[string]any {
	return map[string]any{
		"id":           id,
		"status":       status,
		"started_at":   "2026-01-01T00:00:00Z",
		"completed_at": "2026-01-01T01:00:00Z",
		"snapshot_id":  snapshotID,
	}
}

// pipeStdin replaces os.Stdin with a pipe containing lines and returns a
// cleanup function that restores the original stdin.
func pipeStdin(lines ...string) (restore func()) {
	pr, pw, _ := os.Pipe()
	prev := os.Stdin
	os.Stdin = pr
	for _, line := range lines {
		_, _ = fmt.Fprintln(pw, line)
	}
	pw.Close()
	return func() { os.Stdin = prev }
}

// ── backups policy get ────────────────────────────────────────────────────

func TestBackupsPolicyGetHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, sampleBackupPolicy())
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"policy", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("policy get: %v", err)
	}
	if !strings.Contains(out, "7") {
		t.Errorf("expected retention_days=7 in output: %q", out)
	}
	if !strings.Contains(out, "0 2 * * *") {
		t.Errorf("expected schedule in output: %q", out)
	}
}

func TestBackupsPolicyGetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, sampleBackupPolicy())
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"policy", "get", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("policy get --json: %v", err)
	}
	var got BackupPolicy
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode policy get JSON: %v\nbody=%s", err, out)
	}
	if got.RetentionDays != 7 {
		t.Errorf("retention_days: got %d, want 7", got.RetentionDays)
	}
	if got.Schedule != "0 2 * * *" {
		t.Errorf("schedule: got %q, want '0 2 * * *'", got.Schedule)
	}
	if !got.Enabled {
		t.Errorf("enabled: got false, want true")
	}
}

func TestBackupsPolicyGet404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Project not found.")
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"policy", "get", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

// ── backups policy set ────────────────────────────────────────────────────

func TestBackupsPolicySetHappyPath(t *testing.T) {
	var gotBody BackupPolicy
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"retention_days": float64(14),
			"schedule":       "0 3 * * *",
			"enabled":        true,
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"policy", "set",
		"--project=proj1",
		"--retention-days=14",
		"--schedule=0 3 * * *",
		"--enabled=true",
	})
	out := read()
	if err != nil {
		t.Fatalf("policy set: %v", err)
	}
	if gotBody.RetentionDays != 14 {
		t.Errorf("body retention_days: got %d, want 14", gotBody.RetentionDays)
	}
	if gotBody.Schedule != "0 3 * * *" {
		t.Errorf("body schedule: got %q, want '0 3 * * *'", gotBody.Schedule)
	}
	if !strings.Contains(out, "14") {
		t.Errorf("expected retention_days in output: %q", out)
	}
}

func TestBackupsPolicySetValidationRetentionZero(t *testing.T) {
	// retention_days = 0 must fail before any HTTP call.
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, sampleBackupPolicy())
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"policy", "set",
		"--project=proj1",
		"--retention-days=0",
		"--schedule=0 2 * * *",
	})
	if err == nil {
		t.Fatal("expected validation error for retention_days=0, got nil")
	}
	if !strings.Contains(err.Error(), "retention-days") {
		t.Errorf("expected 'retention-days' in error, got: %v", err)
	}
	if called {
		t.Error("HTTP endpoint should not be called when validation fails")
	}
}

func TestBackupsPolicySetValidationEmptySchedule(t *testing.T) {
	// Empty schedule must fail before any HTTP call.
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, sampleBackupPolicy())
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"policy", "set",
		"--project=proj1",
		"--retention-days=7",
		"--schedule=",
	})
	if err == nil {
		t.Fatal("expected validation error for empty schedule, got nil")
	}
	if !strings.Contains(err.Error(), "schedule") {
		t.Errorf("expected 'schedule' in error, got: %v", err)
	}
	if called {
		t.Error("HTTP endpoint should not be called when schedule is empty")
	}
}

func TestBackupsPolicySet422(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusUnprocessableEntity, "invalid_policy", "Retention days exceeds plan limit.")
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"policy", "set",
		"--project=proj1",
		"--retention-days=365",
		"--schedule=0 2 * * *",
	})
	if err == nil {
		t.Fatal("expected error on 422, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusUnprocessableEntity {
		t.Errorf("expected APIError 422, got: %v", err)
	}
}

func TestBackupsPolicySetJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/policy", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"retention_days": float64(30),
			"schedule":       "0 4 * * *",
			"enabled":        false,
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"policy", "set",
		"--project=proj1",
		"--retention-days=30",
		"--schedule=0 4 * * *",
		"--enabled=false",
	})
	out := read()
	if err != nil {
		t.Fatalf("policy set --json: %v", err)
	}
	var got BackupPolicy
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode policy set JSON: %v\nbody=%s", err, out)
	}
	if got.RetentionDays != 30 {
		t.Errorf("retention_days: got %d, want 30", got.RetentionDays)
	}
	if got.Enabled {
		t.Errorf("enabled: got true, want false")
	}
}

// ── backups snapshots list ────────────────────────────────────────────────

func TestBackupsSnapshotsListHappyPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"snapshots": []any{
				sampleBackupSnapshot("snap-1", "before-migration"),
				sampleBackupSnapshot("snap-2", "nightly"),
			},
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"snapshots", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots list: %v", err)
	}
	if !strings.Contains(out, "snap-1") || !strings.Contains(out, "snap-2") {
		t.Errorf("snapshot IDs missing from output: %q", out)
	}
	if !strings.Contains(out, "before-migration") {
		t.Errorf("snapshot label missing from output: %q", out)
	}
}

func TestBackupsSnapshotsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"snapshots": []any{}})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"snapshots", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots list empty: %v", err)
	}
	if !strings.Contains(out, "(no backup snapshots)") {
		t.Errorf("expected '(no backup snapshots)', got: %q", out)
	}
}

func TestBackupsSnapshotsListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"snapshots": []any{sampleBackupSnapshot("snap-1", "test")},
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"snapshots", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots list --json: %v", err)
	}
	var got struct {
		Snapshots []*BackupSnapshot `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode snapshots list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(got.Snapshots))
	}
	if got.Snapshots[0].ID != "snap-1" {
		t.Errorf("snapshot ID: got %q, want 'snap-1'", got.Snapshots[0].ID)
	}
	if got.Snapshots[0].Label != "test" {
		t.Errorf("snapshot label: got %q, want 'test'", got.Snapshots[0].Label)
	}
}

// ── backups snapshots create ──────────────────────────────────────────────

func TestBackupsSnapshotsCreateWithLabel(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"snapshot": sampleBackupSnapshot("snap-new", "my-label"),
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"snapshots", "create", "--project=proj1", "--label=my-label"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots create: %v", err)
	}
	if gotBody["label"] != "my-label" {
		t.Errorf("body label: got %v, want 'my-label'", gotBody["label"])
	}
	if !strings.Contains(out, "snap-new") {
		t.Errorf("expected snapshot ID in output: %q", out)
	}
}

func TestBackupsSnapshotsCreateNoLabel(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"snapshot": sampleBackupSnapshot("snap-auto", ""),
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"snapshots", "create", "--project=proj1"})
	if err != nil {
		t.Fatalf("snapshots create no-label: %v", err)
	}
	// No label key in body when flag is omitted.
	if _, ok := gotBody["label"]; ok {
		t.Errorf("body should not contain 'label' when flag omitted, got body: %v", gotBody)
	}
}

func TestBackupsSnapshotsCreateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		stubJSON(w, map[string]any{
			"snapshot": sampleBackupSnapshot("snap-j", "json-snap"),
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"snapshots", "create", "--project=proj1", "--label=json-snap"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots create --json: %v", err)
	}
	var got struct {
		Snapshot *BackupSnapshot `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode snapshots create JSON: %v\nbody=%s", err, out)
	}
	if got.Snapshot == nil || got.Snapshot.ID != "snap-j" {
		t.Errorf("snapshot in JSON: %+v", got.Snapshot)
	}
	if got.Snapshot.Label != "json-snap" {
		t.Errorf("snapshot.label: got %q, want 'json-snap'", got.Snapshot.Label)
	}
}

// ── backups snapshots expire ──────────────────────────────────────────────

func TestBackupsSnapshotsExpireConfirmed(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots/snap-1/expire", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		called = true
		stubJSON(w, map[string]any{"expired": true, "id": "snap-1"})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"snapshots", "expire", "--project=proj1", "--yes", "snap-1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots expire: %v", err)
	}
	if !called {
		t.Error("expire endpoint was not called")
	}
	if !strings.Contains(out, "Expired") {
		t.Errorf("expected 'Expired' in output: %q", out)
	}
}

func TestBackupsSnapshotsExpireDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots/snap-1/expire", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"expired": true, "id": "snap-1"})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	restore := pipeStdin("n")
	defer restore()

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"snapshots", "expire", "--project=proj1", "snap-1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots expire declined: unexpected error: %v", err)
	}
	if called {
		t.Error("expire endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestBackupsSnapshotsExpire404(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots/snap-missing/expire", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusNotFound, "not_found", "Snapshot not found.")
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"snapshots", "expire", "--project=proj1", "--yes", "snap-missing"})
	if err == nil {
		t.Fatal("expected error on 404, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusNotFound {
		t.Errorf("expected APIError 404, got: %v", err)
	}
}

func TestBackupsSnapshotsExpireAlreadyExpired(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots/snap-done/expire", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "already_expired", "Snapshot is already expired.")
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"snapshots", "expire", "--project=proj1", "--yes", "snap-done"})
	if err == nil {
		t.Fatal("expected error for already-expired snapshot, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusConflict {
		t.Errorf("expected APIError 409, got: %v", err)
	}
}

func TestBackupsSnapshotsExpireJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots/snap-1/expire", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"expired": true, "id": "snap-1"})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"snapshots", "expire", "--project=proj1", "--yes", "snap-1"})
	out := read()
	if err != nil {
		t.Fatalf("snapshots expire --json: %v", err)
	}
	var got struct {
		Expired bool   `json:"expired"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode expire JSON: %v\nbody=%s", err, out)
	}
	if !got.Expired {
		t.Errorf("expired: got false, want true")
	}
	if got.ID != "snap-1" {
		t.Errorf("id: got %q, want 'snap-1'", got.ID)
	}
}

// ── backups restore ───────────────────────────────────────────────────────

func TestBackupsRestoreSameProject(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"restore_job_id": "job-1",
			"status":         "pending",
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"restore", "--project=proj1", "--from=snap-1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if gotBody["snapshot_id"] != "snap-1" {
		t.Errorf("body snapshot_id: got %v, want 'snap-1'", gotBody["snapshot_id"])
	}
	if _, ok := gotBody["into_branch_ref"]; ok {
		t.Errorf("body should not contain 'into_branch_ref' when --into is absent, got: %v", gotBody)
	}
	if !strings.Contains(out, "job-1") {
		t.Errorf("expected restore job ID in output: %q", out)
	}
}

func TestBackupsRestoreWithIntoBranch(t *testing.T) {
	var gotBody map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		stubJSON(w, map[string]any{
			"restore_job_id": "job-2",
			"status":         "running",
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"restore", "--project=proj1", "--from=snap-1", "--into=branch-ref-1", "--yes"})
	if err != nil {
		t.Fatalf("restore --into: %v", err)
	}
	if gotBody["snapshot_id"] != "snap-1" {
		t.Errorf("body snapshot_id: got %v, want 'snap-1'", gotBody["snapshot_id"])
	}
	if gotBody["into_branch_ref"] != "branch-ref-1" {
		t.Errorf("body into_branch_ref: got %v, want 'branch-ref-1'", gotBody["into_branch_ref"])
	}
}

func TestBackupsRestoreDeclined(t *testing.T) {
	var called bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore", func(w http.ResponseWriter, r *http.Request) {
		called = true
		stubJSON(w, map[string]any{"restore_job_id": "job-x", "status": "pending"})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	restore := pipeStdin("n")
	defer restore()

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"restore", "--project=proj1", "--from=snap-1"})
	out := read()
	if err != nil {
		t.Fatalf("restore declined: unexpected error: %v", err)
	}
	if called {
		t.Error("restore endpoint should not be called when declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted' in output: %q", out)
	}
}

func TestBackupsRestoreConflict(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore", func(w http.ResponseWriter, r *http.Request) {
		stubError(w, http.StatusConflict, "restore_in_progress", "A restore is already in progress.")
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"restore", "--project=proj1", "--from=snap-1", "--yes"})
	if err == nil {
		t.Fatal("expected error on 409 conflict, got nil")
	}
	if ae, ok := AsAPIError(err); !ok || ae.HTTPStatus != http.StatusConflict {
		t.Errorf("expected APIError 409, got: %v", err)
	}
}

func TestBackupsRestoreMissingFrom(t *testing.T) {
	mux := http.NewServeMux()
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	err := cmdBackups(bgFlag(srv), []string{"restore", "--project=proj1", "--yes"})
	if err == nil {
		t.Fatal("expected error for missing --from, got nil")
	}
	if !strings.Contains(err.Error(), "--from") {
		t.Errorf("expected '--from' in error, got: %v", err)
	}
}

func TestBackupsRestoreJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"restore_job_id": "job-j",
			"status":         "pending",
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"restore", "--project=proj1", "--from=snap-1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("restore --json: %v", err)
	}
	var got struct {
		RestoreJobID string `json:"restore_job_id"`
		Status       string `json:"status"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode restore JSON: %v\nbody=%s", err, out)
	}
	if got.RestoreJobID != "job-j" {
		t.Errorf("restore_job_id: got %q, want 'job-j'", got.RestoreJobID)
	}
	if got.Status != "pending" {
		t.Errorf("status: got %q, want 'pending'", got.Status)
	}
}

// ── backups restore-jobs list ─────────────────────────────────────────────

func TestBackupsRestoreJobsListPopulated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		stubJSON(w, map[string]any{
			"jobs": []any{
				sampleRestoreJob("job-1", "completed", "snap-1"),
				sampleRestoreJob("job-2", "running", "snap-2"),
			},
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"restore-jobs", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("restore-jobs list: %v", err)
	}
	if !strings.Contains(out, "job-1") || !strings.Contains(out, "job-2") {
		t.Errorf("job IDs missing from output: %q", out)
	}
	if !strings.Contains(out, "completed") {
		t.Errorf("job status missing from output: %q", out)
	}
}

func TestBackupsRestoreJobsListEmpty(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore/jobs", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{"jobs": []any{}})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"restore-jobs", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("restore-jobs list empty: %v", err)
	}
	if !strings.Contains(out, "(no restore jobs)") {
		t.Errorf("expected '(no restore jobs)', got: %q", out)
	}
}

func TestBackupsRestoreJobsListInFlight(t *testing.T) {
	// In-flight job: no completed_at — verify it renders as "—".
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore/jobs", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"jobs": []any{
				map[string]any{
					"id":          "job-3",
					"status":      "running",
					"started_at":  "2026-01-01T00:00:00Z",
					"snapshot_id": "snap-3",
					// no completed_at
				},
			},
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlag(srv), []string{"restore-jobs", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("restore-jobs list in-flight: %v", err)
	}
	if !strings.Contains(out, "running") {
		t.Errorf("expected 'running' in output: %q", out)
	}
	if !strings.Contains(out, "—") {
		t.Errorf("expected em-dash for missing completed_at: %q", out)
	}
}

func TestBackupsRestoreJobsListJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/restore/jobs", func(w http.ResponseWriter, r *http.Request) {
		stubJSON(w, map[string]any{
			"jobs": []any{sampleRestoreJob("job-j", "completed", "snap-j")},
		})
	})
	srv := backupsServer(t, mux)
	setBackupsEnv(t, srv)

	read := captureStdout(t)
	err := cmdBackups(bgFlagJSON(srv), []string{"restore-jobs", "list", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("restore-jobs list --json: %v", err)
	}
	var got struct {
		Jobs []*RestoreJob `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode restore-jobs list JSON: %v\nbody=%s", err, out)
	}
	if len(got.Jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(got.Jobs))
	}
	if got.Jobs[0].ID != "job-j" {
		t.Errorf("job.id: got %q, want 'job-j'", got.Jobs[0].ID)
	}
	if got.Jobs[0].Status != "completed" {
		t.Errorf("job.status: got %q, want 'completed'", got.Jobs[0].Status)
	}
	if got.Jobs[0].SnapshotID != "snap-j" {
		t.Errorf("job.snapshot_id: got %q, want 'snap-j'", got.Jobs[0].SnapshotID)
	}
}

// ── dispatcher / help ─────────────────────────────────────────────────────

func TestBackupsUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdBackups(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

func TestBackupsNoArgs(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdBackups(g, []string{})
	if err == nil {
		t.Fatal("expected error when no subcommand given")
	}
}

func TestBackupsHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}
	err := cmdBackups(g, []string{"help"})
	if err != nil {
		t.Fatalf("backups help: %v", err)
	}
}
