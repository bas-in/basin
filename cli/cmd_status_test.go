// cmd_status_test — unit tests for `basin status`.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newStatusServer builds an httptest.Server that handles the two
// endpoints status calls: GET /v1/projects/{ref} and
// GET /v1/projects/{ref}/migrations.
func newStatusServer(
	t *testing.T,
	proj map[string]any,
	migrations []map[string]any,
) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path

		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/migrations"):
			// GET /v1/projects/{ref}/migrations
			_ = json.NewEncoder(w).Encode(map[string]any{
				"migrations": migrations,
			})
		case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/projects/"):
			// GET /v1/projects/{ref}
			if proj == nil {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"error": map[string]any{"code": "not_found", "message": "not found"},
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project": proj,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestStatusNotLinked verifies that `basin status` exits 0 and prints
// a clear message when no project is linked.
func TestStatusNotLinked(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	// Init but do NOT link.
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus (not linked): %v", err)
	}
	out := read()
	if !strings.Contains(out, "no project linked") {
		t.Errorf("expected 'no project linked' in output, got: %q", out)
	}
}

// TestStatusNotLinkedJSON verifies the --json path for an unlinked directory.
func TestStatusNotLinkedJSON(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	g := &globalFlags{json: true}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus --json (not linked): %v", err)
	}
	out := read()

	var got struct {
		Linked bool `json:"linked"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode status JSON: %v\nbody=%s", err, out)
	}
	if got.Linked {
		t.Errorf("linked: got true, want false")
	}
}

// TestStatusLinkedCleanMigrations verifies that `basin status` works
// when local and remote migrations are in sync.
func TestStatusLinkedCleanMigrations(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	// Init + link to a project.
	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "clean-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	// Place one local migration file.
	migDir := filepath.Join(dir, "basin", "migrations")
	if err := os.WriteFile(filepath.Join(migDir, "0001_init.sql"),
		[]byte("CREATE TABLE foo (id int);"), 0o644); err != nil {
		t.Fatalf("WriteFile migration: %v", err)
	}

	// Remote has the same migration.
	srv := newStatusServer(t,
		map[string]any{
			"ref": "clean-proj", "name": "Clean", "region": "jnb",
			"status": "active", "default_branch": "main",
		},
		[]map[string]any{
			{"id": "m1", "source": "0001_init.sql"},
		},
	)
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus: %v", err)
	}
	out := read()

	// Expect the project ref, status, and no pending migrations.
	if !strings.Contains(out, "clean-proj") {
		t.Errorf("output should contain project ref: %q", out)
	}
	if !strings.Contains(out, "active") {
		t.Errorf("output should contain status 'active': %q", out)
	}
	if !strings.Contains(out, "applied:") {
		t.Errorf("output should contain 'applied:': %q", out)
	}
}

// TestStatusLinkedPendingMigrations verifies drift detection when local
// and remote sets diverge.
func TestStatusLinkedPendingMigrations(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "drift-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	migDir := filepath.Join(dir, "basin", "migrations")
	// Local has two files.
	if err := os.WriteFile(filepath.Join(migDir, "0001_init.sql"),
		[]byte("CREATE TABLE foo (id int);"), 0o644); err != nil {
		t.Fatalf("WriteFile 0001: %v", err)
	}
	if err := os.WriteFile(filepath.Join(migDir, "0002_add_col.sql"),
		[]byte("ALTER TABLE foo ADD COLUMN name text;"), 0o644); err != nil {
		t.Fatalf("WriteFile 0002: %v", err)
	}

	// Remote has only 0001 applied + one remote-only migration.
	srv := newStatusServer(t,
		map[string]any{"ref": "drift-proj", "status": "active", "region": "jnb"},
		[]map[string]any{
			{"id": "m1", "source": "0001_init.sql"},
			{"id": "m2", "source": "0003_remote_only.sql"},
		},
	)
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus: %v", err)
	}
	out := read()

	// applied = 1 (0001 in both), local_only = 1 (0002), remote_only = 1 (0003).
	if !strings.Contains(out, "applied:") {
		t.Errorf("output should have applied count: %q", out)
	}
	if !strings.Contains(out, "local_only:") {
		t.Errorf("output should have local_only count: %q", out)
	}
	if !strings.Contains(out, "remote_only:") {
		t.Errorf("output should have remote_only count: %q", out)
	}
}

// TestStatusPausedProject verifies that a paused project shows "paused" status.
func TestStatusPausedProject(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "paused-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newStatusServer(t,
		map[string]any{"ref": "paused-proj", "status": "paused", "region": "jnb"},
		nil,
	)
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus: %v", err)
	}
	out := read()

	if !strings.Contains(out, "paused") {
		t.Errorf("output should show 'paused' status: %q", out)
	}
}

// TestStatusJSONOutputShape verifies the complete JSON envelope from
// `basin status --json` when linked.
func TestStatusJSONOutputShape(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "json-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	// Put one local migration that matches a remote one.
	migDir := filepath.Join(dir, "basin", "migrations")
	if err := os.WriteFile(filepath.Join(migDir, "0001_init.sql"),
		[]byte("CREATE TABLE t (id int);"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	srv := newStatusServer(t,
		map[string]any{
			"ref": "json-proj", "status": "active", "region": "us-east",
			"default_branch": "main",
		},
		[]map[string]any{
			{"id": "m1", "source": "0001_init.sql"},
		},
	)
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus --json: %v", err)
	}
	out := read()

	var got struct {
		Linked        bool   `json:"linked"`
		ProjectRef    string `json:"project_ref"`
		Status        string `json:"status"`
		Region        string `json:"region"`
		DefaultBranch string `json:"default_branch"`
		Migrations    struct {
			Applied    int `json:"applied"`
			LocalOnly  int `json:"local_only"`
			RemoteOnly int `json:"remote_only"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode status JSON: %v\nbody=%s", err, out)
	}
	if !got.Linked {
		t.Errorf("linked: got false, want true")
	}
	if got.ProjectRef != "json-proj" {
		t.Errorf("project_ref: got %q, want json-proj", got.ProjectRef)
	}
	if got.Status != "active" {
		t.Errorf("status: got %q, want active", got.Status)
	}
	if got.Region != "us-east" {
		t.Errorf("region: got %q, want us-east", got.Region)
	}
	if got.DefaultBranch != "main" {
		t.Errorf("default_branch: got %q, want main", got.DefaultBranch)
	}
	if got.Migrations.Applied != 1 {
		t.Errorf("migrations.applied: got %d, want 1", got.Migrations.Applied)
	}
	if got.Migrations.LocalOnly != 0 {
		t.Errorf("migrations.local_only: got %d, want 0", got.Migrations.LocalOnly)
	}
	if got.Migrations.RemoteOnly != 0 {
		t.Errorf("migrations.remote_only: got %d, want 0", got.Migrations.RemoteOnly)
	}
}

// TestStatusMissingBasinDir verifies that `basin status` in a directory
// with no basin/ directory (and no parent basin/ in temp hierarchy)
// returns a clean "not linked" response (nil error, not-linked message).
func TestStatusMissingBasinDir(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	// No init — no basin/ directory whatsoever.
	g := &globalFlags{}
	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus with no basin dir: %v", err)
	}
	out := read()
	if !strings.Contains(out, "no project linked") {
		t.Errorf("expected 'no project linked': %q", out)
	}
}

// TestStatusJSONDriftCounts verifies the migrations drift JSON fields
// when local and remote sets fully diverge (no overlap).
func TestStatusJSONDriftCounts(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "drift-json"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	migDir := filepath.Join(dir, "basin", "migrations")
	// Two local-only files.
	for _, name := range []string{"local_a.sql", "local_b.sql"} {
		if err := os.WriteFile(filepath.Join(migDir, name), []byte("-- local"), 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", name, err)
		}
	}

	// Three remote-only migrations (different names from local).
	srv := newStatusServer(t,
		map[string]any{"ref": "drift-json", "status": "active"},
		[]map[string]any{
			{"id": "r1", "source": "remote_x.sql"},
			{"id": "r2", "source": "remote_y.sql"},
			{"id": "r3", "source": "remote_z.sql"},
		},
	)
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus: %v", err)
	}
	out := read()

	var got struct {
		Migrations struct {
			Applied    int `json:"applied"`
			LocalOnly  int `json:"local_only"`
			RemoteOnly int `json:"remote_only"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if got.Migrations.Applied != 0 {
		t.Errorf("applied: got %d, want 0", got.Migrations.Applied)
	}
	if got.Migrations.LocalOnly != 2 {
		t.Errorf("local_only: got %d, want 2", got.Migrations.LocalOnly)
	}
	if got.Migrations.RemoteOnly != 3 {
		t.Errorf("remote_only: got %d, want 3", got.Migrations.RemoteOnly)
	}
}

// TestStatusRemoteOnlyWithNoSourceFallsBackToID verifies that a remote
// MigrationRow with no Source field uses the ID as its key, so it counts
// as remote_only (distinct from any local file).
func TestStatusRemoteOnlyWithNoSourceFallsBackToID(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "id-fallback"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	// Remote migration has no Source field; ID is "m-abc".
	// Local has no files.
	srv := newStatusServer(t,
		map[string]any{"ref": "id-fallback", "status": "active"},
		[]map[string]any{
			{"id": "m-abc"}, // no source field
		},
	)
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdStatus(g, nil); err != nil {
		t.Fatalf("cmdStatus: %v", err)
	}
	out := read()

	var got struct {
		Migrations struct {
			Applied    int `json:"applied"`
			LocalOnly  int `json:"local_only"`
			RemoteOnly int `json:"remote_only"`
		} `json:"migrations"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if got.Migrations.Applied != 0 {
		t.Errorf("applied: got %d, want 0", got.Migrations.Applied)
	}
	if got.Migrations.RemoteOnly != 1 {
		t.Errorf("remote_only: got %d, want 1", got.Migrations.RemoteOnly)
	}
}
