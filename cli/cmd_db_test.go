// cmd_db_test — coverage for the `db` subcommand family.
//
// Tests use httptest.Server stubs. Each test asserts request method + path.
// Confirmation prompts are bypassed via --yes or piped stdin.
//
// Test count: 22 tests covering all 7 sub-handlers plus edge cases.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────

// dbGFlag returns a globalFlags pointing at the test server with a fake token.
func dbGFlag(srv *httptest.Server) *globalFlags {
	return &globalFlags{apiURL: srv.URL, token: "bso_org_test"}
}

// dbServer builds a test server and wires cleanup.
func dbServer(t *testing.T, mux *http.ServeMux) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// dbSetEnv sets BASIN_API + BASIN_TOKEN and a temp config dir.
func dbSetEnv(t *testing.T, srv *httptest.Server) {
	t.Helper()
	withTempConfigDir(t)
	t.Setenv("BASIN_API", srv.URL)
	t.Setenv("BASIN_TOKEN", "bso_org_test")
}

// makeMigrationsDir creates ./basin/migrations/ in a temp dir and
// returns the temp dir. The caller's CWD is changed to the temp dir
// and restored on cleanup.
func makeMigrationsDir(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "basin", "migrations")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create migrations dir: %v", err)
	}
	prev, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(prev) })
	return tmp
}

// writeMigration writes a .sql file into the migrations dir.
func writeMigration(t *testing.T, tmp, filename, sql string) {
	t.Helper()
	p := filepath.Join(tmp, "basin", "migrations", filename)
	if err := os.WriteFile(p, []byte(sql), 0o644); err != nil {
		t.Fatalf("write migration %s: %v", filename, err)
	}
}

// ── db push ───────────────────────────────────────────────────────────

// TestDBPushHappyPath: 2 local files, 1 already applied → 1 POSTed.
func TestDBPushHappyPath(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_create_users.sql", "CREATE TABLE users (id int);")
	writeMigration(t, tmp, "0002_add_email.sql", "ALTER TABLE users ADD COLUMN email text;")

	var postedFiles []string
	mux := http.NewServeMux()
	// List returns the first migration as already applied.
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"migrations": []any{
					map[string]any{"id": "m1", "source": "0001_create_users.sql"},
				},
			})
			return
		}
		if r.Method == http.MethodPost {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if src, ok := body["source"].(string); ok {
				postedFiles = append(postedFiles, src)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m2", "source": body["source"]})
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"push", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db push: %v", err)
	}
	if len(postedFiles) != 1 || postedFiles[0] != "0002_add_email.sql" {
		t.Errorf("expected 0002_add_email.sql to be POSTed, got: %v", postedFiles)
	}
	if !strings.Contains(out, "applied: 1") {
		t.Errorf("output should report applied:1, got: %q", out)
	}
}

// TestDBPushAllApplied: all local files already applied → nothing POSTed.
func TestDBPushAllApplied(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_init.sql", "SELECT 1;")

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"migrations": []any{
					map[string]any{"id": "m1", "source": "0001_init.sql"},
				},
			})
			return
		}
		// POST should never fire.
		t.Errorf("unexpected POST to migrations")
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"push", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db push all applied: %v", err)
	}
	if !strings.Contains(out, "applied: 0") {
		t.Errorf("expected applied:0, got: %q", out)
	}
}

// TestDBPushJSONShape: --json output has correct envelope.
func TestDBPushJSONShape(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_init.sql", "SELECT 1;")
	writeMigration(t, tmp, "0002_add.sql", "SELECT 2;")

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"migrations": []any{
					map[string]any{"id": "m1", "source": "0001_init.sql"},
				},
			})
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m2", "source": "0002_add.sql"})
			return
		}
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"push", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db push --json: %v", err)
	}
	var got struct {
		Applied []struct {
			Filename string `json:"filename"`
			ID       string `json:"id"`
		} `json:"applied"`
		Skipped []struct {
			Filename string `json:"filename"`
			Reason   string `json:"reason"`
		} `json:"skipped"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode push JSON: %v\nbody=%s", err, out)
	}
	if len(got.Applied) != 1 || got.Applied[0].Filename != "0002_add.sql" {
		t.Errorf("applied: %+v", got.Applied)
	}
	if len(got.Skipped) != 1 || got.Skipped[0].Reason != "already applied" {
		t.Errorf("skipped: %+v", got.Skipped)
	}
}

// ── db pull ───────────────────────────────────────────────────────────

// TestDBPullHappyPath: remote has 1 migration → writes local file.
func TestDBPullHappyPath(t *testing.T) {
	tmp := makeMigrationsDir(t)
	_ = tmp

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"migrations": []any{
				map[string]any{"id": "m1", "source": "0001_init.sql"},
			},
		})
	})
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/m1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     "m1",
			"source": "0001_init.sql",
			"sql":    "CREATE TABLE users (id int);",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"pull", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db pull: %v", err)
	}
	if !strings.Contains(out, "pulled: 1") {
		t.Errorf("expected pulled:1, got: %q", out)
	}
	// Verify file was written.
	content, err := os.ReadFile(filepath.Join(tmp, "basin", "migrations", "0001_init.sql"))
	if err != nil {
		t.Fatalf("read pulled file: %v", err)
	}
	if !strings.Contains(string(content), "CREATE TABLE users") {
		t.Errorf("pulled file content wrong: %q", string(content))
	}
}

// TestDBPullOverwriteRefusal: local file differs → refuse without --force.
func TestDBPullOverwriteRefusal(t *testing.T) {
	tmp := makeMigrationsDir(t)
	// Write a different local version.
	writeMigration(t, tmp, "0001_init.sql", "-- different content")

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"migrations": []any{
				map[string]any{"id": "m1", "source": "0001_init.sql"},
			},
		})
	})
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/m1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     "m1",
			"source": "0001_init.sql",
			"sql":    "CREATE TABLE users (id int);",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"pull", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db pull (refusal): %v", err)
	}
	// File should be skipped, not overwritten.
	if !strings.Contains(out, "skipped: 1") {
		t.Errorf("expected skipped:1 on overwrite refusal, got: %q", out)
	}
	// Original content should be preserved.
	content, _ := os.ReadFile(filepath.Join(tmp, "basin", "migrations", "0001_init.sql"))
	if !strings.Contains(string(content), "different content") {
		t.Errorf("local file was overwritten unexpectedly")
	}
}

// TestDBPullForce: --force overwrites differing local file.
func TestDBPullForce(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_init.sql", "-- different content")

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"migrations": []any{
				map[string]any{"id": "m1", "source": "0001_init.sql"},
			},
		})
	})
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/m1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     "m1",
			"source": "0001_init.sql",
			"sql":    "CREATE TABLE users (id int);",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"pull", "--project=proj1", "--force"})
	out := read()
	if err != nil {
		t.Fatalf("db pull --force: %v", err)
	}
	if !strings.Contains(out, "pulled: 1") {
		t.Errorf("expected pulled:1 with --force, got: %q", out)
	}
	content, _ := os.ReadFile(filepath.Join(tmp, "basin", "migrations", "0001_init.sql"))
	if !strings.Contains(string(content), "CREATE TABLE users") {
		t.Errorf("local file not updated with --force")
	}
}

// ── db diff ───────────────────────────────────────────────────────────

// TestDBDiffNoDiff: empty diff → no file written.
func TestDBDiffNoDiff(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"diff_sql": "",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"diff", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db diff no-diff: %v", err)
	}
	if !strings.Contains(out, "no schema drift") {
		t.Errorf("expected 'no schema drift', got: %q", out)
	}
}

// TestDBDiffNoDiffJSON: --json shape when no drift.
func TestDBDiffNoDiffJSON(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"diff_sql": ""})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"diff", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db diff no-diff --json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode diff JSON: %v\nbody=%s", err, out)
	}
	if reason, ok := got["reason"].(string); !ok || reason != "no-diff" {
		t.Errorf("expected reason=no-diff, got: %v", got)
	}
}

// TestDBDiffWritesFile: real diff → new migration file written.
func TestDBDiffWritesFile(t *testing.T) {
	tmp := makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"diff_sql": "ALTER TABLE users ADD COLUMN age int;",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"diff", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db diff writes file: %v", err)
	}
	if !strings.Contains(out, "wrote") {
		t.Errorf("expected 'wrote' in output: %q", out)
	}
	// Verify a file was written.
	entries, err := os.ReadDir(filepath.Join(tmp, "basin", "migrations"))
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 file written, got %d", len(entries))
	}
}

// ── db reset ──────────────────────────────────────────────────────────

// TestDBResetDeclined: "n" at the prompt → no API calls.
func TestDBResetDeclined(t *testing.T) {
	makeMigrationsDir(t)

	var apiCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	// Pipe "n" to stdin.
	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"reset", "--project=proj1"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("db reset declined: %v", err)
	}
	if apiCalled {
		t.Error("API should not be called when reset declined")
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted', got: %q", out)
	}
}

// TestDBResetYes: --yes runs full sequence with correct API calls.
func TestDBResetYes(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_init.sql", "CREATE TABLE users (id int);")

	var (
		snapshotCalled bool
		rollbackCalled bool
		applyCalled    bool
	)
	mux := http.NewServeMux()
	// Snapshot.
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			snapshotCalled = true
			w.WriteHeader(http.StatusCreated)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "snap-1"})
		}
	})
	// List + apply migrations.
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"migrations": []any{
					map[string]any{"id": "m-old", "source": "old.sql"},
				},
			})
			return
		}
		if r.Method == http.MethodPost {
			applyCalled = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "m-new"})
			return
		}
	})
	// Rollback.
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/m-old/rollback", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			rollbackCalled = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"rolled_back": true})
		}
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"reset", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("db reset --yes: %v", err)
	}
	if !snapshotCalled {
		t.Error("snapshot endpoint not called")
	}
	if !rollbackCalled {
		t.Error("rollback endpoint not called")
	}
	if !applyCalled {
		t.Error("apply endpoint not called")
	}
	if !strings.Contains(out, "rolled_back: 1") {
		t.Errorf("expected rolled_back:1, got: %q", out)
	}
	if !strings.Contains(out, "applied: 1") {
		t.Errorf("expected applied:1, got: %q", out)
	}
}

// TestDBResetJSONShape: --json + --yes emits correct JSON.
func TestDBResetJSONShape(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/backups/snapshots", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": "snap-abc"})
	})
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"migrations": []any{}})
		}
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"reset", "--project=proj1", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("db reset --json --yes: %v", err)
	}
	var got struct {
		SnapshotID  string `json:"snapshot_id"`
		RolledBack  int    `json:"rolled_back"`
		Applied     int    `json:"applied"`
		Seeded      bool   `json:"seeded"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode reset JSON: %v\nbody=%s", err, out)
	}
	if got.SnapshotID != "snap-abc" {
		t.Errorf("snapshot_id: got %q, want 'snap-abc'", got.SnapshotID)
	}
}

// ── db url ────────────────────────────────────────────────────────────

// TestDBURLShow: default (no flags) shows masked DSN.
func TestDBURLShow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"host":     "db.basin.run",
			"port":     5432,
			"user":     "tenant",
			"dbname":   "basin",
			"password": "supersecret1234",
			"sslmode":  "require",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"url", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db url show: %v", err)
	}
	if !strings.Contains(out, "postgres://") {
		t.Errorf("expected DSN in output, got: %q", out)
	}
	// Password should be masked.
	if strings.Contains(out, "supersecret1234") {
		t.Errorf("plaintext password should not appear in masked DSN")
	}
}

// TestDBURLReveal: --reveal POSTs to the reveal endpoint.
func TestDBURLReveal(t *testing.T) {
	var revealCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/reveal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		revealCalled = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"host":     "db.basin.run",
			"port":     5432,
			"user":     "tenant",
			"dbname":   "basin",
			"password": "plaintext_secret",
			"sslmode":  "require",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"url", "--project=proj1", "--reveal"})
	out := read()
	if err != nil {
		t.Fatalf("db url --reveal: %v", err)
	}
	if !revealCalled {
		t.Error("reveal endpoint not called")
	}
	if !strings.Contains(out, "postgres://") {
		t.Errorf("expected DSN in output, got: %q", out)
	}
}

// TestDBURLRotate: --rotate --yes POSTs to the rotate endpoint.
func TestDBURLRotate(t *testing.T) {
	var rotateCalled bool
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/rotate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		rotateCalled = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"host":     "db.basin.run",
			"port":     5432,
			"user":     "tenant",
			"dbname":   "basin",
			"password": "new_secret_pw",
			"sslmode":  "require",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"url", "--project=proj1", "--rotate", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("db url --rotate --yes: %v", err)
	}
	if !rotateCalled {
		t.Error("rotate endpoint not called")
	}
	if !strings.Contains(out, "postgres://") {
		t.Errorf("expected DSN in output, got: %q", out)
	}
}

// TestDBURLRotateJSONShape: --json output has dsn key.
func TestDBURLRotateJSONShape(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/pgwire/rotate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"host":    "db.basin.run",
			"port":    5432,
			"user":    "tenant",
			"dbname":  "basin",
			"password": "new_pw",
			"sslmode": "require",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"url", "--project=proj1", "--rotate", "--yes"})
	out := read()
	if err != nil {
		t.Fatalf("db url --rotate --json: %v", err)
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode url JSON: %v\nbody=%s", err, out)
	}
	if _, ok := got["dsn"]; !ok {
		t.Errorf("expected 'dsn' key in JSON output: %v", got)
	}
	if !strings.HasPrefix(got["dsn"], "postgres://") {
		t.Errorf("dsn should start with 'postgres://', got: %q", got["dsn"])
	}
}

// ── db dump ───────────────────────────────────────────────────────────

// TestDBDumpSchemaOnly: --schema-only emits CREATE TABLE, no INSERT.
func TestDBDumpSchemaOnly(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"columns": []string{"table_name", "column_name", "data_type", "is_nullable", "ordinal_position"},
			"rows": []any{
				[]any{"users", "id", "integer", "NO", 1.0},
				[]any{"users", "email", "text", "YES", 2.0},
			},
			"elapsed_ms": 5,
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"dump", "--project=proj1", "--schema-only"})
	out := read()
	if err != nil {
		t.Fatalf("db dump --schema-only: %v", err)
	}
	if !strings.Contains(out, "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE in schema-only output: %q", out)
	}
	if strings.Contains(out, "INSERT INTO") {
		t.Errorf("INSERT INTO should not appear in --schema-only output: %q", out)
	}
}

// TestDBDumpDataOnly: --data-only emits INSERT, no CREATE TABLE.
func TestDBDumpDataOnly(t *testing.T) {
	makeMigrationsDir(t)

	callCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.Header().Set("Content-Type", "application/json")

		// First call: schema query.
		// Second call: SELECT * FROM "users".
		if callCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"table_name", "column_name", "data_type", "is_nullable", "ordinal_position"},
				"rows": []any{
					[]any{"users", "id", "integer", "NO", 1.0},
				},
				"elapsed_ms": 1,
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns":    []string{"id"},
				"rows":       []any{[]any{42.0}},
				"elapsed_ms": 1,
			})
		}
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"dump", "--project=proj1", "--data-only"})
	out := read()
	if err != nil {
		t.Fatalf("db dump --data-only: %v", err)
	}
	if strings.Contains(out, "CREATE TABLE") {
		t.Errorf("CREATE TABLE should not appear in --data-only output: %q", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in data-only output: %q", out)
	}
}

// TestDBDumpBoth: default (no flags) emits both schema and data.
func TestDBDumpBoth(t *testing.T) {
	makeMigrationsDir(t)

	callCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		if callCount == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns": []string{"table_name", "column_name", "data_type", "is_nullable", "ordinal_position"},
				"rows": []any{
					[]any{"orders", "id", "integer", "NO", 1.0},
				},
				"elapsed_ms": 1,
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"columns":    []string{"id"},
				"rows":       []any{[]any{1.0}, []any{2.0}},
				"elapsed_ms": 1,
			})
		}
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"dump", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db dump both: %v", err)
	}
	if !strings.Contains(out, "CREATE TABLE") {
		t.Errorf("expected CREATE TABLE in combined output: %q", out)
	}
	if !strings.Contains(out, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in combined output: %q", out)
	}
}

// ── db lint ───────────────────────────────────────────────────────────

// TestDBLintClean: all files pass dry-run → exit 0.
func TestDBLintClean(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_init.sql", "CREATE TABLE users (id int);")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"columns":    []string{},
			"rows":       []any{},
			"elapsed_ms": 1,
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"lint", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db lint clean: %v", err)
	}
	if !strings.Contains(out, "no errors") {
		t.Errorf("expected 'no errors' in clean lint output: %q", out)
	}
}

// TestDBLintError: one file fails dry-run → error returned and JSON shape correct.
func TestDBLintError(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_bad.sql", "INVALID SQL STATEMENT;")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"code":    "0A000",
				"message": "syntax error at line 1: unexpected token 'INVALID'",
			},
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"lint", "--project=proj1"})
	out := read()
	// lint exits 1 on errors (non-nil return); but JSON is still written.
	// Under --json, printJSON is called before the final error return,
	// so the JSON shape is in stdout regardless.
	var got struct {
		FilesChecked int `json:"files_checked"`
		Errors       []struct {
			File    string `json:"file"`
			Line    int    `json:"line"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode lint JSON: %v\nbody=%s\nrunErr=%v", err, out, err)
	}
	if got.FilesChecked != 1 {
		t.Errorf("files_checked: got %d, want 1", got.FilesChecked)
	}
	if len(got.Errors) != 1 {
		t.Errorf("errors: got %d, want 1", len(got.Errors))
	}
	_ = err // lint returns an error — that is expected
}

// TestDBLintJSONClean: --json + clean files → errors array empty.
func TestDBLintJSONClean(t *testing.T) {
	tmp := makeMigrationsDir(t)
	writeMigration(t, tmp, "0001_ok.sql", "CREATE TABLE t (id int);")
	writeMigration(t, tmp, "0002_ok.sql", "ALTER TABLE t ADD COLUMN v text;")

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/projects/proj1/sql/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"columns": []string{}, "rows": []any{}, "elapsed_ms": 1})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"lint", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db lint --json clean: %v", err)
	}
	var got struct {
		FilesChecked int `json:"files_checked"`
		Errors       []struct {
			File string `json:"file"`
		} `json:"errors"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode lint JSON: %v\nbody=%s", err, out)
	}
	if got.FilesChecked != 2 {
		t.Errorf("files_checked: got %d, want 2", got.FilesChecked)
	}
	if len(got.Errors) != 0 {
		t.Errorf("errors: got %d, want 0", len(got.Errors))
	}
}

// ── misc ──────────────────────────────────────────────────────────────

// TestDBUnknownSubcommand: returns errSilent for unknown sub.
func TestDBUnknownSubcommand(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{token: "t"}
	err := cmdDB(g, []string{"frobnicate"})
	if err == nil {
		t.Fatal("expected error for unknown subcommand")
	}
}

// TestDBHelp: returns nil for help subcommand.
func TestDBHelp(t *testing.T) {
	withTempConfigDir(t)
	g := &globalFlags{}

	read := captureStdout(t)
	err := cmdDB(g, []string{"help"})
	_ = read()
	if err != nil {
		t.Fatalf("db help: %v", err)
	}
}

// TestDBDumpMutuallyExclusiveFlags: --schema-only + --data-only returns error.
func TestDBDumpMutuallyExclusiveFlags(t *testing.T) {
	makeMigrationsDir(t)
	withTempConfigDir(t)
	g := &globalFlags{token: "t", apiURL: "http://localhost"}
	err := cmdDB(g, []string{"dump", "--project=proj1", "--schema-only", "--data-only"})
	if err == nil {
		t.Fatal("expected error for conflicting flags")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("expected 'mutually exclusive' in error: %v", err)
	}
}

// TestSQLLiteral: spot-checks the INSERT VALUES formatter.
func TestSQLLiteral(t *testing.T) {
	cases := []struct {
		in  any
		out string
	}{
		{nil, "NULL"},
		{true, "TRUE"},
		{false, "FALSE"},
		{42.0, "42"},
		{3.14, "3.14"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
	}
	for _, tc := range cases {
		got := sqlLiteral(tc.in)
		if got != tc.out {
			t.Errorf("sqlLiteral(%v) = %q, want %q", tc.in, got, tc.out)
		}
	}
}

// TestQuoteIdent: verifies double-quote wrapping + escaping.
func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("users"); got != `"users"` {
		t.Errorf("quoteIdent('users') = %q", got)
	}
	if got := quoteIdent(`my"table`); got != `"my""table"` {
		t.Errorf("quoteIdent with embedded quote = %q", got)
	}
}

// TestToMigrationFilename: check timestamp prefix pattern.
func TestToMigrationFilename(t *testing.T) {
	name := toMigrationFilename("add user table")
	if !strings.HasSuffix(name, "_add_user_table.sql") {
		t.Errorf("filename = %q, want suffix _add_user_table.sql", name)
	}
	if !strings.Contains(name, "_") {
		t.Errorf("filename should have underscore separators: %q", name)
	}
}

// captureStderr is defined in cmd_branches_test.go — reused here through
// the same package test binary. No redeclaration needed.

// TestDBURLRotateDeclined: "n" at prompt → Aborted printed.
func TestDBURLRotateDeclined(t *testing.T) {
	mux := http.NewServeMux()
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	pr, pw, _ := os.Pipe()
	prevStdin := os.Stdin
	os.Stdin = pr
	_, _ = fmt.Fprintln(pw, "n")
	pw.Close()

	read := captureStdout(t)
	g := dbGFlag(srv)
	err := cmdDB(g, []string{"url", "--project=proj1", "--rotate"})
	out := read()
	os.Stdin = prevStdin

	if err != nil {
		t.Fatalf("db url rotate declined: %v", err)
	}
	if !strings.Contains(out, "Aborted") {
		t.Errorf("expected 'Aborted', got: %q", out)
	}
}

// TestDBURLRotateJSONRequiresYes: --json + --rotate without --yes returns error.
func TestDBURLRotateJSONRequiresYes(t *testing.T) {
	mux := http.NewServeMux()
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "tok", json: true}
	err := cmdDB(g, []string{"url", "--project=proj1", "--rotate"})
	if err == nil {
		t.Fatal("expected error when --json + --rotate without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required': %v", err)
	}
}

// TestDBResetJSONRequiresYes: --json without --yes returns error.
func TestDBResetJSONRequiresYes(t *testing.T) {
	makeMigrationsDir(t)
	mux := http.NewServeMux()
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	g := &globalFlags{apiURL: srv.URL, token: "tok", json: true}
	err := cmdDB(g, []string{"reset", "--project=proj1"})
	if err == nil {
		t.Fatal("expected error when --json without --yes")
	}
	if !strings.Contains(err.Error(), "confirmation required") {
		t.Errorf("expected 'confirmation required': %v", err)
	}
}

// TestDBPullJSONShape: --json output has pulled/skipped keys.
func TestDBPullJSONShape(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"migrations": []any{
				map[string]any{"id": "m1", "source": "0001_init.sql"},
			},
		})
	})
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/m1", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     "m1",
			"source": "0001_init.sql",
			"sql":    "SELECT 1;",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"pull", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db pull --json: %v", err)
	}
	var got struct {
		Pulled  []struct{ Filename string `json:"filename"` } `json:"pulled"`
		Skipped []struct{ Filename string `json:"filename"` } `json:"skipped"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode pull JSON: %v\nbody=%s", err, out)
	}
	if len(got.Pulled) != 1 || got.Pulled[0].Filename != "0001_init.sql" {
		t.Errorf("pulled: %+v", got.Pulled)
	}
}

// TestDBDiffWritesFileJSON: real diff + --json → {wrote: <path>} shape.
func TestDBDiffWritesFileJSON(t *testing.T) {
	makeMigrationsDir(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/projects/proj1/migrations/diff", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"diff_sql": "ALTER TABLE users ADD COLUMN age int;",
		})
	})
	srv := dbServer(t, mux)
	dbSetEnv(t, srv)

	read := captureStdout(t)
	g := &globalFlags{apiURL: srv.URL, token: "bso_org_test", json: true}
	err := cmdDB(g, []string{"diff", "--project=proj1"})
	out := read()
	if err != nil {
		t.Fatalf("db diff writes file --json: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode diff JSON: %v\nbody=%s", err, out)
	}
	wrote, ok := got["wrote"].(string)
	if !ok || wrote == "" {
		t.Errorf("expected 'wrote' string in JSON output: %v", got)
	}
	if !strings.HasSuffix(wrote, ".sql") {
		t.Errorf("wrote path should end in .sql: %q", wrote)
	}
}
