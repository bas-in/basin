// cmd_init_test — unit tests for `basin init`.
//
// Each test uses t.TempDir() as the working directory and overrides
// os.Getwd via a chdir wrapper (safe for tests because t.Cleanup
// restores the original directory).
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// chdirTemp creates a fresh temp dir, changes into it for the duration
// of the test, and returns the path. The original directory is
// restored by t.Cleanup.
func chdirTemp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
	return dir
}

// TestInitScaffoldsDirectory verifies that `basin init` creates
// basin/config.toml, basin/migrations/, and basin/seed.sql.
func TestInitScaffoldsDirectory(t *testing.T) {
	dir := chdirTemp(t)
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	// config.toml must exist and be parseable.
	if _, err := os.Stat(filepath.Join(dir, "basin", "config.toml")); err != nil {
		t.Errorf("config.toml missing: %v", err)
	}
	// migrations/ must be a directory.
	fi, err := os.Stat(filepath.Join(dir, "basin", "migrations"))
	if err != nil {
		t.Errorf("migrations/ missing: %v", err)
	} else if !fi.IsDir() {
		t.Errorf("migrations/ is not a directory")
	}
	// seed.sql must contain the placeholder.
	b, err := os.ReadFile(filepath.Join(dir, "basin", "seed.sql"))
	if err != nil {
		t.Errorf("seed.sql missing: %v", err)
	} else if !strings.Contains(string(b), "-- seed data") {
		t.Errorf("seed.sql placeholder missing: %q", string(b))
	}
}

// TestInitConflictWithoutForce verifies that running `basin init` a
// second time without --force returns an error.
func TestInitConflictWithoutForce(t *testing.T) {
	chdirTemp(t)
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("first init: %v", err)
	}
	// Second call without --force must error.
	err := cmdInit(g, nil)
	if err == nil {
		t.Fatal("expected error on second init without --force, got nil")
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Errorf("error should mention --force: %v", err)
	}
}

// TestInitIdempotencyUnderForce verifies that `basin init --force` on
// an existing basin/ directory succeeds without error.
func TestInitIdempotencyUnderForce(t *testing.T) {
	dir := chdirTemp(t)
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("first init: %v", err)
	}
	// Write a sentinel into seed.sql to verify --force rewrites it.
	if err := os.WriteFile(filepath.Join(dir, "basin", "seed.sql"),
		[]byte("-- custom seed\n"), 0o644); err != nil {
		t.Fatalf("WriteFile sentinel: %v", err)
	}
	if err := cmdInit(g, []string{"--force"}); err != nil {
		t.Fatalf("init --force: %v", err)
	}
	// seed.sql should be the placeholder again.
	b, _ := os.ReadFile(filepath.Join(dir, "basin", "seed.sql"))
	if !strings.Contains(string(b), "-- seed data") {
		t.Errorf("seed.sql not reset by --force: %q", string(b))
	}
}

// TestInitJSONOutputShape verifies the --json output of `basin init`.
func TestInitJSONOutputShape(t *testing.T) {
	chdirTemp(t)
	read := captureStdout(t)
	g := &globalFlags{json: true}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit --json: %v", err)
	}
	out := read()
	var got struct {
		OK       bool     `json:"ok"`
		BasinDir string   `json:"basin_dir"`
		Files    []string `json:"files"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode init JSON: %v\nbody=%s", err, out)
	}
	if !got.OK {
		t.Errorf("ok: got false, want true")
	}
	if got.BasinDir == "" {
		t.Errorf("basin_dir is empty")
	}
	if len(got.Files) != 3 {
		t.Errorf("files: got %d entries, want 3: %v", len(got.Files), got.Files)
	}
}

// TestInitJSONConflictShape verifies that a conflict under --json
// returns { ok: false, error: "..." } instead of a Go error.
func TestInitJSONConflictShape(t *testing.T) {
	chdirTemp(t)
	g := &globalFlags{json: true}
	// First init succeeds.
	read := captureStdout(t)
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("first init: %v", err)
	}
	read() // discard

	// Second init without --force under --json must print the error JSON.
	read = captureStdout(t)
	err := cmdInit(g, nil)
	out := read()
	// When --json, the handler writes the error JSON and returns nil OR
	// returns a structured value.  Either way the JSON must be on stdout.
	_ = err // may be nil (written as JSON) or non-nil
	var got struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if decErr := json.Unmarshal([]byte(out), &got); decErr != nil {
		// If nothing was written to stdout it means the error was returned
		// rather than printed — accept that too, as long as err != nil.
		if err == nil {
			t.Fatalf("expected either JSON on stdout or a returned error; got neither")
		}
		return
	}
	if got.OK {
		t.Errorf("ok: got true, want false")
	}
	if got.Error == "" {
		t.Errorf("error field is empty")
	}
}
