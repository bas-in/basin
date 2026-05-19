// cmd_unlink_test — unit tests for `basin unlink`.
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// initLinkedDir is a test helper that calls `basin init` then writes a
// project_ref into basin/config.toml, simulating a linked directory.
func initLinkedDir(t *testing.T, dir, ref string) {
	t.Helper()
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("initLinkedDir/cmdInit: %v", err)
	}
	wp := workingProject{ProjectRef: ref, DefaultBranch: "main"}
	if err := saveWorkingProject(dir, wp); err != nil {
		t.Fatalf("initLinkedDir/saveWorkingProject: %v", err)
	}
}

// TestUnlinkLinkedSuccess verifies that unlink removes project_ref from
// a directory that is currently linked.
func TestUnlinkLinkedSuccess(t *testing.T) {
	dir := chdirTemp(t)
	initLinkedDir(t, dir, "my-proj")

	g := &globalFlags{}
	if err := cmdUnlink(g, nil); err != nil {
		t.Fatalf("cmdUnlink: %v", err)
	}

	// Reload and confirm project_ref is gone.
	wp, err := loadWorkingProject(dir)
	if err != nil {
		t.Fatalf("loadWorkingProject after unlink: %v", err)
	}
	if wp != nil && wp.ProjectRef != "" {
		t.Errorf("project_ref not cleared: %q", wp.ProjectRef)
	}
}

// TestUnlinkNotLinkedNoOp verifies that unlink on a directory with no
// project_ref exits 0 without modifying anything.
func TestUnlinkNotLinkedNoOp(t *testing.T) {
	dir := chdirTemp(t)
	// Init but don't link.
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	// Record the mtime of config.toml before unlink.
	configPath := filepath.Join(dir, "basin", "config.toml")
	stBefore, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat config.toml: %v", err)
	}

	if err := cmdUnlink(g, nil); err != nil {
		t.Fatalf("cmdUnlink on unlinkd dir: %v", err)
	}

	// The file's mtime should be unchanged (no write occurred).
	stAfter, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat config.toml after unlink: %v", err)
	}
	if !stAfter.ModTime().Equal(stBefore.ModTime()) {
		t.Errorf("config.toml was rewritten on no-op unlink (mtime changed)")
	}
}

// TestUnlinkJSONOutputLinked verifies the --json output when a project
// is unlinked.
func TestUnlinkJSONOutputLinked(t *testing.T) {
	dir := chdirTemp(t)
	initLinkedDir(t, dir, "proj-abc")

	read := captureStdout(t)
	g := &globalFlags{json: true}
	if err := cmdUnlink(g, nil); err != nil {
		t.Fatalf("cmdUnlink --json: %v", err)
	}
	out := read()

	var got struct {
		OK          bool   `json:"ok"`
		WasLinked   bool   `json:"was_linked"`
		UnlinkedRef string `json:"unlinked_ref"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode unlink JSON: %v\nbody=%s", err, out)
	}
	if !got.OK {
		t.Errorf("ok: got false, want true")
	}
	if !got.WasLinked {
		t.Errorf("was_linked: got false, want true")
	}
	if got.UnlinkedRef != "proj-abc" {
		t.Errorf("unlinked_ref: got %q, want proj-abc", got.UnlinkedRef)
	}
}

// TestUnlinkJSONOutputNotLinked verifies the --json output when
// unlink is a no-op (not linked).
func TestUnlinkJSONOutputNotLinked(t *testing.T) {
	chdirTemp(t)
	g := &globalFlags{json: true}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	read := captureStdout(t)
	g2 := &globalFlags{json: true}
	if err := cmdUnlink(g2, nil); err != nil {
		t.Fatalf("cmdUnlink --json (not linked): %v", err)
	}
	out := read()

	var got struct {
		OK        bool `json:"ok"`
		WasLinked bool `json:"was_linked"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode unlink no-op JSON: %v\nbody=%s", err, out)
	}
	if !got.OK {
		t.Errorf("ok: got false, want true")
	}
	if got.WasLinked {
		t.Errorf("was_linked: got true, want false")
	}
}

// TestUnlinkPreservesMigrations verifies that unlink leaves the
// migrations/ directory and seed.sql intact.
func TestUnlinkPreservesMigrations(t *testing.T) {
	dir := chdirTemp(t)
	initLinkedDir(t, dir, "preserve-test")

	// Place a migration file to confirm it survives.
	mig := filepath.Join(dir, "basin", "migrations", "0001_init.sql")
	if err := os.WriteFile(mig, []byte("CREATE TABLE foo (id int);"), 0o644); err != nil {
		t.Fatalf("WriteFile migration: %v", err)
	}

	g := &globalFlags{}
	if err := cmdUnlink(g, nil); err != nil {
		t.Fatalf("cmdUnlink: %v", err)
	}

	// migrations/0001_init.sql must still exist.
	if _, err := os.Stat(mig); err != nil {
		t.Errorf("migration file removed by unlink: %v", err)
	}
	// seed.sql must still exist.
	if _, err := os.Stat(filepath.Join(dir, "basin", "seed.sql")); err != nil {
		t.Errorf("seed.sql removed by unlink: %v", err)
	}
	// config.toml must still exist (just with no project_ref).
	b, err := os.ReadFile(filepath.Join(dir, "basin", "config.toml"))
	if err != nil {
		t.Errorf("config.toml removed by unlink: %v", err)
	}
	if strings.Contains(string(b), "project_ref") {
		t.Errorf("project_ref still present in config.toml after unlink: %q", string(b))
	}
}
