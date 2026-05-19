// cmd_link_test — unit tests for `basin link`.
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

// newLinkServer builds a minimal httptest.Server that handles
// GET /v1/projects/{ref} returning 200 for known refs and 404 for unknown.
func newLinkServer(t *testing.T, knownRefs map[string]bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only handle GET /v1/projects/<ref>.
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1/projects/") {
			http.NotFound(w, r)
			return
		}
		// Extract ref from path.
		ref := strings.TrimPrefix(r.URL.Path, "/v1/projects/")
		// Strip any trailing path components.
		if idx := strings.Index(ref, "/"); idx >= 0 {
			ref = ref[:idx]
		}
		if known, ok := knownRefs[ref]; !ok || !known {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"code":    "not_found",
					"message": "project not found",
				},
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project": map[string]any{
				"ref":    ref,
				"name":   "Demo Project",
				"region": "jnb",
				"status": "active",
			},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestLinkSuccess verifies that `basin link --project=<ref>` writes
// project_ref into basin/config.toml when the cloud confirms the ref.
func TestLinkSuccess(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	// Scaffold basin/ directory.
	g := &globalFlags{}
	if err := cmdInit(g, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"proj-1": true})
	g = &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	if err := cmdLink(g, []string{"--project=proj-1"}); err != nil {
		t.Fatalf("cmdLink: %v", err)
	}

	// Verify the config.toml was updated.
	wp, err := loadWorkingProject(dir)
	if err != nil {
		t.Fatalf("loadWorkingProject: %v", err)
	}
	if wp == nil || wp.ProjectRef != "proj-1" {
		t.Errorf("project_ref: got %v, want proj-1", wp)
	}
}

// TestLinkNotFound verifies that a 404 from the cloud produces a typed
// error message containing the ref name.
func TestLinkNotFound(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{}) // no known refs
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	err := cmdLink(g, []string{"--project=does-not-exist"})
	if err == nil {
		t.Fatal("expected error for unknown project, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Errorf("error should mention the ref: %v", err)
	}
}

// TestLinkAlreadyLinkedNoForce verifies that attempting to link to a
// different ref without --force is refused.
func TestLinkAlreadyLinkedNoForce(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	// Link to proj-1 first.
	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "proj-1"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	// The already-linked check fires before any network call, so the
	// server is not needed here — but we still build it for completeness.
	srv := newLinkServer(t, map[string]bool{"proj-2": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	err := cmdLink(g, []string{"--project=proj-2"})
	if err == nil {
		t.Fatal("expected error when already linked without --force, got nil")
	}
	if !strings.Contains(err.Error(), "--force") {
		t.Errorf("error should mention --force: %v", err)
	}
	// Confirm the binding was NOT changed.
	wp, _ := loadWorkingProject(dir)
	if wp == nil || wp.ProjectRef != "proj-1" {
		t.Errorf("project_ref should still be proj-1, got %v", wp)
	}
}

// TestLinkAlreadyLinkedWithForce verifies that --force overwrites an
// existing binding.
func TestLinkAlreadyLinkedWithForce(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "proj-1"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"proj-2": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	if err := cmdLink(g, []string{"--project=proj-2", "--force"}); err != nil {
		t.Fatalf("cmdLink --force: %v", err)
	}

	wp, err := loadWorkingProject(dir)
	if err != nil {
		t.Fatalf("loadWorkingProject: %v", err)
	}
	if wp == nil || wp.ProjectRef != "proj-2" {
		t.Errorf("project_ref: got %v, want proj-2", wp)
	}
}

// TestLinkMissingBasinDir verifies that `basin link` returns a hint
// pointing at `basin init` when ./basin/ does not exist.
func TestLinkMissingBasinDir(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	// Do NOT call cmdInit — no basin/ directory.
	srv := newLinkServer(t, map[string]bool{"proj-1": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	err := cmdLink(g, []string{"--project=proj-1"})
	if err == nil {
		t.Fatal("expected error for missing basin/ dir, got nil")
	}
	if !strings.Contains(err.Error(), "basin init") {
		t.Errorf("error should hint at `basin init`: %v", err)
	}
}

// TestLinkJSONOutputShape verifies the --json envelope on success.
func TestLinkJSONOutputShape(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"proj-json": true})
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdLink(g, []string{"--project=proj-json"}); err != nil {
		t.Fatalf("cmdLink --json: %v", err)
	}
	out := read()

	var got struct {
		ProjectRef  string `json:"project_ref"`
		Linked      bool   `json:"linked"`
		PreviousRef string `json:"previous_ref"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode link JSON: %v\nbody=%s", err, out)
	}
	if !got.Linked {
		t.Errorf("linked: got false, want true")
	}
	if got.ProjectRef != "proj-json" {
		t.Errorf("project_ref: got %q, want proj-json", got.ProjectRef)
	}
	// previous_ref should be empty on first link.
	if got.PreviousRef != "" {
		t.Errorf("previous_ref: got %q, want empty", got.PreviousRef)
	}
}

// TestLinkJSONErrorEnvelope verifies that a 404 under --json writes a
// JSON error envelope to stdout (not a Go error).
func TestLinkJSONErrorEnvelope(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{}) // no known refs
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	_ = cmdLink(g, []string{"--project=ghost"})
	out := read()

	var got struct {
		Linked bool   `json:"linked"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode link JSON error envelope: %v\nbody=%s", err, out)
	}
	if got.Linked {
		t.Errorf("linked: got true, want false")
	}
	if got.Error == "" {
		t.Errorf("error field is empty")
	}
	if !strings.Contains(got.Error, "ghost") {
		t.Errorf("error should mention ref %q: %q", "ghost", got.Error)
	}
}

// TestLinkJSONMissingBasinDirEnvelope verifies that a missing basin/
// directory under --json returns a JSON error envelope.
func TestLinkJSONMissingBasinDirEnvelope(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	// No cmdInit — no basin/ directory.
	srv := newLinkServer(t, map[string]bool{"proj-1": true})
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	_ = cmdLink(g, []string{"--project=proj-1"})
	out := read()

	var got struct {
		Linked bool   `json:"linked"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if got.Linked {
		t.Errorf("linked: got true, want false")
	}
	if !strings.Contains(got.Error, "basin init") {
		t.Errorf("error should hint at `basin init`: %q", got.Error)
	}
}

// TestLinkMissingProjectFlag verifies that --project is required.
func TestLinkMissingProjectFlag(t *testing.T) {
	withTempConfigDir(t)
	chdirTemp(t)

	g := &globalFlags{}
	err := cmdLink(g, nil)
	if err == nil {
		t.Fatal("expected error for missing --project, got nil")
	}
	if !strings.Contains(err.Error(), "--project") {
		t.Errorf("error should mention --project: %v", err)
	}
}

// TestLinkJSONAlreadyLinkedNoForceEnvelope verifies the JSON error
// envelope when already linked and --force is absent.
func TestLinkJSONAlreadyLinkedNoForceEnvelope(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "existing-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"new-proj": true})
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	_ = cmdLink(g, []string{"--project=new-proj"})
	out := read()

	var got struct {
		Linked     bool   `json:"linked"`
		Error      string `json:"error"`
		CurrentRef string `json:"current_ref"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode JSON: %v\nbody=%s", err, out)
	}
	if got.Linked {
		t.Errorf("linked: got true, want false")
	}
	if got.CurrentRef != "existing-proj" {
		t.Errorf("current_ref: got %q, want existing-proj", got.CurrentRef)
	}
}

// TestLinkSameRefIdempotent verifies that linking to the already-linked
// ref (same ref) is a no-op success without needing --force.
func TestLinkSameRefIdempotent(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "same-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"same-proj": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	if err := cmdLink(g, []string{"--project=same-proj"}); err != nil {
		t.Fatalf("cmdLink same ref: %v", err)
	}

	// Binding unchanged.
	wp, _ := loadWorkingProject(dir)
	if wp == nil || wp.ProjectRef != "same-proj" {
		t.Errorf("project_ref: got %v, want same-proj", wp)
	}
}

// TestLinkPreservesExistingFields verifies that linking preserves other
// workingProject fields (DefaultBranch, EngineVersionPin).
func TestLinkPreservesExistingFields(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	// Pre-populate extra fields.
	if err := saveWorkingProject(dir, workingProject{
		DefaultBranch:    "dev",
		EngineVersionPin: "0.5.0",
	}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"proj-x": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	if err := cmdLink(g, []string{"--project=proj-x"}); err != nil {
		t.Fatalf("cmdLink: %v", err)
	}

	wp, err := loadWorkingProject(dir)
	if err != nil {
		t.Fatalf("loadWorkingProject: %v", err)
	}
	if wp.ProjectRef != "proj-x" {
		t.Errorf("project_ref: got %q, want proj-x", wp.ProjectRef)
	}
	if wp.DefaultBranch != "dev" {
		t.Errorf("DefaultBranch should be preserved: got %q", wp.DefaultBranch)
	}
	if wp.EngineVersionPin != "0.5.0" {
		t.Errorf("EngineVersionPin should be preserved: got %q", wp.EngineVersionPin)
	}
}

// TestLinkJSONForceOverwriteShape verifies the JSON shape when --force
// overwrites an existing binding, including previous_ref.
func TestLinkJSONForceOverwriteShape(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}
	if err := saveWorkingProject(dir, workingProject{ProjectRef: "old-proj"}); err != nil {
		t.Fatalf("saveWorkingProject: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"new-proj": true})
	g := &globalFlags{json: true, token: "bso_org_test", apiURL: srv.URL}

	read := captureStdout(t)
	if err := cmdLink(g, []string{"--project=new-proj", "--force"}); err != nil {
		t.Fatalf("cmdLink --force --json: %v", err)
	}
	out := read()

	var got struct {
		ProjectRef  string `json:"project_ref"`
		Linked      bool   `json:"linked"`
		PreviousRef string `json:"previous_ref"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode link JSON: %v\nbody=%s", err, out)
	}
	if !got.Linked {
		t.Errorf("linked: got false, want true")
	}
	if got.ProjectRef != "new-proj" {
		t.Errorf("project_ref: got %q, want new-proj", got.ProjectRef)
	}
	if got.PreviousRef != "old-proj" {
		t.Errorf("previous_ref: got %q, want old-proj", got.PreviousRef)
	}
}

// TestLinkWritesConfigTOMLFile is an integration check that the
// basin/config.toml file on disk actually contains the project_ref line.
func TestLinkWritesConfigTOMLFile(t *testing.T) {
	withTempConfigDir(t)
	dir := chdirTemp(t)

	if err := cmdInit(&globalFlags{}, nil); err != nil {
		t.Fatalf("cmdInit: %v", err)
	}

	srv := newLinkServer(t, map[string]bool{"toml-proj": true})
	g := &globalFlags{token: "bso_org_test", apiURL: srv.URL}

	if err := cmdLink(g, []string{"--project=toml-proj"}); err != nil {
		t.Fatalf("cmdLink: %v", err)
	}

	b, err := os.ReadFile(filepath.Join(dir, "basin", "config.toml"))
	if err != nil {
		t.Fatalf("read config.toml: %v", err)
	}
	if !strings.Contains(string(b), "toml-proj") {
		t.Errorf("config.toml does not contain project ref: %q", string(b))
	}
}
