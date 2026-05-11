package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// withTempCLIVersion overrides the package-level `version` var for one
// test and restores it on cleanup. Mirrors the pattern used by
// health_test.go in basin-cloud.
func withTempCLIVersion(t *testing.T, v string) {
	t.Helper()
	orig := version
	t.Cleanup(func() { version = orig })
	version = v
}

func TestWarnSuppressedUnderQuiet(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	// Cloud at 1.0.0 — outside window, but --quiet suppresses prose.
	_ = upsertCloudVersion("", CloudVersion{Version: "1.0.0"}, "https://api.basin.run")

	g := &globalFlags{
		apiURL: "https://api.basin.run",
		quiet:  true,
	}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("--quiet should suppress all output; got %q", buf.String())
	}
}

func TestWarnSkippedWhenCLIUnstamped(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "dev")

	_ = upsertCloudVersion("", CloudVersion{Version: "1.0.0"}, "https://api.basin.run")

	g := &globalFlags{apiURL: "https://api.basin.run"}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("unstamped CLI should skip the warning; got %q", buf.String())
	}
}

func TestWarnSilentWhenInWindow(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	// Cloud at 1.4.0 — inside the N-1 window.
	_ = upsertCloudVersion("", CloudVersion{Version: "1.4.0"}, "https://api.basin.run")

	g := &globalFlags{apiURL: "https://api.basin.run"}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("in-window CLI should not warn; got %q", buf.String())
	}
}

func TestWarnEmittedWhenOutOfWindow(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	// Cloud at 1.0.0 — N-5, outside window.
	_ = upsertCloudVersion("", CloudVersion{Version: "1.0.0"}, "https://api.basin.run")

	g := &globalFlags{apiURL: "https://api.basin.run"}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	out := buf.String()
	if !strings.Contains(out, "outside the cloud's two-minor support window") {
		t.Errorf("expected warning prose, got %q", out)
	}
	if !strings.Contains(out, "1.5.0") || !strings.Contains(out, "1.0.0") {
		t.Errorf("warning should name both versions; got %q", out)
	}
}

func TestWarnFetchesWhenCacheMiss(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	// No cached entry — resolveCloudVersionString must fetch. Stand
	// up a stub /v1/version that returns a version outside the window
	// so the warning surfaces, proving the fetch fired.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/version" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"1.0.0"}`))
	}))
	defer srv.Close()

	g := &globalFlags{apiURL: srv.URL}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if !strings.Contains(buf.String(), "two-minor support window") {
		t.Errorf("expected fetch + warning, got %q", buf.String())
	}

	// Side effect — the just-fetched version should now be cached.
	entry, ok := lookupCloudVersion("", srv.URL)
	if !ok || entry.Version != "1.0.0" {
		t.Errorf("expected cache write after fetch; got entry=%+v ok=%v", entry, ok)
	}
}

func TestWarnSkipsWhenEndpointAbsent(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	// Cloud returns 404 — older deployment, predates /v1/version.
	// resolveCloudVersionString collapses to "", warn must skip.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	g := &globalFlags{apiURL: srv.URL}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("404 cloud should silence the warning; got %q", buf.String())
	}
}

// withTempGithubURL swaps the package-level githubLatestReleaseURL for
// the duration of one test (the httptest.Server lifetime). Mirrors the
// withTempCLIVersion / withTempConfigDir pattern.
func withTempGithubURL(t *testing.T, u string) {
	t.Helper()
	orig := githubLatestReleaseURL
	t.Cleanup(func() { githubLatestReleaseURL = orig })
	githubLatestReleaseURL = u
}

// ── self-update check (warnIfSelfUpdateAvailable) ──────────────────

func TestSelfUpdateSuppressedUnderQuiet(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.0.0")
	_ = upsertCLIRelease(CLIRelease{TagName: "v2.0.0", HTMLURL: "https://example/release/v2.0.0"})

	g := &globalFlags{quiet: true}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("--quiet should suppress all output; got %q", buf.String())
	}
}

func TestSelfUpdateSkippedWhenCLIUnstamped(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "dev")
	_ = upsertCLIRelease(CLIRelease{TagName: "v2.0.0"})

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("unstamped CLI should skip; got %q", buf.String())
	}
}

func TestSelfUpdateSilentOnSameMinor(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")
	// Patch drift is silent — spec says "warn when behind by ≥1 minor".
	_ = upsertCLIRelease(CLIRelease{TagName: "v1.5.7"})

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("same-minor patch drift should not warn; got %q", buf.String())
	}
}

func TestSelfUpdateSilentWhenAhead(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "2.0.0")
	// User is on a pre-release ahead of latest tag — never warn.
	_ = upsertCLIRelease(CLIRelease{TagName: "v1.5.0"})

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("running ahead of latest release should not warn; got %q", buf.String())
	}
}

func TestSelfUpdateEmittedOnMinorBehind(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")
	_ = upsertCLIRelease(CLIRelease{
		TagName: "v1.7.0",
		HTMLURL: "https://github.com/bas-in/basin-cli/releases/tag/v1.7.0",
	})

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	out := buf.String()
	if !strings.Contains(out, "newer release available") {
		t.Errorf("expected upgrade prose, got %q", out)
	}
	if !strings.Contains(out, "1.7.0") || !strings.Contains(out, "1.5.0") {
		t.Errorf("warning should name both versions; got %q", out)
	}
	if !strings.Contains(out, "github.com/bas-in/basin-cli/releases/tag/v1.7.0") {
		t.Errorf("warning should include the release URL; got %q", out)
	}
}

func TestSelfUpdateFetchesWhenCacheMiss(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.0.0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/releases/latest") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tag_name":"v2.0.0","html_url":"https://example/v2"}`))
	}))
	defer srv.Close()
	withTempGithubURL(t, srv.URL+"/repos/bas-in/basin-cli/releases/latest")

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if !strings.Contains(buf.String(), "newer release available") {
		t.Errorf("expected fetch + warning, got %q", buf.String())
	}
	entry, ok := lookupCLIRelease()
	if !ok || entry.TagName != "v2.0.0" {
		t.Errorf("expected cache write after fetch; got %+v ok=%v", entry, ok)
	}
}

func TestSelfUpdateSilentOn404(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.0.0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()
	withTempGithubURL(t, srv.URL+"/repos/bas-in/basin-cli/releases/latest")

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("404 (no releases yet) should silence the warning; got %q", buf.String())
	}
}

func TestSelfUpdateRefetchesWhenCacheStale(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.0.0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tag_name":"v1.0.5","html_url":"https://example/v1.0.5"}`))
	}))
	defer srv.Close()
	withTempGithubURL(t, srv.URL+"/repos/bas-in/basin-cli/releases/latest")

	// Seed a stale entry pointing at a hypothetical v3.0.0; resolver
	// should detect staleness and refetch into v1.0.5, which is same
	// minor → silent.
	cf, err := readConfigFile()
	if err != nil || cf == nil {
		cf = &configFile{}
	}
	cf.CLIRelease = &cachedCLIRelease{
		TagName:       "v3.0.0",
		LastCheckedAt: time.Now().Add(-(CLIReleaseTTL + time.Hour)),
	}
	if err := writeConfigFile(cf); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	g := &globalFlags{}
	var buf bytes.Buffer
	warnIfSelfUpdateAvailableTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("stale->refetch should land on same-minor 1.0.5 (silent); got %q", buf.String())
	}
	entry, ok := lookupCLIRelease()
	if !ok || entry.TagName != "v1.0.5" {
		t.Errorf("expected refreshed cache to be v1.0.5, got %+v", entry)
	}
}

func TestWarnRefetchesWhenCacheStale(t *testing.T) {
	withTempConfigDir(t)
	withTempCLIVersion(t, "1.5.0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"1.4.0"}`))
	}))
	defer srv.Close()

	// Seed a stale entry pointing at the same api_url; resolver should
	// detect the staleness and refetch from `srv`, then warn-or-not
	// based on the freshly fetched (in-window) value.
	cf, err := readConfigFile()
	if err != nil || cf == nil {
		cf = &configFile{}
	}
	cf.CloudVersions = map[string]cachedCloudVersion{
		"": {
			Version:       "1.0.0",
			APIURL:        srv.URL,
			LastCheckedAt: time.Now().Add(-(CloudVersionTTL + time.Hour)),
		},
	}
	if err := writeConfigFile(cf); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	g := &globalFlags{apiURL: srv.URL}
	var buf bytes.Buffer
	warnIfVersionOutOfWindowTo(g, &buf)
	if buf.Len() != 0 {
		t.Errorf("stale->refetch should land on in-window 1.4.0; got %q", buf.String())
	}

	// Confirm the entry was refreshed.
	entry, ok := lookupCloudVersion("", srv.URL)
	if !ok || entry.Version != "1.4.0" {
		t.Errorf("expected refreshed cache to be 1.4.0, got %+v", entry)
	}
}
