// config_test — round-trip the on-disk config + verify XDG override
// + confirm the perms guard rejects world-readable files.
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// withTempConfigDir points $XDG_CONFIG_HOME at a fresh temp dir so
// each test has an isolated config state. Returns a cleanup that
// restores the previous env.
func withTempConfigDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	prev, hadPrev := os.LookupEnv("XDG_CONFIG_HOME")
	os.Setenv("XDG_CONFIG_HOME", dir)
	t.Cleanup(func() {
		if hadPrev {
			os.Setenv("XDG_CONFIG_HOME", prev)
		} else {
			os.Unsetenv("XDG_CONFIG_HOME")
		}
	})
	return dir
}

func TestConfigPathHonoursXDG(t *testing.T) {
	dir := withTempConfigDir(t)
	got, err := configPath()
	if err != nil {
		t.Fatalf("configPath: %v", err)
	}
	want := filepath.Join(dir, "basin", "config.json")
	if got != want {
		t.Errorf("configPath: got %q, want %q", got, want)
	}
}

func TestReadConfigFileMissing(t *testing.T) {
	withTempConfigDir(t)
	cf, err := readConfigFile()
	if err != nil {
		t.Fatalf("readConfigFile: %v", err)
	}
	if cf != nil {
		t.Errorf("expected nil for missing file, got %+v", cf)
	}
}

func TestUpsertAndReadConfig(t *testing.T) {
	withTempConfigDir(t)
	if _, _, err := upsertToken("", "bso_org_default"); err != nil {
		t.Fatalf("upsert default: %v", err)
	}
	if _, _, err := upsertToken("acme", "bso_org_acme"); err != nil {
		t.Fatalf("upsert acme: %v", err)
	}
	cf, err := readConfigFile()
	if err != nil {
		t.Fatalf("readConfigFile: %v", err)
	}
	if cf == nil {
		t.Fatal("expected config, got nil")
	}
	if cf.DefaultToken != "bso_org_default" {
		t.Errorf("DefaultToken: got %q", cf.DefaultToken)
	}
	if cf.Tokens["acme"] != "bso_org_acme" {
		t.Errorf("Tokens.acme: got %q", cf.Tokens["acme"])
	}

	// Resolution ladder spot-check.
	g := &globalFlags{}
	if got := resolveToken(g, cf); got != "bso_org_default" {
		t.Errorf("resolveToken default: got %q", got)
	}
	g.orgSlug = "acme"
	if got := resolveToken(g, cf); got != "bso_org_acme" {
		t.Errorf("resolveToken acme: got %q", got)
	}
	g.token = "bso_org_flag"
	if got := resolveToken(g, cf); got != "bso_org_flag" {
		t.Errorf("resolveToken --token override: got %q", got)
	}
}

func TestRemoveTokenIdempotent(t *testing.T) {
	withTempConfigDir(t)
	if err := removeToken(""); err != nil {
		t.Fatalf("removeToken on missing: %v", err)
	}
	_, _, _ = upsertToken("", "bso_org_default")
	_, _, _ = upsertToken("acme", "bso_org_acme")
	if err := removeToken(""); err != nil {
		t.Fatalf("removeToken default: %v", err)
	}
	cf, _ := readConfigFile()
	if cf.DefaultToken != "" {
		t.Errorf("default not cleared: %+v", cf)
	}
	if cf.Tokens["acme"] != "bso_org_acme" {
		t.Errorf("acme should still exist: %+v", cf)
	}
	if err := removeToken("*"); err != nil {
		t.Fatalf("removeToken all: %v", err)
	}
	cf, _ = readConfigFile()
	if len(cf.Tokens) != 0 {
		t.Errorf("Tokens should be empty: %+v", cf)
	}
}

func TestPermsGuardRejectsWorldReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("perms check skipped on windows")
	}
	dir := withTempConfigDir(t)
	cfgDir := filepath.Join(dir, "basin")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	body, _ := json.Marshal(&configFile{DefaultToken: "leaked"})
	p := filepath.Join(cfgDir, "config.json")
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := readConfigFile(); err == nil {
		t.Fatalf("expected error on world-readable config, got nil")
	}
}

func TestWritePermissionsAreRestrictive(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("perms check skipped on windows")
	}
	withTempConfigDir(t)
	if _, _, err := upsertToken("", "bso_org_x"); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	p, _ := configPath()
	st, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("config perms: got %o, want 0600", st.Mode().Perm())
	}
}

func TestUpsertCloudVersionRoundtrip(t *testing.T) {
	withTempConfigDir(t)

	cv := CloudVersion{Version: "1.5.0", Commit: "abc1234", Go: "go1.23.4"}
	if err := upsertCloudVersion("", cv, "https://api.basin.run"); err != nil {
		t.Fatalf("upsertCloudVersion: %v", err)
	}

	got, ok := lookupCloudVersion("", "https://api.basin.run")
	if !ok {
		t.Fatal("lookupCloudVersion returned false; expected hit")
	}
	if got.Version != "1.5.0" || got.Commit != "abc1234" || got.Go != "go1.23.4" {
		t.Errorf("roundtrip mismatch: %+v", got)
	}
	if got.APIURL != "https://api.basin.run" {
		t.Errorf("APIURL = %q, want https://api.basin.run", got.APIURL)
	}
	if got.LastCheckedAt.IsZero() {
		t.Error("LastCheckedAt should be stamped on upsert")
	}
}

func TestLookupCloudVersionPerOrgIsolation(t *testing.T) {
	withTempConfigDir(t)

	if err := upsertCloudVersion("acme", CloudVersion{Version: "1.5.0"}, "https://api.basin.run"); err != nil {
		t.Fatalf("upsert acme: %v", err)
	}
	if err := upsertCloudVersion("other", CloudVersion{Version: "0.9.0"}, "https://api.basin.run"); err != nil {
		t.Fatalf("upsert other: %v", err)
	}

	// Lookup-by-slug returns the right entry.
	got, ok := lookupCloudVersion("acme", "https://api.basin.run")
	if !ok || got.Version != "1.5.0" {
		t.Errorf("acme lookup: %+v ok=%v, want 1.5.0", got, ok)
	}
	got, ok = lookupCloudVersion("other", "https://api.basin.run")
	if !ok || got.Version != "0.9.0" {
		t.Errorf("other lookup: %+v ok=%v, want 0.9.0", got, ok)
	}

	// Default-slot is empty (no upsert with empty slug yet).
	if _, ok := lookupCloudVersion("", "https://api.basin.run"); ok {
		t.Error("default-slot lookup hit; expected miss")
	}
}

func TestLookupCloudVersionAPIURLMismatch(t *testing.T) {
	withTempConfigDir(t)

	if err := upsertCloudVersion("", CloudVersion{Version: "1.5.0"}, "https://api.basin.run"); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Same slug, different api_url — cache is stale, must miss.
	if _, ok := lookupCloudVersion("", "https://api.dev.basin.run"); ok {
		t.Error("lookup hit despite api_url mismatch; expected miss")
	}
}

func TestCloudVersionStale(t *testing.T) {
	// Zero LastCheckedAt is always stale (treated as a corrupt entry).
	if !cloudVersionStale(cachedCloudVersion{}) {
		t.Error("zero entry should be stale")
	}

	// Fresh entry — not stale.
	fresh := cachedCloudVersion{LastCheckedAt: time.Now().UTC()}
	if cloudVersionStale(fresh) {
		t.Error("fresh entry should not be stale")
	}

	// Older than TTL — stale.
	old := cachedCloudVersion{LastCheckedAt: time.Now().UTC().Add(-(CloudVersionTTL + time.Minute))}
	if !cloudVersionStale(old) {
		t.Error("aged entry past TTL should be stale")
	}
}
