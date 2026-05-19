// config — read/write the on-disk config file.
//
// File format is JSON (single canonical shape). Path resolution
// follows the XDG base-dir spec: $XDG_CONFIG_HOME/basin/config.json
// or, when XDG_CONFIG_HOME is unset, $HOME/.config/basin/config.json.
// On Windows we honour %APPDATA% as a final fallback. The config
// file is created with mode 0600; reads refuse to load a token
// from a world-readable file.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// configFile is the on-disk representation. Tokens is keyed by
// org slug; default_token is the catch-all when --org isn't set.
//
// The JSON envelope is intentionally tiny so a user can hand-edit it
// without re-reading the docs each time.
type configFile struct {
	DefaultToken string            `json:"default_token,omitempty"`
	APIURL       string            `json:"api_url,omitempty"`
	DefaultOrg   string            `json:"default_org,omitempty"`
	Tokens       map[string]string `json:"tokens,omitempty"`
	// CloudVersions caches the most-recent `GET /v1/version` response
	// per org slug, plus the api_url it was fetched against (so a
	// reconfigure invalidates the cache automatically). Keyed by org
	// slug; empty-string key holds the default-org cache. Refreshed on
	// `basin login` and lazily on every command when the cached entry
	// is older than CloudVersionTTL.
	CloudVersions map[string]cachedCloudVersion `json:"cloud_versions,omitempty"`
	// Telemetry, when true, lets the CLI POST minimal anonymous usage
	// pings (cmd / duration_ms / version / os / ok) to
	// `{api_url}/v1/cli/telemetry`. Default false — opt-in only. Toggle
	// via `basin config set telemetry on|off`.
	Telemetry bool `json:"telemetry,omitempty"`
	// CLIRelease caches the most-recent
	// `GET api.github.com/repos/bas-in/basin-cli/releases/latest` response.
	// One slot total (the release is global, not per-org or per-api_url).
	// Lazily refreshed when older than CLIReleaseTTL; consumed by the
	// self-update warning emitted alongside the cloud-version warning.
	CLIRelease *cachedCLIRelease `json:"cli_release,omitempty"`
}

// cachedCloudVersion is the on-disk shape of one cached `/v1/version`
// response. We persist the api_url + last-checked timestamp alongside
// the version triple so callers can detect a stale entry without
// re-fetching every time.
type cachedCloudVersion struct {
	Version       string    `json:"version"`
	Commit        string    `json:"commit,omitempty"`
	Go            string    `json:"go,omitempty"`
	APIURL        string    `json:"api_url"`
	LastCheckedAt time.Time `json:"last_checked_at"`
}

// CloudVersionTTL is the cache window before the emit-warning path
// refetches `/v1/version`. Six hours strikes the balance the policy
// promised (`never block; patch drift always OK`): long enough that
// most commands don't fire the network round-trip, short enough that
// a same-day cloud upgrade flips the warning into accuracy.
const CloudVersionTTL = 6 * time.Hour

// cachedCLIRelease is the on-disk shape of the most-recent GitHub
// Releases API response. Stores just the tag + landing URL plus the
// last-checked timestamp; the diagnostics body / assets list are
// dropped because the warning only needs the version compare + one
// link.
type cachedCLIRelease struct {
	TagName       string    `json:"tag_name"`
	HTMLURL       string    `json:"html_url,omitempty"`
	LastCheckedAt time.Time `json:"last_checked_at"`
}

// CLIReleaseTTL is the cache window before the self-update warning
// refetches GitHub's Releases API. Twenty-four hours: releases happen
// far less often than cloud upgrades, the unauth-quota budget on
// api.github.com is 60/hr per IP, and a one-day lag on "new version
// out" is in line with how `brew outdated` reports its own cache.
const CLIReleaseTTL = 24 * time.Hour

// configDir resolves the on-disk directory holding config.json. It
// honours XDG_CONFIG_HOME (Linux/macOS convention) and falls back to
// $HOME/.config; on Windows we add %APPDATA% as a final fallback.
func configDir() (string, error) {
	if v := os.Getenv("XDG_CONFIG_HOME"); v != "" {
		return filepath.Join(v, "basin"), nil
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return filepath.Join(h, ".config", "basin"), nil
	}
	if runtime.GOOS == "windows" {
		if v := os.Getenv("APPDATA"); v != "" {
			return filepath.Join(v, "basin"), nil
		}
	}
	return "", errors.New("could not determine config dir (set XDG_CONFIG_HOME or HOME)")
}

// configPath is the full file path including the basename.
func configPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.json"), nil
}

// readConfigFile loads the on-disk config. A missing file returns
// (nil, nil) — a fresh install is normal. A world-readable file
// returns an error so we never silently honour a leaked token.
func readConfigFile() (*configFile, error) {
	p, err := configPath()
	if err != nil {
		return nil, err
	}
	st, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	// Refuse a world-readable token file. Skip the perms check on
	// Windows where the unix bits don't carry meaning.
	if runtime.GOOS != "windows" {
		if st.Mode().Perm()&0o077 != 0 {
			return nil, fmt.Errorf("refusing to read %s: file is group- or world-readable (chmod 600)", p)
		}
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var cf configFile
	if err := json.Unmarshal(b, &cf); err != nil {
		return nil, fmt.Errorf("parse %s: %w", p, err)
	}
	return &cf, nil
}

// writeConfigFile atomically writes the config to disk with mode
// 0600. Atomic via write-to-temp + rename so a Ctrl-C between two
// writes can't leave a half-written file.
func writeConfigFile(cf *configFile) error {
	dir, err := configDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	p := filepath.Join(dir, "config.json")
	b, err := json.MarshalIndent(cf, "", "  ")
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// upsertToken stamps a token onto either the per-org map (when
// orgSlug is non-empty) or default_token. Returns the path that
// was written, plus the resulting *configFile (handy for the login
// command's confirmation print).
func upsertToken(orgSlug, token string) (*configFile, string, error) {
	cf, err := readConfigFile()
	if err != nil {
		// A corrupt file is recoverable: log and start fresh. The
		// caller can decide whether to surface the warning.
		cf = &configFile{}
	}
	if cf == nil {
		cf = &configFile{}
	}
	if orgSlug == "" {
		cf.DefaultToken = token
	} else {
		if cf.Tokens == nil {
			cf.Tokens = map[string]string{}
		}
		cf.Tokens[orgSlug] = token
	}
	if err := writeConfigFile(cf); err != nil {
		return nil, "", err
	}
	p, _ := configPath()
	return cf, p, nil
}

// removeToken wipes either the per-org entry or default_token.
// Empty orgSlug clears default_token only; "*" wipes everything.
func removeToken(orgSlug string) error {
	cf, err := readConfigFile()
	if err != nil || cf == nil {
		// Nothing to remove. Treat as success so `basin logout` is
		// idempotent (calling it twice never errors).
		return nil
	}
	switch orgSlug {
	case "":
		cf.DefaultToken = ""
	case "*":
		cf.DefaultToken = ""
		cf.Tokens = nil
	default:
		delete(cf.Tokens, orgSlug)
	}
	return writeConfigFile(cf)
}

// ── cloud-version cache ──────────────────────────────────────────

// upsertCloudVersion records the most-recent `/v1/version` response
// for `orgSlug` (empty = default-org slot). Stamps LastCheckedAt to
// now. APIURL is captured so a later reconfigure (`basin --api-url=…`)
// invalidates the entry on lookup.
//
// Atomic via writeConfigFile's temp-then-rename. Tolerates a missing
// or corrupt on-disk file by starting from a fresh configFile{}.
func upsertCloudVersion(orgSlug string, cv CloudVersion, apiURL string) error {
	cf, err := readConfigFile()
	if err != nil {
		// Corrupt file is recoverable — overwrite with a fresh shape.
		cf = &configFile{}
	}
	if cf == nil {
		cf = &configFile{}
	}
	if cf.CloudVersions == nil {
		cf.CloudVersions = map[string]cachedCloudVersion{}
	}
	cf.CloudVersions[orgSlug] = cachedCloudVersion{
		Version:       cv.Version,
		Commit:        cv.Commit,
		Go:            cv.Go,
		APIURL:        apiURL,
		LastCheckedAt: time.Now().UTC(),
	}
	return writeConfigFile(cf)
}

// lookupCloudVersion reads the cached entry for `orgSlug` (empty =
// default slot). Returns (entry, true) when an entry exists AND its
// APIURL matches the active one (so a reconfigured CLI doesn't show
// a warning derived from a stale cloud's version). A miss / stale
// entry returns (zero, false) and the caller refetches.
func lookupCloudVersion(orgSlug, apiURL string) (cachedCloudVersion, bool) {
	cf, err := readConfigFile()
	if err != nil || cf == nil {
		return cachedCloudVersion{}, false
	}
	entry, ok := cf.CloudVersions[orgSlug]
	if !ok {
		return cachedCloudVersion{}, false
	}
	if entry.APIURL != apiURL {
		// Operator pointed the CLI at a different cloud — the cached
		// version is meaningless. Bail and force a refetch.
		return cachedCloudVersion{}, false
	}
	return entry, true
}

// cloudVersionStale reports whether `entry` is older than
// CloudVersionTTL relative to now. Caller treats stale entries the
// same as a miss — refetch + upsert.
func cloudVersionStale(entry cachedCloudVersion) bool {
	if entry.LastCheckedAt.IsZero() {
		return true
	}
	return time.Since(entry.LastCheckedAt) > CloudVersionTTL
}

// ── cli-release cache ────────────────────────────────────────────

// upsertCLIRelease records the most-recent GitHub Releases API
// response. Stamps LastCheckedAt to now. One slot total — the release
// is global, not per-org. Atomic via writeConfigFile's temp-then-rename.
func upsertCLIRelease(rel CLIRelease) error {
	cf, err := readConfigFile()
	if err != nil {
		cf = &configFile{}
	}
	if cf == nil {
		cf = &configFile{}
	}
	cf.CLIRelease = &cachedCLIRelease{
		TagName:       rel.TagName,
		HTMLURL:       rel.HTMLURL,
		LastCheckedAt: time.Now().UTC(),
	}
	return writeConfigFile(cf)
}

// lookupCLIRelease reads the cached entry. Returns (entry, true) when
// an entry exists; a miss (no cache, file unreadable, no tag) returns
// (zero, false) and the caller refetches.
func lookupCLIRelease() (cachedCLIRelease, bool) {
	cf, err := readConfigFile()
	if err != nil || cf == nil || cf.CLIRelease == nil {
		return cachedCLIRelease{}, false
	}
	if cf.CLIRelease.TagName == "" {
		return cachedCLIRelease{}, false
	}
	return *cf.CLIRelease, true
}

// cliReleaseStale reports whether `entry` is older than CLIReleaseTTL
// relative to now. Caller treats stale entries the same as a miss —
// refetch + upsert.
func cliReleaseStale(entry cachedCLIRelease) bool {
	if entry.LastCheckedAt.IsZero() {
		return true
	}
	return time.Since(entry.LastCheckedAt) > CLIReleaseTTL
}

// ── directory-scoped working project ────────────────────────────────

// workingProject is the in-memory representation of ./basin/config.toml.
// All fields are optional: a freshly-scaffolded directory may have
// only a placeholder until `basin link` writes project_ref.
type workingProject struct {
	ProjectRef        string // project_ref = <slug>
	DefaultBranch     string // default_branch = <name>
	EngineVersionPin  string // engine_version_pin = <semver>
}

// workingProjectFile is the relative path (from the project root)
// where the directory-scoped config lives.
const workingProjectFile = "basin/config.toml"

// loadWorkingProject walks up from cwd looking for basin/config.toml,
// following the same convention as git/cargo/fly. Returns
// (nil, nil) when no config file is found anywhere in the tree.
// Returns a non-nil error only on I/O or parse failures.
func loadWorkingProject(cwd string) (*workingProject, error) {
	dir := cwd
	for {
		p := filepath.Join(dir, workingProjectFile)
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				// Walk one level up.
				parent := filepath.Dir(dir)
				if parent == dir {
					// Filesystem root — not found.
					return nil, nil
				}
				dir = parent
				continue
			}
			return nil, err
		}
		// Found the file — parse it.
		wp, err := parseWorkingProjectTOML(b)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", p, err)
		}
		return wp, nil
	}
}

// saveWorkingProject writes wp to <cwd>/basin/config.toml, creating
// the basin/ directory if needed. The file is written atomically
// (write-to-temp + rename) with mode 0o644.
func saveWorkingProject(cwd string, wp workingProject) error {
	dir := filepath.Join(cwd, "basin")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	p := filepath.Join(dir, "config.toml")
	b := marshalWorkingProjectTOML(wp)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, []byte(b), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// parseWorkingProjectTOML is a hand-rolled key=value TOML parser.
// It handles only the three keys the workingProject struct needs;
// unknown keys are ignored. No nested tables, no arrays — v0.1 needs
// none of that. Lines starting with '#' or blank are skipped.
//
// Grammar accepted (each line independently):
//
//	key = value          # bare value, may be quoted with " or '
//	# comment
//	<blank line>
func parseWorkingProjectTOML(b []byte) (*workingProject, error) {
	var wp workingProject
	lines := strings.Split(string(b), "\n")
	for i, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Strip inline comment: find first unquoted '#'.
		if idx := strings.Index(line, " #"); idx >= 0 {
			line = strings.TrimSpace(line[:idx])
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			return nil, fmt.Errorf("line %d: expected key=value, got %q", i+1, raw)
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		// Strip optional surrounding quotes (" or ').
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		switch key {
		case "project_ref":
			wp.ProjectRef = val
		case "default_branch":
			wp.DefaultBranch = val
		case "engine_version_pin":
			wp.EngineVersionPin = val
		// Unknown keys are silently ignored to stay forward-compatible.
		}
	}
	return &wp, nil
}

// marshalWorkingProjectTOML serialises wp into a key=value TOML
// fragment. Empty fields are omitted so an un-linked directory's
// config.toml stays clean.
func marshalWorkingProjectTOML(wp workingProject) string {
	var sb strings.Builder
	sb.WriteString("# basin project config — managed by the basin CLI\n")
	if wp.ProjectRef != "" {
		sb.WriteString("project_ref = " + wp.ProjectRef + "\n")
	}
	if wp.DefaultBranch != "" {
		sb.WriteString("default_branch = " + wp.DefaultBranch + "\n")
	}
	if wp.EngineVersionPin != "" {
		sb.WriteString("engine_version_pin = " + wp.EngineVersionPin + "\n")
	}
	return sb.String()
}
