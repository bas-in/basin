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
