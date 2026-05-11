// version_warn — CLI↔cloud support-window warning emitter.
//
// Wires the pure semver helpers (`version_check.go`), the HTTP fetch
// (`Client.FetchVersion`), and the on-disk cache (`config.go`) into
// a single side-effecting call invoked from main.go's dispatch before
// each subcommand runs.
//
// Behaviour, in order:
//
//  1. Suppress under --quiet (operator opted out of prose entirely).
//  2. Skip when the CLI binary is unstamped (`version == "" || "dev"`)
//     — unstamped builds happen in local `go run` dev work where
//     compatibility nags are noise.
//  3. Look up the cached cloud version for the active org slug; if
//     missing or older than CloudVersionTTL, refetch via
//     `Client.FetchVersion` (5s short timeout) and persist via
//     `upsertCloudVersion`. ErrVersionEndpointAbsent + any transport
//     error → silently skip (older cloud / network blip).
//  4. Parse the cached cloud version; if unstamped on the cloud side
//     (`dev` / empty), skip.
//  5. If `inSupportWindow(cli, cloud) == false`, write ONE line of
//     warning to stderr. Never block; the command continues.

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
)

// warnIfVersionOutOfWindow emits the support-window warning to stderr
// when applicable. Always returns; callers don't propagate errors.
// This is the single side-effect entry point called from main.go.
func warnIfVersionOutOfWindow(g *globalFlags) {
	warnIfVersionOutOfWindowTo(g, os.Stderr)
}

// warnIfVersionOutOfWindowTo is the test-friendly inner; lets tests
// capture the warning into a buffer instead of stderr.
func warnIfVersionOutOfWindowTo(g *globalFlags, w io.Writer) {
	if g == nil || g.quiet {
		return
	}
	if version == "" || version == "dev" {
		return
	}
	cliVer, err := parseSemver(version)
	if err != nil {
		return
	}

	cloudStr := resolveCloudVersionString(g)
	if cloudStr == "" || cloudStr == "dev" {
		return
	}
	cloudVer, err := parseSemver(cloudStr)
	if err != nil {
		return
	}
	if inSupportWindow(cliVer, cloudVer) {
		return
	}
	fmt.Fprintf(w,
		"basin: cli %s is outside the cloud's two-minor support window (cloud %s).\n"+
			"       update via `brew upgrade basin` or `go install github.com/bas-in/basin-cli@latest`.\n",
		cliVer, cloudVer)
}

// warnIfSelfUpdateAvailable emits a "newer basin release available"
// warning to stderr when the latest published GitHub release is at
// least one minor ahead of the running CLI binary. Skipped under the
// same conditions as warnIfVersionOutOfWindow: --quiet, unstamped CLI,
// network errors / 404 (no releases yet), unparseable tag. Cached on
// disk (CLIReleaseTTL) so most commands skip the network round-trip.
func warnIfSelfUpdateAvailable(g *globalFlags) {
	warnIfSelfUpdateAvailableTo(g, os.Stderr)
}

// warnIfSelfUpdateAvailableTo is the test-friendly inner; lets tests
// capture the warning into a buffer instead of stderr.
func warnIfSelfUpdateAvailableTo(g *globalFlags, w io.Writer) {
	if g == nil || g.quiet {
		return
	}
	if version == "" || version == "dev" {
		return
	}
	cliVer, err := parseSemver(version)
	if err != nil {
		return
	}
	tag, htmlURL := resolveLatestCLIRelease()
	if tag == "" {
		return
	}
	latest, err := parseSemver(tag)
	if err != nil {
		return
	}
	// Only warn on minor (or major) drift. Same-minor patch drift is
	// silent — matches the spec ("warn when behind by ≥1 minor").
	if latest.Major < cliVer.Major {
		return
	}
	if latest.Major == cliVer.Major && latest.Minor <= cliVer.Minor {
		return
	}
	if htmlURL == "" {
		fmt.Fprintf(w,
			"basin: newer release available (%s; running %s).\n"+
				"       update via `brew upgrade basin` or `go install github.com/bas-in/basin-cli@latest`.\n",
			latest, cliVer)
		return
	}
	fmt.Fprintf(w,
		"basin: newer release available (%s; running %s). %s\n"+
			"       update via `brew upgrade basin` or `go install github.com/bas-in/basin-cli@latest`.\n",
		latest, cliVer, htmlURL)
}

// resolveLatestCLIRelease returns (tag, html_url) for the most-recent
// published GitHub release, using the on-disk cache when fresh and
// falling back to a single network fetch when missing or stale. All
// errors collapse to ("", "") so the caller skips the warning.
func resolveLatestCLIRelease() (string, string) {
	if entry, ok := lookupCLIRelease(); ok && !cliReleaseStale(entry) {
		return entry.TagName, entry.HTMLURL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rel, err := FetchLatestCLIRelease(ctx)
	if err != nil {
		return "", ""
	}
	_ = upsertCLIRelease(rel)
	return rel.TagName, rel.HTMLURL
}

// resolveCloudVersionString returns the cloud's version string for
// the active org slot, using the on-disk cache when fresh and falling
// back to a single network fetch when missing or stale. All errors
// collapse to "" so the caller skips the warning entirely.
func resolveCloudVersionString(g *globalFlags) string {
	apiURL := g.apiURL
	orgSlug := g.orgSlug

	if entry, ok := lookupCloudVersion(orgSlug, apiURL); ok && !cloudVersionStale(entry) {
		return entry.Version
	}
	client := NewClient(apiURL, g.token)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cv, err := client.FetchVersion(ctx)
	if err != nil {
		return ""
	}
	// Best-effort persist — write failure (full disk, permission denied)
	// doesn't change whether we emit the warning for this command.
	_ = upsertCloudVersion(orgSlug, cv, apiURL)
	return cv.Version
}
