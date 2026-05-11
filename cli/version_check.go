// version_check — semver parsing + support-window arithmetic for the
// CLI↔cloud compatibility check.
//
// Policy (per README §"Compatibility with Basin Cloud"): a `basin` CLI
// on minor version N is supported against a Basin Cloud running minor
// N-1, N, or N+1. Patch drift is always supported. Cross-major drift
// is always outside the window.
//
// This file is intentionally pure — no I/O, no network, no os.* calls.
// The fetch + cache + warning-emit pieces live in their own files
// (added in subsequent firings) and consume these helpers.
package main

import (
	"errors"
	"strconv"
	"strings"
)

// semver carries the three numeric components of a "M.m.p" version.
// We deliberately drop pre-release + build metadata — the support
// window cares about minor drift, not RC identifiers.
type semver struct {
	Major, Minor, Patch int
}

// parseSemver accepts "1.2.3", "v1.2.3", "1.2.3-rc.4", "1.2.3+build5",
// and trims surrounding whitespace. Returns an error when fewer than
// three numeric components are present or when any component fails
// to parse as a non-negative integer.
//
// We don't validate against the full semver 2.0 grammar — this is a
// pragmatic parser used to compare cloud + CLI versions, and the
// alternative is dragging in a third-party module which conflicts with
// the stdlib-only design.
func parseSemver(s string) (semver, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	// Strip pre-release / build metadata.
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) < 3 {
		return semver{}, errors.New("version: need at least major.minor.patch")
	}
	out := semver{}
	for idx, dst := range []*int{&out.Major, &out.Minor, &out.Patch} {
		n, err := strconv.Atoi(parts[idx])
		if err != nil {
			return semver{}, errors.New("version: component " + parts[idx] + " is not a non-negative integer")
		}
		if n < 0 {
			return semver{}, errors.New("version: components must be non-negative")
		}
		*dst = n
	}
	return out, nil
}

// String renders the canonical "M.m.p" form. Convenience for log lines.
func (v semver) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// inSupportWindow reports whether the cloud version sits inside the
// CLI's two-minor support window (CLI N supports cloud N-1, N, N+1).
//
// Rules, in order:
//
//  1. Different majors → never compatible (major bumps are breaking).
//  2. abs(cli.Minor - cloud.Minor) > 1 → outside window.
//  3. Otherwise → in window. Patch drift is always fine.
//
// Note both arguments are zero-valued when their source is "unknown".
// Callers should special-case that before invoking: a zero version
// means we couldn't fetch / cache it, which is not the same as
// "version 0.0.0 is incompatible".
func inSupportWindow(cli, cloud semver) bool {
	if cli.Major != cloud.Major {
		return false
	}
	diff := cli.Minor - cloud.Minor
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1
}
