# Contributing to basin-cli

Thanks for considering a contribution. `basin` is a small stdlib-only Go
binary; the bar for landing changes is high but the surface is tiny, so
most patches read in a few minutes.

## Filing issues

Use the GitHub issue templates under `.github/ISSUE_TEMPLATE/` when they
exist — bug reports, feature requests, and CLI-↔-cloud compatibility
problems each have their own. If none fit, open a blank issue and
include:

- `basin --version` output (version + build date + os/arch)
- the exact command that failed + the rendered error
- the cloud version (`curl https://api.basin.run/v1/version`) when the
  bug looks like an API-shape mismatch

## Pull request review

- One topic per PR. Small + focused beats sprawling.
- Cover behaviour changes with a test. The existing `*_test.go` files
  are the template: drive `run([]string{…})` against an `httptest.Server`
  rather than shelling out.
- Match the existing style — terse, no narrative comments, no emojis.
- `go build ./... && go test ./...` MUST pass before requesting review.
  CI gates the same matrix (Ubuntu + macOS, Go 1.23).

## No third-party dependencies

This is a hard rule. `go.mod` has no `require` block and `go.sum`
doesn't exist. Patches that would introduce a dependency get bounced
without further review — vendor the few lines you need into a stdlib-
backed helper instead. The whole CLI is small enough that this stays
practical: net/http, encoding/json, flag, text/tabwriter cover the
ground.

## Commit signing

Commits to `main` must be signed (GPG or SSH; either is fine — see
GitHub's `git config commit.gpgsign true` docs). The release pipeline
also signs tag artefacts; that's automated, not something contributors
need to set up.

## Support window

The CLI follows a two-minor support window against Basin Cloud: a CLI
on minor `N` is supported against cloud `N-1`, `N`, or `N+1`. Anything
outside the window emits a stderr warning but never blocks. Breaking
`/v1/*` changes on the cloud side require a one-minor `Sunset:` header
deprecation. Patches that would silently break older CLIs get bounced
back asking for a versioned shim.
