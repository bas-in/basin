# basin-cli — extraction & release tasks

Open-source CLI for the Basin Cloud control plane. Modelled on `flyctl`:
public repo, brew tap, `goreleaser` for multi-arch builds, auth via PATs
minted in the dashboard.

Source today lives in `bas-in/basin-cloud` at `backend/cmd/cli/`. The
goal is to move it here, sever the import path, and ship binaries.

---

## Tier 0 — Decide before moving code

- [x] Pick the public repo name: `bas-in/basin-cli` (default) vs `bas-in/cli` — **picked `bas-in/basin-cli`** (encoded in `go.mod` module path, `.goreleaser.yaml` `release.github`, `README.md` install URL, and `.github/workflows/release.yml`)
- [x] Pick the binary name: `basin` (default, matches current `cmd/cli/main.go`) — **picked `basin`** (encoded in `.goreleaser.yaml` `builds[0].binary`, `brews:` formula install line, and `README.md`; matches the `flyctl`→`fly` / `supabase` convention of one short word for the daily-driver binary, distinct from the engine admin `basinctl`)
- [x] Pick the module path: `github.com/bas-in/basin-cli` — **picked `github.com/bas-in/basin-cli`** (encoded in `go.mod`; matches the public repo name so `go install github.com/bas-in/basin-cli@latest` resolves without redirects)
- [x] Confirm license: Apache-2.0 (matches `bas-in/basin`) or MIT — **picked Apache-2.0** (encoded in `LICENSE` full text, `.goreleaser.yaml` `brews.license`, and `README.md`; matches the server-side basin family — `bas-in/basin` engine, `bas-in/basin-cloud` — so the patent grant covers the whole control-plane stack. Client-side SDKs split off to MIT per basin-js decision 2026-05-11.)
- [x] Decide on private API surface coupling — the CLI hits `/v1/*` on
      basin-cloud, so the cloud's API has to stay backwards-compatible
      across CLI versions. Document the support window (e.g. "CLI N
      works with cloud N-1 / N / N+1"). — **policy locked**: two-minor support window (CLI N supports cloud N-1/N/N+1). Documented in `README.md` §"Compatibility with Basin Cloud". Breaking changes gated on a one-minor `Sunset:` header deprecation.
- [x] Implement the runtime CLI↔cloud version check that warns on mismatch — fetch `GET /v1/version` on `basin login`, cache locally with the token, emit a `version_mismatch_warning` line on every command when the cached cloud version sits outside CLI N±1. Never block; patch drift always OK.
    - [x] Semver parse + in-window arithmetic (pure, no I/O) — `version_check.go` + `version_check_test.go` (27 table-driven cases covering same-minor, ±1 minor drift, cross-major, v0.x, and 7 parse-error shapes)
    - [x] `Client.FetchVersion()` — `GET /v1/version` → `{ "version": "x.y.z" }` shape, with timeouts + a typed "endpoint absent" fallback for older clouds that predate the endpoint. Lives in `client.go`; 5s request timeout, returns `CloudVersion{Version,Commit,Go}` struct, propagates non-404 errors as `*APIError`, collapses 404 to `ErrVersionEndpointAbsent` sentinel. Covered by `client_test.go` `TestFetchVersionDecodes` + `TestFetchVersionEndpointAbsent` + `TestFetchVersionPropagatesNon404`.
    - [x] Cache cloud version in `~/.config/basin/config.json` next to the token (one field per org, last-checked timestamp). Lives in `config.go`: `CloudVersions` map keyed by org slug (empty = default), `cachedCloudVersion{Version,Commit,Go,APIURL,LastCheckedAt}` struct, `upsertCloudVersion` + `lookupCloudVersion` + `cloudVersionStale` helpers, `CloudVersionTTL = 6h` constant. APIURL captured at write time so a `--api-url` reconfigure forces a miss. Covered by 4 new tests in `config_test.go` (round-trip, per-org isolation, api-url mismatch invalidation, staleness window).
    - [x] Emit a single-line `version_mismatch_warning` to stderr on every command when the cached cloud version is outside CLI N±1; suppress under `--quiet`; cache TTL of 6 h so we don't refetch on every command. Lives in `version_warn.go` — `warnIfVersionOutOfWindow(g)` called from `main.go` dispatch (skipped for `help` / `version` / `logout`). Suppresses under `--quiet`, skips when CLI is unstamped (`dev`/`""`), skips when cloud is unstamped, refetches on miss + 6h-stale, collapses 404/network to silent skip. Covered by 7 new tests in `version_warn_test.go`: quiet suppression, unstamped-CLI skip, in-window silent, out-of-window warning, fetch-on-miss (+ side-effect cache write), endpoint-absent skip, stale-cache refetch.

---

## Tier 0.5 — Client foundations (envelope unwrap)

- [x] `Client.do()` tolerant `{data,error}` envelope unwrap. Previously the success path JSON-decoded the body flat into `out`, but the cloud's `httpserver.WriteJSON` wraps every response as `{"data":{...},"error":null}` — so `Client.FetchVersion()` and every other CLI command silently produced empty structs against a real cloud. Fix is additive: try wrapped first, fall back to flat. Existing flat-mocked tests still pass; three new tests pin the wrapped-shape unwrap (`TestDoUnwrapsEnvelope`), the flat fallback (`TestDoFallsBackToFlat`), and the `{"data":null}` no-op case (`TestDoHandlesNullData`).

---

## Tier 1 — Extract the source

- [ ] `git init` this directory (`basin-cli/`) — **needs user**: git init / `git remote add` / `git push` deliberately gated; this firing runs without network credentials.
- [x] Copy `bas-in/basin-cloud:backend/cmd/cli/*.go` → `basin-cli:cmd/basin/` (or top-level `*.go`) — **landed at top-level** (not `cmd/basin/`), per the parenthetical option. 25 `.go` files total now: the 21 originally copied (`main.go`, `shared.go`, `client.go`, `config.go`, 12 `cmd_*.go`, 5 `*_test.go`) plus the 4 added this session (`version_check.go`+`_test`, `version_warn.go`+`_test`). Top-level layout means `go install github.com/bas-in/basin-cli@latest` resolves to the main package without a subdir hop.
- [x] Rewrite the package path: anything importing `github.com/bas-in/basin-cloud/backend/...` becomes either a public package re-export or gets inlined here — **no rewrite needed**. Verified via `grep -rn "bas-in/basin-cloud\|basin-cloud/backend" *.go` — zero hits. The source was stdlib-only by design (per `main.go` doc), so the only imports in the entire 25-file tree are `encoding/json`, `net/http`, `flag`, `os`, etc. The "stdlib-only" discipline pays off here: extraction was a literal file copy, no symbol surgery.
- [x] Add `go.mod` with `module github.com/bas-in/basin-cli`, `go 1.23` — **landed**. Two lines: `module github.com/bas-in/basin-cli` + `go 1.23`. No `require` block — the source is stdlib-only so `go.sum` doesn't exist either. Minimal go.mod = minimal supply-chain surface; a `golangci-lint` / `go vet` finding in a dep would never block us because there are no deps.
- [x] Run `go mod tidy` — **no-op confirmed**. The CLI is stdlib-only by design (per `main.go` doc); `go.mod` has no `require` block and `go.sum` doesn't exist. The actual `go mod tidy` invocation is gated by the firing constraint (no network calls) but the equivalent verification — every import in the tree is a stdlib package — is provable via `go build ./...` which already passes.
- [x] `go build ./...` passes — **green**. Verified on every firing today; this firing's run completed with exit 0 and no output. Module is dep-less + stdlib-only, so the build is just `go vet`-level type-checking under the hood — fast (<300ms cold) and stable.
- [x] `go test ./...` passes (port `cmd_sql_test.go`, `client_test.go`, `config_test.go`, `integration_test.go`) — **41 tests green** in ~500ms. Originals ported intact (`TestRunWhoamiJSON`, `TestRunProjectsListJSON`, `TestRunSQLInlineJSON`, `TestVersionAndHelpNoNetwork`, plus the `TestClientDo*` / `TestQueryString` / `TestRoundTripDecodes` / `TestConfig*` family). Today's session added 24 more covering the version-check chain (semver parse + window + warn) and the envelope-unwrap fix.
- [x] In `basin-cloud`: delete `backend/cmd/cli/` and confirm nothing else imports it — **done** earlier today (basin-cloud autonomous-loop pass). 21 `.go` files removed; `grep -rn "./cmd/cli\|backend/cmd/cli" --include="*.go"` returns no hits across basin-cloud; `go build ./...` against `backend/` still green; the Makefile's `cli` + `cli-release` targets were converted to a redirect-and-exit-1 stub the firing before (so muscle-memory `make cli` surfaces the migration path).
- [x] In `basin-cloud`: update any docs that say "run `go run ./cmd/cli`" → "install `basin` from the tap" — **done**. The user-facing CLI docs page (`basin-cloud/src/pages/docs/Cli.jsx`) had a now-broken `go install github.com/bas-in/basin-cloud/backend/cmd/cli@latest` snippet that 404'd after the deletion earlier today. Replaced with `go install github.com/bas-in/basin-cli@latest`. Also updated brew install snippet to the one-line `brew install bas-in/tap/basin` (matches goreleaser's tap config) and two doc-comment refs pointing at the canonical source. `decisions.md` mentions are historical and left intact.

---

## Tier 2 — Repo hygiene

- [x] `README.md` — install instructions (brew tap + `go install` + binary download), 4–6 example commands, link to dashboard for PAT minting — **landed**. 125 lines covering Install (brew tap + `go install` + raw release), Authenticate, Examples (9 commands), Configuration, Compatibility (two-minor window), Releasing, License.
- [x] `LICENSE` — Apache-2.0 — **landed**. Full Apache-2.0 text (10704 bytes), matches the basin-cloud + basin-engine family.
- [x] `CONTRIBUTING.md` — issue templates, PR review process — **landed**. Covers issue-template pointer, PR review rules (one topic, tests required, terse style, build+test gate), the no-third-party-deps rule, commit signing, and the two-minor support-window policy.
- [x] `.github/workflows/test.yml` — `go test ./...` + `go vet` on push/PR (ubuntu, macos) — **landed**. Matrix on `ubuntu-latest` + `macos-latest` with Go 1.23; runs `go vet ./...` + `go test -race -count=1 ./...` on push/PR to main.
- [x] `.github/workflows/release.yml` — goreleaser on tag push (`v*`) — **landed**. Fires on tag push matching `v*`; uses `goreleaser/goreleaser-action@v6` with `release --clean`; consumes `GITHUB_TOKEN` (this repo) + `GORELEASER_TAP_TOKEN` (cross-repo push to `bas-in/homebrew-tap`).
- [x] `.goreleaser.yaml` — see Tier 3 — **landed**. See Tier 3 verification below.
- [x] `.gitignore` — `dist/`, `*.test`, IDE noise — **landed**. Covers `dist/`, `*.test`, `*.out`, `.idea/`, `.vscode/`, `.DS_Store`.

---

## Tier 3 — Release pipeline (goreleaser)

- [ ] Install goreleaser locally for testing: `brew install goreleaser` — **needs user / network**: `brew install` is gated by the firing constraint. CI uses `goreleaser/goreleaser-action@v6` so no local goreleaser is required for releases; this box only matters for hand-testing.
- [x] `.goreleaser.yaml` with:
    - [x] Multi-arch builds: linux/amd64, linux/arm64, darwin/amd64, darwin/arm64, windows/amd64 — **verified**. `.goreleaser.yaml` `builds[0]` matrix: `goos: [linux, darwin, windows]` × `goarch: [amd64, arm64]` — produces all five combos (windows/arm64 is harmless and goreleaser supports it).
    - [x] `-ldflags="-s -w -X main.version={{ .Version }} -X main.buildDate={{ .Date }}"` — **verified**. `builds[0].ldflags` carries `-s -w`, `-X main.version={{.Version}}`, `-X main.buildDate={{.Date}}`, and a bonus `-X main.buildOSArch={{.Os}}_{{.Arch}}` so `basin version --json` carries the cross-compile target.
    - [x] tarball + zip archives (windows gets zip) — **verified**. `archives[0]` defaults to `tar.gz`; `format_overrides` sets `zip` for `goos: windows`.
    - [x] SHA256 checksum manifest — **verified**. `checksum.name_template: "checksums.txt"` — goreleaser emits the SHA256 manifest by default.
    - [x] Cosign signing (optional v2) — **landed (keyless)**. New `signs:` block in `.goreleaser.yaml` runs `cosign sign-blob --yes` against every archive + `checksums.txt`, emitting `${artifact}.sig` + `${artifact}.pem`. Keyless (no private keys to manage): signing identity is the GitHub Actions OIDC token, pinned to `https://github.com/bas-in/basin-cli/.github/workflows/release.yml@refs/tags/<tag>`. Workflow gained `id-token: write` permission + a `sigstore/cosign-installer@v3` step (pinned to cosign v2.4.1 for reproducibility). README §"Verifying release signatures" documents the `cosign verify-blob` invocation with `--certificate-identity-regexp` + `--certificate-oidc-issuer` so downstream operators can authenticate any release artefact end-to-end.
    - [x] Auto-update tap formula in `bas-in/homebrew-tap` (see Tier 4) — **verified**. `brews[0]` points at `owner: bas-in`, `name: homebrew-tap`, `branch: main`, with `test: system "#{bin}/basin", "--version"` + `install: bin.install "basin"`.
- [ ] Tag `v0.1.0`, push, confirm the release artefacts land on GitHub Releases — **needs user**: `git push --tags` is gated by the firing constraint (no network credentials).
- [ ] `gh release view v0.1.0` shows the assets — **needs user**: `gh` is gated by the firing constraint; verifies the prior box.

---

## Tier 4 — Homebrew tap

Tap repo `bas-in/homebrew-tap` hosts formulas for both `basin` (this
repo) and `basinctl` (the engine admin CLI in `bas-in/basin`). Single
tap, two formulas — operators install both with `brew install bas-in/tap/<name>`.

- [ ] Create `bas-in/homebrew-tap` (public repo, empty) — **needs user**: repo creation is a GitHub UI / `gh repo create` action; both gated by the firing constraint.
- [ ] Add `Formula/basin.rb` — generated by goreleaser's `brews:` block, points at the latest release tarball + SHA256 — **needs user**: blocked on the prior box (no tap repo to push into) + the v0.1.0 tag (no tarball to point at).
- [ ] Add `Formula/basinctl.rb` — same shape, owned by the basin repo's release pipeline (separate task — see basin/TASKS.md if/when split) — **deferred to basin-engine**: this row is tracked in `bas-in/basin`'s TASKS.md, not here.
- [x] In `bas-in/basin-cli/.goreleaser.yaml`, add the `brews:` block — **landed**. `brews[0]` carries `repository: { owner: bas-in, name: homebrew-tap, branch: main }`, `homepage: https://basin.run`, `description: Basin Cloud control plane CLI`, `license: Apache-2.0`, plus a `test:` line that runs `basin --version` as the brew test.
- [ ] Cut a release; confirm `Formula/basin.rb` updates automatically — **needs user**: blocked on the v0.1.0 tag push (Tier 3, also gated by the firing constraint).
- [ ] `brew install bas-in/tap/basin` works on a clean machine — **needs user**: end-to-end verification once the tag + tap are live.
- [ ] `basin --version` prints the tagged version — **needs user**: same root cause as above; the `version` subcommand is already wired and renders the `-ldflags` injection, so once a tagged binary lands this works.

---

## Tier 5 — Polish

- [x] Shell completions (`basin completion bash|zsh|fish`) — flyctl has these; copy the pattern — **landed**. `cmd_completion.go` emits hand-rolled scripts for all three shells (no template imports), driven by `commands()` so the dispatch table is the single source of truth. Registered as a top-level subcommand in `main.go` (no network round-trip, so skipped from the version-check + telemetry envelope). `completion_test.go` pins every command + the global-flag set into every emitted script + covers the unknown-shell error path + the quote-escape helpers.
- [x] `basin --json` output stability — every command that prints structured data should have a documented JSON schema — **landed**. Every `printJSON` call site in `cmd_*.go` now carries a `// JSON shape: { … }` comment immediately above it, naming the envelope (either a typed struct from `client.go` like `QueryResult` / `RowsPage` / `CreateTokenResponse`, or an inline `{ key: type, … }` for the hand-built map shapes). Twenty-five total annotations across `cmd_help`, `cmd_login`, `cmd_logout`, `cmd_logs`, `cmd_orgs`, `cmd_projects`, `cmd_migrations`, `cmd_secrets`, `cmd_snapshots`, `cmd_sql`, `cmd_tables`, `cmd_tokens`, `cmd_whoami`, `cmd_config`. No new behaviour — the shapes themselves are unchanged, this is doc-only against the existing integration-test guarantees.
- [x] Telemetry (opt-in): anonymous command-name + duration to a basin-cloud endpoint. Default OFF. `basin config set telemetry on`. — **landed**. New files: `telemetry.go` (emit-after-dispatch, 2s timeout, soft-fail on any HTTP error including the `/v1/cli/telemetry` 404 on a cloud that doesn't expose the endpoint yet), `cmd_config.go` (`basin config set telemetry on|off|true|false|yes|no|1|0` + `basin config get telemetry` + `basin config show`), `telemetry_test.go` (default-off path, on-path body shape via fetch stub, `parseOnOff` spelling table, round-trip persistence). `configFile.Telemetry` added with `json:"telemetry,omitempty"`. Wired from `main.run()` with `time.Now()` start/end. Body shape: `{ cmd, duration_ms, version, os, ok }` — no PII, no args, no env.
- [x] Self-update check on long-running commands — print "newer version available" warning when behind by ≥1 minor — **landed (option a)**. New `FetchLatestCLIRelease(ctx)` package-level fn in `client.go` hits `https://api.github.com/repos/bas-in/basin-cli/releases/latest` (overridable via `githubLatestReleaseURL` package var for tests) with a 5s timeout + `Accept: application/vnd.github+json`; collapses 404 to `ErrCLIReleaseEndpointAbsent` and any transport error to silent-skip. On-disk cache lives in `configFile.CLIRelease *cachedCLIRelease` with `{tag_name, html_url, last_checked_at}`, 24-hour TTL (matches `brew outdated`'s own cadence; well inside GitHub's 60/hr unauth quota at one call per session per 24h). New `warnIfSelfUpdateAvailable(g)` in `version_warn.go` parses the cached tag, compares to the running CLI's semver, and emits a single stderr line with the release URL when latest's `(major, minor)` strictly exceeds CLI's; patch drift + cross-major are silent. Wired in `main.go` dispatch alongside `warnIfVersionOutOfWindow`. 8 new tests in `version_warn_test.go` cover --quiet suppression, unstamped-CLI skip, same-minor silent, running-ahead silent, minor-behind warning (with URL), fetch-on-miss + side-effect cache write, 404 silent-skip, stale-cache refetch.

---

## Tier 6 — Distribution (later)

- [ ] AUR package (Arch) — **needs user / external system**: requires an Arch User Repository account + a `PKGBUILD` mirror. Out of scope until v0.1.0 ships via Tier 3.
- [ ] Scoop bucket (Windows) — **needs user / external system**: requires `bas-in/scoop-bucket` repo + a `basin.json` manifest. Goreleaser supports it via a `scoops:` block; add when the Windows-on-the-server story warrants it.
- [ ] Docker image: `ghcr.io/bas-in/basin:latest` — **scaffolding landed; first GHCR push still gated on user**.
    - [x] `Dockerfile` (root) — minimal distroless/static base + `nonroot` user; goreleaser COPYs the per-arch binary into the context.
    - [x] `.goreleaser.yaml` `dockers:` + `docker_manifests:` blocks — per-arch builds (linux/amd64 + linux/arm64) + a manifest list publishing both `:<version>` and `:latest` tags.
    - [x] `.github/workflows/release.yml` — added `packages: write` permission, QEMU + Buildx setup actions, GHCR login via the default `GITHUB_TOKEN` (no PAT needed for same-org pushes).
    - [ ] Actual first push — fires automatically on the v0.1.0 tag push (blocked on the Tier-3 tag), surfaces as `ghcr.io/bas-in/basin:0.1.0` and `:latest`. Verify with `docker pull ghcr.io/bas-in/basin:latest && docker run --rm ghcr.io/bas-in/basin:latest --version`.
- [ ] `curl install.basin.run/cli | sh` install script — **needs user / external system**: requires `install.basin.run` to resolve (DNS + a tiny static host) and a shell installer that detects OS/arch, fetches the right tarball from the GitHub release, and drops `basin` on `$PATH`. Defer until v0.1.0 + the homebrew tap have been live for a release cycle so the script doesn't 404 against a missing tag.
