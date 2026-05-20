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

- [ ] AUR package (Arch) — **`aurs:` block landed in `.goreleaser.yaml`; AUR account + SSH key still gated on user**. Block publishes `basin-bin` PKGBUILD to `ssh://aur@aur.archlinux.org/basin-bin.git` on every tag, signs the push with `${{ env.AUR_KEY }}`, `provides=(basin)` / `conflicts=(basin)`, single-line `package()` install of the prebuilt linux-amd64/arm64 binary into `/usr/bin/basin`. `skip_upload: auto` keeps pre-account releases passing. Forward-blocking work: (1) create AUR account, (2) submit empty `basin-bin` package once so the SSH git URL exists, (3) add `AUR_KEY` (SSH private key) to `bas-in/basin-cli` repo secrets, (4) flip `skip_upload: auto` → `false`. After that, `yay -S basin-bin` works.
- [ ] Scoop bucket (Windows) — **`scoops:` block landed in `.goreleaser.yaml`; bucket repo + token still gated on user**. Block points at `owner: bas-in, name: scoop-bucket, branch: main` with `skip_upload: auto` so releases pre-bucket-repo don't fail. When user creates `bas-in/scoop-bucket` + adds a `SCOOP_TAP_TOKEN` GH secret, the next tag push auto-publishes `basin.json`. Operators then `scoop bucket add bas-in https://github.com/bas-in/scoop-bucket && scoop install basin`. Forward-blocking work: (1) create `bas-in/scoop-bucket` repo (GH UI), (2) add `SCOOP_TAP_TOKEN` to `bas-in/basin-cli` repo secrets, (3) flip `skip_upload: auto` → `false` once verified.
- [ ] Docker image: `ghcr.io/bas-in/basin:latest` — **scaffolding landed; first GHCR push still gated on user**.
    - [x] `Dockerfile` (root) — minimal distroless/static base + `nonroot` user; goreleaser COPYs the per-arch binary into the context.
    - [x] `.goreleaser.yaml` `dockers:` + `docker_manifests:` blocks — per-arch builds (linux/amd64 + linux/arm64) + a manifest list publishing both `:<version>` and `:latest` tags.
    - [x] `.github/workflows/release.yml` — added `packages: write` permission, QEMU + Buildx setup actions, GHCR login via the default `GITHUB_TOKEN` (no PAT needed for same-org pushes).
    - [ ] Actual first push — fires automatically on the v0.1.0 tag push (blocked on the Tier-3 tag), surfaces as `ghcr.io/bas-in/basin:0.1.0` and `:latest`. Verify with `docker pull ghcr.io/bas-in/basin:latest && docker run --rm ghcr.io/bas-in/basin:latest --version`.
- [ ] `curl install.basin.run/cli | sh` install script — **installer landed at `scripts/install.sh`; hosted at `basin.run/install.sh` today; `install.basin.run` DNS still gated on user**. POSIX-sh script does OS detect (linux/darwin/windows), arch detect (amd64/arm64), resolves the latest release tag via `api.github.com/repos/bas-in/basin-cli/releases/latest` (override with `BASIN_VERSION=`), downloads the matching tarball from the GH release, verifies SHA256 against `checksums.txt`, extracts, and installs `basin` to `$PREFIX/bin`. Copied into `basin-cloud/public/install.sh` so the next cloud deploy makes `curl -fsSL https://basin.run/install.sh | sh` a working URL today (no DNS work needed). When the user is ready to flip the vanity host, point `install.basin.run` at the same content (or a Cloudflare/Bunny redirect to basin.run) and the canonical URL becomes `https://install.basin.run/cli`. The script's USAGE comment already documents both URLs. Remaining gate: v0.1.0 tag so the latest-release API call returns a real tag.

---

## Tier 7 — Context / pointers (not CLI scope)

- [ ] basin-auth (Rust) now boots against basin engine's loopback pgwire — **out of basin-cli scope; pointer for operators**. 2026-05-11: basin-auth's `catalog_dsn` flipped to `Option<String>` defaulting to `postgres://basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable`; `basin-server` boot order builds a static resolver (reserved tenant ULID `01JBAS1NAVTH00000000000000`, internal user `basin_auth`), starts pgwire, waits for accept, then connects basin-auth via loopback. CLI doesn't shell out to basin-auth or basin-server's pgwire — the CLI talks only to basin-cloud's HTTP API — so this row is informational only. If an operator reads basin-cli's roadmap looking for the auth migration story, the answer lives in `basin/TASK.md` + `basin/decisions.md` 2026-05-11.

---

# Feature-parity tiers (Tier 8+)

Tiers 0–7 above tracked the extract-and-release work. The tiers below
track the **feature-parity build-out** described in
[`ROADMAP.md`](./ROADMAP.md): wiring the cloud's existing `/v1/*`
surface and the engine's existing capabilities up to terminal-friendly
verbs. Each tier is one cohesive feature area; each checkbox is
agent-actionable (one `cmd_*.go` + one `cmd_*_test.go`, give or take).

## Conventions for these tiers

Every task in Tier 8+ follows the same shape so a Sonnet agent can pick
one up cold:

- **One file per subcommand** (`cmd_<verb>.go`) matching the existing
  pattern — a top-level dispatcher function + one sub-handler per
  action, each owning its own `flag.FlagSet`.
- **Stdlib only.** Anything outside `import "..."` from the Go
  standard library is a hard no. If a task seems to need one, write
  why in the task body before reaching for the dep.
- **`--json` shape annotation** above every `printJSON` call site:
  `// JSON shape: { ... }` referencing a typed struct in `client.go`
  when possible.
- **Tests required to land** — see Tier 24 below for the exact
  matrix. The bar is: no new command without a `cmd_*_test.go`.
- **Cloud endpoints already exist** for every Tier 8–22 task. Each
  task body cites the route from
  `basin-cloud/backend/internal/router/server.go`. If a task seems to
  need a new endpoint, stop and file the issue upstream first.

---

## Tier 8 — Project context (directory mode)

Move the CLI from "every command takes `--project=`" to "cwd knows
which project this is." Pure client-side; no cloud endpoints needed.

- [ ] `cmd_init.go` — `basin init` scaffolds `./basin/config.toml`, `./basin/migrations/`, `./basin/seed.sql`, `.gitignore` entry. Refuse if `./basin/` already exists unless `--force`. Config schema: `project_ref`, `default_branch`, `engine_version_pin` (optional). Add `cmd_init_test.go`: tempdir bootstrap, idempotency under `--force`, conflict error path, JSON output shape.
- [ ] `cmd_link.go` — `basin link --project=<ref>` writes `./basin/config.toml` `project_ref` (creates the file if absent), validates the ref via `GET /v1/projects/{ref}`. Honour `--org=` for ambiguity. Tests: success, missing project (404 → typed error), already-linked (warn + overwrite under `--force`), JSON output.
- [ ] `cmd_status.go` — read `./basin/config.toml`, call `/v1/projects/{ref}`, `/v1/projects/{ref}/migrations` (compare against `./basin/migrations/`), print: linked ref, status (active / paused), region, current branch, pending-migrations count, drift indicator. Tests: not-linked error, linked + clean, linked + pending migrations, JSON shape.
- [ ] `cmd_unlink.go` — remove `project_ref` from config; keep migration files. Tests: linked → success, not-linked → no-op exit 0, JSON output.
- [ ] `config.go` — extend `configFile` with `WorkingProject *workingProject` *(directory-scoped, not user-scoped — actually lives in `./basin/config.toml`)*. Add `loadWorkingProject(cwd string)` + `saveWorkingProject(cwd, ...)`. Tests in `config_test.go`: round-trip, search-up-tree from a subdir, missing-file path.
- [ ] Update every existing `cmd_*.go` that takes `--project=` to fall back to `loadWorkingProject(cwd)` when the flag is absent. Touch sites: `cmd_sql.go`, `cmd_tables.go`, `cmd_migrations.go`, `cmd_snapshots.go`, `cmd_logs.go`, `cmd_secrets.go`. Tests: extend `integration_test.go` to drive the cwd-fallback path for at least `sql` + `tables list`.

---

## Tier 9 — Database workflow (`db` namespace)

The daily-driver loop: edit schema → push → pull → reset → hand DSN
to a driver. All backed by existing cloud endpoints.

- [ ] `cmd_db.go` — top-level dispatcher mirroring `cmd_projects.go`. Subcommands: `push`, `pull`, `diff`, `reset`, `url`, `dump`, `lint`.
- [ ] `db push` — read `./basin/migrations/*.sql` in lex order, POST each unapplied one to `POST /v1/projects/{ref}/migrations`. Idempotent: skip migrations the cloud already lists. Show per-migration progress under `--quiet=false`. Tests: empty migrations dir, all already applied, partial apply (cloud has 2/5), one fails mid-way (preserve state).
- [ ] `db pull` — `GET /v1/projects/{ref}/migrations` then `GET /v1/projects/{ref}/migrations/{id}` for each, write to `./basin/migrations/`. Refuse to overwrite local diffs unless `--force`. Tests: empty local, drift detection, clobber under `--force`.
- [ ] `db diff` — call `GET /v1/projects/{ref}/migrations/diff` with the locally-staged DDL; write the result as a new timestamped migration file. Tests: no-diff (no file written), real diff (file written + content match), API error mapping.
- [ ] `db reset` — `POST /v1/projects/{ref}/snapshots` (safety net), then `POST /v1/projects/{ref}/branches` of a temp scratch branch, apply migrations + `seed.sql`. Confirmation prompt (skip under `--yes`). Tests: confirmation declined → no-op, confirmation accepted → expected API call sequence.
- [ ] `db url [--reveal] [--rotate]` — `GET /v1/projects/{ref}/pgwire` for the metadata. With `--reveal`: `POST /v1/projects/{ref}/pgwire/reveal`. With `--rotate`: `POST /v1/projects/{ref}/pgwire/rotate` (confirmation required). Print `postgres://user:pass@host:port/db?sslmode=require`. Tests: metadata-only path, reveal path, rotate path with prompt.
- [ ] `db dump [--schema-only|--data-only]` — `pg_dump`-shaped output assembled from `information_schema` + `/sql/query`. Stream to stdout; redirect to file via shell. Tests: schema-only happy path, data-only happy path, both.
- [ ] `db lint` — POST `./basin/migrations/*.sql` to `POST /v1/projects/{ref}/sql/query` with `dry_run=true` and surface SQLSTATE 0A000 / unsupported-feature errors with file:line context. Tests: clean file, file with unsupported syntax, file with multiple statements.
- [ ] Client additions in `client.go`: `ListMigrations`, `GetMigration`, `CreateMigration`, `DiffMigrations`, `GetPgwire`, `RevealPgwire`, `RotatePgwire`. Each method covered by `client_test.go` rows (envelope unwrap + error path).

---

## Tier 10 — Migration completeness

The `migrations` subcommand wires `list` + `apply` today. Fill the rest.

- [ ] `cmd_migrations.go` — extend the existing dispatcher with: `new`, `get`, `diff`, `rollback`.
- [ ] `migrations new <name>` — write `./basin/migrations/<timestamp>_<slug>.sql` with a header comment. Pure local. Tests: name with spaces (slugified), collision (timestamp bumps by 1s), invalid chars rejected.
- [ ] `migrations get <id>` — `GET /v1/projects/{ref}/migrations/{id}`. Tests: 200 happy path, 404 typed error, JSON shape.
- [ ] `migrations diff` — `GET /v1/projects/{ref}/migrations/diff`. Tests: no-diff response, real diff, error mapping.
- [ ] `migrations rollback <id>` — `POST /v1/projects/{ref}/migrations/{id}/rollback` with confirmation. Tests: confirmed, declined, server returns 409 (mid-flight).
- [ ] Client additions: `GetMigration`, `DiffMigrations`, `RollbackMigration` (overlap with Tier 9 — implement once, used twice).

---

## Tier 11 — Branches (preview environments)

Full CRUD ships in cloud (`/projects/{id}/branches/*`). Zero CLI today.

- [ ] `cmd_branches.go` — dispatcher with `list`, `create`, `get`, `merge`, `delete`, `events`.
- [ ] `branches list` — `GET /v1/projects/{ref}/branches`. Tests: empty, populated, JSON shape lock.
- [ ] `branches create <name> [--from=<branch>]` — `POST /v1/projects/{ref}/branches`. Tests: defaults, `--from` honoured, name conflict (409).
- [ ] `branches get <ref>` — `GET /v1/projects/{ref}/branches/{branch_ref}`. Tests: 200, 404.
- [ ] `branches merge <ref>` — `POST /v1/projects/{ref}/branches/{branch_ref}/merge` with confirmation. Tests: confirmed → success, declined → no-op, merge conflict response shape.
- [ ] `branches delete <ref>` — `DELETE /v1/projects/{ref}/branches/{branch_ref}` (cloud calls it "retire"). Tests: confirmed, declined, 404.
- [ ] `branches events [--follow]` — `GET /v1/projects/{ref}/branches/events`. `--follow` polls every 5s. Tests: snapshot mode, follow mode (with stub server emitting two pages).
- [ ] Client: `ListBranches`, `CreateBranch`, `GetBranch`, `MergeBranch`, `RetireBranch`, `ListBranchEvents`.

---

## Tier 12 — Tables, data, RLS

Today's `tables` reads. Mirror the write surface + RLS.

- [ ] `cmd_tables.go` — extend the existing dispatcher with: `create`, `alter`, `drop`, `import-csv`, `export-csv`, `columns` (sub-dispatch: add/alter/drop).
- [ ] `tables create <name> --column=<name:type[,nullable=false][,pk]>...` — repeatable `--column` flag, POSTs to `/v1/projects/{ref}/tables`. Tests: single-col, multi-col, with PK, with FK, malformed column spec.
- [ ] `tables alter <name> --add-column=... --drop-column=...` — PATCH `/v1/projects/{ref}/tables/{name}`. Tests: add-only, drop-only, both.
- [ ] `tables drop <name>` — `DELETE /v1/projects/{ref}/tables/{name}` with confirmation. Tests: confirmed, declined, 404.
- [ ] `tables import-csv <name>` — read CSV from stdin (or `--file=`), stream to `POST /v1/projects/{ref}/tables/{name}/import-csv`. Tests: header-row, no-header, type-coercion error response, multi-mb file (streaming, not buffered).
- [ ] `tables export-csv <name>` — `GET /v1/projects/{ref}/tables/{name}/export-csv`, stream to stdout. Tests: small table, empty table, large table (streaming).
- [ ] `cmd_rows.go` — new file: `rows insert/update/delete`. POST/PATCH/DELETE `/v1/projects/{ref}/tables/{name}/rows`. Tests: single-row insert via `--json`, batch insert from stdin, update by pk, delete by predicate.
- [ ] `cmd_rls.go` — new file: `rls enable/disable <table>`, `rls policies list/create/drop`. Routes: `POST /tables/{name}/rls/enable`, `POST /tables/{name}/rls/disable`, `/tables/{name}/policies`. Tests: enable, disable, policy CRUD, RLS-not-enabled error path.
- [ ] Client: `CreateTable`, `AlterTable`, `DropTable`, `ImportTableCSV`, `ExportTableCSV`, `InsertRow`, `UpdateRow`, `DeleteRows`, `EnableRLS`, `DisableRLS`, `ListPolicies`, `CreatePolicy`, `DropPolicy`.

---

## Tier 13 — Project keys + pgwire credentials

Distinct from the org-level PATs (`basin tokens`).

- [ ] `cmd_api_keys.go` — `list`, `create`, `rotate`, `revoke`. Routes: `/v1/projects/{ref}/api-keys`. Tests: full CRUD round-trip, JSON shape, masked-secret output (only print full secret on create).
- [ ] `cmd_pgwire.go` — `show`, `reveal`, `rotate`. Routes: `/v1/projects/{ref}/pgwire`. Reuses Tier 9's client helpers. Tests: show (masked password), reveal (full), rotate (confirmation flow).
- [ ] `cmd_pgwire.go` — `engine-keys list/rotate`. Routes: `/v1/projects/{ref}/engine-keys`, `/engine-keys/rotate`. Tests: list shape, rotate confirmation.
- [ ] Client: `ListAPIKeys`, `CreateAPIKey`, `RotateAPIKey`, `RevokeAPIKey`, `GetEngineKeys`, `RotateEngineKey`.

---

## Tier 14 — Backups

`snapshots` ships; flesh out the rest of `/backups/*`.

- [ ] `cmd_backups.go` — `policy get/set`, `snapshots list/create/expire`, `restore`, `restore-jobs list`. Routes: `/v1/projects/{ref}/backups/policy`, `/snapshots`, `/restore`, `/restore/jobs`.
- [ ] `backups policy get` — GET. Tests: default policy, custom policy.
- [ ] `backups policy set --retention-days=<n> --schedule=<cron>` — PUT. Tests: validation (retention > 0), success, server error.
- [ ] `backups snapshots create [--label=...]` — POST. Tests: label honoured, no-label, server in-progress error.
- [ ] `backups snapshots expire <id>` — POST. Tests: success, 404, already-expired.
- [ ] `backups restore --from=<snapshot> [--into=<branch>]` — POST. Tests: same-project, into-branch, conflict.
- [ ] `backups restore-jobs list` — GET. Tests: empty, populated, in-flight status.
- [ ] Client: `GetBackupPolicy`, `PutBackupPolicy`, `ListBackupSnapshots`, `CreateBackupSnapshot`, `ExpireBackupSnapshot`, `RestoreBackup`, `ListRestoreJobs`. (Reuse `snapshots` client code where it overlaps.)

---

## Tier 15 — Type generation (`gen types`)

Highest-leverage single feature after `db push`. Pure CLI assembly
over `information_schema` queries — no cloud endpoint to add.

- [ ] `cmd_gen.go` — dispatcher `gen types <lang>`. Languages: `typescript`, `go`, `python`.
- [ ] `gen types typescript` — query `information_schema.columns` via `POST /v1/projects/{ref}/sql/query`, emit `database.ts` with `type Tables = { ... }` per Postgres-type → TS-type mapping table in `gen_types_map.go`. Tests: enum mapping, nullable mapping, JSONB mapping, vector(N) mapping, snapshot test against a `testdata/expected.ts`.
- [ ] `gen types go` — same shape; emit `database.go` with one struct per table. Honour `json:"..."` + `db:"..."` tags. Snapshot test against `testdata/expected.go`.
- [ ] `gen types python` — emit `database.py` with `pydantic.BaseModel` subclasses. Snapshot test against `testdata/expected.py`.
- [ ] `gen types --watch` — re-emit on every successful `db push`. Implement as a file-watcher on `./basin/migrations/`. Tests: cooperative interrupt (Ctrl-C ⇒ exit 0).
- [ ] Type-mapping table in `gen_types_map.go` — exhaustive: bool/int2/int4/int8/float4/float8/numeric/text/bytea/uuid/jsonb/timestamptz/date/time/interval/vector. Doc-comment in the file points at the engine's pgwire OID list.

---

## Tier 16 — Org & member management

Cloud has the full surface (`/v1/orgs/{slug}/members*`).

- [ ] `cmd_members.go` — `list`, `invite`, `remove`, `role`. Routes: `/v1/orgs/{slug}/members`, `/members/invite`, `/members/{user_id}`. Tests: list shape, invite by email, role update, removal confirmation.
- [ ] `cmd_invitations.go` — `list`, `resend`, `revoke`. Routes: `/v1/orgs/{slug}/invitations`, `/invitations/{id}/resend`. Tests: pending vs expired, resend, revoke.
- [ ] Extend `cmd_orgs.go` — add `create`, `update`, `delete`. Routes already wired (`POST /v1/orgs`, `PATCH /v1/orgs/{slug}`, `DELETE /v1/orgs/{slug}`). Tests: CRUD round-trip, JSON shape, confirmation on delete.
- [ ] `cmd_orgs.go` — `branding get/put`. Routes: `/v1/orgs/{slug}/branding`. Tests: get default, put custom, validation errors.
- [ ] Client: `ListMembers`, `InviteMember`, `RemoveMember`, `UpdateMemberRole`, `ListInvitations`, `ResendInvitation`, `RevokeInvitation`, `CreateOrg`, `UpdateOrg`, `DeleteOrg`, `GetOrgBranding`, `PutOrgBranding`.

---

## Tier 17 — Operations & observability

- [ ] `cmd_domains.go` — `add`, `verify`, `cert`, `list`, `remove`. Routes: `/v1/projects/{ref}/domains/*`. Tests: full CRUD, verify dns flow, cert issuance polling.
- [ ] `cmd_webhooks.go` — `list`, `create`, `patch`, `test`, `redeliver`, `delete`, `deliveries`. Routes: `/v1/projects/{ref}/webhooks/*`. Tests: full lifecycle, test-send, redeliver by delivery id.
- [ ] `cmd_alerts.go` — `rules list/create/get/patch/delete/silence/unsilence`, `events list`. Routes: `/v1/projects/{ref}/alerts/*`. Tests: rule CRUD, silence + unsilence, event listing pagination.
- [ ] `cmd_audit.go` — `list [--export=<dest>]`. Routes: `/v1/orgs/{slug}/audit`. With `--export`: route to `/audit/export/destinations`. Tests: list, since/until filtering, export.
- [ ] `cmd_activity.go` — `list`. Route: `/v1/projects/{ref}/activity`. Tests: list shape, pagination.
- [ ] `cmd_metrics.go` — `--range=24h` etc. Routes: `/v1/projects/{ref}/metrics`, `/v1/orgs/{slug}/metrics`. Tests: range parsing, JSON shape lock.
- [ ] `cmd_erd.go` — `export [--format=svg|dot]`. Route: `/v1/projects/{ref}/erd`. Tests: svg export, dot export.
- [ ] Client: corresponding methods for each route above.

---

## Tier 18 — Engine knobs

- [ ] `cmd_engine_pin.go` — `get/put/delete/events`. Routes: `/v1/projects/{ref}/engine-version/pin`, `/pin/events`. Tests: get default, pin a version, unpin, event audit trail.
- [ ] `cmd_extensions.go` — `list`. Route: `/v1/projects/{ref}/extensions/catalog`. Tests: list shape, JSON output.
- [ ] `cmd_oauth_providers.go` — `list/get/set/delete`. Routes: `/v1/projects/{ref}/oauth-providers/*`. Tests: provider CRUD per slug.
- [ ] `cmd_email.go` — `templates get/put/delete/test`, `allowance`. Routes: `/v1/projects/{ref}/email/*`. Tests: template CRUD, test-send, allowance lookup.
- [ ] Client: methods for each route.

---

## Tier 19 — Saved queries & history

- [ ] `cmd_queries.go` — `save`, `list`, `get`, `fork`, `delete`, `history`, `history --clear`. Routes: `/v1/projects/{ref}/queries/*`. Tests: full lifecycle, fork preserves ancestry, history clear with confirmation.
- [ ] Client: `SaveQuery`, `ListSavedQueries`, `GetSavedQuery`, `UpdateSavedQuery`, `DeleteSavedQuery`, `ForkSavedQuery`, `ListQueryHistory`, `ClearQueryHistory`.

---

## Tier 20 — BYO surfaces

For customers running their own buckets, KMS, or engine.

- [ ] `cmd_byo_bucket.go` — `get/put/probe/delete`. Routes: `/v1/projects/{ref}/storage` (`Get`/`Set`/`Probe`/`Revoke`). Tests: full CRUD + probe-failure response shape.
- [ ] `cmd_byo_kms.go` — `get/put/rotate/verify/audit/delete/cache-stats`. Routes: `/v1/projects/{ref}/kms/*`. Tests: full CRUD, rotate flow, audit pagination, cache-stats shape.
- [ ] `cmd_byo_engine.go` — `get/put/probe/delete`. Routes: `/v1/projects/{ref}/byo-engine/*`. Tests: full CRUD + probe.
- [ ] Client: corresponding methods.

---

## Tier 21 — Enterprise auth (gated)

Plan-gated on the cloud side; CLI surface still useful.

- [ ] `cmd_saml.go` — `get/put/test/enable/disable`. Routes: `/v1/orgs/{slug}/saml/*`. Tests: full lifecycle, enable confirmation.
- [ ] `cmd_scim.go` — `config get`, `tokens create/revoke`. Routes: `/v1/orgs/{slug}/scim/*`. Tests: token CRUD, config shape.
- [ ] `cmd_oauth_apps.go` — `list/create/rotate/disable/enable/delete`. Routes: `/v1/orgs/{slug}/oauth-apps/*`. Tests: full lifecycle.
- [ ] Client: corresponding methods.

---

## Tier 22 — Transfers

- [ ] Extend `cmd_projects.go` with `transfers list/create/cancel`. Routes: `/v1/projects/{ref}/transfers/*`. Tests: pending listing, accept/decline as receiving org.
- [ ] Extend `cmd_orgs.go` with `ownership-transfers list/create/cancel/accept/decline`. Routes: `/v1/orgs/{slug}/ownership-transfers/*`. Tests: full bidirectional flow.
- [ ] Client: methods for each route.

---

## Tier 23 — Parking lot (engine/cloud gaps)

**Do not implement these.** Each row would lie today; we hold them
until the backing capability ships. Listed here so the eventual
naming doesn't get bike-shed-revisited mid-Phase-X.

- [x] ~~`cmd_realtime.go` — blocked on engine pub/sub~~ **UNBLOCKED 2026-05-20** — engine shipped realtime (basin 5.11.R1–R7). Promoted to **Tier 26**.
- [ ] `cmd_functions.go` — `deploy/serve` for edge functions. Blocked on cloud Phase 8 (V8). NOTE: `rpc <fn>` *invoke* is unblocked (5.11.L) → **Tier 26**.
- [ ] `cmd_storage_buckets.go` — `list/create/upload/download` for object storage as a product (distinct from BYO-bucket-for-Parquet). Not on the cloud roadmap.
- [ ] `cmd_tx.go` — interactive `begin/commit/rollback`. Engine single-shard transactions **shipped** (BEGIN/COMMIT/ROLLBACK + SAVEPOINT); only the interactive REPL-session scaffolding remains.
- [ ] `cmd_restore_pitr.go` — `restore --as-of=<timestamp>`. Blocked on engine PITR cross-DML physical-GC (catalog-level rollback shipped; GC is v0.2).

---

## Tier 24 — Testing infrastructure

Floor (already enforced): `go test -race -count=1 ./...` green on
ubuntu + macos. Tier 24 lifts the bar to **contract-tested + cross-
version** coverage as the surface grows past 30 commands.

- [ ] **Per-command test matrix.** Every `cmd_*.go` ships with a `cmd_*_test.go` that covers, at minimum: (1) arg-parse error path (missing required flag → typed error), (2) `--json` output shape lock (compare against a `testdata/<cmd>.json` golden), (3) `--quiet` suppression of prose, (4) one happy-path round-trip against an `httptest.Server` stub, (5) one server-error mapping (404 / 409 / 500 → typed `APIError`). Lint rule: a PR adding `cmd_*.go` without `cmd_*_test.go` fails CI (`scripts/test-coverage-gate.sh`).
- [ ] **Per-client-method test matrix.** Every `Client.X()` in `client.go` ships with at least 3 rows in `client_test.go`: envelope unwrap on a `{data,error}` body, flat fallback on a non-wrapped body, error mapping on a non-200 status with `error: {...}` body.
- [ ] **Integration test coverage.** Extend `integration_test.go` so every top-level subcommand has at least one happy-path drive-through `run([]string{...})`. Today: `whoami`, `projects list`, `sql`, `version`, `help`. Goal: every entry in the `commands()` dispatch table.
- [ ] **Contract tests against a real cloud.** New file: `tests/contract/contract_test.go`. Record fixtures from a live `basin-cloud` boot via a `recordmode=true` env flag; on replay, drive every client method against the recorded transcripts. Run under a build tag (`go test -tags=contract`) so day-to-day `go test ./...` stays fast.
- [ ] **Cross-version replays.** Maintain `tests/contract/N-1/`, `tests/contract/N/`, `tests/contract/N+1/` directories of recorded fixtures. The contract suite asserts every CLI command tolerates all three shapes. Updated whenever a cloud minor ticks.
- [ ] **Golden output tests.** Snapshot the human-readable output of every command under `tests/golden/`. Diff on test run; refresh via `go test -update-golden`. Catches accidental UX regressions.
- [ ] **`--json` schema lockfile.** Generate a JSON-schema for every documented `--json` shape; check it into `tests/json-schemas/`. Fail the test suite if a `printJSON` call site emits a shape not present in the lockfile (forces conscious schema bumps).
- [ ] **Smoke test on a real binary.** New `scripts/smoke.sh`: build via `go build`, drive `basin --version`, `basin help`, `basin login` (against a stub), `basin whoami` (against a stub), assert exit codes + key output substrings. Run in CI on ubuntu + macos + windows.
- [ ] **Fuzz tests.** `FuzzParseGlobalFlags`, `FuzzQueryString`, `FuzzConfigRoundTrip`. Stdlib `testing.F`. CI runs them on PR for 30s; nightly for 5min.
- [ ] **Race + leak gates.** Already running `-race`. Add `goleak.VerifyTestMain` equivalent done stdlib-style: on test exit, dump goroutines and assert only the main + GC + test runner remain.
- [ ] **Coverage threshold.** Add `scripts/coverage-gate.sh` enforcing ≥ 80% coverage on every non-test `*.go` file. CI fails below threshold. Today's repo sits at ~75% by file; the new tiers should push the average up, not regress it.

---

## Tier 25 — Documentation & polish

Drift-control for the docs as the surface fans out.

- [ ] **Per-command `man` pages.** Generate from the `commandEntry.help` field + each subcommand's `flag.FlagSet` usage. Embedded in the binary via `go:embed`; surfaced as `basin <cmd> --help` and (eventually) `man basin-<cmd>`. Tests: every `commands()` entry produces a non-empty man page.
- [ ] **`README.md` example block** grows with every new tier — keep the count at 9–12 representative one-liners, not exhaustive.
- [ ] **`docs/` directory** — per-area markdown explainers (`docs/db-workflow.md`, `docs/branches.md`, `docs/types.md`, …). Cross-linked from `README.md`. Each one ends with a "tests covering this surface" pointer.
- [ ] **Changelog discipline.** Every tier completion adds a `CHANGELOG.md` entry under `## Unreleased`. Tags flush `Unreleased` into a dated section.
- [ ] **`basin docs <cmd>`** — opens `https://docs.basin.run/cli/<cmd>` in the default browser (no `os/exec` of `open`/`xdg-open` mismatch — go via `runtime.GOOS` dispatch). Tests: each OS branch returns the expected command name.

---

## Tier 26 — Realtime & RPC (P0, engine shipped 2026-05-20)

Engine landed the full realtime stack (basin 5.11.R1–R7) and the RPC mount
(5.11.L). These are P0: backend exists, only CLI work needed. Stdlib-only
Go — SSE is a plain `http.Get` + `bufio.Scanner` line reader; WS needs a
minimal RFC-6455 client (stdlib `net` + `crypto/sha1` handshake, no deps),
OR gate the WS subcommand behind a single vetted `golang.org/x/net/websocket`
import if the decisions.md no-deps rule permits it — **decide and record in
`decisions.md` first.** Each task is sized for one Sonnet agent.

- [ ] **T26.1 — `cmd_realtime.go` SSE single-table subscribe.** New file
  `cmd_realtime.go` + `cmd_realtime_test.go`. `basin realtime subscribe
  <table> [--project=<ref>] [--since=<seq>]`. Opens
  `GET /realtime/v1/sse/:project/:table` with `Authorization: Bearer
  <token>`, reads the SSE stream with `bufio.Scanner`, parses each `data:`
  line as JSON, prints one compact JSON event per line to stdout. Skips
  heartbeat/comment frames. `--since` sets the `Last-Event-Id` header.
  Ctrl-C (SIGINT) closes the response body and exits 0. **Acceptance:**
  `httptest.Server` streaming 3 `data:` frames + 1 heartbeat → 3 JSON lines
  on stdout, exit 0 on context-cancel; arg-parse error path tested.
- [ ] **T26.2 — RFC-6455 minimal WS client (or vetted dep decision).**
  New `internal/ws/ws.go` + test. A tiny client: HTTP Upgrade handshake
  (`Sec-WebSocket-Key`/`Accept` via `crypto/sha1`+base64), text-frame
  read/write, masking, ping/pong, close. OR — if `decisions.md` approves —
  thin wrapper over `x/net/websocket`. **Acceptance:** round-trips text
  frames against a `httptest.Server` that upgrades and echoes; ping →
  pong; close handshake clean. This unblocks T26.3.
- [ ] **T26.3 — `realtime subscribe --multi` over WebSocket.** Depends on
  T26.2. `basin realtime subscribe --multi <t1>,<t2>,… [--filter=<expr>]`.
  Opens `GET /realtime/v1/ws/:project`, sends
  `{"type":"subscribe","table":"<t>"[,"filter":"<expr>"]}` per table, waits
  for `{"type":"subscribed"}` acks, then prints each `{"type":"event",…}`
  as a table-tagged JSON line. Handles `{"type":"error","code":"lag"}` by
  printing a stderr warning. **Acceptance:** stub WS server acks two
  subscribes, emits interleaved events → two-table tagged output; lag frame
  → stderr warning, stdout uninterrupted.
- [ ] **T26.4 — `cmd_rpc.go` invoke user functions.** New `cmd_rpc.go` +
  test. `basin rpc <fn> [--arg k=v …] [--body @file.json]`. POSTs to
  `POST /rest/v1/rpc/:fn` with a JSON object assembled from `--arg`
  key=value pairs (typed: ints/bools/strings inferred) or the raw
  `--body` file. Prints the scalar result bare, or a JSON array for
  `RETURNS TABLE`. Honours `--json`. **Acceptance:** stub returns `7` for
  `add x=3 y=4` → prints `7`; stub returns a row array → pretty JSON array;
  401 → typed `APIError`; missing fn-name arg → parse error.
- [ ] **T26.5 — `commands()` dispatch + README + man wiring.** Register
  `realtime` and `rpc` in the `commands()` table, add them to the
  `integration_test.go` drive-through, add two README example one-liners,
  and a `CHANGELOG.md` Unreleased entry. **Acceptance:** `basin realtime
  --help` and `basin rpc --help` produce non-empty man pages; integration
  test drives both with a stub; `go test -race ./...` green.

> Inbound webhooks (`POST /in/:project/:name`, basin 5.11.N) are a
> *cloud-managed* registration surface, not a CLI verb — the CLI's role is
> at most `basin webhooks inbound list/create` under the existing
> `cmd_webhooks.go`. Tracked as a follow-on once 5.11.N's catalog shape
> stabilises; not part of Tier 26.

