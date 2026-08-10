# basin

The `basin` CLI manages [Basin Cloud](https://basin.run) projects from
the terminal — orgs, projects, secrets, migrations, SQL, logs,
snapshots, tokens.

Modelled on `gh` / `flyctl` / `supabase`. Rust binary (clap + reqwest),
no system dependencies, one self-contained file per subcommand.

## Install

### Homebrew (macOS, Linux)

```sh
brew install vul-os/tap/basin
```

### `cargo install`

The CLI lives in the `cli/` subdirectory of the basin monorepo and is
deliberately detached from the engine's Cargo workspace, so
`cargo install --git` cannot resolve it — that flag only finds crates
reachable from the repo's root manifest. Clone and install from the path:

```sh
git clone https://github.com/vul-os/basin
cargo install --path basin/cli
```

### Pre-built binaries

Grab the right archive for your platform from the latest
[GitHub release](https://github.com/vul-os/basin/releases) and
drop the `basin` binary on your `PATH`.

## Authenticate

Mint a personal access token at <https://basin.run/app/account/tokens>
(prefix `bso_org_…`), then:

```sh
basin login                   # prompts for the token, stores under ~/.config/basin/config.json
basin whoami                  # confirms the active token + org
```

Or pass via env / flag for one-off calls:

```sh
BASIN_TOKEN=bso_org_… basin orgs list
basin --token=bso_org_… orgs list
```

## Examples

```sh
basin init && basin link --project=staging          # scaffold + bind directory
basin db push                                        # apply local migrations
basin db url --reveal                                # print full pgwire DSN
basin branches create preview-x --kind=preview       # spin up a preview environment
basin gen types typescript --output=src/database.ts  # emit TypeScript types
basin tables import-csv users < users.csv            # stream CSV into a table
basin byo bucket put --project=staging               # configure BYO S3 bucket
basin realtime subscribe orders --project=staging    # stream live INSERT/UPDATE/DELETE
basin rpc add --arg x=3 --arg y=4 --project=staging  # invoke a SQL/WASM function
basin alerts rules create --project=staging          # create an alert rule
```

Every command honours:

- `--json` — machine-readable output
- `-q` / `--quiet` — suppress non-essential prose
- `--no-color` — disable ANSI
- `--api-url=<url>` — override the default `https://api.basin.run`
  (also `BASIN_API` env var)

## In-depth docs

Per-area explainers live under `docs/`:

- [Database workflow](docs/db-workflow.md) — migrations, branches, seeding, reset.
- [Branches](docs/branches.md) — preview environments, merge, retire.
- [Types](docs/types.md) — `gen types` for TypeScript, Go, and Python.

Each ends with the test surface that covers it.

## Configuration

Token + default org live at `~/.config/basin/config.json`:

```json
{
  "default_org": "acme",
  "default_token": "bso_org_…",
  "tokens": {
    "acme":   "bso_org_…",
    "other":  "bso_org_…"
  }
}
```

Lookup order for the active token:

1. `--token=<value>` flag
2. `$BASIN_TOKEN`
3. `tokens.<--org slug>` from the config file
4. `default_token` from the config file

## Compatibility with Basin Cloud

The CLI talks to the cloud's stable `/v1/*` HTTP surface. Both sides
follow a **two-minor support window**:

> A `basin` CLI on minor version **N** is supported against a Basin
> Cloud running minor **N-1**, **N**, or **N+1**.

So `basin v0.5.x` works against cloud `v0.4`, `v0.5`, or `v0.6`. Older
or newer than that and the CLI may emit deprecation warnings or fail
fast with a clear `version_mismatch` error.

The cloud advertises its version at `GET /v1/version`. The CLI fetches
this on `basin login` and warns on every command when the window is
exceeded — never blocks. Patch-version drift is always supported.

Breaking changes to `/v1/*` are gated on a one-minor deprecation:
endpoints carrying a `Sunset: <date>` header keep working but warn,
then return `410 Gone` one cloud minor later. CLI versions inside the
window must continue to work against cloud HEAD without re-release.

## Releasing

Tagged releases ship multi-arch tarballs + a Docker image + a Homebrew
formula update, built by `.github/workflows/release.yml`.

```sh
git tag v0.1.1
git push origin v0.1.1
# CI cross-compiles for linux/{amd64,arm64}, darwin/{amd64,arm64},
# windows/amd64, uploads archives to the GitHub release, and pushes
# the multi-arch GHCR image.
```

## Verifying release signatures

Every release artefact (tarball, zip, `checksums.txt`) is signed via
Sigstore keyless mode. The signing identity is the GitHub Actions
workflow that produced the build — there are no private keys to leak,
and verification proves the binary came from the public release
pipeline at the matching tag.

Install [`cosign`](https://docs.sigstore.dev/system_config/installation/),
download the artefact + its `.sig` and `.pem` siblings from the GitHub
release page, then verify against the workflow identity:

```sh
ART="basin_0.1.0_linux_amd64.tar.gz"

cosign verify-blob \
  --certificate "${ART}.pem" \
  --signature   "${ART}.sig" \
  --certificate-identity-regexp '^https://github\.com/vul-os/basin/\.github/workflows/cli-release\.yml@refs/tags/cli-v.*$' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  "${ART}"
```

A successful run prints `Verified OK`. Anything else — wrong issuer,
mismatched tag, edited binary — fails closed.

> The identity above must match the SAN Fulcio puts in the certificate, which
> for a GitHub Actions OIDC token is
> `https://github.com/<owner>/<repo>/.github/workflows/<file>@<ref>` — no
> `tree/<branch>/` segment, the workflow path relative to the **repo root**, and
> the **tag that triggered the run**. This block previously named
> `tree/main/cli/.github/workflows/release.yml@refs/tags/v.*`, which was wrong
> three times over: the SAN has no `tree/main` segment, the workflow was hoisted
> out of `cli/` to the repo root and renamed `cli-release.yml`, and CLI releases
> are tagged `cli-v*` (the bare `v*` namespace belongs to the engine release).
> `cosign verify-blob` therefore refused every signature it was ever pointed at.
> Failing closed is the right direction to be wrong in — but the documented
> command could not print `Verified OK`, so nobody could complete a verification
> by following it.

## Self-hosted OSS engine

If you are running a bare `basin-server` (the open-source OSS engine at
[`vul-os/basin`](https://github.com/vul-os/basin)) with no control plane in
front of it, only a subset of `basin` commands will work:

| Command group | Works against OSS `basin-server`? |
|---|---|
| `rpc` | Yes — calls `POST /rest/v1/rpc/:fn` directly on the engine |
| `storage` | Yes — calls `GET/POST /storage/v1/*` directly on the engine |
| `realtime` | Yes — calls `GET /realtime/v1/sse/:project/:table` on the engine |
| Everything else | **No** — requires a control plane |

Commands that require a control plane (not available against bare OSS):

- `sql`, `db push/pull/diff/reset/url/dump/lint`
- `migrations *`, `branches *`, `snapshots *`, `backups *`
- `tables *`, `rows *`, `rls *`
- `secrets *`
- `login`, `logout`, `whoami`, `orgs *`, `projects *`
- `tokens *`, `members *`, `domains *`, `webhooks *`, `alerts *`, `audit *`
- `gen types`, `migrate-from-pg`, `dump`, `restore`

These commands call `/v1/projects/:ref/*` routes that a control plane
exposes. Against a bare OSS engine they will return 404.

**In short:** use `basin rpc`, `basin storage`, and `basin realtime` against
a self-hosted engine. All other commands need a control plane.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
