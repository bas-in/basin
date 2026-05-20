# basin

The `basin` CLI manages [Basin Cloud](https://basin.run) projects from
the terminal — orgs, projects, secrets, migrations, SQL, logs,
snapshots, tokens.

Modelled on `gh` / `flyctl` / `supabase`. Rust binary (clap + reqwest),
no system dependencies, one self-contained file per subcommand.

## Install

### Homebrew (macOS, Linux)

```sh
brew install bas-in/tap/basin
```

### `cargo install`

```sh
cargo install --git https://github.com/bas-in/basin-cli basin
```

### Pre-built binaries

Grab the right archive for your platform from the latest
[GitHub release](https://github.com/bas-in/basin-cli/releases) and
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
basin branches merge preview-x --project=staging --yes  # merge and retire branch
basin gen types typescript --output=src/database.ts  # emit TypeScript types
basin tables import-csv users < users.csv            # stream CSV into a table
basin alerts rules create --project=staging          # create an alert rule
basin byo bucket put --project=staging               # configure BYO S3 bucket
```

Every command honours:

- `--json` — machine-readable output
- `-q` / `--quiet` — suppress non-essential prose
- `--no-color` — disable ANSI
- `--api-url=<url>` — override the default `https://api.basin.run`
  (also `BASIN_API` env var)

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
  --certificate-identity-regexp 'https://github.com/bas-in/basin-cli/\.github/workflows/release\.yml@refs/tags/v.*' \
  --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
  "${ART}"
```

A successful run prints `Verified OK`. Anything else — wrong issuer,
mismatched tag, edited binary — fails closed.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
