---
title: "basin-cli v0.1 design spec"
nav_section: meta
sidebar_position: 30
summary: "Historical design spec for the basin CLI. Superseded by the shipped implementation in cli/ — kept for the rationale, not as a description of the code."
---

# basin-cli v0.1 design spec

> **Status: HISTORICAL — superseded by the shipped CLI in [`cli/`](../cli/).**
>
> This document was written as a forward-spec for a CLI that would live in a
> separate `bas-in/basin-cli` repository. That never happened, and the parts of
> this document that describe *how* the CLI is built are now wrong. Read it for
> the command-surface rationale; read the code for what exists.
>
> | This document says | What actually shipped |
> |---|---|
> | Repository `bas-in/basin-cli` "does not yet exist" | Lives in this repo at [`cli/`](../cli/) — a standalone crate, detached from the engine workspace. `bas-in/basin-cli` was never created and the org 404s. |
> | Written in **Go**, stdlib-only, no Cobra/Viper | Written in **Rust**, using `clap`, `reqwest`, `tokio-postgres`, `rustyline`. See [`cli/Cargo.toml`](../cli/Cargo.toml). |
> | Release artefacts signed via **GoReleaser** | Released by [`.github/workflows/cli-release.yml`](../.github/workflows/cli-release.yml): `cargo build --release` per target, uploaded by `taiki-e/upload-rust-binary-action`, signed keyless with **cosign** (Sigstore OIDC — no long-lived key). |
> | Binary name `basin-cli` | Binary is `basin`; the engine admin tool is `basinctl`. |
>
> Live docs for the shipped CLI: [`cli/README.md`](../cli/README.md),
> [`cli/PARITY.md`](../cli/PARITY.md), [`cli/decisions.md`](../cli/decisions.md).

---

## 1. Purpose and scope

`basin-cli` is the **operator and developer daily-driver** for Basin. Its
audience is the engineer who has a Basin project (self-hosted or managed)
and wants to interact with it from a terminal — authenticate,
inspect and manage projects, run SQL, drive schema migrations, and surface
query-performance insights without leaving their shell.

The tool is written in **Go using stdlib-only** (no Cobra, no Viper, no
third-party HTTP client framework). Stdlib-only is a deliberate scope
constraint: it keeps the binary small, the dependency tree auditable, and the
build reproducible without a vendor cache. Release artefacts are
**Sigstore-signed** via GoReleaser.

`basin-cli` is **not** a DBA suite. It does not expose server configuration
knobs, storage-engine tuning parameters, or shard topology management. Those
are operator concerns addressed through `basin-server`'s env-var configuration
surface and `basinctl` (the internal admin CLI in `services/basinctl/`).
`basin-cli` is also **not** a migration framework with its own schema language
or ORM model — it wraps file-based SQL migrations and delegates all schema
logic to the engine via pgwire. The engine's SQL surface is the schema
language; `basin-cli` is the delivery vehicle.

The scope boundary is simple: if the operation requires a live connection to a
Basin project and fits in a single command, it belongs here. If it requires
knowledge of Basin internals (WAL segments, Vortex file layout, shard
assignments), it belongs in `basinctl` or the engine itself.

---

## 2. Command surface for v0.1

All commands share a `--project <id>` flag (or `BASIN_PROJECT` env var) and
the global `--json` / `--quiet` flags described in Section 5.

| Command | Description |
|---|---|
| `basin login` | Starts the basin-auth OAuth/JWT flow. Stores the resulting JWT and refresh token in the OS keychain (see Section 3). |
| `basin logout` | Revokes the refresh token via `POST /auth/v1/signout` and deletes the stored credential. |
| `basin projects list` | Calls `GET /v1/projects` on basin-rest; prints project ID, name, region, and status. |
| `basin projects create <name>` | Calls `POST /v1/projects`; prints the new project ID and the ready postgres URL. |
| `basin projects delete <id>` | Calls `DELETE /v1/projects/<id>`; requires `--confirm` flag to prevent accidents. |
| `basin projects connect <id>` | Prints the `postgres://<user>:<token>@<host>:<port>/<db>` URL for the project. Safe to pipe into `psql` or a `.env` file. |
| `basin sql run <project> "<sql>"` | Connects over pgwire, executes the SQL, and prints results as an ASCII table (or JSON with `--json`). |
| `basin sql shell <project>` | Opens an interactive REPL. Line-editing via `readline`-compatible terminal raw mode (stdlib `term` package). Semicolon terminates a statement; `\q` exits. |
| `basin schema dump <project>` | Introspects the catalog and emits a `pg_dump`-compatible DDL script to stdout. Suitable for `git` versioning or seeding a new project. |
| `basin schema load <project> <file>` | Applies a DDL script via pgwire; wraps each statement in a transaction; stops on first error. |
| `basin migrations new <project> <name>` | Creates `migrations/<timestamp>_<name>.up.sql` and `migrations/<timestamp>_<name>.down.sql` in the working directory. |
| `basin migrations run <project>` | Applies all pending `.up.sql` files in timestamp order via pgwire; records applied migrations in `basin_migrations` tracking table on the project. |
| `basin migrations status <project>` | Lists all migration files and whether each has been applied. |
| `basin migrations rollback <project>` | Applies the most recent `.down.sql` file; decrements the tracking table. |
| `basin debug query-shapes <project>` | Queries the `basin_stat_statements` view (Phase 5.16.D) and renders a table of plan shapes ranked by p99 latency. Surfaces "your slowest queries" without requiring the operator to know the view name. |
| `basin debug explain <project> "<sql>"` | Runs `EXPLAIN ANALYZE <sql>` over pgwire and pretty-prints the plan tree. |
| `basin tail <project> <table>` | Subscribes to the table's change-event stream over WebSocket (Phase 3 of the pivot plan). Streams JSON-encoded row events to stdout. Ctrl-C to stop. |

`basin tail` is v0.1 surface but its server-side dependency (WebSocket
change-event stream) lands in a later phase; the command is present and
returns a clear `not yet available on this server version` error until the
server-side ships.

---

## 3. Auth and state model

### Credential storage

On first `basin login`, the CLI writes credentials to
`~/.basin/credentials.json` with mode `0600`. On platforms where the OS
keychain is available (macOS Keychain, Linux libsecret, Windows Credential
Manager), the JWT access token and refresh token are stored in the keychain
and `credentials.json` holds only the reference key and metadata (org ID,
project URL, token expiry). The file-only fallback (token in the file
directly) is used when no keychain is detected.

`credentials.json` structure:

```
{
  "version": 1,
  "default_org": "<org_id>",
  "orgs": {
    "<org_id>": {
      "url": "https://api.basin.run",
      "email": "operator@example.com",
      "token_expires_at": "<RFC3339>",
      "keychain_key": "basin:<org_id>"   // absent if file-only fallback
    }
  }
}
```

### Multi-org support

`basin-cli` tracks multiple org credentials simultaneously. `basin login
--org <alias>` adds or refreshes a named org. `--org <alias>` on any command
selects a non-default org. `basin projects list --org all` lists across all
authenticated orgs.

### Token refresh

On every command execution, the CLI checks whether the stored access token
expires within 5 minutes. If so, it silently calls `POST /auth/v1/refresh`
before the main operation. A failed refresh (expired or revoked refresh token)
exits with error code 1 and a clear message directing the operator to run
`basin login` again.

### CLI version pinning

The CLI embeds its own version string (semver + git SHA, injected at build
time via `ldflags`). On every command execution it calls `GET /v1/cli-version`
(a lightweight endpoint on basin-rest); if the response indicates the current
version is below the minimum-supported floor, the CLI prints a one-line
upgrade notice to stderr and continues. Hard-block only if the server returns
`426 Upgrade Required`.

---

## 4. Wire protocol picks

Three protocols, all of which the basin-server already speaks:

**pgwire (PostgreSQL wire protocol v3)** — used for all SQL operations
(`basin sql run`, `basin sql shell`, `basin schema dump/load`, `basin
migrations run/rollback`, `basin debug explain`). The Go side uses the stdlib
`database/sql` interface with `pgx/v5` as the driver (the one acceptable
third-party dependency, given that a pgwire implementation in stdlib-only Go is
prohibitively large). All SQL commands connect per-invocation; no persistent
connection pool in the CLI process.

**REST (basin-rest `/v1/*` admin API)** — used for project lifecycle commands
(`basin projects list/create/delete/connect`) and the CLI version check. Plain
`net/http` from stdlib. All requests carry `Authorization: Bearer <jwt>` from
the stored credential.

**WebSocket** — used for `basin tail`. Plain `golang.org/x/net/websocket`
(part of the Go extended stdlib, not a third-party dependency). The server-side
change-event WebSocket endpoint is defined in ADR 0012; the CLI subscribes and
streams events to stdout.

The choice to use pgwire for SQL (rather than going through basin-rest) is
deliberate: pgwire is the canonical Basin query interface, it returns typed
binary results, and it avoids the JSON encoding overhead that REST imposes.
`basin debug query-shapes` is the only SQL command that goes through REST
rather than pgwire — it hits a dedicated endpoint that wraps the
`basin_stat_statements` view, so the server can enforce access control
independently of the project's connection credentials.

---

## 5. Output format

**Default (human-readable):** ASCII table for result sets; key-value pairs for
single-object responses; progress lines for long operations (migrations, schema
load). Errors print to stderr with a short message and an exit code.

**`--json`:** Every command emits a single JSON object or array to stdout.
Errors emit `{"error": "<message>", "code": "<code>"}` to stderr. Designed for
`jq` pipelines and CI scripts that parse basin-cli output.

**`--quiet`:** Suppresses all output except errors. Exit code is the signal.
Intended for CI steps where only success/failure matters (e.g. `basin
migrations run --quiet && deploy`).

Exit codes follow Unix convention: 0 for success, 1 for operational error
(auth failure, SQL error, network error), 2 for usage error (bad flags,
missing arguments).

---

## 6. Repo structure

```
bas-in/basin-cli/
  cmd/
    basin/
      main.go              — entry point; flag parsing; dispatch
  internal/
    auth/                  — login/logout/refresh; keychain integration
    rest/                  — basin-rest client (net/http wrapper)
    pgwire/                — pgx connection factory; query helpers
    sql/                   — sql run + shell REPL
    migrations/            — migration file discovery; tracking table
    schema/                — dump + load
    debug/                 — query-shapes + explain
    tail/                  — WebSocket subscriber
    output/                — table renderer; JSON emitter; quiet mode
    version/               — embedded version string; version check
  testdata/
    migrations/            — fixture SQL files for integration tests
  .goreleaser.yaml
  .github/
    workflows/
      ci.yaml              — go test ./...; staticcheck
      release.yaml         — GoReleaser + Sigstore sign on tag push
  LICENSE                  — Apache-2.0
  README.md
```

Each `internal/` package has a single responsibility and is tested in
isolation. The `cmd/basin/main.go` file contains only flag wiring and
dispatches into `internal/` packages; it has no business logic of its own. The
`output` package is the single place all terminal rendering lives — no `fmt.Printf`
outside of `output` and `main`.

---

## 7. Release flow

Releases are produced by **GoReleaser** triggered on a semver tag push
(`v*.*.*`) to `main`. Every release binary is **Sigstore-signed** using
`cosign` in keyless mode (OIDC identity from the GitHub Actions runner). The
`.sig` and `.cert` files are uploaded alongside the binaries to the GitHub
release.

**Build targets:**

| OS | Arch | Binary name |
|---|---|---|
| macOS | x86\_64 | `basin_darwin_amd64` |
| macOS | arm64 | `basin_darwin_arm64` |
| Linux | x86\_64 | `basin_linux_amd64` |
| Linux | arm64 | `basin_linux_arm64` |
| Windows | x86\_64 | `basin_windows_amd64.exe` |

**Distribution channels:**

- **GitHub Releases** — all binaries + signatures; primary source of truth.
- **Homebrew tap** (`bas-in/homebrew-basin`) — `brew install vul-os/basin/basin`.
- **apt repo** — `.deb` packages hosted on a GitHub Pages–backed apt repo under `bas-in/apt`.
- **`cargo install`** — `cargo install --git https://github.com/vul-os/basin --bin basin` for developers who prefer source builds.

The Homebrew tap and apt repo are the recommended installation paths for
end-users. `cargo install` is for contributors and CI environments with a
Rust toolchain already present.

Verification instructions are included in the release notes for every tag:

```sh
cosign verify-blob --certificate basin_linux_amd64.cert \
  --signature basin_linux_amd64.sig \
  basin_linux_amd64
```

---

## 8. What's deferred to v0.2

**Admin role splits.** v0.1 treats the authenticated user as the project owner.
v0.2 introduces `--role` aware commands: read-only tokens, scoped tokens for
CI, and the ability to issue sub-tokens for other team members. Depends on
basin-auth's role hierarchy being fleshed out beyond the flat `roles[]` claim.

**Audit log access.** `basin audit tail <project>` and `basin audit export
<project> --since <timestamp>`. Depends on a control-plane audit pipeline
(the Phase 5.16 cloud-side companion items, which are out of scope for this
repo — the cloud roadmap was removed from this tree in `39fb9f64`, since a
cloud roadmap does not belong in the OSS tree).

**IDE plugins.** VSCode extension and JetBrains plugin that wrap basin-cli
commands behind a GUI. These are separate projects; basin-cli's `--json` output
mode is the stable interface they consume.

**Multi-region project routing.** `basin projects list` today shows a single
endpoint per project. v0.2 adds read-replica endpoint selection
(`--replica-region`) once ADR 0004 / ADR 0009 multi-region work ships.

**`basin schema diff <project>`** — compares the live schema against a local
DDL file and outputs a migration plan. Requires a schema-diffing library;
deferred until the migration workflow is validated with real users.

---

## 9. Estimate

A single focused engineer should be able to ship a functional v0.1 in
**2–4 person-weeks**, broken down roughly as:

| Work item | Estimate |
|---|---|
| Project scaffolding, CLI flag wiring, output package | 2 days |
| `basin login` / `logout` / token refresh + keychain integration | 2 days |
| `basin projects` commands (REST client) | 1 day |
| `basin sql run` + `basin sql shell` REPL | 2 days |
| `basin schema dump` / `schema load` | 2 days |
| `basin migrations` (4 sub-commands + tracking table) | 3 days |
| `basin debug query-shapes` + `explain` | 1 day |
| `basin tail` stub (command present, server-side pending) | 0.5 day |
| GoReleaser config + Sigstore signing + Homebrew tap | 1 day |
| Integration tests against a local basin-server | 2 days |
| **Total** | **~16–18 working days** |

The estimate assumes basin-auth and basin-rest are already shipping (they are,
as of Phase 5.10) and the engineer has prior Go experience. The pgwire
dependency on `pgx/v5` is not novel work — it is the same driver the existing
`services/basinctl` Rust prototype would use in Go form.

The 4-week ceiling covers a less experienced Go engineer or a scope that grows
to include the `basin tail` WebSocket path. The 2-week floor is achievable if
`basin sql shell`, `basin schema dump`, and `basin migrations` are treated as
stretch goals for v0.1.1 rather than v0.1.0.
