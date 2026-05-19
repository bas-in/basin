# basin-cli — roadmap

Long-term vision for the `basin` command-line interface. The CLI is the
terminal-first surface for everything the cloud and engine already do:
if it's a real capability of the platform, it should be one short
invocation away from a developer's shell.

This is the *destination*, not the build plan. Concrete, ordered,
agent-pickable units of work live in [`TASKS.md`](./TASKS.md) — every
roadmap item links to one or more tiers there.

> **Cross-references**
> - Engine capabilities: `../basin/CAPABILITIES.md`
> - Cloud product roadmap: `../basin-cloud/CLOUD_ROADMAP.md`
> - Today's CLI surface: [`README.md`](./README.md)

---

## Today's surface (shipped)

`login` · `logout` · `whoami` · `orgs` · `projects` · `sql` · `tables`
(read-only) · `migrations` (list, apply) · `snapshots` · `logs` ·
`secrets` · `tokens` · `config` · `completion` · `version` · `help`.

Foundations: stdlib-only Go binary, `--json` everywhere, two-minor
support window vs cloud, soft-warn out-of-window, opt-in telemetry,
self-update check, shell completions, multi-arch `goreleaser`
pipeline, Sigstore keyless signing.

---

## Prioritisation rule

Work is ranked by **backend-readiness**:

- **P0** — cloud `/v1/*` endpoint exists *and* engine supports it.
  Ship now; only CLI work needed.
- **P1** — cloud + engine support it, but the CLI needs new local
  scaffolding (config files, schema diffing, codegen) on top of the
  HTTP surface.
- **P2** — partially supported (cloud has the surface, engine has a
  partial implementation, or vice versa). Ship a working subset, flag
  the gaps in `--help`.
- **P3** — parking lot. Engine or cloud doesn't expose the capability
  yet; opening a CLI command would lie. Tracked here so we don't
  forget the shape we want when the backend lands.

Within each priority, ordering follows daily-driver value: the verbs a
developer types twenty times a day come before the ones they type
twice a quarter.

---

## Capability areas

### 1. Project context — P1

The CLI today is stateless: every command takes `--project=<slug>`.
The endgame is a directory-scoped context (`./basin/config.toml` +
`./basin/migrations/` + `./basin/seed.sql`) so a developer in a
checked-out repo never has to repeat the project flag.

- `basin init` — scaffold a fresh project directory
- `basin link --project=<ref>` — bind cwd to a remote project
- `basin status` — show linked project, drift, pending migrations,
  pause state, current branch
- `basin unlink` — remove the binding

Tracked: TASKS.md Tier 8.

---

### 2. Database workflow (`db` namespace) — P0 + P1

The most-used loop: change a schema locally, push it, pull what the
team merged, reset for a fresh test run, hand the DSN to a driver.

Backed entirely by existing cloud endpoints
(`/migrations/diff`, `/migrations/{id}/rollback`, `/pgwire`,
`/pgwire/reveal`, `/pgwire/rotate`, `/sql/query`):

- `basin db push` — apply local `./basin/migrations/*` to remote
- `basin db pull` — materialise remote schema as local SQL
- `basin db diff [--schema=public]` — generate a new migration from
  the live-vs-local diff
- `basin db reset` — drop + re-apply all migrations + seed
- `basin db url [--reveal]` — print the pgwire DSN (rotated key
  optional)
- `basin db dump [--data-only|--schema-only]` — pg_dump-shaped export
- `basin db lint` — run the engine's SQL-compat linter against
  `./basin/migrations/`

Tracked: TASKS.md Tier 9.

---

### 3. Migration completeness — P0

`migrations list` + `migrations apply` ship today. The other half of
the surface — already wired in cloud — is:

- `basin migrations new <name>` — timestamp-prefixed file
- `basin migrations diff` — surface `/migrations/diff`
- `basin migrations get <id>`
- `basin migrations rollback <id>` — surface `/migrations/{id}/rollback`

Tracked: TASKS.md Tier 10.

---

### 4. Branches (preview environments) — P0

Cloud ships full branch CRUD + merge + events
(`/projects/{id}/branches/*`). Zero CLI today.

- `basin branches list`
- `basin branches create <name> [--from=<ref>]`
- `basin branches get <ref>`
- `basin branches merge <ref>`
- `basin branches delete <ref>`
- `basin branches events --follow`

Tracked: TASKS.md Tier 11.

---

### 5. Tables, data, RLS — P0

Today's `tables` subcommand only lists / describes / paginates rows.
Cloud has the full surface; we mirror it:

- `basin tables create <name> --schema=<sql-or-json>`
- `basin tables alter <name> --add-column=... --drop-column=...`
- `basin tables drop <name>`
- `basin tables import-csv <name> < file.csv`
- `basin tables export-csv <name> > file.csv`
- `basin rows insert/update/delete <name>`
- `basin rls enable <table>` / `disable <table>`
- `basin policies list/create/drop <table>`

Tracked: TASKS.md Tier 12.

---

### 6. Project keys + connection strings — P0

Cloud separates org-level PATs (already wired as `basin tokens`) from
**project-scoped API keys** and **pgwire credentials**. The CLI is
missing both:

- `basin api-keys list/create/rotate/revoke` — `/projects/{id}/api-keys`
- `basin pgwire show` — `GET /pgwire`
- `basin pgwire reveal` — `POST /pgwire/reveal`
- `basin pgwire rotate` — `POST /pgwire/rotate`
- `basin pgwire engine-keys list/rotate` — internal engine keys

Tracked: TASKS.md Tier 13.

---

### 7. Backups — P0

`snapshots` ships, but only covers part of the surface. Cloud has
backup *policy* + *restore jobs* under `/backups/*`:

- `basin backups policy get/set`
- `basin backups snapshots list/create/expire`
- `basin backups restore --from=<snapshot> [--into=<branch>]`
- `basin backups restore-jobs list`

Tracked: TASKS.md Tier 14.

---

### 8. Type generation — P1

The single highest-leverage developer feature after `db push`. Drives
type-safe client code without manually mirroring the schema. Backed
by `information_schema` + `/tables`+`/columns`; assembled CLI-side.

- `basin gen types typescript` — write `database.ts`
- `basin gen types go` — write `database.go`
- `basin gen types python` — write `database.py` (Pydantic)
- `basin gen types --watch` — re-emit on every push

Tracked: TASKS.md Tier 15.

---

### 9. Org & member management — P0

- `basin members list/invite/remove/role`
- `basin invitations list/resend/revoke`
- `basin orgs create/update/delete`
- `basin orgs branding get/put`

Tracked: TASKS.md Tier 16.

---

### 10. Operations & observability — P0

Cloud already exposes everything below; surfacing it in the terminal
is the ops-engineer happy path.

- `basin domains add/verify/cert/list/remove`
- `basin webhooks list/create/test/redeliver/delete`
- `basin alerts rules list/create/silence/unsilence`
- `basin alerts events list`
- `basin audit list [--export=<dest>]`
- `basin activity list`
- `basin metrics --project=<ref> [--range=24h]`
- `basin erd export <project> > erd.svg`

Tracked: TASKS.md Tier 17.

---

### 11. Engine knobs — P0/P2

Project-scoped runtime configuration.

- `basin engine-pin get/put/delete` — `/engine-version/pin`
- `basin extensions list` — `/extensions/catalog`
- `basin oauth-providers list/set/delete` — `/oauth-providers`
- `basin email templates get/put/delete/test`
- `basin email allowance` — quota lookup

Tracked: TASKS.md Tier 18.

---

### 12. Saved queries & history — P0

- `basin queries save/list/get/fork/delete`
- `basin queries history [--clear]`

Tracked: TASKS.md Tier 19.

---

### 13. BYO surfaces — P0/P2

For customers running their own buckets, KMS, or engine binaries:

- `basin byo bucket get/put/probe/delete`
- `basin byo kms get/put/rotate/verify/audit/delete`
- `basin byo engine get/put/probe/delete`

Tracked: TASKS.md Tier 20.

---

### 14. Enterprise auth — P2

- `basin saml get/put/test/enable/disable`
- `basin scim config get/tokens create/revoke`
- `basin oauth-apps list/create/rotate/disable/enable/delete`

Tracked: TASKS.md Tier 21.

---

### 15. Project transfers — P0

- `basin transfers list/create/cancel/accept/decline`
- `basin orgs ownership-transfers list/create/cancel/accept/decline`

Tracked: TASKS.md Tier 22.

---

## Parking lot — engine or cloud must ship first (P3)

These commands would lie if we shipped them now. We hold them until
the backing capability exists, but design the shape upfront so the
naming doesn't churn.

| Command (eventual) | Blocked on |
|---|---|
| `basin realtime subscribe <table>` | engine `LISTEN`/`NOTIFY` — `🚫` on the capabilities matrix (no pub/sub today) |
| `basin functions deploy / serve / list / delete` | cloud Phase 8 (V8 edge functions, "8–12 weeks once unblocked") |
| `basin storage buckets list/create/upload/download` | object-storage product distinct from BYO-bucket-for-Parquet; not on cloud roadmap |
| `basin tx begin / commit / rollback` (interactive) | engine transactions are `◻️ planned`, single-shard when shipped |
| `basin restore --as-of=<timestamp>` | engine PITR cross-DML rollback is v0.2 work |

Tracked: TASKS.md Tier 23 — design-only, no implementation until the
blocker clears.

---

## Cross-cutting principles

### Backend-first, never CLI-first
A `basin X` command exists only when `/v1/X` (or an engine primitive)
exists. The CLI is a thin, opinionated shell over the platform's
public contract — never a place where new product surface gets
invented. The two exceptions (`init`, `link`, `gen types`) are pure
client-side concerns and explicitly called out as such.

### Stdlib only
Every new file in this repo is `import "..."` from the Go standard
library. No cobra, no urfave/cli, no spf13/viper, no testify, no
mocking framework. The release binary stays small and the supply-
chain surface stays at zero. If a new feature *seems* to need a dep,
the task description must justify it.

### `--json` is the contract
Every command that prints structured data ships a documented JSON
shape. UI output is for humans; agents and downstream scripts read
`--json`. Schema changes are gated on a deprecation flag the same way
breaking `/v1/*` changes are.

### Honour `--quiet` and `--no-color`
Non-essential prose (progress dots, hints, "did you know") goes
silent under `-q`. ANSI escapes go off under `--no-color`. Errors
still print.

### Support window
A CLI on minor version *N* must work against a cloud on *N-1*, *N*,
or *N+1*. This is a contract, not aspiration — every PR that touches
HTTP calls runs the contract test against a stub server pinned to
N-1 + N + N+1 shapes.

---

## Testing posture

See TASKS.md Tier 24 for the test plan. Summary:

- **Every new `cmd_*.go` ships with `cmd_*_test.go`** covering
  arg-parse error path, `--json` shape lock, `--quiet` suppression,
  missing-flag error, and one happy-path round trip against an
  `httptest.Server` stub.
- **Every new `Client.X()` method ships with `client_test.go` rows**
  covering envelope unwrap, error mapping, query-string assembly,
  and at least one regression of the actual cloud's response shape.
- **Integration tests** in `integration_test.go` drive
  `run([]string{...})` end-to-end through the dispatch path so the
  flag plumbing + telemetry + version-check envelopes stay wired.
- **Contract tests** (Tier 24) pin the wire shape between CLI and
  cloud — recorded fixtures from a real `basin-cloud` boot, replayed
  by the CLI test suite. Updated whenever cloud minor ticks.
- **Cross-CLI-version replays** — record fixtures under
  `tests/contract/N-1/`, `tests/contract/N/`, `tests/contract/N+1/`
  and assert every CLI command tolerates all three.
- `go test -race -count=1 ./...` is green on every PR in CI on
  `ubuntu-latest` + `macos-latest`.

The bar is: **a new command without tests is not landed**. The
existing 41-test foundation is the floor, not the ceiling.

---

## Out of scope

- Building anything *new* in the engine or the cloud. If you find
  yourself wanting to add an endpoint while writing a CLI command,
  stop and open the issue in the upstream repo first.
- Replacing the dashboard. Dashboard and CLI are peer surfaces over
  the same `/v1/*` contract; neither one front-runs the other.
- Plugin systems / third-party command authoring. The dispatch table
  in `main.go` is a flat switch on purpose; we'll revisit only if a
  named external project asks for it with a concrete use case.

---

*Last updated: 2026-05-19.*
