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
> - Cloud product roadmap: `../basin-cloud/ROADMAP.md` (+ `TASKS.md` for agent-dispatchable work)
> - Today's CLI surface: [`README.md`](./README.md)

---

## Today's surface (shipped)

All Tiers 8–27 from TASKS.md are complete. The full command surface:

`login` · `logout` · `whoami` · `orgs` (CRUD + branding + ownership-transfers) ·
`members` · `invitations` · `projects` (CRUD + pause/resume + transfers) ·
`init` · `link` · `status` · `unlink` · `sql` · `tx` (interactive REPL) ·
`tables` (CRUD + CSV import/export) · `rows` · `rls` + `policies` ·
`migrations` (list/apply/new/get/diff/rollback) · `db` (push/pull/diff/reset/url/dump/lint) ·
`branches` (CRUD + merge + events) · `api-keys` · `pgwire` (show/reveal/rotate + engine-keys) ·
`backups` (policy + snapshots + restore + restore-jobs) · `snapshots` ·
`gen types` (ts/go/py/ruby/rust/java/csharp/php/dart/swift + --watch) ·
`dev` (local engine: binary + Docker modes) · `stop` ·
`functions` (deploy/list/logs/delete) · `rpc` ·
`realtime subscribe` (SSE single-table; WS multi-table deferred) ·
`storage` (buckets + upload/download/list/rm/sign + policy) ·
`secrets` · `tokens` · `api-keys` · `domains` · `webhooks` (inbound + outbound) ·
`alerts` · `audit` · `activity` · `metrics` · `logs` · `erd` ·
`index-advisor` · `explain` · `replication-slots` ·
`engine-pin` · `extensions` · `oauth-providers` · `email` ·
`queries` (save/list/get/fork/delete/history) ·
`byo` (bucket/kms/engine) · `saml` · `scim` · `oauth-apps` ·
`migrate-from-pg` · `dump` · `restore` ·
`config` · `completion` · `docs` · `version` · `help`.

Foundations: Rust binary, `--json` everywhere, two-minor support window
vs cloud, soft-warn out-of-window, opt-in telemetry, self-update check,
shell completions (bash/zsh/fish), multi-arch `goreleaser` pipeline,
Sigstore keyless signing. 1321 tests (unit + integration + contract +
golden snapshot + JSON schema locks + fuzz). Stdlib-only design replaced
by Rust for the full command surface.

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

## Shipped capability areas (Tiers 8–27 complete)

All 15 original capability areas plus the Realtime/RPC tier and
migrate-from-pg are shipped. See TASKS.md for per-tier detail.

### ✅ 1. Project context (Tier 8)
`init` · `link` · `status` · `unlink` — directory-scoped project
binding with `./basin/config.toml` + migration walk-up.

### ✅ 2. Database workflow `db` namespace (Tier 9)
`push` · `pull` · `diff` · `reset` · `url` · `dump` · `lint`

### ✅ 3. Migration completeness (Tier 10)
`new` · `get` · `diff` · `rollback` (extending existing `list`/`apply`)

### ✅ 4. Branches (Tier 11)
`list` · `create` · `get` · `merge` · `delete` · `events --follow`

### ✅ 5. Tables, data, RLS (Tier 12)
Full table CRUD + CSV · `rows insert/update/delete` · `rls enable/disable` + policies

### ✅ 6. Project keys + pgwire credentials (Tier 13)
`api-keys` CRUD · `pgwire show/reveal/rotate` + `engine-keys`

### ✅ 7. Backups (Tier 14)
`policy get/set` · `snapshots list/create/expire` · `restore` · `restore-jobs`

### ✅ 8. Type generation (Tier 15)
`gen types ts/go/py/ruby/rust/java/csharp/php/dart/swift` + `--watch`

### ✅ 9. Org & member management (Tier 16)
`members` CRUD · `invitations` · `orgs create/update/delete/branding`

### ✅ 10. Operations & observability (Tier 17)
`domains` · `webhooks` (inbound + outbound) · `alerts` · `audit` ·
`activity` · `metrics` · `erd` · `index-advisor` · `explain` · `replication-slots`

### ✅ 11. Engine knobs (Tier 18)
`engine-pin` · `extensions` · `oauth-providers` · `email templates + allowance`

### ✅ 12. Saved queries & history (Tier 19)
`queries save/list/get/fork/delete/history`

### ✅ 13. BYO surfaces (Tier 20)
`byo bucket/kms/engine`

### ✅ 14. Enterprise auth (Tier 21)
`saml` · `scim` · `oauth-apps`

### ✅ 15. Project transfers (Tier 22)
`projects transfers` · `orgs ownership-transfers`

### ✅ 16. Realtime & RPC (Tier 26)
`realtime subscribe` (SSE) · `rpc <fn>` — WS multi-table blocked on tungstenite dep decision

### ✅ 17. migrate-from-pg (Tier 27)
`migrate-from-pg` — guided Postgres → Basin migration

---

## Remaining open work (honest)

### Upstream-blocked items (do not implement until the blocker clears)

| Command | Blocked on |
|---|---|
| `realtime subscribe --multi` (WS multi-table) | tungstenite dep decision (T26.2/T26.3). Record in decisions.md first. |
| `functions serve` (hot-reload dev loop) | Local Wasm runtime host; unblocks once `basin dev` is stable in practice. |
| `login --browser` (OAuth device flow) | Cloud OAuth device endpoint not yet in `backend-rs/src/handlers/`. |
| `restore --as-of=<timestamp>` (PITR) | Engine physical-file GC for cross-DML rollback is v0.2 work. |

### Distribution milestones (user-gated)

| Item | Gate |
|---|---|
| First Homebrew tap publish | Needs v0.1.0 tag push + `bas-in/homebrew-tap` repo created |
| AUR `basin-bin` package | Needs AUR account + `AUR_KEY` secret |
| Scoop bucket | Needs `vul-os/scoop-bucket` repo + `SCOOP_TAP_TOKEN` secret |
| `ghcr.io/vul-os/basin` Docker image | Fires automatically on first v* tag |
| `basin dev --docker` zero-config | Needs `ghcr.io/vul-os/basin-server` published |
| Cross-version replay fixtures (N-1/N/N+1) | Future work once cloud minor version ticks |

### Low-priority backlog

- Dedicated `ip-allow` command (covered adequately by `tokens create/patch --ip-allowlist`; implement only if user demand warrants).
- Per-command `man` pages (requires dispatch refactor to `clap::Command` tree; see decisions.md 2026-05-23).

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

*Last updated: 2026-06-14. Tiers 8–27 complete. Remaining work: 4 upstream-blocked items + distribution milestones (user-gated).*
