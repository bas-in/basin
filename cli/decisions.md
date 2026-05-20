# basin-cli — decisions log

Architectural decisions that shape the CLI, captured at point-of-
choosing so future readers (human or agent) know *why* a thing is the
way it is. Append-only — entries get superseded, never deleted.

Format: each entry is dated, names the decision, lists the considered
alternatives, and writes down the "trigger" that would change our
mind. Inspired by [`basin/docs/decisions/`](../basin/docs/decisions/).

---

## 2026-05-08 — Stdlib-only Go binary

**Chosen:** every `.go` file in this repo imports only from the Go
standard library. `go.mod` has no `require` block; `go.sum` doesn't
exist.

**Rejected:** cobra (ergonomic subcommand DSL), urfave/cli (similar),
spf13/viper (config management), testify (assertion lib), pkg/errors
(error wrapping — stdlib `fmt.Errorf("%w", ...)` is enough), pflag
(POSIX-shaped flags).

**Why:** (1) the release binary stays small (~6 MB stripped vs ~12 MB
with cobra alone); (2) supply-chain surface is zero — no transitive
deps to audit, no `dependabot` PRs, no critical-CVE drills; (3) the
flat `switch` dispatch in `main.go` is 30 lines and reads like the
README's command list, so the help text and the code can't drift.

**Trigger to revisit:** a single command needs functionality that
takes more than ~200 lines of stdlib code to replicate cleanly (no
realistic candidate today; HTTP, JSON, TLS, flag parsing, env are
all stdlib).

---

## 2026-05-08 — Two-minor cloud support window

**Chosen:** a CLI on minor *N* supports cloud on *N-1*, *N*, *N+1*.
Outside the window, soft-warn on stderr but never block. Patch drift
always works.

**Rejected:** lock-step (same minor required — too brittle), single-
version (CLI = cloud, breaks anyone on a stale tap), unbounded (any
CLI talks to any cloud — explodes the test matrix).

**Why:** developer machines update on their own schedule (brew, AUR,
manual). The two-minor window matches `fly` / `gh` / `kubectl`
norms and is the smallest window that survives a typical 6-week
release train without forcing every customer to upgrade.

**Trigger to revisit:** customer pain — i.e., support tickets where
the window cost someone hours.

---

## 2026-05-08 — Goreleaser + Sigstore keyless signing

**Chosen:** multi-arch builds (linux/darwin/windows × amd64/arm64) via
`goreleaser`; archives signed via `cosign sign-blob --yes` using the
GitHub Actions OIDC identity. No private keys to manage.

**Rejected:** GPG-signed releases (private-key drama, key-rotation
toil), Sigstore with maintained keys (defeats the keyless win),
manual `go build` per platform (drift between architectures).

**Why:** keyless removes the only secret we'd otherwise have to
rotate. The signing identity binds each release to a specific
GitHub Actions workflow at a specific tag — verifiable end-to-end
by any user with `cosign verify-blob`.

**Trigger to revisit:** Sigstore project pivot away from keyless, or
a customer in a regulated environment that requires HSM-backed
signing.

---

## 2026-05-15 — Backend-first product surface

**Chosen:** a `basin X` command exists only if `/v1/X` (or an engine
primitive) exists upstream. The CLI is a thin, opinionated shell over
the platform's public contract.

**Rejected:** "CLI is a product surface" model where the CLI invents
verbs the dashboard doesn't have. Slower, branchy, fragments the
docs.

**Why:** every divergence costs ongoing maintenance (docs, tests,
support). Keeping the CLI behind the cloud means a new cloud
endpoint is the only way to add a new top-level command — discipline
is enforced by the dispatch table being short.

**Exceptions** (called out explicitly in `ROADMAP.md`):
- `basin init` / `basin link` / `basin status` — directory-context
  scaffolding is inherently client-side.
- `basin gen types` — assembled from `information_schema` queries
  via the existing `/sql/query` endpoint; no new endpoint needed.
- `basin completion` / `basin config` — shell + local config.

**Trigger to revisit:** a customer-driven pull where a CLI-only
workflow saves enough developer-minutes to justify the
documentation / test cost. Concrete number not picked because there
is no candidate today.

---

## 2026-05-19 — Directory-mode for project context

**Chosen:** `./basin/config.toml` + `./basin/migrations/` +
`./basin/seed.sql`. Every `cmd_*.go` falls back to
`loadWorkingProject(cwd)` when `--project=` isn't provided. CLI
walks up the directory tree to find the config file (same pattern as
`git`, `cargo`, `fly`).

**Rejected:** `$HOME/.config/basin/default-project` (single global
default, breaks for engineers who work on multiple projects),
forcing `--project=` on every invocation (existing UX, repetitive),
environment-variable-only (`$BASIN_PROJECT`, easy to forget).

**Why:** matches every adjacent tool (`fly`, `flyctl`, `gh`,
`vercel`, `cargo`) and gives a natural place to store local
migration files and seed data that need to be checked in alongside
the project's repo.

**Trigger to revisit:** if `.basin/` directory pollution becomes a
real complaint, fall back to a per-cwd entry in
`$HOME/.config/basin/projects.json` keyed by repo root.

---

## 2026-05-19 — `gen types` is CLI-assembled, not a `/v1/types` endpoint

**Chosen:** the `gen types <lang>` subcommand assembles output by
querying `information_schema.columns` via `POST /v1/projects/{ref}/sql/query`
and walking the result. Type mapping table lives in the CLI repo
(`gen_types_map.go`).

**Rejected:** a dedicated `GET /v1/projects/{ref}/types?lang=ts`
endpoint that returns ready-to-write source code.

**Why:** (1) the type mapping is *opinionated* per language (e.g.,
how nullable maps to `T | null` vs `Optional[T]`, how `vector(N)`
maps to `number[]` vs `np.ndarray`); the right place to keep
opinions is the same repo where the language-specific golden files
live. (2) Keeping it client-side means the same SQL surface a
customer can run themselves drives codegen — no magic that needs
its own docs. (3) Future languages don't require a cloud release.

**Trigger to revisit:** if three+ CLIs (Python, Rust, JS) start
re-implementing the same mapping, lift the canonical table to a
versioned JSON document under `/v1/types/mappings` and let each CLI
fetch it.

---

## 2026-05-19 — Parking lot for engine/cloud gaps

**Chosen:** keep `cmd_realtime.go`, `cmd_functions.go`,
`cmd_storage_buckets.go`, `cmd_tx.go`, `cmd_restore_pitr.go` as
**named-but-unimplemented** entries in TASKS.md Tier 23. Don't ship
them as no-op stubs that error out — that's a worse UX than the
command not existing.

**Rejected:** shipping stubs with `not implemented yet` errors
(misleading — looks like product surface to skimmers), inventing CLI-
only workarounds (e.g., a `cmd_realtime.go` that polls — slow,
fragile, sets a precedent for shadow-implementations).

**Why:** the right time to design a command's UX is when its
backend lands, not before. Holding the names in TASKS.md prevents
bike-shed-revisiting later; not shipping them prevents lying.

**Trigger to revisit:** any individual blocker in
[`ROADMAP.md` §Parking lot](./ROADMAP.md) clears.

---

## 2026-05-19 — `--json` is the contract; human output can churn

**Chosen:** every command that prints structured data has a
documented `--json` shape (`// JSON shape: { ... }` annotation above
every `printJSON` call site). Schema-changes to `--json` go through
the same one-minor deprecation as `/v1/*` changes.

**Rejected:** treating human output as a stability surface (kills
all output polish), no schema discipline (downstream scripts break
silently).

**Why:** agents and CI scripts consume `--json`; humans tolerate
formatting churn. Locking the structured shape costs nothing if
done from day one and prevents a future migration nightmare.

**Trigger to revisit:** never — this is a one-way door.

---

## 2026-05-19 — Tests-required-to-land

**Chosen:** every new `cmd_*.go` ships with `cmd_*_test.go` covering
the matrix in TASKS.md Tier 24 (arg-parse error, `--json` shape lock,
`--quiet`, missing flag, one happy path, one server-error mapping).
PR fails CI without the matching test file.

**Rejected:** "tests later" (never happens), test-by-sampling (gaps
accumulate), integration-only (slow + hides per-command regressions).

**Why:** the CLI is the user-facing surface; a regression here ships
to every install. The 41-test foundation from extraction stays the
floor — Tier 8+ work pushes it higher.

**Trigger to revisit:** the test-write toil becomes the binding
constraint on shipping new commands (no evidence yet — the existing
test patterns are template-able in ~50 lines per new command).

---

## 2026-05-19 — Autonomous build-out: 2 sonnet agents, 15-min heartbeat

**Chosen:** Tier 8–25 work is dispatched to **max 2 Sonnet sub-agents
in parallel** per dispatcher tick, with a 15-minute `ScheduleWakeup`
heartbeat for up to 4 hours of autonomous progress. The dispatcher
session is *this* Opus session.

**Rejected:** single-agent serial (under-utilises the parallel
opportunity for independent tasks like `cmd_init.go` + `gen_types_map.go`),
unbounded agent fan-out (risks file-collision on shared `client.go` /
`main.go` edits — 2 is the empirically-safe ceiling for this repo's
file layout), no heartbeat (loses progress when agents stall).

**Why:** the Tier 8+ tasks are mostly *independent* (one new file
each, plus an additive `commands()` registration in `main.go`). Two
parallel agents on non-overlapping file sets is the sweet spot. The
15-min wakeup is a safety net — agents notify on completion, so the
wakeup mostly fires as a fallback heartbeat for stalled work or
between-batch dispatch.

**Concurrency rules** the dispatcher follows:
1. Never give two agents tasks that touch the same file (including
   shared edits to `main.go` `commands()` table — collapse those
   into a single agent's batch).
2. Prefer "one new `cmd_X.go` + matching test file + matching
   `Client.X*` methods + the `main.go` registration" as one agent's
   scope. Self-contained = parallel-safe.
3. After each batch lands, run `go build ./... && go test -race
   -count=1 ./...` from the dispatcher session to catch
   cross-batch interactions before the next dispatch tick.

**Trigger to revisit:** if file-collision rate exceeds ~1 per 10
batches, drop to single-agent serial. If parallel completion is
trivially deconflictable for ≥3 batches in a row, try 3 agents.

---

---

## 2026-05-19 — Migrations CLI sticks with `/admin/v1/projects/{ref}/migrations`

**Chosen:** all migration calls from the CLI use `/admin/v1/projects/{ref}/migrations`
(list, apply, new, get, diff, rollback). This includes the migration drift
check inside `cmd_status.go`.

**Context:** the cloud router (`basin-cloud/backend/internal/router/server.go`)
mounts the migration handlers under `r.Route("/admin/v1/projects/{ref}", …)`
at line ~1163 — `ListProjectMigrations`, `DiffProjectMigrations`,
`GetProjectMigration`, `RollbackProjectMigration`. The `/v1/projects/{ref}`
route group at line ~610 does **not** duplicate those routes. A previous
draft of this entry chose `/v1/` as the "canonical" path on the bet that
the cloud would mirror them under `/v1/` shortly; reverted because
shipping a CLI that 404s against the production cloud is a worse failure
mode than path-family asymmetry.

**Rejected:** (1) putting `/v1/` in the CLI and filing a parallel cloud-side
PR to add the alias — couples CLI shipping to a cross-repo round-trip we
don't need; (2) shipping a fallback that tries `/v1/` then `/admin/v1/` —
twice the requests, hides the mismatch instead of fixing it.

**Endpoint set settled on:**
- `GET  /admin/v1/projects/{ref}/migrations` — list
- `POST /admin/v1/projects/{ref}/migrations/{id}/rollback` — apply / rollback
- `GET  /admin/v1/projects/{ref}/migrations/{id}` — get
- `GET  /admin/v1/projects/{ref}/migrations/diff` — diff

**Trigger to revisit:** if the cloud router migrates these handlers to the
`/v1/projects/{ref}` group (e.g., as part of a public-API consolidation
pass), bump CLI to match in the same commit that the cloud route lands.
Track via a `// TODO(cloud-route-move):` annotation at each call site so a
later grep finds them all.

---

---

## 2026-05-20 — tungstenite for WebSocket multi-table realtime subscribe (T26.2/T26.3)

**Chosen:** `tungstenite = { version = "0.24", default-features = false, features = ["handshake", "rustls-tls-webpki-roots"] }` (sync, no tokio) as the sole dep for the `--multi` WS path in `realtime subscribe`.

**Rejected:** (1) hand-rolled RFC-6455 — SHA-1 key exchange + masking + frame parsing is ~300 lines of fiddly code with no test oracle; (2) `tokio-tungstenite` — brings in the full tokio async runtime, incompatible with our blocking `reqwest` thread model and doubles binary size; (3) `fastwebsockets` — tokio-only, same objection.

**Why:** tungstenite is the de facto sync WS library in the Rust ecosystem (the async tokio-tungstenite is built on top of it). The `rustls-tls-webpki-roots` feature avoids an OpenSSL system dep, matching the choice already made for `reqwest`. The `handshake` feature (which includes RFC-6455 key negotiation via `sha1` + `data-encoding`) is required for client connections. Binary size impact: +~0.5 MB stripped vs. hand-rolling ~0 MB, justified by correctness (ping/pong, masking, close handshake all handled).

**Integration gap:** a full in-test WS server stub (RFC-6455 handshake + frame exchange) would require a real tungstenite server in the test binary, which is heavy for a unit-test file. Frame-building, routing, and line-formatting logic is unit-tested directly (`handle_ws_text`, `build_ws_url`, frame shape assertions). End-to-end connect→subscribe→event path is an integration gap — covered once the engine's WS endpoint is available for testing.

**Trigger to revisit:** if the async CLI rewrite lands (T-future), swap for `tokio-tungstenite` in the same commit.

*Last updated: 2026-05-20.*
