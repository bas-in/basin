# basin-cli — Parity matrix vs Neon CLI + Supabase CLI

Audited against:
- `src/commands/*.rs` (command surface as of the current HEAD)
- `../basin-cloud/backend-rs/src/handlers/` (cloud `/v1/*` endpoints)
- `../basin/CAPABILITIES.md` (engine capabilities)
- `ROADMAP.md` + `TASKS.md` (completed tiers 8–27)
- Neon CLI (`neonctl`) public docs + `neonctl --help` surface
- Supabase CLI `supabase --help` surface

Priority bands:
- **P0** — cloud + engine support it; only CLI work needed. Ship now.
- **P1** — cloud + engine support it, but CLI needs new local scaffolding (config
  files, schema-diff tooling, local stack launch). Slightly heavier lift.
- **P2** — platform gap: cloud or engine does not fully expose the capability yet.
  Document the backend requirement; do not ship stub commands.

---

## Capability matrix

| Capability | basin-cli has it? | Neon has it | Supabase has it | Gap / Priority |
|---|---|---|---|---|
| **Auth** | | | | |
| `login` (token paste) | ✅ `login` | ✅ | ✅ | — |
| `logout` | ✅ `logout` | ✅ | ✅ | — |
| `whoami` | ✅ `whoami` | ✅ | ✅ | — |
| Oauth / browser-flow login | ❌ | ✅ | ✅ | P2 — needs cloud OAuth device flow |
| **Orgs / teams** | | | | |
| Orgs list / get / create / update / delete | ✅ `orgs list/get/create/update/delete` | ✅ | ✅ (teams) | — |
| Org branding get / put | ✅ `orgs branding get/put/delete` | ❌ | ❌ | Basin-only |
| Members list / invite / remove / role | ✅ `members list/invite/remove/role` | ✅ | ✅ | — |
| Invitations list / resend / revoke | ✅ `invitations list/resend/revoke` | ❌ | ✅ | — |
| **Projects** | | | | |
| Projects list / get / create / delete | ✅ `projects list/get/create/delete` | ✅ | ✅ | — |
| Pause / resume | ✅ `projects pause/resume` | ✅ | ❌ | — |
| Project init (directory scaffold) | ✅ `init` | ❌ | ✅ | — |
| Link / unlink cwd to project | ✅ `link` / `unlink` | ❌ | ✅ | — |
| Status (drift, pending migrations) | ✅ `status` | ❌ | ✅ | — |
| Project transfers | ✅ `projects transfers` | ❌ | ❌ | Basin-only |
| **SQL exec** | | | | |
| Inline SQL query | ✅ `sql -c "SELECT …"` | ✅ | ✅ | — |
| Interactive REPL / `tx` | ✅ `tx` | ✅ | ✅ | — |
| `dump` (pg_dump-shaped export) | ✅ `dump` | ✅ | ✅ | — |
| `restore` (replay dump) | ✅ `restore` | ✅ | ✅ | — |
| `migrate-from-pg` (guided Postgres → Basin) | ✅ `migrate-from-pg` | ❌ | ❌ | Basin-only |
| **Tables / schema** | | | | |
| Tables list / describe | ✅ `tables list/describe` | ✅ | ✅ | — |
| Tables create / alter / drop | ✅ `tables create/alter/drop` | ❌ | ✅ | — |
| CSV import / export | ✅ `tables import-csv/export-csv` | ❌ | ❌ | Basin-only |
| Row insert / update / delete | ✅ `rows insert/update/delete` | ❌ | ✅ | — |
| **Migrations** | | | | |
| Migrations list | ✅ `migrations list` | ❌ | ✅ | — |
| Migrations apply / up | ✅ `migrations apply` | ❌ | ✅ | — |
| Migrations new (create file) | ✅ `migrations new <name>` | ❌ | ✅ | — |
| Migrations get | ✅ `migrations get <id>` | ❌ | ❌ | — |
| Migrations diff | ✅ `migrations diff` | ❌ | ✅ | — |
| Migrations rollback | ✅ `migrations rollback <id>` | ❌ | ✅ | — |
| `db push` (apply local → remote) | ✅ `db push` | ❌ | ✅ | — |
| `db pull` (materialise remote → local) | ✅ `db pull` | ❌ | ✅ | — |
| `db diff` (generate migration from live drift) | ✅ `db diff` | ❌ | ✅ | — |
| `db reset` (drop + re-apply + seed) | ✅ `db reset` | ❌ | ✅ | — |
| `db lint` (SQL-compat lint) | ✅ `db lint` | ❌ | ❌ | Basin-only |
| `db dump` (schema + data export) | ✅ `db dump` | ❌ | ✅ | — |
| **Connection string** | | | | |
| `db url` (print pgwire DSN) | ✅ `db url [--reveal] [--rotate]` | ✅ (`connection-string`) | ✅ | — |
| pgwire show / reveal / rotate | ✅ `pgwire show/reveal/rotate` | ✅ | ✅ | — |
| Engine keys list / rotate | ✅ `pgwire engine-keys list/rotate` | ❌ | ❌ | Basin-only |
| **Branching (preview environments)** | | | | |
| Branches list / create / get / delete | ✅ `branches list/create/get/delete` | ✅ | ✅ (preview branches) | — |
| Branches merge | ✅ `branches merge` | ✅ | ❌ | — |
| Branch events --follow | ✅ `branches events [--follow]` | ✅ | ❌ | — |
| **Project API keys** | | | | |
| API keys list / create / rotate / revoke | ✅ `api-keys list/create/rotate/revoke` | ❌ | ✅ | — |
| **Tokens (org-scoped PATs)** | | | | |
| Tokens list / create / revoke / show | ✅ `tokens list/create/revoke/show` | ✅ | ✅ | — |
| Token IP allowlist (on create / patch) | ✅ (flag on `tokens create/patch`) | ✅ | ❌ | — |
| Dedicated IP-allowlist command | ❌ | ✅ `ip-allow` | ❌ | P0 — backed by `PATCH /v1/orgs/:slug/api-tokens/:id` (ip_allowlist field). Low-DX impact; the `tokens` create/patch flags cover the use case |
| **Secrets / env-overrides** | | | | |
| Secrets list / set / remove | ✅ `secrets list/set/remove` | ❌ | ✅ | — |
| **Functions** | | | | |
| Functions deploy (compile TS/JS → Wasm + upload) | ✅ `functions deploy` | ❌ | ✅ | — |
| Functions list | ✅ `functions list` | ❌ | ✅ | — |
| Functions logs | ✅ `functions logs` | ❌ | ✅ | — |
| Functions delete | ✅ `functions delete` | ❌ | ✅ | — |
| Functions `serve` (local dev server) | ❌ | ❌ | ✅ | P2 — needs local Wasm runtime host; the W2 `fn/v1/:name` data-plane route can serve invocations already, but a local file-watch + hot-reload dev loop needs the engine running locally. See local-dev-stack section. |
| `rpc <fn>` (invoke SQL/Wasm function) | ✅ `rpc <fn>` | ❌ | ✅ (`functions invoke`) | — |
| Inbound webhooks list / create / delete / rotate | ❌ | ❌ | ❌ | P0 — backed by `/v1/projects/:ref/inbound-webhooks/*` (fully wired in cloud T-094). CLI verb would be `basin webhooks inbound list/create/delete/rotate`. TASKS.md T26 footnote calls this out as a follow-on to `webhooks`. |
| **Realtime** | | | | |
| Realtime SSE subscribe (single table) | ✅ `realtime subscribe <table>` | ❌ | ✅ | — |
| Realtime WS subscribe (multi-table) | ❌ (WS deferred — needs tungstenite) | ❌ | ✅ | P0 — engine 5.11.R3 ships WS; CLI blocked on tungstenite dep decision (decisions.md). Cloud route: `GET /realtime/v1/ws/:project`. |
| **Object storage** | | | | |
| Storage buckets list / create / delete | ✅ `storage buckets list/create/delete` | ❌ | ✅ | — |
| Storage upload / download / list / rm / sign | ✅ `storage upload/download/list/rm/sign` | ❌ | ✅ | — |
| Storage object policies (RLS-style) | ❌ | ❌ | ✅ | P0 — `GET/PUT /v1/projects/:ref/storage-objects/policy` (cloud T-135). CLI verb: `basin storage policy get/put`. |
| **Row-level security** | | | | |
| RLS enable / disable | ✅ `rls enable/disable` | ❌ | ✅ | — |
| RLS policies list / create / drop | ✅ `rls policies list/create/drop` | ❌ | ✅ | — |
| **Snapshots / PITR** | | | | |
| Snapshots list / create / restore | ✅ `snapshots list/create/restore` | ✅ | ❌ | — |
| Backups policy get / set | ✅ `backups policy get/set` | ✅ | ❌ | — |
| Backups snapshots list / create / expire | ✅ `backups snapshots list/create/expire` | ✅ | ❌ | — |
| Backups restore / restore-jobs | ✅ `backups restore/restore-jobs list` | ✅ | ❌ | — |
| PITR `restore --as-of=<timestamp>` | ❌ | ✅ | ❌ | P2 — engine PITR physical-GC not yet done (catalog-level rollback shipped; GC is v0.2). Do not implement until the blocker clears. |
| **Logs / observability** | | | | |
| Logs stream / history | ✅ `logs` | ✅ | ✅ | — |
| Metrics (project + org) | ✅ `metrics` | ✅ | ✅ | — |
| Activity list | ✅ `activity list` | ❌ | ✅ | — |
| Audit list / export | ✅ `audit list [--export=<dest>]` | ✅ | ❌ | — |
| Webhooks list / create / test / redeliver / delete / deliveries | ✅ `webhooks` | ❌ | ✅ | — |
| Alerts rules + events | ✅ `alerts rules/events` | ✅ | ❌ | — |
| ERD export (svg / dot / json) | ✅ `erd export` | ❌ | ❌ | Basin-only |
| Index advisor | ❌ | ❌ | ❌ | P0 — `GET /v1/projects/:ref/index-advisor` (cloud T-125). CLI verb: `basin index-advisor [--project=<ref>]`. Single GET, no body. |
| EXPLAIN proxy | ❌ | ❌ | ❌ | P0 — `GET /v1/projects/:ref/explain?sql=<url-encoded>&analyze=<bool>`. CLI verb: `basin explain <sql> [--analyze] [--project=<ref>]`. |
| Replication slots list / create / drop | ❌ | ✅ | ❌ | P0 — `GET/POST /v1/projects/:ref/replication-slots` + `DELETE /v1/projects/:ref/replication-slots/:name` (cloud T-121). CLI verb: `basin replication-slots list/create/drop`. |
| **Domains / SSL** | | | | |
| Domains add / verify / cert / list / remove | ✅ `domains add/verify/cert/list/remove` | ✅ | ✅ | — |
| **Engine knobs** | | | | |
| Engine version pin get / put / delete / events | ✅ `engine-pin get/put/delete/events` | ❌ | ❌ | Basin-only |
| Extensions list | ✅ `extensions list` | ✅ | ✅ | — |
| OAuth providers list / get / set / delete | ✅ `oauth-providers list/get/set/delete` | ❌ | ✅ | — |
| Email templates get / put / delete / test + allowance | ✅ `email templates .../allowance` | ❌ | ✅ | — |
| **Saved queries + history** | | | | |
| Queries save / list / get / fork / delete / history | ✅ `queries save/list/get/fork/delete/history` | ❌ | ❌ | Basin-only |
| **BYO / enterprise** | | | | |
| BYO bucket / kms / engine | ✅ `byo bucket/kms/engine` | ❌ | ❌ | Basin-only |
| SAML get / put / test / enable / disable | ✅ `saml` | ❌ | ❌ | Basin-only |
| SCIM config + provisioning tokens | ✅ `scim` | ❌ | ❌ | Basin-only |
| OAuth apps list / create / rotate / enable / disable / delete | ✅ `oauth-apps` | ❌ | ❌ | Basin-only |
| **Type generation** | | | | |
| `gen types typescript` | ✅ `gen types typescript` | ❌ | ✅ | — |
| `gen types go` | ✅ `gen types go` | ❌ | ❌ | — |
| `gen types python` (Pydantic) | ✅ `gen types python` | ❌ | ❌ | — |
| `gen types --watch` | ✅ `gen types --watch` | ❌ | ❌ | Basin-only |
| **Local dev stack** | | | | |
| `dev` / `start` (boot local engine) | ❌ | ❌ | ✅ | P1 — see §Local dev stack verdict below |
| `stop` (stop local engine) | ❌ | ❌ | ✅ | P1 |
| Local seed / reset | ❌ `db reset` covers remote | ❌ | ✅ | P1 |
| **Utilities** | | | | |
| Shell completion (bash / zsh / fish) | ✅ `completion bash/zsh/fish` | ✅ | ✅ | — |
| `config get/set` | ✅ `config get/set` | ✅ | ✅ | — |
| `docs <cmd>` (open docs URL) | ✅ `docs <cmd>` | ❌ | ❌ | Basin-only |
| `version` | ✅ `version` | ✅ | ✅ | — |
| `help` | ✅ `help` | ✅ | ✅ | — |
| Self-update check | ✅ (warn on stderr) | ✅ | ✅ | — |
| Version-window compatibility check | ✅ (warn on stderr) | ✅ | ❌ | — |

---

## Gap counts

| Priority | Count | Area |
|---|---|---|
| **P0** | 6 | inbound-webhooks, storage policy, realtime WS multi-table, index-advisor, explain, replication-slots |
| **P1** | 3 | local dev stack (`basin dev/start/stop`), dedicated IP-allowlist command |
| **P2** | 3 | PITR restore, functions serve (local Wasm host), browser-flow OAuth login |

---

## P0 gaps — backend exists, CLI work only

### 1. `basin webhooks inbound list/create/get/delete/rotate`

Cloud routes (T-094, fully wired):
```
GET    /v1/projects/:ref/inbound-webhooks
POST   /v1/projects/:ref/inbound-webhooks
GET    /v1/projects/:ref/inbound-webhooks/:id
DELETE /v1/projects/:ref/inbound-webhooks/:id
POST   /v1/projects/:ref/inbound-webhooks/:id/rotate
```

New file: `src/commands/webhooks.rs` extension (add `inbound` sub-dispatcher) or
a new `src/commands/inbound_webhooks.rs`. Mirrors the outbound `webhooks` shape.

### 2. `basin storage policy get/put`

Cloud routes (T-135):
```
GET /v1/projects/:ref/storage-objects/policy
PUT /v1/projects/:ref/storage-objects/policy
```

Extend `src/commands/storage.rs` with a `policy` sub-dispatcher.

### 3. `basin realtime subscribe --multi <t1>,<t2>,…`

Engine 5.11.R3 ships the WS multiplex path (`GET /realtime/v1/ws/:project`).
Blocked on a tungstenite dep decision (see ROADMAP.md T26.2/T26.3). Decision
needed before implementation; do not add async dep without recording in
`decisions.md`.

### 4. `basin index-advisor [--project=<ref>]`

Cloud route (T-125):
```
GET /v1/projects/:ref/index-advisor
```

Single GET; returns per-column JSONB index hints with recommended DDL. High
DX value: surfaces which columns need GIN indexes with the exact `CREATE INDEX`
DDL pre-filled. New file: `src/commands/index_advisor.rs`.

### 5. `basin explain <sql> [--analyze] [--project=<ref>]`

Cloud route:
```
GET /v1/projects/:ref/explain?sql=<url-encoded>&analyze=true|false
```

Proxies to the engine's `EXPLAIN ANALYZE`. Useful for DX without a psql
connection. New file: `src/commands/explain.rs`. `sql` can come from
positional arg, `--file`, or stdin.

### 6. `basin replication-slots list/create/drop`

Cloud routes (T-121):
```
GET    /v1/projects/:ref/replication-slots
POST   /v1/projects/:ref/replication-slots
DELETE /v1/projects/:ref/replication-slots/:name
```

Neon surfaces this as `neonctl replication-slot`. Useful for CDC pipelines
(Debezium, River). New file: `src/commands/replication_slots.rs`.

---

## P1 gaps — local scaffolding needed

### Local dev stack (`basin dev` / `basin start`)

**Verdict: partially implementable today, full Supabase-parity requires
Docker and a released engine image.**

The OSS `basin-server` binary is available in `../basin/` as a Rust workspace;
the `dev/docker-compose.yml` in the engine repo already wires catalog-pg +
MinIO + basin-server into a three-service stack. A `basin dev` command could:

**Light version (no Docker, prereq: a pre-built basin-server on PATH):**
1. Check `basin-server` is on PATH; if not, print install hint.
2. Create a temp dir for `BASIN_DATA_DIR` and `BASIN_WAL_DIR`.
3. Spawn `basin-server` with local FS storage (`BASIN_STORAGE_BACKEND=local`),
   a single ephemeral project (`BASIN_PROJECTS=dev=*`), and port 5432.
4. Poll pgwire readiness (tcp connect to 127.0.0.1:5432).
5. Print `postgres://dev@127.0.0.1:5432/dev` and hang, forwarding Ctrl-C to
   child process.

**Full version (Docker required):**
Shell-compose the `dev/docker-compose.yml` from the basin engine repo, or
vendor a smaller single-service compose that only needs Docker installed.

**What's blocked:**
- No released `basin-server` binary or image yet (engine is pre-v0.1 release).
  The `ghcr.io/bas-in/basin-server:latest` image referenced in the quickstart
  guide only ships post-tag.
- Without a binary or image, `basin dev` cannot start the engine unless the
  user builds from source (`cargo build --release -p basin-server`).
- There is no `basin-cloud` equivalent exposed locally; all cloud-API commands
  (`migrations apply`, `branches create`, etc.) still need the remote cloud.
  `basin dev` would be engine-only (OSS commands: `rpc`, `storage`, `realtime`,
  direct `psql` — same surface listed in README §"Self-hosted OSS engine").

**Recommended implementation (P1, add when engine image ships):**
- `src/commands/dev.rs` — `basin dev [--port=5432] [--data-dir=<path>]`
- Checks for `basin-server` on PATH (or `docker` + image); spawns the engine;
  waits for ready; prints connection string.
- `--apply-migrations` flag: after ready, apply `./basin/migrations/*.sql` via
  pgwire (reuse the `db push` logic against localhost).
- `--seed` flag: run `./basin/seed.sql` via pgwire.
- Does not require basin-cloud; works entirely against the OSS engine.

**Backend handoff**: engine maintainers need to (a) publish a tagged release
of `basin-server` and (b) publish `ghcr.io/bas-in/basin-server:<tag>` before
`basin dev` with the Docker path can be wired end-to-end.

---

## P2 gaps — blocked upstream

| Command | Blocker |
|---|---|
| `basin restore --as-of=<timestamp>` | Engine PITR physical-GC is v0.2 work. Catalog-level rollback ships; physical file GC (needed for point-in-time restore beyond snapshot boundaries) is not done. Tracked in `basin/TASK.md`. Do not implement; would lie. |
| `basin functions serve` | Local Wasm runtime host (W2 mount, `fn/v1/:name` data-plane) works on a running engine, but a local file-watch + hot-reload dev loop requires the local dev stack (P1 above). Once `basin dev` is P1-shipped, `basin functions serve` becomes purely CLI scaffolding: `basin dev` for the engine + `--watch` loop re-deploying on file changes. Implement after P1 clears. |
| `basin login --browser` | Cloud OAuth device-flow (`GET /v1/oauth/device`) not yet in `../basin-cloud/backend-rs/src/handlers/oauth.rs`. The existing PAT-paste `basin login` covers the immediate need. Implement when device flow ships in cloud. |

---

## Basin-only capabilities (no neon / supabase equivalent)

The following commands have no neon or supabase analogue and represent
unique platform surface:

- `basin tx` — interactive BEGIN/COMMIT/ROLLBACK REPL
- `basin db lint` — SQL-compat linter against migration files
- `basin erd export` — entity-relationship diagram export
- `basin queries save/list/fork/history` — saved SQL query library
- `basin gen types go` / `basin gen types python` — non-TS type generation
- `basin gen types --watch` — live type re-emission on migration change
- `basin audit --export` — multi-destination audit export (S3/SFTP/Splunk/Datadog)
- `basin byo bucket/kms/engine` — bring-your-own infrastructure configuration
- `basin engine-pin get/put/delete/events` — engine version management
- `basin migrate-from-pg` — guided Postgres → Basin migration
- `basin orgs branding get/put` — org white-label branding
- `basin tables import-csv/export-csv` — direct CSV ingest/export without psql

---

## What stays blocked upstream (summary for handoff)

1. **PITR `restore --as-of`** — requires engine v0.2 physical GC. File issue in
   `bas-in/basin` once GC ships; open basin-cli command in the same PR.

2. **Local dev stack (`basin dev`)** — requires a published `basin-server` binary
   or Docker image. Tag a `basin-server` release in `bas-in/basin` first;
   then this becomes a 1-day CLI sprint.

3. **`functions serve` hot-reload** — depends on (2) above.

4. **Realtime WS multi-table** — dep decision needed first: record in
   `decisions.md` whether `tungstenite` is acceptable (it is a production-
   quality, audited crate with no transitive C deps). If approved, T26.2 +
   T26.3 from TASKS.md become straightforward.

5. **Browser-flow `login`** — requires cloud OAuth device-flow endpoint.

6. **Storage object policies** (`GET/PUT /storage-objects/policy`) — T-135 in
   cloud; endpoint fully wired. CLI gap only.

7. **Index advisor** — T-125 in cloud; endpoint fully wired. CLI gap only.

8. **Explain proxy** — endpoint fully wired. CLI gap only.

9. **Replication slots** — T-121 in cloud; endpoint fully wired. CLI gap only.

10. **Inbound webhooks** — T-094 in cloud; endpoint fully wired. CLI gap only.

---

*Generated by parity audit — 2026-06-13. Basis: HEAD commit `be8c791`, cloud
handlers at `../basin-cloud/backend-rs/src/handlers/`, engine CAPABILITIES.md
at `../basin/CAPABILITIES.md`, TASKS.md tiers 8–27 all ticked.*
