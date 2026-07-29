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
| Inbound webhooks list / create / delete / rotate | ✅ `webhooks inbound list/create/get/delete/rotate` | ❌ | ❌ | — |
| **Realtime** | | | | |
| Realtime SSE subscribe (single table) | ✅ `realtime subscribe <table>` | ❌ | ✅ | — |
| Realtime WS subscribe (multi-table) | ❌ (WS deferred — needs tungstenite) | ❌ | ✅ | P0 — engine 5.11.R3 ships WS; CLI blocked on tungstenite dep decision (decisions.md). Cloud route: `GET /realtime/v1/ws/:project`. |
| **Object storage** | | | | |
| Storage buckets list / create / delete | ✅ `storage buckets list/create/delete` | ❌ | ✅ | — |
| Storage upload / download / list / rm / sign | ✅ `storage upload/download/list/rm/sign` | ❌ | ✅ | — |
| Storage object policies (RLS-style) | ✅ `storage policy get/put` | ❌ | ✅ | — |
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
| Index advisor | ✅ `index-advisor` | ❌ | ❌ | — |
| EXPLAIN proxy | ✅ `explain [--analyze]` | ❌ | ❌ | — |
| Replication slots list / create / drop | ✅ `replication-slots list/create/drop` | ✅ | ❌ | — |
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
| `gen types ruby` (Struct) | ✅ `gen types ruby` | ❌ | ❌ | Basin-only |
| `gen types rust` (serde) | ✅ `gen types rust` | ❌ | ❌ | Basin-only |
| `gen types java` (Jackson POJOs) | ✅ `gen types java` | ❌ | ❌ | Basin-only |
| `gen types csharp` (System.Text.Json records) | ✅ `gen types csharp` | ❌ | ❌ | Basin-only |
| `gen types php` (readonly classes) | ✅ `gen types php` | ❌ | ❌ | Basin-only |
| `gen types dart` (fromJson/toJson classes) | ✅ `gen types dart` | ❌ | ❌ | Basin-only |
| `gen types swift` (Codable structs) | ✅ `gen types swift` | ❌ | ❌ | Basin-only |
| `gen types --watch` | ✅ `gen types --watch` | ❌ | ❌ | Basin-only |
| **Local dev stack** | | | | |
| `dev` / `start` (boot local engine) | ✅ `dev` (binary mode + Docker mode) | ❌ | ✅ | — P1 shipped |
| `stop` (stop local engine) | ✅ `stop` (Docker Compose mode) | ❌ | ✅ | — P1 shipped |
| Local seed / reset | ✅ `dev --seed` + `dev --apply-migrations` | ❌ | ✅ | — |
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
| **P0** | 1 | realtime WS multi-table (tungstenite dep decision pending) |
| **P1** | 1 | dedicated IP-allowlist command (covered by `tokens create/patch` flags — low-DX gap) |
| **P2** | 3 | PITR restore, functions serve (local Wasm host), browser-flow OAuth login |

All six original P0 gaps (inbound-webhooks, storage policy, index-advisor, explain,
replication-slots) were shipped in the prior session. The local dev stack (P1) is now
shipped. gen types covers the full 10-SDK matrix.

---

## P0 gaps — SHIPPED (all CLI-only items done)

### 1. ✅ `basin webhooks inbound list/create/get/delete/rotate`
Shipped in `src/commands/inbound_webhooks.rs` (commit 54660b8).

### 2. ✅ `basin storage policy get/put`
Shipped as `storage policy` sub-dispatcher in `src/commands/storage.rs` (commit 49421a1).

### 3. `basin realtime subscribe --multi <t1>,<t2>,…` — STILL BLOCKED

Engine 5.11.R3 ships the WS multiplex path (`GET /realtime/v1/ws/:project`).
Blocked on a tungstenite dep decision (see ROADMAP.md T26.2/T26.3). Decision
needed before implementation; do not add async dep without recording in
`decisions.md`.

### 4. ✅ `basin index-advisor [--project=<ref>]`
Shipped in `src/commands/index_advisor.rs` (commit 0229f70).

### 5. ✅ `basin explain <sql> [--analyze] [--project=<ref>]`
Shipped in `src/commands/explain.rs` (commit 7330d94).

### 6. ✅ `basin replication-slots list/create/drop`
Shipped in `src/commands/replication_slots.rs` (commit 54660b8).

---

## P1 gaps — SHIPPED

### ✅ Local dev stack (`basin dev` / `basin stop`)

**Shipped** in `src/commands/dev.rs` with two modes:

**Binary mode (default, no Docker):**
1. Resolves `basin-server` from `--server-bin` / `BASIN_SERVER_BIN` / PATH.
2. Creates `BASIN_DATA_DIR` and `BASIN_WAL_DIR` under `/tmp/basin-dev`.
3. Spawns `basin-server` with `BASIN_STORAGE_BACKEND=local`,
   `BASIN_PROJECTS=dev=*`, and the specified port.
4. Polls TCP readiness on 127.0.0.1:<port> (configurable timeout, default 60 s).
5. Prints `postgres://dev@127.0.0.1:<port>/dev` and blocks until Ctrl-C.

**Docker mode (`--docker`):**
1. Resolves docker-compose.yml from `--compose-file` / `BASIN_COMPOSE_FILE` /
   `../basin/dev/docker-compose.yml`.
2. Runs `docker compose up -d --remove-orphans`.
3. Polls TCP readiness.
4. `basin stop` runs `docker compose down`.

Both modes support `--apply-migrations` (applies `./basin/migrations/*.sql`
via `psql`) and `--seed=<path>` (runs a seed SQL file via `psql`).

**Prerequisites (binary mode):** build `basin-server` from the engine repo:
```
cargo build --release -p basin-server  # in vul-os/basin checkout
export BASIN_SERVER_BIN=/path/to/target/release/basin-server
```

**Notes:**
- No released `basin-server` binary or Docker image yet (engine pre-v0.1).
  The command works today for anyone who has the engine built from source.
- Cloud-API commands (`migrations apply`, `branches create`, etc.) still
  require the remote cloud; `basin dev` is engine-only (OSS surface).
- When `ghcr.io/bas-in/basin-server` is published, `--docker` will work
  out-of-the-box without a local build.

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
- `basin gen types go/python/ruby/rust/java/csharp/php/dart/swift` — non-TS type generation for full SDK matrix
- `basin gen types --watch` — live type re-emission on migration change
- `basin audit --export` — multi-destination audit export (S3/SFTP/Splunk/Datadog)
- `basin byo bucket/kms/engine` — bring-your-own infrastructure configuration
- `basin engine-pin get/put/delete/events` — engine version management
- `basin migrate-from-pg` — guided Postgres → Basin migration
- `basin orgs branding get/put` — org white-label branding
- `basin tables import-csv/export-csv` — direct CSV ingest/export without psql
- `basin index-advisor` — JSONB GIN index recommendations with exact CREATE INDEX DDL
- `basin webhooks inbound list/create/get/delete/rotate` — HMAC inbound webhook receiver management

---

## What stays blocked upstream (summary for handoff)

1. **PITR `restore --as-of`** — requires engine v0.2 physical GC. File issue in
   `vul-os/basin` once GC ships; open basin-cli command in the same PR.

2. **`basin dev` Docker mode — full zero-config** — works today when the user
   has the engine repo checked out as a sibling (`../basin/dev/docker-compose.yml`).
   When `ghcr.io/bas-in/basin-server` is published it will work out-of-the-box
   for any user with Docker (no engine source needed).

3. **`functions serve` hot-reload** — depends on `basin dev` being available.
   Once a user has `basin dev` running, `functions serve` becomes a file-watch
   loop that re-deploys on change. Implement when `basin dev` is stable.

4. **Realtime WS multi-table** — dep decision needed first: record in
   `decisions.md` whether `tungstenite` is acceptable (it is a production-
   quality, audited crate with no transitive C deps). If approved, T26.2 +
   T26.3 from TASKS.md become straightforward.

5. **Browser-flow `login`** — requires cloud OAuth device-flow endpoint
   (`GET /v1/oauth/device`). Not yet in cloud handlers.

6. **Dedicated IP-allowlist command** — covered at the UX level by
   `basin tokens create/patch --ip-allowlist=...`; a dedicated `ip-allow`
   command would add minimal DX value. Implement only if user demand warrants.

---

*Updated 2026-06-14. All six original P0 gaps (T-094, T-121, T-125, T-135,
explain, index-advisor) shipped. P1 local dev stack shipped. gen types expanded
to the full 10-SDK matrix (ts/go/py/ruby/rust/java/csharp/php/dart/swift).*
