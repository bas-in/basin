# Changelog

All notable changes to `basin` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[Semantic Versioning](https://semver.org/). Tags flush the `## Unreleased` section
into a dated section on release.

---

## Unreleased

### Added

- **`basin index-advisor`** — JSONB index recommendations via
  `GET /v1/projects/:ref/index-advisor` (cloud T-125). Returns per-column hints
  with table/column/reason/ddl/patterns_seen. Table rendering for humans; `--json`
  returns the raw array; `jq '.[].ddl'` pipes directly to psql. (P0 gap — commit `0229f70`)

- **`basin explain [--analyze]`** — `EXPLAIN [ANALYZE]` proxy via
  `GET /v1/projects/:ref/explain?sql=<url>&analyze=<bool>`. SQL from positional arg,
  `--file`, or stdin. No psql connection needed. (P0 gap — commit `7330d94`)

- **`basin replication-slots list/create/drop`** — logical replication slot management
  via `/v1/projects/:ref/replication-slots`. Default plugin: pgoutput; also accepts
  wal2json. Covers CDC pipelines (Debezium, River). Equivalent to `neonctl
  replication-slot`. (P0 gap — commit `54660b8`)

- **`basin webhooks inbound list/create/get/delete/rotate`** — inbound webhook receiver
  lifecycle (cloud T-094). HMAC-verified receive-and-invoke: external services POST to
  a `receive_url`; the cloud verifies sha256 and calls a user SQL/Wasm function with the
  payload. HMAC secret revealed once on create and rotate. (P0 gap — commit `a5e3bc6`)

- **`basin storage policy get/put`** — object-storage access policy document management
  via `GET/PUT /v1/projects/:ref/storage-objects/policy` (cloud T-135). RLS-style
  per-(project, bucket, key_prefix) rules. `put` accepts raw JSON or `@file`. (P0 gap —
  commit `49421a1`)

- **PARITY.md** — full neon/supabase 3-column parity matrix: 60+ capability rows,
  6 P0 gaps (CLI-only, cloud ready), 3 P1 gaps (local-dev-stack), 3 P2 gaps (platform
  gaps). Local-dev-stack verdict + backend handoffs for each blocked item.

- **`basin init` / `basin link` / `basin status` / `basin unlink`** — directory-mode
  project context. `basin init` scaffolds `./basin/config.toml`,
  `./basin/migrations/`, and `./basin/seed.sql`. `basin link --project=<ref>` binds
  a directory to a remote project. `basin status` reports drift and migration count.
  (Tier 8 — commit `07ba5ff`, `44c69a0`)

- **`basin db push/pull/diff/reset/url/dump/lint`** — daily-driver database workflow.
  Push local migrations, pull remote state, generate drift migrations, reset a project
  cleanly, retrieve the pgwire DSN, emit a `pg_dump`-shaped export, and lint SQL files
  for engine compatibility. (Tier 9 — commit `2337f99`)

- **`basin migrations new/get/diff/rollback`** — migration lifecycle commands extending
  the existing `list` + `apply` surface. Timestamp-prefixed file creation, single
  migration fetch, diff endpoint, and rollback with confirmation prompt. (Tier 10 —
  commit `07ba5ff`)

- **`basin branches list/create/get/merge/delete/events`** — branch (preview
  environment) CRUD. `--follow` mode on `events` polls every 5 s with de-duplication
  by event ID. Merge and delete require confirmation (`--yes` to bypass). JSON shapes
  locked per command. (Tier 11 — commit `07ba5ff`)

- **`basin gen types typescript/go/python`** — generate typed database interfaces from
  `information_schema` via `POST /v1/projects/{ref}/sql/query`. TypeScript emits
  `export interface` + a `Database` wrapper; Go emits structs with `json` + `db` tags;
  Python emits Pydantic `BaseModel` subclasses. Type-mapping table in
  `gen_types_map.go` covers all ✅ engine types from `CAPABILITIES.md`. (Tier 15 —
  commit `44c69a0`, `07ba5ff`)

- **`basin tables create/alter/drop/import-csv/export-csv`** — write surface for the
  `tables` namespace. CSV import streams from stdin or `--file=`; export streams to
  stdout. (Tier 12 — commit `2337f99`)

- **`basin rows insert/update/delete`** — single-row and batch row mutations via
  `--json` body or stdin. (Tier 12 — commit `2337f99`)

- **`basin rls enable/disable` + `rls policies list/create/drop`** — row-level
  security management per table. (Tier 12 — commit `2337f99`)

- **`basin api-keys list/create/rotate/revoke`** — project-scoped API key management.
  Full secret shown only on create; masked on list. (Tier 13 — commit `30ad7b7`)

- **`basin pgwire show/reveal/rotate` + `pgwire engine-keys list/rotate`** — pgwire
  credential management and internal engine JWT key rotation. (Tier 13 — commit
  `30ad7b7`)

- **`basin backups policy get/set`**, **`basin backups snapshots list/create/expire`**,
  **`basin backups restore`**, **`basin backups restore-jobs list`** — backup lifecycle
  beyond the original `snapshots` surface. (Tier 14 — commit `30ad7b7`)

- **`basin members list/invite/remove/role`** and
  **`basin invitations list/resend/revoke`** — org membership and invitation
  management. (Tier 16 — commit `30ad7b7`)

- **`basin orgs create/update/delete`** + **`basin orgs branding get/put`** — org
  lifecycle mutations extending the existing `orgs list/get` surface. (Tier 16 —
  commit `30ad7b7`)

- **`basin domains add/verify/cert/list/remove`** — custom domain management per
  project. (Tier 17 — commit `2337f99`)

- **`basin webhooks list/create/patch/test/redeliver/delete/deliveries`** — webhook
  lifecycle and delivery inspection. (Tier 17 — commit `2337f99`)

- **`basin alerts rules list/create/get/patch/delete/silence/unsilence`** +
  **`basin alerts events list`** — alert rule CRUD and event listing. (Tier 17 —
  commit `99f9ef7`)

- **`basin audit list`** — org audit log with `--since` / `--until` filtering. (Tier
  17 — commit `99f9ef7`)

- **`basin activity list`** — project activity event stream. (Tier 17 — commit
  `99f9ef7`)

- **`basin metrics`** — project and org metrics with `--range` and `--metric` flags.
  (Tier 17 — commit `99f9ef7`)

- **`basin erd export`** — export project entity–relationship diagram as SVG, DOT, or
  JSON. (Tier 17 — commit `99f9ef7`)

- **`basin engine-pin get/put/delete/events`** — engine version pinning per project.
  (Tier 18 — commit `99f9ef7`)

- **`basin extensions list`** — list installed engine extensions. (Tier 18 — commit
  `99f9ef7`)

- **`basin oauth-providers list/get/set/delete`** — project OAuth provider
  configuration. (Tier 18 — commit `99f9ef7`)

- **`basin email templates get/put/delete/test`** + **`basin email allowance`** —
  project email template CRUD and quota lookup. (Tier 18 — commit `99f9ef7`)

- **`basin queries save/list/get/fork/delete`** + **`basin queries history [--clear]`**
  — saved query management and history. (Tier 19 — commit `99f9ef7`)

- **`basin byo bucket get/put/probe/delete`** — bring-your-own S3-compatible bucket
  configuration. (Tier 20 — commit `99f9ef7`)

- **`basin byo kms get/put/rotate/verify/audit/delete/cache-stats`** — bring-your-own
  KMS configuration and key management. (Tier 20 — commit `99f9ef7`)

- **`basin byo engine get/put/probe/delete`** — bring-your-own engine configuration.
  (Tier 20 — commit `99f9ef7`)

- **`basin docs [<subcommand>]`** — open `https://docs.basin.run/cli[/<subcommand>]`
  in the default browser. OS dispatch via `runtime.GOOS` (darwin → `open`, linux →
  `xdg-open`, windows → `rundll32`). Under `--json`, prints the URL without spawning
  a browser. (Tier 25 — this release)

- **`docs/db-workflow.md`** — per-area markdown explainer for the `basin db` namespace,
  including setup, common workflows, subcommand reference, and test pointers. (Tier 25)

- **`docs/branches.md`** — markdown explainer for `basin branches`, covering
  preview-environment patterns, `--follow` events, merge + delete semantics, and JSON
  shapes. (Tier 25)

- **`docs/types.md`** — markdown explainer for `basin gen types`, covering the
  per-language nullable conventions, the full engine-type → target-type mapping table,
  `--watch` future plans, and golden file maintenance. (Tier 25)

---

## Earlier work (pre-changelog)

The `initial` commit (`658509b`) established the module, top-level dispatch, auth,
`--json` / `--quiet` / `--no-color` global flags, version check, telemetry, and shell
completions. See `TASKS.md` Tiers 0–7 for the full extraction and release-pipeline
history.

---

*This project adheres to [Semantic Versioning](https://semver.org/). Release tags
flush `## Unreleased` into a dated section automatically via the release workflow.*
