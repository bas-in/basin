---
title: "Database Workflow (basin db)"
description: "Daily-driver guide to pushing, pulling, diffing, resetting, and inspecting your Basin Cloud database from the CLI."
---

# Database Workflow — `basin db`

The `basin db` namespace is the workhorse of the CLI. It covers every step of the
schema-change loop: apply local migrations, pull remote state, generate drift
migrations, reset a project for testing, retrieve the connection string, dump
schema or data, and lint SQL files before pushing.

All `db` subcommands resolve the project reference with the same priority order:

1. `--project=<ref>` flag
2. `./basin/config.toml` `project_ref` field (written by `basin link`)

---

## Overview of subcommands

| Subcommand | One-liner |
|---|---|
| `db push` | Apply unapplied local migration files to the remote project. |
| `db pull` | Write remote migrations to `./basin/migrations/`. |
| `db diff` | Call the cloud's diff endpoint and write the result as a new migration file. |
| `db reset` | Snapshot → rollback all migrations → re-apply local ones → run `seed.sql`. |
| `db url` | Print the pgwire DSN (masked, full, or after rotation). |
| `db dump` | Emit a `pg_dump`-shaped SQL dump to stdout (schema and/or data). |
| `db lint` | Dry-run each `./basin/migrations/*.sql` file and surface SQLSTATE errors. |

---

## Setup

### 1. Scaffold a project directory

```sh
basin init
```

Creates:

```
./basin/
  config.toml      # project_ref, default_branch, engine_version_pin
  migrations/      # empty; your .sql files live here
  seed.sql         # placeholder; add INSERT statements for local dev
```

### 2. Bind to a remote project

```sh
basin link --project=<ref>
```

Writes `project_ref = "<ref>"` to `./basin/config.toml` after verifying the ref
exists via `GET /v1/projects/{ref}`. Pass `--org=<slug>` if you have multiple orgs.

### 3. Directory layout

```
./basin/
  config.toml
  migrations/
    0000000001_create_users.sql
    0000000002_add_email_index.sql
  seed.sql
```

Migration filenames follow the `<timestamp>_<slug>.sql` convention so lexicographic
order matches application order. `basin migrations new <name>` writes a timestamped
file for you.

---

## Common workflows

### Edit schema → push

```sh
# 1. Write your DDL in a new migration file.
basin migrations new add_email_verified_column

# edit ./basin/migrations/<timestamp>_add_email_verified_column.sql

# 2. Push all unapplied files in lex order.
basin db push

# Output:
#   applied: 1  skipped: 2
```

`db push` compares the filenames of local files against the cloud's applied list
and skips anything already there. The push is additive and idempotent: running it
twice on the same set of files is always safe.

### Pull remote state

When a teammate pushes a migration directly or the cloud applies one automatically:

```sh
basin db pull
```

Pulls every remote migration and writes it to `./basin/migrations/`. Refuses to
overwrite a local file whose content differs from the remote copy unless you pass
`--force`.

### Generate a drift migration

```sh
basin db diff
```

Calls `GET /admin/v1/projects/{ref}/migrations/diff`, and if the cloud reports
schema drift between the live schema and the last applied migration, writes a new
timestamped file to `./basin/migrations/` so you can review and commit it.

Nothing is written if there is no drift (the command prints `no schema drift`).

### Reset for a fresh test

Useful before integration tests or demo environments:

```sh
basin db reset --yes
```

The sequence is:
1. Create a safety snapshot (`POST /v1/projects/{ref}/backups/snapshots`).
2. Roll back all applied migrations, newest-first.
3. Re-apply all local `./basin/migrations/*.sql` files in lex order.
4. Run `./basin/seed.sql` if it is non-empty and not the placeholder.

The `--yes` flag skips the confirmation prompt. Without `--yes` (or under `--json`)
the command requires explicit confirmation or errors immediately.

### Branch for a review environment

```sh
basin branches create feature-x --project=<ref>
basin db push --project=<feature-x-ref>
```

See [branches.md](./branches.md) for the full preview-environment workflow.

---

## Reference

### `basin db push`

```
basin db push [--project=<ref>] [-q]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `-q` / `--quiet` | `false` | Suppress per-file progress lines. |
| `--json` | `false` | Emit `{ "applied": [...], "skipped": [...] }`. |

Example:

```sh
basin db push --project=staging
```

JSON shape:

```json
{
  "applied": [{ "filename": "0003_add_index.sql", "id": "mig_abc" }],
  "skipped": [{ "filename": "0001_init.sql", "reason": "already applied" }]
}
```

---

### `basin db pull`

```
basin db pull [--project=<ref>] [--force]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--force` | `false` | Overwrite locally-modified files. |
| `--json` | `false` | Emit `{ "pulled": [...], "skipped": [...] }`. |

Example:

```sh
basin db pull --force
```

---

### `basin db diff`

```
basin db diff [--project=<ref>]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--json` | `false` | Emit `{ "wrote": "<path>" }` or `{ "wrote": null, "reason": "no-diff" }`. |

Example:

```sh
basin db diff
# wrote ./basin/migrations/1716200000_schema_drift.sql
```

---

### `basin db reset`

```
basin db reset [--project=<ref>] [--yes]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--yes` | `false` | Skip the confirmation prompt. |
| `--json` | `false` | Requires `--yes`; emits `{ "snapshot_id", "rolled_back", "applied", "seeded" }`. |

Example:

```sh
basin db reset --yes --project=my-test-env
```

---

### `basin db url`

```
basin db url [--project=<ref>] [--reveal] [--rotate] [--yes]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--reveal` | `false` | Call `POST /pgwire/reveal`; print the full unmasked DSN. |
| `--rotate` | `false` | Call `POST /pgwire/rotate`; print the new DSN (password changed). |
| `--yes` | `false` | Skip the rotation confirmation prompt. |
| `--json` | `false` | Emit `{ "dsn": "postgres://..." }`. |

Examples:

```sh
basin db url                        # masked password
basin db url --reveal               # full DSN (one-time)
basin db url --rotate --yes         # rotate and print new DSN
```

---

### `basin db dump`

```
basin db dump [--project=<ref>] [--schema-only | --data-only]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--schema-only` | `false` | Emit only `CREATE TABLE` statements. |
| `--data-only` | `false` | Emit only `INSERT` statements. |

Output goes to stdout; redirect to a file via shell:

```sh
basin db dump --schema-only > schema.sql
basin db dump --data-only   > data.sql
basin db dump               > full.sql
```

---

### `basin db lint`

```
basin db lint [--project=<ref>]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Remote project ref. |
| `--json` | `false` | Emit `{ "files_checked": N, "errors": [...] }`. |

Each `./basin/migrations/*.sql` file is posted to
`POST /v1/projects/{ref}/sql/query` with `dry_run=true`. SQLSTATE 0A000
(unsupported feature) and parse errors are collected with `file:line` context.

Example:

```sh
basin db lint
# 5 file(s) checked, no errors
```

Error output:

```sh
basin db lint
# 0003_bad_syntax.sql:7: syntax error at or near "CREAT"
# 1 file(s) checked, 1 error(s)
```

---

## Tests covering this surface

The following test files exercise the `db` namespace:

- **`cmd_db_test.go`** — push / pull / diff / reset / url / dump / lint, including:
  idempotency, `--force`, rotation confirmation, lint error extraction, no-diff path.
- **`cmd_status_test.go`** — `basin status` drift indicator which calls the same
  migration list endpoint as `db push`.
- **`cmd_link_test.go`** — project ref resolution that `db` subcommands depend on
  for the `./basin/config.toml` fallback path.

---

*Cross-links: [branches.md](./branches.md) · [types.md](./types.md)*
