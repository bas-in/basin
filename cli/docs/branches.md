---
title: "Branches — Preview Environments (basin branches)"
description: "How to create, manage, merge, and delete Basin Cloud branch environments from the CLI."
---

# Branches — Preview Environments — `basin branches`

A branch in Basin Cloud is a full copy of the parent project's schema running as an
isolated environment. Branches are the standard pattern for:

- **Feature previews** — spin up a branch per pull request, push the branch-specific
  migrations, and tear it down when the PR merges.
- **Safe testing** — run destructive operations (reset, bulk deletes, schema
  experiments) against a branch without touching the parent project's data.
- **Parallel development** — two engineers can each have their own branch of the
  same project without stepping on each other.

Branches are backed by `GET/POST/DELETE /v1/projects/{ref}/branches/*` on the
Basin Cloud control plane. A branch is itself a Project row with
`parent_project_id` set; `basin projects list` will show it alongside parent
projects.

---

## Subcommand overview

| Subcommand | One-liner |
|---|---|
| `branches list` | List all branches for a project. |
| `branches create <name>` | Create a new branch (provisions a new engine replica). |
| `branches get <branch_ref>` | Show details for one branch. |
| `branches merge <branch_ref>` | Mark a branch as merged (records the merge event). |
| `branches delete <branch_ref>` | Retire (permanently delete) a branch. |
| `branches events [--follow]` | List branch lifecycle events; poll with `--follow`. |

Project ref resolution follows the same order as `basin db`:

1. `--project=<ref>` flag
2. `./basin/config.toml` `project_ref` field

---

## Preview-environment use cases

### Per-PR branches

In a CI workflow, create a branch on PR open and delete it on PR merge:

```sh
# On PR open (GitHub Actions example):
basin branches create "pr-${{ github.event.number }}" \
  --project=my-project \
  --kind=preview

# Push the PR's migration files to the branch:
basin db push --project=<branch-ref-from-above>

# On PR merge:
basin branches delete <branch-ref> --project=my-project --yes
```

The branch ref is returned in the `create` response. Store it as a CI environment
variable or in the PR description.

### Developer sandboxes

Each developer keeps a personal branch for experimentation:

```sh
basin branches create "dev-alice" --project=main-project
```

Alice can `basin db reset --project=dev-alice` freely without risk to the shared
project.

---

## `--follow` events

`basin branches events --follow` polls `GET /v1/projects/{ref}/branches/events`
every 5 seconds and prints new events as they arrive. Use it to watch a long-running
branch operation (like provisioning a new engine replica) without polling manually.

```sh
basin branches events --follow --project=my-project
# Following branch events for project my-project — Ctrl-C to stop.
# 2026-05-19T10:00:00Z  branch.created   ...
# 2026-05-19T10:00:12Z  engine.ready     ...
```

De-duplication by event ID prevents duplicate lines on overlapping polls. Press
Ctrl-C to stop; the command exits 0.

Under `--json`, each new event is printed as a separate JSON object on its own
line (newline-delimited JSON / ndjson), suitable for `jq -R -r` piping.

---

## Merge semantics

`basin branches merge <branch_ref>` marks the branch as merged via
`POST /v1/projects/{ref}/branches/{branch_ref}/merge`. This records a `branch.merged`
event and sets `merged_at` on the branch row. It does **not** apply the branch's
migrations to the parent project — that DDL path is owned by your CI pipeline
(push the branch's migration files to the parent after the PR merges).

A merge is a one-time operation: a branch that is already merged returns a 409
conflict.

---

## Delete semantics

`basin branches delete <branch_ref>` calls
`DELETE /v1/projects/{ref}/branches/{branch_ref}` (cloud terminology: "retire").
This is permanent — the branch engine replica is torn down, and the project row is
retired. All connection strings for the branch stop working immediately.

The command requires confirmation (`[y/N]` prompt) unless `--yes` is passed.

Under `--json`, confirmation cannot be interactive, so `--yes` is required or the
command errors with a clear message.

---

## JSON shapes

### `branches list`

```json
{
  "branches": [
    {
      "id": "...",
      "ref": "branch-ref",
      "branch_name": "feature-x",
      "branch_kind": "branch",
      "status": "active",
      "region": "jnb",
      "parent_project_id": "parent-id",
      "merged_at": null,
      "created_at": "2026-05-19T10:00:00Z"
    }
  ]
}
```

### `branches create`

```json
{
  "branch": { /* Branch object — same shape as list row */ },
  "created_event": {
    "id": "...",
    "project_id": "...",
    "kind": "branch.created",
    "created_at": "..."
  },
  "engine_ok": true,
  "engine_msg": "",
  "pgwire": {
    "connection_url": "postgres://user:pass@host:5432/db?sslmode=require"
  }
}
```

The `pgwire.connection_url` is shown only once — on create. Store it immediately;
it cannot be retrieved again without `basin db url --reveal`.

### `branches merge`

```json
{
  "branch": { /* Branch object with merged_at set */ },
  "event": { /* BranchEvent with kind=branch.merged */ }
}
```

### `branches delete`

```json
{
  "retired": true,
  "event": { /* BranchEvent with kind=branch.retired */ }
}
```

### `branches events`

```json
{
  "events": [
    {
      "id": "...",
      "project_id": "...",
      "parent_id": null,
      "kind": "branch.created",
      "message": null,
      "actor_id": "user-id",
      "created_at": "2026-05-19T10:00:00Z"
    }
  ]
}
```

Under `--follow`, each event is printed individually (not wrapped in `{ "events": [...] }`).

---

## Reference

### `basin branches list`

```
basin branches list [--project=<ref>]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from `./basin/config.toml` | Parent project ref. |
| `--json` | `false` | Emit `{ "branches": [...] }`. |

---

### `basin branches create`

```
basin branches create <name> [--from=<branch_ref>] [--kind=<branch|preview>] [--project=<ref>]
```

| Flag | Default | Description |
|---|---|---|
| `<name>` | required | Display name for the branch. |
| `--from=<branch_ref>` | — | Source branch ref (informational; data-copy is v0.2 engine work). |
| `--kind=<branch\|preview>` | `branch` | Branch kind; `preview` is the CI/CD variant. |
| `--project=<ref>` | from config | Parent project ref. |
| `--json` | `false` | Emit the full create response. |

Example:

```sh
basin branches create "feature-payments" --kind=preview --project=my-project
```

---

### `basin branches get`

```
basin branches get <branch_ref> [--project=<ref>]
```

| Flag | Default | Description |
|---|---|---|
| `<branch_ref>` | required | The branch project ref. |
| `--project=<ref>` | from config | Parent project ref (used for display context only). |
| `--json` | `false` | Emit `{ "project": Branch }`. |

---

### `basin branches merge`

```
basin branches merge <branch_ref> [--project=<ref>] [--yes]
```

| Flag | Default | Description |
|---|---|---|
| `<branch_ref>` | required | Branch to merge. |
| `--project=<ref>` | from config | Parent project ref. |
| `--yes` | `false` | Skip the confirmation prompt. |
| `--json` | `false` | Requires `--yes`; emits merge response. |

---

### `basin branches delete`

```
basin branches delete <branch_ref> [--project=<ref>] [--yes]
```

| Flag | Default | Description |
|---|---|---|
| `<branch_ref>` | required | Branch to delete. |
| `--project=<ref>` | from config | Parent project ref. |
| `--yes` | `false` | Skip the confirmation prompt. |
| `--json` | `false` | Requires `--yes`; emits `{ "retired": true, "event": {...} }`. |

---

### `basin branches events`

```
basin branches events [--project=<ref>] [--follow] [--limit=<n>]
```

| Flag | Default | Description |
|---|---|---|
| `--project=<ref>` | from config | Project ref. |
| `--follow` | `false` | Poll every 5s; Ctrl-C to stop. |
| `--limit=<n>` | `50` | Events per page. |
| `--json` | `false` | Emit `{ "events": [...] }` (or per-event objects under `--follow`). |

---

## Tests covering this surface

- **`cmd_branches_test.go`** — full CRUD lifecycle, `--follow` polling with a stub
  server emitting two pages, JSON shape lock, confirmation prompt on merge + delete,
  409 conflict on duplicate branch name.

---

*Cross-links: [db-workflow.md](./db-workflow.md) · [types.md](./types.md)*
