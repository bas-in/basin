---
title: "5-Minute Docker Quickstart"
nav_section: deployment
sidebar_position: 10
summary: "Bring Basin up locally with a single docker compose command, run the smoke test, and connect via psql — all in under five minutes."
version_since: "0.5.0"
tags: [deployment, docker, quickstart, self-host]
---

# 5-Minute Docker Quickstart

Get Basin running locally with a single command. The dev-stack
(`dev/docker-compose.yml`) wires together basin-server, a catalog Postgres 16
instance, and MinIO (S3-compatible object store) — everything you need to
exercise real TCP paths and the full write/read/query flow.

> **Prerequisites:** Docker ≥ 24 with the Compose plugin, `psql` (any
> version), and ~2 GB of free disk for images and volumes.

---

## 1. TL;DR

```sh
bash dev/scripts/up.sh
psql -h 127.0.0.1 -p 5533 -U alice -d postgres
```

That is the entire quickstart. The `up.sh` script builds the basin-server
image, waits for all services to pass their health checks, and prints a
ready banner. Once you see it, the `psql` line above connects you to
replica 0 over pgwire.

To run the full stack manually via Compose directly:

```sh
docker compose -f dev/docker-compose.yml up --build
```

---

## 2. What Comes Up

The dev-stack starts three services. Full service reference is in
[`dev/README.md`](../dev/README.md).

| Service | Host port (default) | What it is |
|---|---|---|
| `catalog-pg` | 5532 | Postgres 16 — Basin's catalog backend |
| `minio` API | 9100 | S3-compatible object store (data blocks) |
| `minio` Console | 9101 | MinIO web UI (browse at `http://localhost:9101`) |
| `basin-server-0` | 5533 | Basin replica 0 — always started |
| `basin-server-1` | 5534 | Basin replica 1 — only with `--replicas 2` |

`basin-server` connects to `catalog-pg` for catalog persistence and to MinIO
for Vortex block storage. The entire stack is ephemeral — volumes are deleted
by `down.sh`. For production persistence, see the operator runbooks
([`docs/operators/`](./operators/)).

### Published image vs. local build

The official GHCR/Docker Hub image (`ghcr.io/exo/basin:latest`) is being
prepared in Phase 5.31.C/D and may not be published yet. Once it is
available:

```sh
docker run --rm -it ghcr.io/exo/basin:latest --help
```

Until then, build locally via `dev/Dockerfile.basin-server`:

```sh
docker build -f dev/Dockerfile.basin-server -t basin-server:local .
```

The `up.sh` script handles this automatically — it builds the image if it is
not already cached.

---

## 3. Verify

### Option A — smoke test (recommended)

```sh
bash dev/scripts/smoke.sh
```

`smoke.sh` runs approximately 10 psql round-trips against `basin-server-0`:
CREATE TABLE, several INSERTs, a range SELECT, an UPDATE, and a DELETE. It
exits 0 on success and prints a pass/fail summary. Failures include the
failing SQL and the server response.

### Option B — manual psql

```sh
psql -h 127.0.0.1 -p 5533 -U alice -d postgres
```

Then:

```sql
-- Create a table in the alice project
CREATE TABLE events (id BIGSERIAL PRIMARY KEY, payload TEXT, ts TIMESTAMPTZ DEFAULT now());

-- Insert a row
INSERT INTO events (payload) VALUES ('hello basin');

-- Read it back
SELECT id, payload, ts FROM events;
```

A result row confirms that the write path (WAL + memtable), the catalog, and
the read path (query engine) are all wired up correctly.

---

## 4. Configuration

All configuration is via environment variables. Export any of the following
before calling `up.sh`, or set them in a `.env` file next to
`dev/docker-compose.yml`.

### Port overrides

```sh
export POSTGRES_PORT=5532        # catalog-pg host port
export MINIO_API_PORT=9100       # MinIO S3 API host port
export MINIO_CONSOLE_PORT=9101   # MinIO web UI host port
export BASIN_PORT_BASE=5533      # basin-server-0 pgwire host port
export BASIN_PORT_REPLICA1=5534  # basin-server-1 pgwire host port (replica mode)
```

### Key basin-server variables

| Variable | Default | Description |
|---|---|---|
| `BASIN_BIND` | `0.0.0.0:5433` | pgwire listen address inside the container |
| `BASIN_CATALOG` | `memory` | `memory` or a `postgres://…` DSN — dev-stack sets this to the `catalog-pg` DSN |
| `BASIN_STORAGE_BACKEND` | `local` | `local`, `s3`, or `tigris` — dev-stack sets `s3` pointed at MinIO |
| `BASIN_STORAGE_ENDPOINT` | — | S3 endpoint URL; dev-stack sets `http://minio:9000` |
| `BASIN_STORAGE_BUCKET` | — | S3 bucket name |
| `BASIN_STORAGE_ACCESS_KEY_ID` | — | S3 access key |
| `BASIN_STORAGE_SECRET_ACCESS_KEY` | — | S3 secret |
| `BASIN_PROJECTS` | `alice=*,bob=*` | Comma-separated `user=project_id` pairs; `*` auto-generates the project ID |
| `BASIN_SHARD_ENABLED` | `0` | Set to `1` to route INSERTs through the WAL + compactor pipeline |
| `BASIN_POOL_ENABLED` | `0` | Set to `1` to enable the session pool |
| `BASIN_DATA_DIR` | `/data/basin` | Local data directory (inside container) |
| `BASIN_WAL_DIR` | `/data/wal` | WAL directory (inside container) |

For a full variable reference, see [`dev/README.md`](../dev/README.md#environment-variable-reference-basin-server).

---

## 5. Multi-Replica Mode

To start two basin-server replicas (ports 5533 and 5534):

```sh
bash dev/scripts/up.sh --replicas 2
```

Or via Compose directly using the `replica-1` profile:

```sh
docker compose -f dev/docker-compose.yml --profile replica-1 up --build
```

Connect to replica 0 on port 5533, replica 1 on port 5534:

```sh
psql -h 127.0.0.1 -p 5533 -U alice -d postgres   # replica 0
psql -h 127.0.0.1 -p 5534 -U alice -d postgres   # replica 1
```

Each replica acquires per-`(project, partition)` leases from `catalog-pg`.
If replica 0 goes down, replica 1 picks up its leases within the heartbeat
budget defined in [ADR 0023](./decisions/0023-leases-and-partition-routing.md)
(20 s for the dev-stack, configurable for production). You can exercise this
with the handoff workload:

```sh
bash dev/scripts/e2e.sh --workload handoff --replicas 2
```

The E2E runner kills replica 0 via `docker compose stop`, polls replica 1
until it returns the lease marker row, and asserts that wall time stays
within the ADR 0023 budget.

---

## 6. Teardown

```sh
bash dev/scripts/down.sh
```

`down.sh` stops all containers and removes volumes. All data written during
the session is discarded. Re-running `up.sh` starts from a clean state.

---

## 7. Production Note

The dev-stack is for local development and CI only. For production
deployments:

- See the **operator runbooks** in [`docs/operators/`](./operators/) for
  storage configuration, backup strategy, and upgrade procedures.
- The lease-ownership model that drives multi-replica routing is described
  in [ADR 0023](./decisions/0023-leases-and-partition-routing.md).
- Catalog persistence requires a production-grade Postgres instance (not
  the dev-stack `catalog-pg`). Set `BASIN_CATALOG` to a managed Postgres
  DSN before starting basin-server.
- MinIO can be replaced with any S3-compatible object store (AWS S3,
  Tigris, Backblaze B2) by setting `BASIN_STORAGE_BACKEND=s3` and
  the corresponding endpoint/credentials variables.
