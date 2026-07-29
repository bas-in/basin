---
title: "5-Minute Docker Quickstart"
nav_section: deployment
sidebar_position: 10
summary: "Get Basin running with a single docker run command and connect via psql in under five minutes."
version_since: "0.1.0"
tags: [deployment, docker, quickstart, self-host]
---

# 5-Minute Docker Quickstart

Get Basin running locally and make your first SQL queries in under five minutes.
No external object store or catalog database is needed — the default mode
stores everything on the local filesystem inside the container volume.

> **Prerequisites:** Docker (any recent version), `psql` (any version).
> No Rust toolchain needed for the published-image path.

---

## 1. Start the server

### Option A — build locally (works today)

The GHCR image (`ghcr.io/vul-os/basin-server:latest`) will be published on the
first tagged release. Until then, build from the repo root:

```sh
# Build the image (takes ~5 min on first run; subsequent builds are fast
# because Docker caches the Cargo dependency layer).
docker build -t basin-server .

# Run it.
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  --name basin \
  basin-server
```

### Option B — published image (after first release)

```sh
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  --name basin \
  ghcr.io/vul-os/basin-server:latest
```

What these flags do:

| Flag | Effect |
|---|---|
| `-p 5432:5432` | Exposes the pgwire port on the host |
| `-v basin-data:/var/basin` | Persists data across container restarts |
| `--name basin` | Names the container for easy `docker stop basin` |

The server is ready when you see a log line like:
```
INFO basin_server: pgwire listener is accept-ready bind=0.0.0.0:5432
```

The container's healthcheck (`nc -z 127.0.0.1 5432`) confirms liveness every
10 seconds. You can check it with `docker inspect --format='{{.State.Health.Status}}' basin`.

---

## 2. Connect with psql

```sh
psql -h 127.0.0.1 -p 5432 -U basin
```

| Parameter | Value | Source |
|---|---|---|
| Host | `127.0.0.1` | localhost via the `-p 5432:5432` mapping |
| Port | `5432` | Dockerfile `EXPOSE 5432` / `BASIN_BIND=0.0.0.0:5432` |
| User | `basin` | `BASIN_PROJECTS=basin=*` auto-provisions this project |
| Password | _(none)_ | No auth enabled in the default dev configuration |

The `basin` user is mapped to a fresh auto-generated project ID at startup.
You will see the provisioned project ULID printed to stderr on first boot.

---

## 3. Run SQL

This is the same round-trip used by the smoke harness
(`tests/integration/scripts/docker-smoke.sh`), so it is known to work:

```sql
-- Create a table.
CREATE TABLE smoke (id int, name text);

-- Insert a row.
INSERT INTO smoke VALUES (1, 'hello basin');

-- Read it back.
SELECT id, name FROM smoke WHERE id = 1;
```

Expected output:

```
 id |    name
----+-------------
  1 | hello basin
(1 row)
```

Try a few more shapes:

```sql
-- Analytical aggregate.
SELECT COUNT(*) FROM smoke;

-- Basin stores data as Vortex columns under the volume.
-- Confirm with: docker exec basin find /var/basin -name '*.vortex'
```

---

## 4. Using basin-cli

[`basin-cli`](https://github.com/vul-os/basin/tree/main/cli) is a separate operator
daily-driver CLI (Go, Apache-2.0). It talks to the basin-cloud control plane,
not directly to a standalone `basin-server` container. For local development,
`psql` and any Postgres-compatible driver are the easiest path.

If you have `basin-cli` installed and want to point it at a self-hosted engine,
see the `--endpoint` flag in `basin-cli --help`.

---

## 5. Stopping and cleaning up

```sh
# Stop the container (data volume is preserved).
docker stop basin

# Remove the volume too (all data is discarded).
docker volume rm basin-data
```

---

## Troubleshooting

### Port conflict: address already in use

Port 5432 is the default Postgres port and may already be in use. Map to a
different host port:

```sh
docker run --rm \
  -p 5433:5432 \           # host 5433 → container 5432
  -v basin-data:/var/basin \
  --name basin \
  basin-server

# Then connect on the host port you chose:
psql -h 127.0.0.1 -p 5433 -U basin
```

### Data not persisting across restarts

The container writes data to `/var/basin` inside the container. If you omit the
`-v basin-data:/var/basin` flag, all data is lost when the container exits.
The named volume `basin-data` survives `docker stop` / `docker start` cycles;
only `docker volume rm basin-data` deletes it.

### Changing the default user or adding more projects

Pass `BASIN_PROJECTS` as an environment variable. Each entry is `user=*`
(auto-generates a project ID) or `user=<ulid>` (fixed ID):

```sh
docker run --rm \
  -p 5432:5432 \
  -v basin-data:/var/basin \
  -e BASIN_PROJECTS="alice=*,bob=*" \
  basin-server
```

Then connect as `psql -h 127.0.0.1 -p 5432 -U alice` or `-U bob`.

### Connecting from a container on the same Docker network

```sh
docker run --rm \
  --network container:basin \
  postgres:16 \
  psql -h 127.0.0.1 -p 5432 -U basin -c "SELECT 1"
```

Or use the container's name as the host on a shared bridge network:

```sh
psql -h basin -p 5432 -U basin
```

---

## Environment variable reference

All configuration is via environment variables. The following are the key
knobs for a single-node local setup:

| Variable | Default in image | Description |
|---|---|---|
| `BASIN_BIND` | `0.0.0.0:5432` | pgwire listen address inside the container |
| `BASIN_DATA_DIR` | `/var/basin` | Data root; mount a volume here for persistence |
| `BASIN_STORAGE_BACKEND` | `local` | `local` (filesystem), `s3`, or `tigris` |
| `BASIN_PROJECTS` | `basin=*` | Comma-separated `user=project_id` pairs; `*` auto-allocates a ULID |
| `BASIN_SHARD_ENABLED` | `0` | Set to `1` to route writes through the WAL + compactor |
| `BASIN_CATALOG` | `memory` | `memory` (volatile) or a `postgres://…` DSN for durable catalog |

For S3/Tigris object storage, production catalog (Postgres DSN), auth, REST,
and other advanced options, see [`docs/deployment.md`](./deployment.md) and
[`docs/operators/`](./operators/).
