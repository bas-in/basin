---
title: "Dev-stack operator runbook"
nav_section: operations
sidebar_position: 10
summary: "Single-command local Basin environment: catalog Postgres 16 + MinIO + basin-server replicas. Includes E2E runner for perf, noisy-neighbor, and lease-handoff scenarios."
tags: [ops, dev, docker, e2e]
---

# Dev-stack operator runbook

The Basin dev-stack (`dev/`) provides a single-command local environment for
developing against, debugging, and validating Basin end-to-end. It is **not**
intended for production use — it is a development and CI validation tool.

## What it provides

| Component | Image / Source | Purpose |
|---|---|---|
| `catalog-pg` | `postgres:16-alpine` | Durable catalog backend (Postgres 16) |
| `minio` | `minio/minio:latest` | S3-compatible object store for Parquet blobs |
| `minio-init` | `minio/mc:latest` (one-shot) | Creates the `basin-dev` bucket at startup |
| `basin-server-0` | Built from `dev/Dockerfile.basin-server` | Replica 0 (always started) |
| `basin-server-1` | Same | Replica 1 (started with `--profile replica-1`) |

## Prerequisites

- Docker Engine 24+ with the compose plugin (`docker compose version`)
- `psql` and `pg_isready` on the PATH (from `postgresql-client` or equivalent)
- `cargo` + Rust stable 1.85+ (for the E2E runner build)
- `curl` (used by `up.sh` to wait for MinIO health)
- `jq` (optional; used by `e2e.sh` for the human-readable summary)

## Bring-up

```sh
# Standard (1 replica):
bash dev/scripts/up.sh

# Two replicas (required for handoff E2E):
bash dev/scripts/up.sh --replicas 2

# Skip Docker build (use cached image):
bash dev/scripts/up.sh --no-build
```

`up.sh` performs the following health-gate sequence:

1. `docker compose up -d` (with `--build` by default)
2. Poll `catalog-pg` via `pg_isready` until ready (≤80 s)
3. Poll MinIO `/minio/health/live` via `curl` until ready (≤60 s)
4. Wait for `minio-init` to exit 0 (bucket bootstrap)
5. Poll each `basin-server` replica via `pg_isready` on its pgwire port (≤80 s)

Only after all gates pass does `up.sh` exit 0.

## Smoke test

```sh
bash dev/scripts/smoke.sh
```

Runs 11 psql statements (CREATE, INSERT, SELECT point, SELECT range, aggregate,
UPDATE, verify UPDATE, DROP) and prints `[PASS]`/`[FAIL]` per check. Exits 0
on full pass.

## E2E suite

```sh
# All workloads (handoff skipped unless --replicas 2):
bash dev/scripts/e2e.sh

# Specific workload:
bash dev/scripts/e2e.sh --workload perf
bash dev/scripts/e2e.sh --workload noisy-neighbor
bash dev/scripts/e2e.sh --workload handoff --replicas 2
```

Results are written to `dev/results/<timestamp>.json`. The format mirrors
`benchmark/data/` JSON:

```json
{
  "kind": "e2e",
  "id": "basin-e2e-perf",
  "name": "Basin E2E — perf",
  "generated_at": "2026-05-21T12:00:00Z",
  "workloads": [
    {
      "name": "perf",
      "passed": true,
      "metrics": [
        { "label": "Point SELECT p99", "value": 12.5, "unit": "ms",
          "threshold": 150.0, "passed": true }
      ]
    }
  ]
}
```

### Workload descriptions

#### `perf`

Exercises point SELECT, range scan, INSERT, and UPDATE over real TCP. Samples
are collected in loops (100 point queries, 20 range scans, 100 inserts, 50
updates) and p50/p99 are reported. Budgets are set to generous dev-stack values
(see `services/basin-e2e-runner/src/perf.rs`).

#### `noisy-neighbor`

Two project connections (alice + bob) share the same basin-server instance. Alice
bursts 200 concurrent INSERT + COUNT operations; Bob issues quiet point queries
throughout. The test asserts that Bob's p99 latency stays within
`BOB_P99_BUDGET_MS` (default 500 ms for Docker dev-stack).

This validates the per-project fairness mechanisms in the pgwire router and
engine scheduler.

#### `handoff`

Requires `--replicas 2`. Procedure:

1. Both replicas verified reachable.
2. Alice writes a marker row via replica-0 (establishing a lease per ADR 0023).
3. `docker compose stop basin-server-0` — kills the leaseholder.
4. E2E runner polls replica-1 every 500 ms until it returns the marker row.
5. Records wall time from stop to first successful query.
6. `docker compose start basin-server-0` — restores the replica.

Budget: **20 s** (ADR 0023 lease TTL = 15 s + 5 s heartbeat + Docker overhead).
Tighten to 16 s for production integration tests with faster stop times.

## Tear-down

```sh
# Stop containers (preserve volumes):
bash dev/scripts/down.sh

# Stop containers AND remove all data volumes (clean slate):
bash dev/scripts/down.sh --volumes
```

## "Bring your own binary" pattern

When the Docker multi-stage build is not suitable (e.g., a CI environment where
the Rust build cache is not available and a full workspace build is too slow),
pre-build the binary on the host and mount it:

```sh
# 1. Build on host:
cargo build --release -p basin-server --no-default-features

# 2. Set env var:
export BASIN_SERVER_BYO_BINARY="$(pwd)/target/release/basin-server"

# 3. In docker-compose.yml, switch basin-server-0/1 to use the
#    commented-out bind-mount + image pattern (see dev/README.md).
```

The bring-up and E2E scripts check for `BASIN_SERVER_BYO_BINARY` and document
whether the test was run against a locally built or Docker-built binary.

## Troubleshooting

### `basin-server-0` fails to start

Check logs:

```sh
docker compose logs basin-server-0 | tail -40
```

Common causes:
- MinIO not healthy yet (wait longer or check `docker compose logs minio`).
- `basin-dev` bucket not created (check `docker compose logs minio-init`).
- `catalog-pg` migration failed (check `docker compose logs catalog-pg`).

### `smoke.sh` fails with "connection refused"

`basin-server-0` pgwire port is not open. Run `up.sh` first, or wait longer
if the binary is still compiling inside the container.

### Handoff test times out

- Ensure you started with `--replicas 2`.
- Check that `docker compose stop` has permissions to stop containers
  (some CI environments restrict this).
- Increase `POLL_TIMEOUT` in `services/basin-e2e-runner/src/handoff.rs` if
  Docker stop is unusually slow in your environment.

## Port reference

| Service | Default host port | Override env var |
|---|---|---|
| catalog-pg | 5532 | `POSTGRES_PORT` |
| MinIO API | 9100 | `MINIO_API_PORT` |
| MinIO Console | 9101 | `MINIO_CONSOLE_PORT` |
| basin-server-0 | 5533 | `BASIN_PORT_BASE` |
| basin-server-1 | 5534 | `BASIN_PORT_REPLICA1` |
