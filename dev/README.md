# Basin Dev-Stack

Single-command local environment: catalog Postgres 16 + MinIO (S3-compatible) +
one or more basin-server replicas, plus an E2E runner that exercises real network
paths.

## Quick start

```sh
# 1. Bring up the stack (builds basin-server image, waits for health)
bash dev/scripts/up.sh

# 2. Smoke test (~10 psql round-trips)
bash dev/scripts/smoke.sh

# 3. Full E2E suite (perf + noisy-neighbor; handoff needs --replicas 2)
bash dev/scripts/e2e.sh

# 4. Tear down
bash dev/scripts/down.sh
```

## Services

| Service | Host port (default) | Description |
|---|---|---|
| `catalog-pg` | 5532 | Postgres 16 catalog backend |
| `minio` API | 9100 | S3-compatible object store |
| `minio` Console | 9101 | MinIO web UI |
| `basin-server-0` pgwire | 5533 | Replica 0 (always started) |
| `basin-server-1` pgwire | 5534 | Replica 1 (`--replicas 2` or `--profile replica-1`) |

Connect to basin-server via psql:

```sh
psql -h 127.0.0.1 -p 5533 -U alice -d postgres
```

## Port overrides

Export any of these before running the scripts:

```sh
export POSTGRES_PORT=5532
export MINIO_API_PORT=9100
export MINIO_CONSOLE_PORT=9101
export BASIN_PORT_BASE=5533
export BASIN_PORT_REPLICA1=5534
```

## Two-replica mode

```sh
bash dev/scripts/up.sh --replicas 2
# Starts basin-server-0 (port 5533) + basin-server-1 (port 5534)

bash dev/scripts/e2e.sh --workload handoff --replicas 2
# Kills replica-0, asserts replica-1 picks up within the ADR 0023 budget
```

## "Bring your own binary" pattern

If the Docker build fails (missing musl toolchain, slow CI, etc.), you can mount
a pre-built basin-server binary instead:

1. Build locally:
   ```sh
   cargo build --release -p basin-server --no-default-features
   ```
2. Set the env var:
   ```sh
   export BASIN_SERVER_BYO_BINARY="$(pwd)/target/release/basin-server"
   ```
3. Use the `byo-basin-server` service in `docker-compose.yml` (un-comment the
   bind-mount section) and disable the build section.

## E2E runner workloads

| Workload | What it tests |
|---|---|
| `perf` | Point SELECT p50/p99, range scan p50/p99, INSERT p50/p99, UPDATE p50/p99 — all over real TCP sockets |
| `noisy-neighbor` | Alice bursts 200 INSERT+COUNT ops; Bob runs quiet point queries concurrently; asserts Bob p99 stays within budget |
| `handoff` | Kills replica-0 via `docker compose stop`; polls replica-1 until it picks up the lease and returns the marker row; asserts wall time ≤ ADR 0023 budget (20 s for dev-stack) |

Results are written to `dev/results/<timestamp>.json` (same JSON schema as
`benchmark/data/`).

## Environment variable reference (basin-server)

| Variable | Default | Description |
|---|---|---|
| `BASIN_BIND` | `0.0.0.0:5433` | pgwire listen address |
| `BASIN_DATA_DIR` | `/data/basin` | Local data directory |
| `BASIN_WAL_DIR` | `/data/wal` | WAL directory |
| `BASIN_CATALOG` | `memory` | `memory` or `postgres://…` DSN |
| `BASIN_STORAGE_BACKEND` | `local` | `local`, `s3`, or `tigris` |
| `BASIN_STORAGE_BUCKET` | — | S3 bucket name |
| `BASIN_STORAGE_ENDPOINT` | — | S3 endpoint URL (for MinIO: `http://minio:9000`) |
| `BASIN_STORAGE_ACCESS_KEY_ID` | — | S3 access key |
| `BASIN_STORAGE_SECRET_ACCESS_KEY` | — | S3 secret |
| `BASIN_PROJECTS` | `alice=*,bob=*` | Comma-separated `user=project_id` pairs (`*` = auto-generate) |
| `BASIN_SHARD_ENABLED` | `0` | `1` = route INSERTs through WAL + compactor |
| `BASIN_POOL_ENABLED` | `0` | `1` = enable session pool |
| `BASIN_DISK_CACHE_MAX_BYTES` | 10 GiB | `0` = disable disk cache |
| `BASIN_PAGE_CACHE_MAX_BYTES` | 1 GiB | `0` = disable page cache |
