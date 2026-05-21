# Basin Phase 5.27.C — `@vercel/postgres` dockerised e2e

This directory contains the Docker + Node.js infrastructure that backs the
`#[ignore]`'d Rust integration test at
`tests/integration/tests/pool_vercel_adapter.rs`.

---

## Driver Substitution

The Phase 5.27.A spec called for `@vercel/postgres`.  That package is **not
usable here** for two reasons:

1. **Deprecated** — the npm package itself carries a deprecation notice
   (confirmed 2026-05-21); Vercel now routes users to the Neon native
   integration.

2. **Requires Neon's WebSocket proxy** — `@vercel/postgres` wraps
   `@neondatabase/serverless`, which tunnels connections through Neon's
   hosted WebSocket proxy.  It cannot speak the raw pgwire protocol to a
   plain Postgres or Basin endpoint.

**Substitution:** `pg` (node-postgres) v8.  This is the standard Postgres
driver used by Vercel serverless functions that talk to plain Postgres databases
(and is itself the underlying transport in `@neondatabase/serverless`'s TCP
path).  The _function shape_ — stateless handler, no persistent cross-invocation
globals, short-lived Pool checkout — is identical to a real Vercel Edge /
Serverless function.

---

## Phase 5.27.A acceptance criteria

| Criterion | How this stack verifies it |
|---|---|
| 0 connection failures across 500 queries | `GET /bench` returns `"failures": 0` |
| p99 query latency ≤ 100 ms | `GET /bench` returns `"p99_ms": ≤ 100` |
| No cursor / session-state bleed | Each invocation asserts the echoed `invocation` and `query_idx` params match; a mismatch increments `failures` |

---

## Directory layout

```
tests/integration/docker/vercel-postgres-app/
├── docker-compose.yml      Two services: basin-server + node-app
│                           basin-server: pre-built basin-server image
│                             BASIN_POOL_MODE=transaction
│                             BASIN_BIND=0.0.0.0:5432
│                           node-app: built from Dockerfile below
│                             POSTGRES_URL=postgres://basin@basin-server:5432/basin?pool_mode=transaction
├── Dockerfile              FROM node:20-alpine; npm install; node --check; CMD node app.js
├── README.md               This file
└── app/
    ├── package.json        { "dependencies": { "pg": "^8.13.3" } }
    └── app.js              HTTP server
                              GET /health  → 200 {"status":"ok"}
                              GET /bench   → runs 50 concurrent invocations
                                            × 10 queries each; returns JSON
                                            { ok, failures, p99_ms, ... }
```

---

## How to run

### Prerequisites

- Docker (any recent version) — `docker --version`
- A built `basin-server` image — `docker build -t basin-server .` from the
  repo root (takes ~5 min on first run; subsequent builds use the Cargo cache).

### One-shot

```sh
# From the repo root:
docker build -t basin-server .

# From this directory:
cd tests/integration/docker/vercel-postgres-app
docker compose up --build --abort-on-container-exit
```

`docker compose` exits with the exit code of the container that stopped first.
`node-app` exits 0 if `/bench` returns `ok=true`, non-zero otherwise.

### Inspect results manually

```sh
# In one terminal, start the stack (without --abort-on-container-exit):
docker compose up --build

# In another terminal, once node-app is healthy:
curl http://localhost:3000/health
curl http://localhost:3000/bench | jq .
```

### Cleanup

```sh
docker compose down -v   # removes containers and the basin-data volume
```

---

## Rust test integration (`pool_vercel_adapter.rs`)

The Rust test at `tests/integration/tests/pool_vercel_adapter.rs` is
`#[ignore]`'d.  It can be activated in a Docker-enabled CI job or locally with:

```sh
cd tests/integration
BASIN_VERCEL_E2E=1 cargo test --test pool_vercel_adapter -- --include-ignored
```

When `BASIN_VERCEL_E2E=1` is set the test:

1. Resolves `docker-compose.yml` relative to `CARGO_MANIFEST_DIR`.
2. Runs `docker compose up -d`.
3. Polls `GET http://localhost:3000/health` until 200 OK or 30 s timeout.
4. Fetches `GET http://localhost:3000/bench` and asserts
   `failures == 0` and `p99_ms <= 100`.
5. Runs `docker compose down -v` in a Drop guard.

Without the env var the test body is a no-op (the `#[ignore]` keeps it out of
the default `cargo test` run entirely).

---

## Environment variable reference

| Variable | Default in compose | Description |
|---|---|---|
| `POSTGRES_URL` | `postgres://basin@basin-server:5432/basin?pool_mode=transaction` | DSN for Basin; `?pool_mode=transaction` is parsed by basin-router |
| `PORT` | `3000` | HTTP port for the node-app |
| `BASIN_POOL_MODE` | `transaction` | Passed to basin-server; selects `PoolMode::Transaction` |
| `BASIN_PROJECTS` | `basin=*` | Provisions the `basin` user with an auto-allocated project ULID |
