---
title: "Getting started with Basin — CRUD, auth, RLS, REST, and Wasm functions"
nav_section: overview
sidebar_position: 1
summary: "End-to-end walkthrough: spin up a dev cluster, create a table, add auth, enable row-level security, call the REST API, and deploy a Wasm function."
tags: [getting-started, tutorial, auth, rls, rest, functions]
---

# Getting started with Basin

This tutorial walks you from zero to a working Basin project on a local dev machine. You will:

1. Start the Basin dev-stack with Docker (or a pre-built binary).
2. Create a table, insert rows, and query them over pgwire.
3. Enable `basin-auth`, sign up a user, and get a JWT.
4. Protect rows with a row-level security policy.
5. Read data over the PostgREST-shaped REST API.
6. Deploy a tiny Wasm function and invoke it over HTTP.

Follow each section in order — later steps depend on what earlier ones set up.

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker | 24+ | With the `docker compose` plugin (v2) |
| `psql` | 14+ | Any Postgres client works; psql ships with every OS Postgres package |
| `curl` | any | For REST and function calls |
| `basin-cli` | latest | For `basin functions deploy`; see [basin-cli design spec](./basin-cli-design.md) |

> **No Docker?** If you have a pre-built `basin-server` binary, skip to
> [Start Basin — binary path](#start-basin--binary-path).

---

## 1. Start Basin

### Option A — Docker (recommended)

The `dev/` directory contains a ready-made compose stack: Postgres 16 as the catalog backend, MinIO as the object store, and one `basin-server` replica.

```bash
# Clone the repo (or cd into it if you already have it).
git clone https://github.com/bas-in/basin.git
cd basin

# Build images and start. The script waits until every service is healthy.
bash dev/scripts/up.sh
```

When the script prints `Dev-stack is UP`, Basin is ready:

```
==> Dev-stack is UP.
    catalog-pg : postgres://basin:basin@localhost:5532/basin
    minio      : http://localhost:9100  (console: http://localhost:9101)
    basin[0]   : postgres://alice@localhost:5533/postgres
```

Basin's pgwire endpoint is `localhost:5533`. The dev stack pre-creates two project users, `alice` and `bob`, both with wildcard project access (`BASIN_PROJECTS=alice=*,bob=*`).

To stop the stack later:

```bash
bash dev/scripts/down.sh
```

### Option B — binary path

If you built `basin-server` from source, set the `BASIN_SERVER_BYO_BINARY` env var and start the compose stack — the Dockerfile stage is skipped and the binary is bind-mounted instead:

```bash
export BASIN_SERVER_BYO_BINARY=/path/to/target/release/basin-server
bash dev/scripts/up.sh --no-build
```

Or run the binary directly (you still need a catalog Postgres and a MinIO or S3 bucket; the compose file shows the required env vars under `x-basin-env`).

### Confirm pgwire is up

```bash
psql "postgres://alice@localhost:5533/postgres" -c "SELECT version();"
```

Expected output includes `Basin` in the version string. If `psql` can't connect, run `docker compose -f dev/docker-compose.yml logs basin-server-0 | tail -30` to inspect the startup logs.

---

## 2. Create your first table

Connect as `alice` and create a table:

```bash
psql "postgres://alice@localhost:5533/postgres"
```

```sql
-- Declare a primary key so the hot-tier memtable can do sub-millisecond
-- point lookups and upserts. See the HTAP guide for why this matters.
CREATE TABLE events (
    id         BIGSERIAL PRIMARY KEY,
    owner_id   TEXT        NOT NULL,
    category   TEXT        NOT NULL,
    payload    JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Insert a few rows:

```sql
INSERT INTO events (owner_id, category, payload) VALUES
    ('alice',  'login',   '{"ip": "1.2.3.4"}'),
    ('alice',  'purchase','{"item": "widget", "amount": 9.99}'),
    ('bob',    'login',   '{"ip": "5.6.7.8"}'),
    ('bob',    'purchase','{"item": "gadget", "amount": 49.99}');
```

Query them back:

```sql
SELECT id, owner_id, category, created_at
FROM events
ORDER BY created_at;
```

All four rows are visible because no security policies are in place yet.

```sql
-- Aggregation works in the same query — Basin merges the hot-tier
-- memtable with the Vortex columnar store transparently.
SELECT owner_id, COUNT(*) AS event_count, SUM((payload->>'amount')::numeric) AS total
FROM events
GROUP BY owner_id;
```

---

## 3. Add auth

Basin's auth subsystem (`basin-auth`) is off by default. To enable it, add the required env vars to the dev stack.

### Configure the dev stack

Create a `dev/.env` file (the compose file reads it automatically):

```bash
# dev/.env
BASIN_AUTH_ENABLED=1
BASIN_AUTH_JWT_SECRET=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20

# Dev SMTP: use Mailpit or any local SMTP relay (port 1025 by default).
# For quick testing you can set BASIN_AUTH_SMTP_TLS=none and point at
# a local Mailpit container.
BASIN_AUTH_SMTP_HOST=localhost
BASIN_AUTH_SMTP_PORT=1025
BASIN_AUTH_SMTP_USERNAME=dev
BASIN_AUTH_SMTP_PASSWORD=dev
BASIN_AUTH_SMTP_FROM=noreply@basin.local
BASIN_AUTH_SMTP_TLS=none
```

> `basin-auth` **will not start** if SMTP env vars are missing — half-configured email is the source of every sign-in support ticket in production. For a quick local run, Mailpit (`docker run -p 1025:1025 -p 8025:8025 axllent/mailpit`) provides a no-config local SMTP server.

Restart the stack to pick up the env:

```bash
bash dev/scripts/down.sh
bash dev/scripts/up.sh
```

### Sign up a user

`basin-auth` exposes an HTTP auth API alongside the pgwire port. On the dev stack it listens on `localhost:5533` under `/auth/v1/`:

```bash
# Sign up alice@example.com
curl -s -X POST http://localhost:5533/auth/v1/signup \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"hunter2hunter2"}' \
  | jq .
```

Response:

```json
{
  "access_token": "eyJ...",
  "token_type": "bearer",
  "expires_in": 3600,
  "refresh_token": "rt_..."
}
```

If email verification is enabled (the default), check your Mailpit inbox at `http://localhost:8025` and click the verification link before proceeding.

### Sign in and capture the JWT

```bash
JWT=$(curl -s -X POST http://localhost:5533/auth/v1/signin \
  -H "Content-Type: application/json" \
  -d '{"email":"alice@example.com","password":"hunter2hunter2"}' \
  | jq -r .access_token)

echo "JWT: ${JWT:0:40}..."
```

Keep `$JWT` in your shell — you will use it in the REST and Wasm sections below.

---

## 4. Row-level security

With identities in place, lock the `events` table so each user can only see their own rows.

Connect as a privileged user (or the `basin_auth_service` role) and enable RLS:

```bash
psql "postgres://alice@localhost:5533/postgres"
```

```sql
-- Enable row-level security. After this, SELECT returns zero rows for
-- any role that doesn't match a policy — not an error, just an empty set.
ALTER TABLE events ENABLE ROW LEVEL SECURITY;

-- Owner-only read policy.
-- auth.uid() returns the `sub` claim from the JWT of the current session.
CREATE POLICY events_owner_read ON events
    FOR SELECT
    USING (owner_id = auth.uid());

-- Owner-only insert policy.
CREATE POLICY events_owner_insert ON events
    FOR INSERT
    WITH CHECK (owner_id = auth.uid());
```

### Verify isolation

Open two separate psql sessions that supply different JWTs. In a real application the JWT comes from the `Authorization` header; in psql you can set it as a connection parameter:

```bash
# Alice's session — set the JWT claim so auth.uid() resolves correctly.
psql "postgres://alice@localhost:5533/postgres" \
  -c "SET request.jwt.claims = '{\"sub\":\"alice\"}'; SELECT id, owner_id FROM events;"
```

Alice sees only her two rows.

```bash
# Bob's session.
psql "postgres://alice@localhost:5533/postgres" \
  -c "SET request.jwt.claims = '{\"sub\":\"bob\"}'; SELECT id, owner_id FROM events;"
```

Bob sees only his two rows. The filter is enforced inside the engine — there is no application-layer `WHERE owner_id = ?` to forget.

> In production the JWT is verified and injected automatically by `basin-rest` and the Wasm function runtime. The `SET request.jwt.claims` pattern is a development introspection tool; it is not how authentication works in production.

---

## 5. REST API

`basin-rest` exposes every table as a PostgREST-compatible endpoint under `/rest/v1/<table>`. The JWT you captured in Section 3 is the auth credential.

### Read your rows

```bash
curl -s "http://localhost:5533/rest/v1/events" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

The response is a JSON array. Because RLS is active, you only see Alice's rows — the engine enforces the `events_owner_read` policy using the `sub` claim from `$JWT`.

### Filter and project

```bash
# Only purchase events, selecting two columns.
curl -s "http://localhost:5533/rest/v1/events?category=eq.purchase&select=id,payload" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

### Insert via REST

```bash
curl -s -X POST "http://localhost:5533/rest/v1/events" \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -H "Prefer: return=representation" \
  -d '{"owner_id":"alice","category":"api_call","payload":{"endpoint":"/rest/v1/events"}}' \
  | jq .
```

The `Prefer: return=representation` header makes Basin return the inserted row (with the server-assigned `id` and `created_at`).

### Supported query parameters (v1)

| Parameter | Example | Effect |
|---|---|---|
| `select` | `select=id,category` | Column projection |
| `<col>=eq.<val>` | `category=eq.login` | Equality filter |
| `<col>=gt.<val>` | `created_at=gt.2026-01-01` | Greater-than |
| `<col>=lt.<val>` | `created_at=lt.2027-01-01` | Less-than |
| `<col>=in.(a,b)` | `category=in.(login,purchase)` | Membership |
| `order` | `order=created_at.desc` | Result ordering |
| `limit` | `limit=10` | Result cap |
| `offset` | `offset=20` | Pagination |

---

## 6. A Wasm function

Basin runs WebAssembly functions inside the engine — no separate runtime process. Author in TypeScript (or any WASI Preview 2 language), deploy via `basin functions deploy`, and invoke over HTTP.

### Write the function

Create a file `fn/summarise.ts`:

```typescript
// fn/summarise.ts
// Returns a JSON summary of how many events each category has.
import { handle, query, log } from "@bas-in/functions";

export default handle(async (req) => {
  log.info("summarise called");

  const rows = await query.exec(`
    SELECT category, COUNT(*) AS n
    FROM events
    GROUP BY category
    ORDER BY n DESC
  `);

  // rows is list<{ columns: list<[string, string]> }>
  const summary = rows.map((r) => ({
    category: r.columns[0][1],
    count:    parseInt(r.columns[1][1], 10),
  }));

  return {
    status:  200,
    headers: [["content-type", "application/json"]],
    body:    new TextEncoder().encode(JSON.stringify({ summary })),
  };
});
```

Key points:

- `query.exec` runs SQL under **the caller's identity**. `auth.uid()` inside the SQL resolves to the JWT `sub` of whoever hits the endpoint.
- RLS policies on `events` fire normally — Alice's invocation sees only Alice's rows.
- `log.info` writes to Basin's structured log, tagged with the function name and project.

### Deploy

```bash
# basin-cli compiles TypeScript → Wasm component via ComponentizeJS,
# then uploads the compiled bytes to the engine catalog.
basin functions deploy ./fn/summarise.ts --name summarise
```

Expected output:

```
Compiled fn/summarise.ts → 142 KiB Wasm component
Deployed function "summarise" (version 1)
Invoke: ANY /fn/v1/summarise
```

### Invoke

```bash
curl -s "http://localhost:5533/fn/v1/summarise" \
  -H "Authorization: Bearer $JWT" \
  | jq .
```

Response (Alice's events only, because RLS applies inside the function):

```json
{
  "summary": [
    { "category": "login",    "count": 1 },
    { "category": "purchase", "count": 1 },
    { "category": "api_call", "count": 1 }
  ]
}
```

The JWT auth gate fires before the function is resolved — a missing or invalid token gets a 401 without the function ever loading.

### Limits (dev defaults)

| Cap | Default |
|---|---|
| CPU time per invocation | 200 ms |
| Memory per invocation | 64 MiB |
| Wall-clock time | 5 s |
| Concurrent invocations per project | 10 |

Caps are configurable per project via `ALTER PROJECT` (see [functions.md](./functions.md) for the full ABI and limit reference).

---

## 7. Next steps

### Sample applications

The following sample applications are in progress and will reference this tutorial as their starting point:

- **SaaS starter** (Phase 5.32.C — coming soon) — a multi-tenant to-do app that demonstrates per-project isolation, RLS, auth, and the REST layer end-to-end.
- **AI / RAG sample** (Phase 5.32.D — coming soon) — vector search over embedded documents with Basin's native vector index; calls OpenAI embeddings from a Wasm function.

### Deeper reading

| Document | What it covers |
|---|---|
| [HTAP guide](./htap-guide.md) | Hot-tier vs cold-tier performance, `basin.sort_by`, memtable caps |
| [Wasm functions](./functions.md) | Full ABI reference, host imports, outbound HTTP, secrets |
| [SQL compatibility](./sql-compatibility.md) | Which Postgres SQL Basin accepts and which it defers |
| [Multi-project SaaS](./multi-project.md) | Project-membership model, per-tenant cost primitives |
| [CAPABILITIES.md](../CAPABILITIES.md) | Full capability matrix: shipped, in-progress, planned, off-roadmap |
| [Operator runbooks](./operators/lease-ownership.md) | Day-2 ops: lease ownership, shard rebalancing, stuck-lease playbook |
| [Deployment](./deployment.md) | Production storage backends, topology, configuration |

### Tear down

```bash
bash dev/scripts/down.sh
# To also remove all volumes (wipes catalog and object store data):
docker compose -f dev/docker-compose.yml down -v
```
