---
title: "Batteries included — the one-binary BaaS surface"
nav_section: overview
sidebar_position: 30
summary: "Auth, REST, Realtime, Blob, Vector, and WASM UDFs ship in one binary. Honest per-piece status and the SQL or HTTP shape each one accepts."
tags: [auth, rest, realtime, vector, wasm, baas]
---

# Batteries included

Basin is one binary. It ships the pieces you'd otherwise wire together
across a Postgres host, a JWT service, a REST gateway, a realtime
broker, a blob store, and a vector database. The wedge isn't *"a
cheaper Postgres"* — it's *"the same stack, but every project is a
bucket prefix and the marginal cost is near zero."*

This page is the long-form companion to the README's "Batteries
included" matrix. For the fine-grained per-row status (and every
SQLSTATE we return for unsupported shapes), read
[`../CAPABILITIES.md`](../CAPABILITIES.md). For the rationale behind
each "no", read the relevant [ADR](./decisions/).

---

## The one-binary, one-bill thesis

The Supabase / Firebase / AWS Amplify shape is "a database plus four
sidecars" — Auth, REST, Realtime, Storage, Edge Functions — each
running as its own service, each priced separately, each with its own
auth surface and its own failure modes. That's the right answer when
the four sidecars are made by four different teams. It's the wrong
answer when the wedge is *"thousands of projects, mostly idle, on
object storage"*: every sidecar you add multiplies the per-project
overhead and undoes the multi-project economics.

Basin's answer: one binary. The auth tables live in a per-project
schema inside the same engine that holds the user's data. The REST
gateway is a route handler in the same process. The realtime broker
reads from the same WAL the SQL engine writes to. The vector index is
a column type, not a sidecar.

You get one bind address, one TLS cert, one set of metrics, one
backup primitive (Iceberg snapshots), one IAM boundary (bucket
prefix). When you provision a project, you provision *everything*
that project needs — and the marginal cost is the bytes it actually
stores.

The per-piece sections below describe the surface, the SQL or HTTP
shape each piece exposes, and the honest v0.1 caveat. Pointers to
the implementing crates are at the end of each section.

---

## Auth (`basin-auth`)

Status: ✅ shipped.

Basin ships signup, signin, magic-link, JWT issuance + refresh-token
rotation, OAuth (per [ADR 0020](./decisions/0020-auth-v2-oauth-mfa.md)),
and per-project API keys. Identity tables (`users`, `sessions`,
`refresh_tokens`, `oauth_identities`) live in the per-project `auth`
schema per [ADR 0013](./decisions/0013-auth-per-project-schema.md), so
RLS policies can reference them without a cross-project hop.

Three session functions land inside the SQL engine — `auth.uid()`,
`auth.role()`, `auth.jwt()` — which means RLS policies look exactly
like Supabase's:

```sql
-- Create a row only the owning user can read.
CREATE TABLE notes (
  id        BIGSERIAL PRIMARY KEY,
  owner_id  UUID NOT NULL DEFAULT auth.uid(),
  body      TEXT NOT NULL
);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY "own rows" ON notes FOR ALL USING (owner_id = auth.uid());
```

The HTTP surface mirrors Supabase Auth shape so the same client code
works:

```
POST /auth/v1/signup           { email, password }
POST /auth/v1/token?grant_type=password
POST /auth/v1/token?grant_type=refresh_token
POST /auth/v1/magiclink        { email }
POST /auth/v1/logout
GET  /auth/v1/user
```

Honest caveat: identity tables are per-project, not global. Cross-
project user federation (one identity across many projects) is an
explicit non-goal in v0.1 — projects are the isolation primitive, and
identities live inside them.

Code: [`crates/basin-auth/`](../crates/basin-auth/).
ADRs: [0005](./decisions/0005-auth-system.md),
[0013](./decisions/0013-auth-per-project-schema.md),
[0020](./decisions/0020-auth-v2-oauth-mfa.md).

---

## REST API (`basin-rest`)

Status: ✅ shipped.

Every table is reachable as a PostgREST-compatible REST surface. Drop
a table, get five endpoints — no codegen step, no schema mirror file.

```
GET    /rest/v1/<table>?select=*&owner_id=eq.<uuid>
POST   /rest/v1/<table>          (body: row or [row, …])
PATCH  /rest/v1/<table>?id=eq.42
DELETE /rest/v1/<table>?id=eq.42
POST   /rest/v1/rpc/<fn>         (body: JSON named arguments)
GET    /rest/v1/_openapi.json    (OpenAPI 3.0 schema, auto-generated)
```

Filters, ordering, cursor pagination, NDJSON streaming responses,
representation-on-write (`Prefer: return=representation`), batch
inserts, and `on_conflict=…` upsert all match the PostgREST shape.
JWTs from `basin-auth` flow into SQL session state, so RLS policies
gate the request just like they gate a pgwire query.

The RPC mount (`POST /rest/v1/rpc/<fn>`) dispatches to SQL functions
created with `CREATE FUNCTION … LANGUAGE sql` or `… LANGUAGE wasm`.
JSON request body fields map positionally or by name to the
function's arguments.

Honest caveat: binary / multipart request bodies (file upload) wait
on `basin-blob` finishing — see the Blob section.

Code: [`crates/basin-rest/`](../crates/basin-rest/).
ADR: [0006](./decisions/0006-rest-api-layer.md).

---

## Realtime (`basin-realtime`)

Status: ✅ shipped (Phases 5.11.R2–R6); integration harness has a few
`#[ignore]`-gated soak slices pending un-ignore.

Row-change events stream over SSE (browsers) or WebSocket (richer
clients) per [ADR 0006](./decisions/0006-rest-api-layer.md). Presence
channels (`track` / `untrack` / `presence_state` / `presence_diff`)
ride the same WebSocket multiplex. Replaces PG `LISTEN` / `NOTIFY`
(which is a non-goal — see
[`../CAPABILITIES.md`](../CAPABILITIES.md#wire-protocol)).

Browser-side SSE:

```ts
// Browser side
const es = new EventSource("/realtime/v1/sse/proj_abc/notes");
es.addEventListener("message", (e) => console.log(JSON.parse(e.data)));
```

WebSocket multiplex (subscribe to multiple tables on one socket):

```ts
const ws = new WebSocket("wss://example.com/realtime/v1/ws/proj_abc");
ws.onopen = () => {
  ws.send(JSON.stringify({ type: "subscribe", table: "notes" }));
  ws.send(JSON.stringify({ type: "subscribe", table: "audit_log" }));
  ws.send(JSON.stringify({ type: "presence.track", room: "doc:42",
                          payload: { user: "alice" } }));
};
ws.onmessage = (m) => console.log(JSON.parse(m.data));
```

Subscriber-side filter pushdown (Phase 5.11.R5) evaluates the
predicate in ≤ 50 µs at the sink before bytes hit the wire, so a
fanout that only cares about `WHERE owner_id = $1` doesn't pay the
serialize-then-drop cost. Per-project realtime memory budget (R6)
keeps a runaway subscriber from starving its siblings.

Honest caveat: implementation is complete and single-client smoke
tests pass on every commit. The cross-client soak harness has a few
slices `#[ignore]`-gated pending the un-ignore pass — these are
coverage, not correctness gaps.

Code: [`crates/basin-realtime/`](../crates/basin-realtime/).

---

## Blob storage (`basin-blob`)

Status: ✅ v1 shipped (ADR 0021).

The catalog-backed `BlobStore` (`basin-blob`) plus the public `/storage/v1/`
REST surface in `basin-rest` are wired end-to-end:

- **Buckets:** create / get / delete (delete purges orphaned objects).
- **Objects:** upload (server-side MIME sniffing), download, list with
  prefix + paging, single delete, bulk delete by prefix.
- **Public fast path:** `GET /storage/v1/object/public/:project/:bucket/*path`
  serves objects in public buckets without a JWT.
- **Signed URLs:** mint (JWT-gated) + verify (no JWT) — HMAC-SHA256 over
  `(project, bucket, path, expiry)`, constant-time verification, and
  independent signing-key rotation that invalidates outstanding tokens.
- **Per-object RLS:** `OwnerEqAuthUid` / role / true / false predicates with
  Postgres permissive OR-merge semantics, enforced on download and list.
- **Quota:** per-project `bytes_written_total` counter, incremented on upload
  and decremented on delete.

Deferred to v1.1: `HEAD` (metadata-only) and `COPY` object operations,
resumable multipart / TUS uploads, image transforms, object versioning, and
cross-project object sharing. Cloud-side quota *enforcement* (rejecting writes
past a limit), billing, and CDN integration live above this crate.

Code: [`crates/basin-blob/`](../crates/basin-blob/).
ADR: [0021](./decisions/0021-object-storage.md).

---

## Vector search (`basin-vector`)

Status: ✅ shipped.

`vector(N)` is a native column type — not an extension, not a
sidecar. The HNSW index lives per-file alongside the Vortex/Parquet
data. The planner auto-routes `ORDER BY x <-> $1 LIMIT k` to the
HNSW probe; you don't write a different query for "use the index"
vs "don't".

```sql
CREATE TABLE docs (
  id        BIGSERIAL PRIMARY KEY,
  text      TEXT NOT NULL,
  embedding vector(1536) NOT NULL
);
CREATE INDEX ON docs USING hnsw (embedding vector_cosine_ops);

SELECT id, text FROM docs
ORDER BY embedding <-> $1::vector
LIMIT 10;
```

Operators: `<->` (L2), `<#>` (negative inner product), `<=>` (cosine
distance). Same surface as `pg_vector` so existing client code works
without changes — but `CREATE EXTENSION pgvector` is not required
(and is rejected per [ADR 0002](./decisions/0002-no-postgres-extensions.md)).

Honest caveat: IVF-flat and the `pg_vector` *binary wire format* are
explicit non-goals — text wire format and HNSW only. See
[ADR 0003](./decisions/0003-native-vector-search.md).

Code: [`crates/basin-vector/`](../crates/basin-vector/).
ADR: [0003](./decisions/0003-native-vector-search.md).

---

## WASM UDFs (`basin-fn`)

Status: ✅ v0.1 scalar — `i32` / `i64` / `f64` plus `text` / `bytea` /
`timestamptz` args and returns.

In-engine compute via WebAssembly, sandboxed by `wasmtime`. The
function body is a bare Wasm module compiled from Rust / Zig / Go /
AssemblyScript — anything that targets `wasm32-unknown-unknown` and
respects the host ABI.

```sql
-- Numeric UDF.
CREATE FUNCTION square(n INT) RETURNS INT
LANGUAGE wasm AS '<base64-encoded-wasm-module>';

SELECT square(5);  -- → 25

-- String UDF: text/bytea cross the boundary over the (ptr,len) ABI.
CREATE FUNCTION upper_ascii(s TEXT) RETURNS TEXT
LANGUAGE wasm AS '<base64-encoded-wasm-module>';
```

Resource caps land per call (linear-memory ceiling, instruction
budget via Wasmtime epoch interruption, fuel limit). A runaway UDF
gets epoch-interrupted instead of pinning a thread.

Variable-length values (`text` / `bytea`) cross the host↔guest boundary
over a `(ptr, len)` linear-memory ABI: the module exports `memory`,
`basin_alloc(i32) -> i32`, and `basin_dealloc(i32, i32)`; the host writes
the bytes into guest memory and the guest packs its return as a
`(ptr << 32) | len` `i64` (`len = -1` signals SQL `NULL`).

Honest caveats:

- **JSONB** is passed by declaring the argument as `text` and parsing the
  canonical JSON bytes inside the module — there is no dedicated `jsonb`
  arg type yet.
- Execution is **per-row**; vectorized (whole-Arrow-array) invocation is
  deferred. For bulk string/JSONB transforms, prefer `LANGUAGE sql` — the
  SQL surface covers `regexp_match`, `jsonb_path_query`, `jsonb_set`,
  `format`, `encode`/`decode`, and the rest of the JSONB-mutating + regex
  families.

Why Wasm and not V8 / Deno: cleaner per-project isolation (linear
memory + epoch-interrupt vs V8 isolate quotas), simpler maintenance
(one runtime, not an isolate-pool sidecar), and the runtime is
already in the binary for the `LANGUAGE wasm` path. See
[ADR 0019](./decisions/0019-declarative-baas-surface.md) for the full
"WebAssembly is the function runtime, V8 never" framing.

Code: [`crates/basin-fn/`](../crates/basin-fn/).

---

## Postgres-extension equivalents

Status: ✅ shipped — see
[`../CAPABILITIES.md#postgres-extension-equivalents`](../CAPABILITIES.md#postgres-extension-equivalents)
for the per-extension matrix.

Basin ships the four or five extensions that real apps actually reach
for, as native crates that boot with the engine. No
`CREATE EXTENSION` step; no per-project `.so` install; no
shared-library version skew.

| PG extension | Basin equivalent | Crate |
|---|---|---|
| `pg_cron` | `cron.schedule(…)` + scheduler | [`crates/basin-cron/`](../crates/basin-cron/) |
| `pg_net` + `http` | `net.http_get` / `net.http_post` UDFs | [`crates/basin-net/`](../crates/basin-net/) |
| `pg_trgm` | `%` similarity operator + trigram index | [`crates/basin-trgm/`](../crates/basin-trgm/) |
| `PostGIS` (POINT subset) | `geo` UDFs (`ST_DWithin`, `ST_Distance` on POINT) | [`crates/basin-geo/`](../crates/basin-geo/) |
| `TimescaleDB` continuous aggregates | `basin-cv` continuous matviews | [`crates/basin-cv/`](../crates/basin-cv/) |
| `pgcrypto`, `uuid-ossp` | native UDFs (`gen_random_uuid`, `crypt`, `digest`, …) | core engine |

What's not in the box: the full PostGIS shape (LINESTRING / POLYGON /
R-tree / GIST geo-index) and `pg_vector` IVF — both are documented
non-goals per [ADRs 0002 and 0003](./decisions/).

---

## What we don't ship — and on purpose

### Edge functions (V8 / Deno isolate pool)

Not shipped. Not on the roadmap. The "compute close to data" use
case that Cloudflare Workers / Supabase Edge Functions / Deno Deploy
serve is covered for Basin's wedge by two primitives that don't
require a V8 isolate pool:

1. **In-engine WASM UDFs** for per-row / per-call compute. Same
   process as the data, no network hop, no isolate spin-up cost.
2. **Declarative inbound webhooks + RPC mount** per
   [ADR 0019](./decisions/0019-declarative-baas-surface.md), which
   covers ~95% of the BaaS edge-function taxonomy (inbound webhook,
   compute-over-HTTP, auth-triggered side-effects) as SQL.

A geographically distributed V8 isolate pool is a different concept
and a different operational shape; the maintenance burden doesn't
justify the wedge.

### Triggers / PL/pgSQL

Not shipped. Replaced by declarative lifecycle columns +
SQL-bodied reactors + `LANGUAGE sql` / `LANGUAGE wasm` functions per
[ADR 0012](./decisions/0012-change-event-primitive.md). That covers
~95% of real-world trigger use cases (audit-log on INSERT, computed
column refresh, fanout on UPDATE) without shipping a PL/pgSQL
interpreter.

The remaining ~5% (cursor-driven loops, `EXCEPTION` handling, complex
control flow inside a trigger body) is an explicit non-goal.

### Postgres extensions (`.so`)

Not shipped. See [ADR 0002](./decisions/0002-no-postgres-extensions.md).
Loadable `.so` extensions break the multi-project security boundary
(any extension can read any project's pages from shared memory), break
the bucket-prefix IAM invariant (extension code runs outside Basin's
project-scoped I/O wrappers), and break the "one binary, one bill"
shape (every extension is now its own dependency to track and
patch).

The common extensions are covered natively — see the table above and
[`../CAPABILITIES.md#postgres-extension-equivalents`](../CAPABILITIES.md#postgres-extension-equivalents).

### `LISTEN` / `NOTIFY`

Shipped — SQL-level pub/sub via a per-engine notify registry
(`notify_registry.rs`), dispatched in `executor.rs`. PG-accurate
transaction buffering: `NOTIFY` inside a transaction is queued and
fanned out only on `COMMIT`, discarded on `ROLLBACK`; channel names
are case-insensitive; `pg_listening_channels()` reflects session
state. Covered end-to-end by
[`tests/integration/tests/listen_notify.rs`](../tests/integration/tests/listen_notify.rs).

`basin-realtime` SSE + WebSocket is the complementary fan-out layer
for change-data delivery across processes / regions — pick the right
tool: `LISTEN/NOTIFY` for in-database pub/sub, `basin-realtime` for
out-of-band push to web clients.

---

## Putting it together: a complete app

The flow for a "notes app with realtime sync, vector search over
note bodies, and a daily summary cron job" is one binary:

```sql
-- Schema + RLS.
CREATE TABLE notes (
  id        BIGSERIAL PRIMARY KEY,
  owner_id  UUID NOT NULL DEFAULT auth.uid(),
  body      TEXT NOT NULL,
  embedding vector(1536),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY "own rows" ON notes FOR ALL USING (owner_id = auth.uid());

-- Vector index for similarity search.
CREATE INDEX ON notes USING hnsw (embedding vector_cosine_ops);

-- Daily summary as a continuous materialised view.
CREATE MATERIALIZED VIEW notes_per_day
WITH (basin.cv = 'continuous', basin.refresh = '1 day')
AS
SELECT owner_id, date_trunc('day', created_at) AS day, count(*) AS n
FROM   notes
GROUP  BY 1, 2;

-- Scheduled audit-log cleanup.
SELECT cron.schedule(
  'gc-old-notes',
  '0 3 * * *',
  'DELETE FROM notes WHERE created_at < now() - INTERVAL ''90 days'''
);
```

Client-side:

```ts
// Auth + REST.
const session = await fetch("/auth/v1/token?grant_type=password", {
  method: "POST",
  body: JSON.stringify({ email, password }),
});
const { access_token } = await session.json();

const notes = await fetch("/rest/v1/notes?select=*&order=created_at.desc", {
  headers: { Authorization: `Bearer ${access_token}` },
});

// Realtime sync.
const es = new EventSource(
  "/realtime/v1/sse/proj_abc/notes",
  { withCredentials: true },
);
es.addEventListener("message", (e) => applyChange(JSON.parse(e.data)));

// Vector search via the RPC mount.
const hits = await fetch("/rest/v1/rpc/search_notes", {
  method: "POST",
  headers: { Authorization: `Bearer ${access_token}` },
  body: JSON.stringify({ q: queryEmbedding, k: 10 }),
});
```

One bind address. One TLS cert. One backup primitive (Iceberg
snapshots). One IAM boundary (bucket prefix). One bill.

---

## Further reading

- [`../CAPABILITIES.md`](../CAPABILITIES.md) — the fine-grained per-row matrix, including SQLSTATEs for unsupported shapes and the "Honest parity gaps" sub-section.
- [`./multi-project.md`](./multi-project.md) — the multi-project SaaS story (per-project isolation, scheduler, cost math).
- [`./architecture.md`](./architecture.md) — the four-layer stack (router → shard → WAL → storage).
- [`./decisions/`](./decisions/) — every "no" with the trigger that would change our mind.
- [`./tutorial.md`](./tutorial.md) — 15-minute end-to-end (auth + REST + RLS + a React/Vite snippet).
