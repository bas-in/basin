# 0006 — REST API layer (basin-rest)

- **Status:** Accepted, deferred. Build is gated on
  [ADR 0005](./0005-auth-system.md) (basin-auth) shipping first —
  REST without auth is a footgun, not a feature.
- **Date:** 2026-05-01
- **Tags:** scope, rest, http, postgrest, depends-on-0005

## Context

PostgREST-style "tables become REST endpoints" is one of the most-loved
parts of the Supabase developer experience. A wedge customer building
a frontend on Basin would absolutely use it. Implementing PostgREST
itself (the actual binary) requires building out `pg_catalog` and
`information_schema` — a 2–4 month slog with ongoing maintenance
([discussion: README "what would it take to ship PostgREST" exchange]).

A native REST layer over Basin is **2–3 weeks** for the 80% case. We
control the protocol, we already have the catalog in-process, and we
can authenticate via the JWT scheme `basin-auth` (ADR 0005) issues.

This ADR locks the design. The build is deferred along with 0005.

## Decision

Basin will ship **`basin-rest`**, a thin HTTP service exposing each
tenant's tables as REST endpoints with PostgREST-compatible URL and
JSON conventions for the subset that matters. **basin-rest depends on
basin-auth.** A REST API without authentication is not a product; it
is a public dump of every tenant's data.

### Endpoint shape

Identical to PostgREST's URL conventions for the supported subset:

```
GET    /rest/v1/<table>?<filters>&<order>&<select>&<limit>&<offset>
GET    /rest/v1/<table>?<col>=eq.<value>     # single-row by primary key
POST   /rest/v1/<table>                       # body: JSON object or array
PATCH  /rest/v1/<table>?<filter>             # body: JSON object (column updates)
DELETE /rest/v1/<table>?<filter>
```

### Supported query params (v1)

- `select=col1,col2`           — column projection
- `<col>=eq.<value>`           — equality filter
- `<col>=gt.<value>`           — greater-than
- `<col>=lt.<value>`           — less-than
- `<col>=in.(a,b,c)`           — membership
- `<col>=is.null`              — null check
- `order=col.desc,other.asc`   — ordering
- `limit=N`                    — result cap
- `offset=N`                   — pagination

### Headers

- `Authorization: Bearer <jwt>` — required on every request. Verified
  via basin-auth.
- `Content-Type: application/json` — required on POST/PATCH.
- `Prefer: return=representation` — return inserted/updated rows.
- `Prefer: return=minimal` — empty body, status 201/204 only.
- `Range: <start>-<end>` — used for pagination on GET.
- `Range-Unit: items` — corresponds to `Content-Range` response.

### Auth flow

1. Client calls `POST /auth/v1/signin` (handled by `basin-auth`),
   receives `{ access_token, refresh_token }`.
2. Client calls `GET /rest/v1/events`, sends `Authorization: Bearer
   <access_token>`.
3. `basin-rest` verifies the JWT via `basin-auth::verify_token`,
   extracts `tenant_id` and `roles[]`.
4. The request runs against `basin-engine` scoped to that `tenant_id`.
5. RLS policies (when implemented) consult `roles[]` from the claims.

### What's *out* of v1

- **Embedded resources** (`?select=author(*)` to inline foreign-key
  rows). Requires foreign-key metadata; deferred until FKs land in the
  engine.
- **Stored functions** (`/rest/v1/rpc/<func>`). Basin doesn't have
  stored functions. Out of scope.
- **Realtime subscriptions** (websocket/SSE for table changes).
  Separate project; needs CDC, which we don't have yet.
- **GraphQL.** Different surface entirely; not on this roadmap.
- **Aggregates** (`?select=sum(amount)`). Defer; the SQL engine can
  do them but the URL syntax is finicky and not v1-critical.
- **Upserts** (`Prefer: resolution=merge-duplicates`). Defer to v2.

### Architecture

A new crate `basin-rest`:

```
crates/basin-rest/
  src/
    lib.rs        — public API: RestService, RestConfig
    server.rs     — axum-based HTTP server
    routes.rs     — endpoint handlers
    parser.rs     — URL → engine query translator
    json.rs       — Arrow → JSON encoder
    auth.rs       — basin-auth integration: extract JWT, verify, populate context
```

It uses **axum** for the HTTP layer (small, well-maintained, hyper
under the hood). No new dependency on a separate web framework.

Integration:

- `basin-rest::RestService::new(RestConfig { engine, auth, … })`
  takes references to `basin-engine::Engine` and `basin-auth::AuthService`.
- `services/basin-server` mounts the REST routes alongside the
  existing pgwire listener — same process, different ports
  (`BASIN_REST_BIND=127.0.0.1:5434` by default).
- The existing pgwire listener stays — REST is additive, not a
  replacement.

### Config

```text
BASIN_REST_ENABLED=1                       # off by default
BASIN_REST_BIND=127.0.0.1:5434
BASIN_REST_MAX_BODY_BYTES=1048576          # 1 MB default
BASIN_REST_DEFAULT_PAGE_SIZE=100
BASIN_REST_MAX_PAGE_SIZE=1000
BASIN_REST_CORS_ORIGINS=https://app.example.com
```

`BASIN_REST_ENABLED=1` requires `BASIN_AUTH_ENABLED=1`. Starting one
without the other is a fatal startup error.

## Consequences

**Positive**

- Frontend-only customers can ship without an application server.
  fetch() against `/rest/v1/...` with the JWT in headers — done.
- Same wedge math (per-tenant cheap multi-tenant) applies to REST
  customers as to pgwire customers.
- Demo gain: `curl https://demo.basin.io/rest/v1/events -H "Authorization: ..."`
  is much easier sales theatre than spinning up `psql`.

**Negative**

- HTTP is a fundamentally different security model from pgwire.
  REST requests come from anywhere on the internet; pgwire connections
  are typically from a private network. Every endpoint must defend
  against unauthenticated abuse, request smuggling, payload bombing.
- JSON encoding overhead. SELECT-and-render is markedly slower than
  the pgwire binary path; we eat the cost on the response side.
- Compared to writing SQL directly, REST customers will hit Basin's
  SQL gaps more visibly (e.g. the engine's UPDATE / DELETE absence
  shows up as 4xx responses on PATCH / DELETE).

**Mitigations**

- All endpoints rate-limited per-tenant via the same `governor` setup
  basin-auth uses.
- Body-size caps and result-row caps default to conservative values.
- The `OPTIONS` preflight is a fast path; CORS origin allowlist is
  required (not `*`).
- A 4xx on missing engine support carries a clear, documented error
  code (`E_ENGINE_UNSUPPORTED`) so client libraries can surface a
  useful message instead of a generic failure.

## Architectural compatibility

`basin-rest` does not change any other crate's public API. It
consumes the engine as a normal `Engine` handle. It is fully omittable
— a deploy that doesn't set `BASIN_REST_ENABLED=1` doesn't link or run
any of it (the crate itself stays in the workspace, but the binary
doesn't include it without the env flag → in practice we'll likely
gate on a Cargo feature so deploys can opt out at link time).

## Trigger to start building

We start building basin-rest when **all** of:

1. ADR 0005 (basin-auth) is accepted and shipping.
2. A customer asks for the REST API (a customer who already pays for
   Basin counts; a prospect does not).
3. The customer has a frontend / mobile / SDK use case that explicitly
   benefits — i.e. they are not just asking for it "to have parity
   with Supabase."

If 0005 is gated on a paid contract trigger and 0006 builds on top of
0005, basin-rest is at least 8 weeks behind any explicit pivot toward
the BaaS strategy. That sequencing is intentional.

## Estimated effort once triggered

| Component | Effort |
|---|---|
| URL parser (filters, order, select, limit, offset) | 1 week |
| Engine integration + JSON encoder | 0.5 week |
| Auth integration via basin-auth | 0.5 week |
| Rate limiting + body / row caps | 0.25 week |
| CORS, compression, content-encoding | 0.25 week |
| Tests (success paths, auth failures, malformed URLs, body bombs) | 1 week |
| **Total** | **~3–4 weeks calendar** (after basin-auth is shipped) |

## Alternatives considered

- **Run real PostgREST against Basin's pgwire endpoint.** Rejected as
  a 2–4 month slog requiring `pg_catalog` + `information_schema` we
  don't otherwise need (per the "PostgREST difficulty" discussion).
  All of that effort goes into an external compat surface, not Basin
  itself.
- **GraphQL instead of REST.** Different audience; defer until a
  customer specifically asks. No conflict — could be added as a
  parallel crate `basin-graphql` later.
- **Build the REST layer without auth in v1.** Hard rejected. A
  public REST endpoint over a multi-tenant database is a P0 incident
  waiting to happen. basin-auth gates this build.
- **Re-use basin-router's pgwire startup auth for REST.** Rejected —
  pgwire's auth is connection-scoped (one tenant per TCP connection);
  REST is per-request. Different shape, different code path.
