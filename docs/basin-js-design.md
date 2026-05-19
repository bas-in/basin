---
title: "basin-js v0.1 design spec"
nav_section: meta
sidebar_position: 31
summary: "Forward-spec for basin-js — TypeScript client SDK. Supabase-shaped API; talks pgwire/REST/WebSocket directly to a Basin engine. MIT-licensed."
---

# basin-js v0.1 design spec

## 1. Purpose & scope

basin-js is the TypeScript client SDK for Basin. Its job is to give
application developers the same ergonomic surface Supabase's JS client
provides — `createClient`, query builders, auth helpers, and a realtime
channel API — but wired directly to a Basin engine rather than to any
hosted cloud intermediary.

The target runtimes are browser (via bundler), Node.js, Deno, Bun, and
Cloudflare Workers. The SDK must run without modification across all five;
transport selection adapts to runtime availability.

**What basin-js IS:**

- A Supabase-API-shaped TypeScript client for application developers
- The primary recommended path for frontend and full-stack apps using Basin
- A thin adapter that translates a friendly query-builder surface into
  basin-rest HTTP requests, WebSocket subscriptions, and (in server
  environments) raw pgwire calls
- MIT-licensed, published to both npm and JSR

**What basin-js IS NOT:**

- An admin SDK — operations like creating projects, managing catalogs, or
  running schema migrations belong to basin-cli. basin-js has no
  `CREATE TABLE` surface and no admin credential flow.
- A migration framework — schema management is out of scope. Application
  developers run basin-cli migrations in CI; basin-js assumes the schema
  already exists.
- A bundled backend — basin-js does not embed or vendor the Basin engine.
  It connects to a running engine over HTTP or WebSocket.
- basin-cloud glue — basin-js talks directly to a Basin engine endpoint.
  If that endpoint is hosted by basin-cloud, fine; the SDK doesn't care.

---

## 2. API surface v0.1

The entry point mirrors the Supabase JS client exactly so application
developers porting an existing Supabase project face minimal diff.

```ts
import { createClient } from '@basin/basin-js'

const basin = createClient('https://my-project.basin.dev', anonKey)
```

**Query (REST under the hood)**

```ts
const { data, error } = await basin
  .from('users')
  .select('id, name')
  .eq('active', true)
  .limit(10)
```

Supported filter operators in v0.1: `eq`, `neq`, `gt`, `gte`, `lt`,
`lte`, `in`, `is` (null check). These map 1:1 to the basin-rest URL
param conventions defined in ADR 0006.

**Mutation**

```ts
// Insert one row
await basin.from('users').insert({ name: 'alice', active: true })

// Insert multiple rows
await basin.from('orders').insert([
  { user_id: 1, amount: 42 },
  { user_id: 2, amount: 99 },
])

// Update
await basin.from('users').update({ active: false }).eq('id', 7)

// Delete
await basin.from('users').delete().eq('id', 7)
```

`Prefer: return=representation` is sent by default; callers can opt into
`return=minimal` via a `.select(false)` modifier for fire-and-forget writes.

**Auth**

```ts
// Sign up
const { user, session, error } = await basin.auth.signUp({ email, password })

// Sign in
const { user, session } = await basin.auth.signInWithPassword({ email, password })

// Magic link
await basin.auth.signInWithMagicLink({ email })

// Get current session
const session = basin.auth.session()

// Sign out
await basin.auth.signOut()
```

Tokens are stored in `localStorage` in browser environments and in memory
in server environments (explicit session passing available for
Worker/serverless runtimes where `localStorage` is absent).

**Realtime (WebSocket)**

```ts
const channel = basin
  .channel('users')
  .on(
    'postgres_changes',
    { event: 'INSERT', schema: 'public', table: 'users' },
    (payload) => {
      console.log('new row', payload.new)
    }
  )
  .subscribe()

// Teardown
channel.unsubscribe()
```

WebSocket subscriptions are Tier 2 scope — the transport is defined here so
the channel API shape is locked before v0.1 ships, but the server-side CDC
that drives events is a separate Basin engine milestone.

**Raw SQL (pgwire via REST tunnel)**

```ts
// Available in all runtimes; routed through the REST tunnel
const { data, error } = await basin.rpc('exec_sql', {
  sql: 'SELECT id, name FROM users WHERE active = true ORDER BY id LIMIT 5',
})

// In Node / Deno / Bun: can also use the direct pgwire subpackage
import { createPgClient } from '@basin/basin-js/pgwire'
const pg = createPgClient({ host: 'localhost', port: 5432, database: 'myproject' })
const rows = await pg.query('SELECT ...', [])
```

---

## 3. Transports

basin-js uses three transport strategies depending on the operation type
and the runtime environment.

### REST (primary)

All CRUD operations — `from().select()`, `from().insert()`,
`from().update()`, `from().delete()` — and auth calls go over HTTPS to the
basin-rest endpoint (ADR 0006). REST is the only transport that works in
all five target runtimes, including browser. The URL shape follows
PostgREST conventions:

```
GET  /rest/v1/users?active=eq.true&select=id,name&limit=10
POST /rest/v1/users
```

The `Authorization: Bearer <jwt>` header is attached automatically from
the stored session. `basin-js` retries once on a 401 using the refresh
token before surfacing the error to the caller.

### WebSocket (realtime)

The `channel()` API opens a single multiplexed WebSocket connection to the
Basin engine's realtime endpoint. All channels for a given `createClient`
instance share one socket. The protocol is a lightweight JSON envelope:
subscribe, unsubscribe, and event messages. The connection reconnects with
exponential backoff on drop.

WebSocket is available in browser and server runtimes. Cloudflare Workers
support WebSocket clients natively (`new WebSocket(url)`); basin-js uses
the same `WebSocket` constructor interface across all runtimes, relying on
a one-line shim in Node < 22.

### pgwire-tunneled-over-REST

`basin.rpc('exec_sql', { sql })` POST-encodes a raw SQL string and returns
JSON rows. This is the escape hatch for queries the REST query builder
cannot express. It goes through the same basin-rest endpoint as CRUD —
no separate port, no special firewall rules. It is browser-safe.

### Direct pgwire (server runtimes only)

The `@basin/basin-js/pgwire` subpackage wraps a standard `pg` (node-postgres)
connection for Node, or `postgres` (postgres.js) for Deno/Bun. This path
gives server-side code prepared statements, binary encoding, and full SQL
without the JSON serialization overhead of the REST tunnel. It is explicitly
**not** available in browser builds — the `pgwire` subpackage is excluded
from the browser bundle entry point. Attempting to import it in a browser
bundler that does not tree-shake server subpackages will emit a build warning
(enforced via `exports` conditions in `package.json`).

---

## 4. Auth model

basin-js wraps the basin-auth JWT flow (ADR 0005) with a stateful session
manager. The session manager is the only stateful object in the SDK; every
other API call is stateless given a session.

**Token storage**

| Runtime | Default storage |
|---|---|
| Browser | `localStorage` under the key `basin-session` |
| Node / Deno / Bun | In-memory (process-scoped) |
| Cloudflare Workers | Caller-provided `KVNamespace` or in-memory |

Callers can override storage by passing a `storage` adapter to
`createClient({ storage })`. The adapter interface is three methods:
`get(key)`, `set(key, value)`, `remove(key)`.

**Refresh flow**

The access token TTL defaults to 1 hour (matches `BASIN_AUTH_TOKEN_TTL`).
On a 401 response, basin-js calls `POST /auth/v1/refresh` with the stored
refresh token, rotates both tokens in storage, and replays the original
request. If the refresh also fails (expired or revoked token), the session
is cleared and the SDK emits an `auth.onSessionExpired` event.

**RLS propagation**

Every REST request carries the JWT in the `Authorization` header.
basin-rest verifies the token and extracts `auth.uid()` and `auth.role()`
from claims, which are available to Row-Level Security policies on the
engine side. basin-js has no special client-side RLS logic — it just
forwards the token and lets the server enforce policy.

**Server-side usage**

For server environments (API routes, background jobs), callers can pass an
explicit access token rather than relying on the session cache:

```ts
const basin = createClient(url, anonKey, {
  global: { headers: { Authorization: `Bearer ${userToken}` } },
})
```

This pattern is safe in Cloudflare Workers and Lambda where there is no
shared process memory between requests.

---

## 5. Package metadata

**npm:** `@basin/basin-js`
**JSR:** `@bas-in/basin-js`

The dual-registry publish serves different audiences: npm is the standard
for Node/bundler ecosystems; JSR is the first-class registry for Deno and
Bun and supports native TypeScript without a compilation step.

**Tree-shaking**

The package uses subpath exports (`exports` field in `package.json`) to
ensure bundlers include only the code the application actually imports.
Entry points:

| Import path | Contents |
|---|---|
| `@basin/basin-js` | Core: REST client + auth. No WebSocket, no pgwire. |
| `@basin/basin-js/realtime` | WebSocket channel client (adds realtime). |
| `@basin/basin-js/pgwire` | Direct pgwire client (server runtimes only). |

**Dependencies**

The core entry point (`@basin/basin-js`) has **zero runtime dependencies**.
JWT decoding, token storage management, and the HTTP fetch layer are
implemented using Web Platform APIs (`fetch`, `localStorage`, `crypto`,
`WebSocket`) that exist in all five target runtimes. No `axios`, no
`jsonwebtoken`, no polyfill bundles.

The `realtime` subpackage has zero additional dependencies — WebSocket is
native.

The `pgwire` subpackage has a single optional peer dependency: the caller's
choice of `pg` (node-postgres) or `postgres` (postgres.js). basin-js wraps
whichever the caller already has; it does not mandate one.

---

## 6. Bundle size budget

| Entry point | Gzipped budget |
|---|---|
| Core (`createClient` + auth + REST) | ≤ 12 KB |
| Realtime add-on (`/realtime`) | ≤ 8 KB |
| pgwire-tunnel add-on (`exec_sql` helper) | ≤ 15 KB |

The 12 KB core budget is approximately half the size of the Supabase JS
client's core bundle. The smaller surface (no GraphQL, no Storage API, no
Edge Functions) makes this achievable without aggressive tricks. Bundle size
is checked in CI using `size-limit` configured in `package.json`.

---

## 7. TypeScript story

basin-js supports a `Database` type parameter on `createClient` that flows
through the query builder, making column names, filter values, and returned
row shapes fully type-safe.

```ts
import { createClient } from '@basin/basin-js'
import type { Database } from './basin-types' // generated file

const basin = createClient<Database>(url, anonKey)

// Type-safe: 'users' is a known table; 'id' and 'name' are known columns
const { data } = await basin
  .from('users')
  .select('id, name')
  .eq('active', true)
// data is inferred as Array<{ id: number; name: string }> | null
```

**Type generation**

basin-rest ships an OpenAPI schema endpoint (Phase 5.10, already shipped).
The type generator (`basin-js generate-types`) introspects that schema and
emits a `basin-types.ts` file — deferred to v0.2 (see Section 10).

For v0.1, users generate types by running basin-cli:

```sh
basin-cli generate-types --url https://my-project.basin.dev > basin-types.ts
```

The shape of `Database` is a top-level type with a `public` (or named
schema) key, tables under it, and `Row`, `Insert`, `Update` variants per
table. This mirrors the Supabase type generation output so tooling built for
Supabase types is compatible.

The TypeScript minimum target is **TypeScript 5.0**. No older compiler
versions are supported; the generic inference used by the query builder
relies on `5.x` conditional type improvements.

---

## 8. Repo structure

```
bas-in/basin-js/
  src/
    auth/
      client.ts        — AuthClient: signUp, signIn, signOut, session, refresh
      storage.ts       — StorageAdapter interface + localStorage / memory impls
      tokens.ts        — JWT decode (no verify — server-side verifies; client reads claims only)
    rest/
      client.ts        — RestClient: from(), rpc(), raw fetch helpers
      builder.ts       — QueryBuilder: select, insert, update, delete, filter chain
      response.ts      — shared { data, error } unwrap logic
    realtime/
      channel.ts       — Channel: on(), subscribe(), unsubscribe()
      socket.ts        — multiplexed WebSocket manager, reconnect logic
      protocol.ts      — JSON envelope types for subscribe/event/ack messages
    pgwire/
      client.ts        — PgwireClient wrapping pg / postgres.js
      index.ts         — re-export; excluded from browser bundle entry
    index.ts           — createClient, re-exports of public types
    types.ts           — Database generic, QueryResult, Session, User types
  tests/
    auth.test.ts
    rest.test.ts
    realtime.test.ts
    pgwire.test.ts
    types.test.ts      — compile-time type tests via tsd
  examples/
    browser/           — Vite app demonstrating CRUD + auth
    node/              — Express API route using basin-js + pgwire subpackage
    deno/              — Deno serve() handler
    bun/               — Bun.serve() handler
    workers/           — Cloudflare Worker with KV session storage
  package.json
  tsconfig.json
  tsconfig.build.json  — builds to dist/, CJS + ESM dual output
  size-limit.config.js — bundle size budget enforcement
  CHANGELOG.md         — Changesets-managed
```

---

## 9. Release flow

**Versioning**

Changesets (`@changesets/cli`) manages the versioning workflow. Contributors
open a PR with a changeset file describing the semver impact (patch, minor,
major). The CI Changesets bot comments on PRs with the pending version bump.

**Publish pipeline**

On merge to `main` when a changeset is present, the Changesets GitHub
Action opens a "Version Packages" PR. When that PR is merged:

1. `package.json` version is bumped.
2. `CHANGELOG.md` is updated.
3. A git tag `v{version}` is pushed.
4. CI publishes to **npm** via `npm publish --access public`.
5. CI publishes to **JSR** via `jsr publish`.

Both publish steps run in the same job using scoped secrets
(`NPM_TOKEN`, `JSR_TOKEN`). The JSR publish uses the `jsr.json` manifest
(mirrors `package.json` exports). Neither publish step runs on feature
branches — only on version tags.

**Pre-1.0 stability contract**

v0.x minor versions may include breaking changes in the TypeScript types
(not the runtime API). v0.x patch versions are safe to upgrade. The
`CHANGELOG.md` marks type-breaking changes explicitly.

---

## 10. What's deferred to v0.2

**Type-generation CLI built into basin-js**

`basin-js generate-types --url <engine-url>` as a first-party command is
deferred. v0.1 users generate types via basin-cli. The basin-js type
generator adds no user value beyond convenience; basin-cli already ships
the capability.

**Offline / optimistic cache**

An offline-first cache (service worker or IndexedDB-backed) that queues
mutations during network loss is architecturally straightforward but
operationally complex to test reliably. Deferred until a customer
explicitly requires it.

**Multi-region failover**

Client-side awareness of Basin's read-replica topology — automatically
routing reads to the nearest region — is deferred. v0.1 takes a single
engine URL. The cloud platform can handle region routing at the DNS layer
without SDK changes.

**OAuth / social sign-in helpers**

`basin.auth.signInWithOAuth({ provider: 'github' })` — deferred because
basin-auth v1 does not ship OAuth providers (ADR 0005). The SDK placeholder
is reserved; the method throws `E_NOT_IMPLEMENTED` in v0.1.

**Subscriptions with filter pushdown**

The v0.1 realtime channel delivers all `INSERT` / `UPDATE` / `DELETE`
events for a given table to the client, which filters client-side. Server-
side filter pushdown (only send events matching `WHERE user_id = $uid`) is a
server-side CDC milestone and deferred alongside it.

---

## 11. Estimate

One engineer, full-time:

| Component | Effort |
|---|---|
| Core REST client + query builder | 1 week |
| Auth client + token storage + refresh flow | 1 week |
| TypeScript generics + type tests | 0.5 week |
| Realtime WebSocket client + reconnect | 1 week |
| pgwire subpackage + examples | 0.5 week |
| Bundle size CI, dual CJS/ESM output, JSR manifest | 0.5 week |
| Tests (unit + integration against a real basin-server) | 1 week |
| **Total** | **~5.5 weeks** |

Range: **4–6 person-weeks** depending on integration test stability and
how many rough edges basin-rest exposes during development. The lower bound
assumes basin-rest and basin-auth are both fully stable before basin-js
development starts; the upper bound absorbs one week of fixes upstream.
