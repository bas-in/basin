# @bas-in/basin-js

Isomorphic JavaScript / TypeScript client for [Basin](https://basin.run).
Speaks **directly** to a deployed [`basin-engine`](https://github.com/bas-in/basin)
(the open-source Rust core, Apache-2.0) — auth, PostgREST-shaped table
queries, object storage, realtime subscriptions, RPC functions, and admin
project tooling.

Works in Node 18+, browsers, Bun, Deno, Cloudflare Workers — anywhere
with a global `fetch`.

## Install

```sh
npm install @bas-in/basin-js
# or: pnpm add @bas-in/basin-js
# or: bun add @bas-in/basin-js
```

Deno: `import { createClient } from "jsr:@bas-in/basin-js";` (ships from the
same source via [JSR](https://jsr.io/@bas-in/basin-js); `jsr.json` is the
companion manifest in this repo). Browser via CDN:
`<script type="module" src="https://esm.sh/@bas-in/basin-js"></script>`.

### Self-host basin

basin-js works against **any** basin engine — the cloud-managed regional
deployments at `https://<region>.basin.run`, or a self-hosted engine you run
yourself (`cargo run -p basin-server`, or the published container). As of
2026-05-11, self-hosting needs **no external Postgres**: basin-auth (the
open-source Rust auth service) defaults to running on the basin engine's own
pgwire listener over loopback, so users / tenants / sessions / MFA / magic-
link state lives on the engine itself. One process. Point `createClient` at
the engine's HTTP base URL (`http://localhost:5434` by default) and the SDK
behaves identically to talking to the managed cloud.

**Self-host notes:**

- **Anon key:** mint your own anon key via `POST /auth/v1/api-keys` on your
  engine (requires an admin JWT). The returned key is the value you pass as
  the second argument to `createClient`.
- **Realtime ports:** the SSE and WebSocket transports bind on separate ports
  from the REST API. Make sure your reverse-proxy forwards both onto the same
  origin that the SDK talks to:
  - SSE — `BASIN_REALTIME_BIND` (default 5435)
  - WebSocket — `BASIN_REALTIME_WS_BIND` (default 5436)
  Without this, `basin.channel(…).subscribe()` will fail with a CORS or
  connection error.
- **Session storage and XSS:** by default `createClient` stores the session
  JWT in `localStorage`, which is readable by any JavaScript on the same
  origin. If your app is XSS-vulnerable, a compromised script can steal the
  token. Alternatives:
  - **Cookie storage (SSR / Next.js / SvelteKit):** use `createServerClient`
    from the `./ssr` sub-path export; it stores the session in an `httpOnly`
    cookie that JavaScript cannot read.
  - **Memory storage:** pass `authStorage: undefined` in the `BasinClientOptions`
    to keep the session only in memory — it is lost on page reload, which is
    the correct trade-off for highly sensitive contexts.
  ```ts
  // Memory-only session (no localStorage):
  const basin = createClient(url, anonKey, { authStorage: undefined });
  ```

## Quickstart

```ts
import { createClient } from "@bas-in/basin-js";

// BASIN_URL points at a deployed basin-engine — NOT basin-cloud.
// Mint BASIN_ANON_KEY at https://basin.run/app/project/<ref>/api-keys
const basin = createClient(
  process.env.BASIN_URL!,        // e.g. https://basin-engine.fly.dev
  process.env.BASIN_ANON_KEY!,
);

// Auth — hits engine /auth/v1/signin
await basin.auth.signInWithPassword({
  email: "you@example.com",
  password: "…",
});

// Query — hits engine /rest/v1/products
const { data, error } = await basin
  .from<{ id: number; name: string; price: number }>("products")
  .select("id, name, price")
  .eq("active", true)
  .order("price", { ascending: true })
  .limit(10);
```

## Architecture

[Basin Cloud](https://basin.run) is the **control plane** — dashboard,
billing, project management, and the place you mint the anon-key JWT
that the SDK ships to the engine. Once you have a URL + key,
basin-cloud is **off the data path**: every `basin.auth.*` and
`basin.from(...)` call lands on `basin-engine` directly. The engine
is open source and deployable to Fly with `./deploy.sh -t engine`
from the `basin-cloud` repo root, or runnable locally via
`cargo run -p basin-server`.

Engine routes: `/auth/v1/{signup,signin,refresh,verify-email,
reset-password,request-password-reset,magic-link,magic-link/consume,
oauth/:provider/authorize,factors/*}`, `/rest/v1/:table`,
`/storage/v1/object/*`, `/realtime/v1/{sse,ws}/*`,
`/admin/v1/projects/*`, `/health`. Two stubs remain:
`vectorSearch` and `asOf` return `BasinError('not_implemented')` until
basin-engine grows those operators.

Full reference: <https://basin.run/docs/js-sdk>.

## API surface

| Namespace          | Methods / notes |
|--------------------|-----------------|
| `basin.auth`       | `signUp`, `signInWithPassword`, `signInWithOAuth`, `signInWithMagicLink`, `consumeMagicLink`, `refreshSession`, `signOut`, `getSession`, `getUser`, `onAuthStateChange`, `requestPasswordReset`, `resetPassword`, `verifyEmail` |
| `basin.auth.mfa`   | `enroll`, `verify`, `challenge`, `challengeVerify`, `unenroll` — TOTP and WebAuthn, routes final per ADR 0020 |
| `basin.from(t)`    | `.select`, `.insert`, `.update`, `.upsert`, `.delete`, `.eq`, `.neq`, `.gt`, `.gte`, `.lt`, `.lte`, `.like`, `.ilike`, `.is`, `.in`, `.contains`, `.containedBy`, `.overlaps`, `.textSearch`, `.match`, `.not`, `.or`, `.filter`, `.order`, `.limit`, `.range`, `.single`, `.maybeSingle`, `.cursor`, `.paginate()`, `.stream()`, `.csv()`, `.geojson()`, `.explain()`, `.returning()`, `.headers()` |
| `basin.storage`    | `from(bucket).upload`, `.download`, `.list`, `.remove`, `.createSignedUrl`, `.getPublicUrl` — routes live per ADR 0021; `.uploadMultipart` / `.uploadResumable` return `not_implemented` (v0.3+) |
| `basin.channel(t)` | `.on('postgres_changes', filter, cb).subscribe()`, `.on('presence', filter, cb)`, `.unsubscribe()` — SSE or WS picked automatically; presence tracking (`track`/`untrack`/`presenceState`) is on the internal `PresenceChannel`, accessible via the WS transport |
| `basin.realtime`   | `.channel(topic)`, `.connect()`, `.disconnect()` |
| `basin.functions`  | `.invoke(fnName, { body })` → `POST /rest/v1/rpc/:fn_name` |
| `basin.admin`      | `projects.provision({ projectId })`, `projects.rotateCredentials(pgwireUser)`, `projects.listCredentials(projectId)` |

## Row Level Security and auth session functions

After `signInWithPassword` (or any sign-in method), the query builder
automatically attaches the JWT as `Authorization: Bearer <at>`. The engine
exposes three SQL session functions you can use in RLS policies and queries:

| Function | Returns | Description |
|---|---|---|
| `auth.uid()` | `uuid` | UUID of the signed-in user |
| `auth.role()` | `text` | `'authenticated'` or `'anon'` |
| `auth.jwt()` | `jsonb` | Full JWT claims |

Enable RLS and create a policy during schema setup (run once):

```sql
ALTER TABLE items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "users see own rows" ON items
  FOR ALL USING (owner_id = auth.uid());
```

After that, `basin.from('items').select('*')` automatically filters to the
signed-in user's rows — no extra code needed:

```ts
await basin.auth.signInWithPassword({ email, password });

// Returns only rows where owner_id = auth.uid()
const { data, error } = await basin.from('items').select('*');
```

Anonymous requests (`auth.role() = 'anon'`) get `auth.uid() = null`.

## Realtime

Subscribe to live table changes with `basin.channel()`. The SDK picks the
cheapest transport automatically — SSE for a simple single-table listener,
WebSocket when you need multiple tables, server-side row filters, or presence.

```ts
// Listen for new rows in the orders table (uses SSE automatically)
basin
  .channel("orders-feed")
  .on("postgres_changes", { event: "INSERT", table: "orders" }, (payload) => {
    console.log("new order:", payload.new);
  })
  .subscribe();

// Filtered subscription (uses WebSocket)
basin
  .channel("paid-orders")
  .on(
    "postgres_changes",
    { event: "INSERT", table: "orders", filter: "status=eq.paid" },
    (payload) => console.log(payload.new),
  )
  .subscribe();

// Presence — track online users in a channel (uses WebSocket)
const presenceChannel = basin
  .channel("room:lobby")
  .on("presence", { event: "sync" }, (members) => {
    console.log("online:", members);
  })
  .subscribe();

// presenceChannel.unsubscribe(); // close transport
```

Full guide: [`docs/realtime.md`](./docs/realtime.md).

## RPC / functions

Call server-side SQL or Wasm functions with `basin.functions.invoke()`.
The SDK POSTs named arguments to `/rest/v1/rpc/:fn_name` and returns the
result — a bare scalar for single-value functions, an array of row objects
for `RETURNS TABLE` functions.

```ts
// Scalar function: add(x int, y int) RETURNS int
const { data, error } = await basin.functions.invoke<number>("add", {
  body: { x: 3, y: 4 },
});
// data === 7

// RETURNS TABLE function
type User = { id: string; email: string };
const { data: users } = await basin.functions.invoke<User[]>("active_users", {
  body: { min_logins: 5 },
});
```

The active session JWT is forwarded automatically. Per-call auth overrides
and custom headers are supported via the `headers` option.

Full guide: [`docs/rpc.md`](./docs/rpc.md).

## API keys and pgwire connections

**API key format:** `basin_{tenant_id}_{base64}`. The SDK forwards the
key opaquely in the `apikey` header — no client-side parsing is done.
Keys are minted at `https://basin.run/app/project/<ref>/api-keys`.

**Direct pgwire connections** (advanced — for psql, DBeaver, migration
tools, etc.) use the engine's pgwire listener (default port 5433):

```
# Session/JWT auth — pass the access token as the username:
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API-key auth — username is {tenant_id}_{hex}, password is the full key:
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

After connecting via pgwire, `auth.uid()` / `auth.role()` / `auth.jwt()`
work identically to the REST path — the same RLS policies apply.

## Streaming and pagination

The query builder supports two streaming modes for large result sets:

```ts
// .stream() — NDJSON row-by-row (low memory, any size)
for await (const row of basin.from('events').select('*').stream()) {
  process(row);
}

// .paginate() — cursor-based transparent pagination
for await (const row of basin.from('events').select('*').order('id').paginate()) {
  process(row);
}
```

The engine auto-promotes responses > 1 MiB or > 10 000 rows to NDJSON.
The `then()` path on the builder handles this transparently — large
`.select()` calls always work even without explicit streaming.

## Not-implemented stubs

Two query-builder methods exist as forward-compat placeholders and return
`BasinError('not_implemented')` at runtime until the engine grows the
corresponding operators:

- `.vectorSearch(column, query, opts)` — vector ANN search (v0.3+)
- `.asOf(snapshotId)` — MVCC time-travel snapshot read (v0.3+)

Both storage upload variants are also stubs:
- `storage.from(bucket).uploadMultipart(...)` — multipart > 5 MB (v0.3+)
- `storage.from(bucket).uploadResumable(...)` — TUS resumable upload (v0.3+)

## Tree-shaking

Sub-path imports for consumers who only want one namespace:

```ts
import { AuthClient } from "@bas-in/basin-js/auth";
import { PostgrestQueryBuilder } from "@bas-in/basin-js/postgrest";
import { StorageClient } from "@bas-in/basin-js/storage";
import { createServerClient } from "@bas-in/basin-js/ssr";
import { FunctionsClient } from "@bas-in/basin-js/functions";
import { AdminClient } from "@bas-in/basin-js/admin";
```

## License

MIT — see [`LICENSE`](./LICENSE).
