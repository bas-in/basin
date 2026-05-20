# @bas-in/basin-js

Isomorphic JavaScript / TypeScript client for [Basin](https://basin.run).
Speaks **directly** to a deployed [`basin-engine`](https://github.com/bas-in/basin)
(the open-source Rust core, Apache-2.0) — auth, PostgREST-shaped table
queries today; storage, realtime, and edge functions in engine v0.2.

Works in Node 18+, browsers, Bun, Deno, Cloudflare Workers — anywhere
with a global `fetch`.

> **Status — `0.0.1` skeleton.** Public API surface is staked out so
> app code can be written against it. Method bodies land tier-by-tier;
> see [`TASKS.md`](./TASKS.md).

## Install

```sh
npm install @bas-in/basin-js
# or: pnpm add @bas-in/basin-js
# or: bun add @bas-in/basin-js
```

Deno: `import { createClient } from "jsr:@bas-in/basin-js";` (ships from the
same source via [JSR](https://jsr.io/@bas-in/basin-js); `jsr.json` is the
companion manifest in this repo). Browser via CDN:
`<script type="module" src="https://esm.sh/@bas-in/basin-js@0.1"></script>`.

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

Engine v0.1 routes: `/auth/v1/{signup,signin,refresh,verify-email,
reset-password,request-password-reset,magic-link,magic-link/consume,
api-keys}`, `/rest/v1/:table`, `/health`. OAuth, MFA, storage,
realtime, and functions return `BasinError('not_implemented')` until
engine v0.2.

Full reference: <https://basin.run/docs/js-sdk>.

## API surface

| Namespace        | Methods                                                                                                       |
|------------------|---------------------------------------------------------------------------------------------------------------|
| `basin.auth`     | `signUp`, `signInWithPassword`, `signInWithMagicLink`, `consumeMagicLink`, `refreshSession`, `signOut`, `getSession`, `getUser`, `onAuthStateChange`, `requestPasswordReset`, `resetPassword`, `verifyEmail` |
| `basin.from(t)`  | `.select`, `.insert`, `.update`, `.upsert`, `.delete`, `.eq`, `.neq`, `.gt`, `.gte`, `.lt`, `.lte`, `.like`, `.ilike`, `.is`, `.in`, `.order`, `.limit`, `.range`, `.single`, `.maybeSingle` |
| `basin.storage`  | `from(bucket).upload`, `.download`, `.list`, `.remove`, `.createSignedUrl`, `.getPublicUrl` — **v0.2** |
| `basin.realtime` | `.channel(name).on('postgres_changes', …).subscribe()`, `.on('presence', …)`, `.track()`, `.untrack()`, `.presenceState()` |
| `basin.functions`| `.invoke(fnName, { body })` → `POST /rest/v1/rpc/:fn_name` |

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
const channel = basin
  .channel("room:lobby")
  .on("presence", { event: "sync" }, (members) => {
    console.log("online:", members);
  })
  .subscribe();

channel.track({ userId: "u_42", status: "online" });
// channel.untrack();   // stop announcing
// channel.unsubscribe(); // close transport
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

## Tree-shaking

Sub-path imports for consumers who only want one namespace:

```ts
import { AuthClient } from "@bas-in/basin-js/auth";
import { PostgrestQueryBuilder } from "@bas-in/basin-js/postgrest";
import { StorageClient } from "@bas-in/basin-js/storage";
```

## License

MIT — see [`LICENSE`](./LICENSE).
