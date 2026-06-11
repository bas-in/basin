# basin-js

TypeScript client for [Basin](../../README.md)'s HTTP surfaces: REST data
API, auth, realtime (WebSocket), functions, and object storage.

- **Zero runtime dependencies** — built on the WHATWG `fetch` and `WebSocket`
  standards (Node 18+, Deno, Bun, browsers). Both are injectable for tests.
- **Derived from the server code, not invented** — every binding cites the
  Rust route it talks to. What isn't bound yet is listed at the bottom.

```ts
import { createClient } from "basin-js";

const basin = createClient("http://localhost:8080", BASIN_KEY, {
  projectId: "01J...", // optional; decoded from a JWT key/session when omitted
});

// Auth (per-project email/password; see flow notes below)
await basin.auth.signIn({ email: "a@b.c", password: "..." });

// REST query builder
const { rows, nextCursor } = await basin.from("orders")
  .select("id,total,status")
  .eq("status", "paid")
  .gte("total", 100)
  .order("total", { ascending: false })
  .limit(50);

// Writes
await basin.from("orders").insert({ total: 12, status: "new" });
await basin.from("orders").eq("id", 7).update({ status: "paid" });
await basin.from("orders").eq("id", 7).delete(); // may throw E_ENGINE_UNSUPPORTED

// RPC + HTTP-handler functions
const sum = await basin.rpc<number>("add", { a: 40, b: 2 });
const res = await basin.functions.invoke("resize", { body: { width: 100 } });

// Realtime
const channel = await basin.realtime.subscribe(
  "orders",
  { onEvent: (e) => console.log(e.op, e.after, e.seq) },
  { filter: "NEW.status = 'paid'" },
);
await channel.unsubscribe();

// Storage
await basin.storage.createBucket("avatars", { public: true });
await basin.storage.from("avatars").upload("u/1.png", bytes, { contentType: "image/png" });
const { data } = await basin.storage.from("avatars").download("u/1.png");
```

## Auth model

Everything is `Authorization: Bearer <token>`. The server tries JWT
verification first, then falls back to API-key lookup
(`crates/basin-rest/src/server.rs` `authorize`) — so `createClient(url, key)`
accepts either. After `auth.signIn(...)`, the session's access token takes
precedence over the static key, and is **auto-refreshed** via
`POST /auth/v1/refresh` when `access_expires_at` has passed. Refresh tokens
rotate; reusing a rotated token surfaces as `E_REVOKED_TOKEN`.

`signOut()` is **local-only**: the server exposes no sign-out / token-revoke
route today.

## Error handling

Every non-2xx response is thrown as `BasinApiError { code, status, message }`,
mirroring the server envelope `{ "code": "E_...", "message": "..." }`
(`crates/basin-rest/src/errors.rs`). Match on `code`, never on `message`.
Exception: `functions.invoke` proxies the guest function's own responses
verbatim (a function returning 418 is not an error) — only Basin-layer
envelopes (401/404/500) throw.

## Route bindings (SDK method → verified server route)

| SDK method | Route | Source |
|---|---|---|
| `auth.signUp` | `POST /auth/v1/signup` | `crates/basin-rest/src/server.rs:250` |
| `auth.signIn` | `POST /auth/v1/signin` | `server.rs:251` |
| `auth.refreshSession` (+auto-refresh) | `POST /auth/v1/refresh` | `server.rs:252` |
| `auth.verifyEmail` | `POST /auth/v1/verify-email` | `server.rs:253` |
| `auth.resetPassword` | `POST /auth/v1/reset-password` | `server.rs:254` |
| `auth.requestPasswordReset` | `POST /auth/v1/request-password-reset` | `server.rs:255-258` |
| `auth.requestMagicLink` | `POST /auth/v1/magic-link` (204) | `server.rs:262` |
| `auth.consumeMagicLink` | `POST /auth/v1/magic-link/consume` | `server.rs:263-266` |
| `auth.createApiKey` / `listApiKeys` | `POST/GET /auth/v1/api-keys` | `server.rs:267-270` |
| `auth.deleteApiKey` | `DELETE /auth/v1/api-keys/:id` | `server.rs:271-274` |
| `from(t)` GET (`select/eq/.../order/limit/offset/cursor/stream`) | `GET /rest/v1/:table` | `server.rs:243-249`, grammar `parser.rs` |
| `from(t).insert` | `POST /rest/v1/:table` (201) | `server.rs:246`, `routes/data.rs` |
| `from(t).update` | `PATCH /rest/v1/:table?filters` | `server.rs:247` |
| `from(t).delete` | `DELETE /rest/v1/:table?filters` (may 501) | `server.rs:248`, `data.rs:delete_table` |
| `rpc` / `functions.rpc` | `POST /rest/v1/rpc/:fn_name` | `server.rs:236`, `routes/rpc.rs` |
| `functions.invoke` | `ANY /fn/v1/:name` | `server.rs:238`, `routes/fn_handler.rs` |
| `realtime.subscribe/unsubscribe/presence*` | `GET /realtime/v1/ws/:project` + JSON frames | `crates/basin-realtime/src/ws.rs:191` |
| `storage.createBucket` | `POST /storage/v1/bucket` | `server.rs:373-376` |
| `storage.getBucket` / `deleteBucket` | `GET/DELETE /storage/v1/bucket/:name` | `server.rs:377-381` |
| `storage.from(b).upload/download/remove` | `POST/GET/DELETE /storage/v1/object/:bucket/*path` | `server.rs:409-414` |
| `storage.from(b).list` | `POST /storage/v1/object/list/:bucket` | `server.rs:417-420` |
| `storage.from(b).removeByPrefixes` | `DELETE /storage/v1/object/:bucket` | `server.rs:421-424` |
| `storage.from(b).getPublicUrl` | `GET /storage/v1/object/public/:project/:bucket/*path` | `server.rs:384-387` |
| `storage.from(b).createSignedUrl` | `POST /storage/v1/object/sign/upload/:bucket/*path` | `server.rs:397-400`, `routes/storage_sign.rs` |
| `health` | `GET /health` | `server.rs:368` |

The query grammar is Basin's PostgREST-*style* dialect (ops
`eq|neq|gt|gte|lt|lte|in|is`, `order=col.desc`, `limit/offset/cursor`). It is
**not** full PostgREST — no `or=`, `not.`, `like/ilike`, embedded resource
selects, or `Prefer` headers; filters always AND together.

### Realtime details

- Auth on upgrade uses the `Sec-WebSocket-Protocol: basin-v1,<token>`
  subprotocol (browsers can't set headers on WS upgrades); requires a **JWT**
  whose `project_id` matches the path (API keys won't work here).
- Server frames handled: `event` (`{before?, after?, seq}`), `subscribed`,
  `unsubscribed`, `error` (`lag` / `invalid_filter`), `gap` (replay-ring
  eviction — cold re-sync needed), `presence_state` / `presence_diff`.
- Pass `lastEventId` on subscribe to replay events missed while disconnected.

## Not bound yet (gap list)

- **OAuth** (`GET /auth/v1/oauth/:provider/{authorize,callback}`) — browser
  redirect flow; needs design beyond a fetch wrapper.
- **MFA** (`/auth/v1/factors*`) — TOTP/WebAuthn enroll + step-up challenge.
- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — WS covers the
  same events; SSE left for environments without WebSocket.
- **Admin surface** (`/admin/v1/*`: project provisioning, function
  deploy/list/logs/rollback, signing-key rotation) — operator-grade,
  deliberately out of an app SDK. Use `client.request()` as an escape hatch.
- **Inbound webhooks** (`POST /in/:project_id/:name`) — called *by external
  services*, not by app clients. `basin-webhooks` itself is an outbound
  delivery worker with no client-facing HTTP routes.
- **Sign-out / token revocation** — no server route exists; `signOut()` is
  local-only by design.
- **Signed upload URLs** — the `sign/upload` route mints time-boxed
  *download* URLs only (the `upload` path literal is an axum-router
  disambiguation, see `storage_sign.rs`).

## Development

```sh
npm install
npm test          # vitest unit tests (mocked fetch/WebSocket)
npm run typecheck
BASIN_URL=http://localhost:8080 npm test   # also runs the live integration spec
```
