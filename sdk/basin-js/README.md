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

`signOut()` calls `POST /auth/v1/signout` to write a server-side revocation
row for the refresh token, then clears the local session. The local session is
cleared first, so the client is always considered signed out even if the server
call fails. With no active session the call is a no-op.

## Error handling

Every non-2xx response is thrown as `BasinApiError { code, status, message }`,
mirroring the server envelope `{ "code": "E_...", "message": "..." }`
(`crates/basin-rest/src/errors.rs`). Match on `code`, never on `message`.
Exception: `functions.invoke` proxies the guest function's own responses
verbatim (a function returning 418 is not an error) — only Basin-layer
envelopes (401/404/500) throw.

## OAuth

`auth.getOAuthAuthorizeUrl(provider, opts)` calls
`GET /auth/v1/oauth/:provider/authorize?project_id=&redirect_to=` and returns
`{ redirect_url, state }`. The server builds the full provider URL (with PKCE
+ signed CSRF state) — **the SDK cannot perform the browser redirect itself**.
Redirect the user's browser to `result.redirect_url`.

After the OAuth flow completes, Basin's callback handler
(`GET /auth/v1/oauth/:provider/callback`) exchanges the code and issues a
standard token body (Session). Typically your app receives these tokens via a
redirect to your `redirect_to` URL.

```ts
// 1. Get the authorize URL (server-side or before redirecting)
const { redirect_url } = await basin.auth.getOAuthAuthorizeUrl("github", {
  redirectTo: "https://app.example.com/auth/callback",
});

// 2. Redirect the user's browser — SDK can't do this:
window.location.href = redirect_url;

// 3. After the provider redirects back, your callback page receives the
//    tokens from the server and can call auth.setSession(...) to restore them.
```

Supported providers (preset): `google`, `github`, `apple`, `bitbucket`,
`discord`, `figma`, `gitlab`, `linkedin`, `microsoft` (`azure_ad`), `notion`,
`slack`, `spotify`, `twitch`, `twitter_x`. Custom OIDC providers can be
registered server-side; pass their name string here.

## MFA (TOTP and WebAuthn)

MFA adds a second authentication factor. After enrollment and verification, use
`challengeFactor` + `verifyChallenge` to obtain an **AAL2 JWT** — a session
whose access token carries `aal2` in its claims. Some server operations require
AAL2 (e.g. `unenrollFactor`).

### TOTP (authenticator app)

```ts
// 1. Enroll — get the secret and otpauth URI
const enroll = await basin.auth.enrollFactor("totp", {
  friendlyName: "Authenticator App",
});
// enroll.factor_type === "totp"
// Show enroll.otpauth_uri as a QR code, or display enroll.secret_b32

// 2. Verify enrollment with the first OTP code
const { ok, recovery_codes } = await basin.auth.verifyFactor(enroll.factor_id, {
  code: "123456",
});
// recovery_codes is present only on the first verified factor — store securely.

// 3. Step-up challenge (sign-in flow, sensitive operations)
const { challenge_id } = await basin.auth.challengeFactor(enroll.factor_id);

// 4. Complete challenge → receive AAL2 session (stored on the client automatically)
const session = await basin.auth.verifyChallenge(
  enroll.factor_id,
  challenge_id,
  { code: "654321" },
);
// session.access_token now carries aal2; client.auth.getSession() is updated.
```

### WebAuthn (hardware key / platform authenticator)

```ts
// 1. Enroll — get creation options for navigator.credentials.create()
const enroll = await basin.auth.enrollFactor("webauthn", {
  friendlyName: "YubiKey 5",
});
// enroll.factor_type === "webauthn"
// enroll.creation_options is already JSON-parsed, ready for the browser API:
const credential = await navigator.credentials.create({
  publicKey: enroll.creation_options,
});

// 2. Verify enrollment with the attestation
await basin.auth.verifyFactor(enroll.factor_id, {
  attestation: JSON.stringify(credential),
  challengeId: enroll.challenge_id,
});

// 3. Challenge → get request options for navigator.credentials.get()
const challenge = await basin.auth.challengeFactor(enroll.factor_id);
// "request_options" in challenge → WebAuthnChallengeResult
const assertion = await navigator.credentials.get({
  publicKey: challenge.request_options,
});

// 4. Complete challenge → AAL2 session
await basin.auth.verifyChallenge(
  enroll.factor_id,
  challenge.challenge_id,
  { assertion: JSON.stringify(assertion) },
);
```

### Factor management

```ts
// List enrolled factors
const factors = await basin.auth.listFactors();
// [{ id, factor_type, status, friendly_name, created_at, updated_at }, ...]

// Unenroll (requires AAL2 session)
await basin.auth.unenrollFactor(factorId);
```

## Route bindings (SDK method → verified server route)

| SDK method | Route | Source |
|---|---|---|
| `auth.signUp` | `POST /auth/v1/signup` | `crates/basin-rest/src/server.rs:250` |
| `auth.signIn` | `POST /auth/v1/signin` | `server.rs:251` |
| `auth.refreshSession` (+auto-refresh) | `POST /auth/v1/refresh` | `server.rs:252` |
| `auth.signOut` | `POST /auth/v1/signout` | `server.rs:253` |
| `auth.verifyEmail` | `POST /auth/v1/verify-email` | `server.rs:254` |
| `auth.resetPassword` | `POST /auth/v1/reset-password` | `server.rs:255` |
| `auth.requestPasswordReset` | `POST /auth/v1/request-password-reset` | `server.rs:256` |
| `auth.requestMagicLink` | `POST /auth/v1/magic-link` (204) | `server.rs:262` |
| `auth.consumeMagicLink` | `POST /auth/v1/magic-link/consume` | `server.rs:263` |
| `auth.createApiKey` / `listApiKeys` | `POST/GET /auth/v1/api-keys` | `server.rs:267` |
| `auth.deleteApiKey` | `DELETE /auth/v1/api-keys/:id` | `server.rs:271` |
| `auth.getOAuthAuthorizeUrl` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:277` |
| `auth.enrollFactor` | `POST /auth/v1/factors` | `server.rs:286` |
| `auth.listFactors` | `GET /auth/v1/factors` | `server.rs:287` |
| `auth.verifyFactor` | `POST /auth/v1/factors/:id/verify` | `server.rs:290` |
| `auth.challengeFactor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:294` |
| `auth.verifyChallenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:298` |
| `auth.unenrollFactor` | `DELETE /auth/v1/factors/:id` | `server.rs:302` |
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

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — WS covers the
  same events; SSE left for environments without WebSocket.
- **Admin surface** (`/admin/v1/*`: project provisioning, function
  deploy/list/logs/rollback, signing-key rotation) — operator-grade,
  deliberately out of an app SDK. Use `client.request()` as an escape hatch.
- **Inbound webhooks** (`POST /in/:project_id/:name`) — called *by external
  services*, not by app clients. `basin-webhooks` itself is an outbound
  delivery worker with no client-facing HTTP routes.
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
