# basin-sdk

Python client for [Basin](../../README.md)'s HTTP surfaces: REST data API,
auth, functions, and object storage. Sync and async variants share the same
route/serialization layer via `httpx`.

- **Sync and async** — `BasinClient` (httpx.Client) and `AsyncBasinClient`
  (httpx.AsyncClient) with identical method surfaces.
- **Derived from the server code, not invented** — every binding cites the
  Rust route it talks to (see route table below).
- **Arrow support** — `to_arrow()` on any query result converts rows to a
  `pyarrow.Table`. This is a client-side JSON→Arrow conversion; native Arrow
  IPC transport from the server is not yet available (see notes).

## Install

```sh
pip install basin-sdk           # core (httpx only)
pip install "basin-sdk[arrow]"  # + pyarrow for .to_arrow()
```

## Quickstart

### Sync

```python
from basin import create_client

basin = create_client("http://localhost:8080", "my-api-key",
                       project_id="01J...")   # optional if key is a JWT

# Health check
basin.health()  # → "ok"

# Query builder
result = basin.table("orders") \
    .select("id,total,status") \
    .eq("status", "paid") \
    .gte("total", 100) \
    .order("total", ascending=False) \
    .limit(50) \
    .run()

for row in result.rows:
    print(row)

# Keyset pagination
page = basin.table("orders").limit(100).page()
next_page = basin.table("orders").cursor(page.next_cursor).limit(100).page()

# NDJSON streaming (large result sets)
for row in basin.table("events").stream():
    process(row)

# Writes
basin.table("orders").insert({"total": 12, "status": "new"})
basin.table("orders").eq("id", 7).update({"status": "paid"})
basin.table("orders").eq("id", 7).delete()  # may raise E_ENGINE_UNSUPPORTED

# RPC / functions
total = basin.rpc("add", {"a": 40, "b": 2})
res = basin.functions.invoke("resize", body={"width": 100})

# Arrow conversion (client-side JSON→Arrow; requires pyarrow)
table = basin.table("metrics").limit(10_000).to_arrow()
```

### Async

```python
import asyncio
from basin import create_async_client

async def main():
    async with create_async_client("http://localhost:8080", "my-key") as basin:
        result = await basin.table("orders").select("id,total").gte("total", 100).run()
        table = await basin.table("orders").to_arrow()

asyncio.run(main())
```

### Auth (email/password, per-project)

```python
basin = create_client("http://localhost:8080", project_id="01J...")

# Sign up / sign in
basin.auth.sign_up(email="alice@example.com", password="secret")
session = basin.auth.sign_in(email="alice@example.com", password="secret")
# session stored; access token auto-refreshes before expiry

# API keys (JWT-gated)
key = basin.auth.create_api_key("ci-pipeline")
print(key.secret)   # shown exactly once
basin.auth.delete_api_key(key.id)

# Magic links
basin.auth.request_magic_link("alice@example.com")
session = basin.auth.consume_magic_link("token-from-email")

# Sign out — local only; no server route exists
basin.auth.sign_out()
```

### Storage

```python
# Create a public bucket
basin.storage.create_bucket("avatars", public=True)

# Upload / download
bucket = basin.storage.from_bucket("avatars")
bucket.upload("users/alice.png", open("alice.png", "rb").read(), content_type="image/png")
result = bucket.download("users/alice.png")
print(result.data, result.content_type)

# List objects
objects = bucket.list(prefix="users/")

# Signed URL (time-boxed download, no JWT needed by caller)
signed = bucket.create_signed_url("users/alice.png", expires_in=3600)
print(signed.absolute_url)

# Public URL (bucket must have public=True)
url = bucket.get_public_url("users/alice.png")
```

## Auth model

Everything is `Authorization: Bearer <token>`. The server tries JWT
verification first, then falls back to API-key lookup
(`crates/basin-rest/src/server.rs` `authorize`) — so `create_client(url, key)`
accepts either. After `auth.sign_in(...)`, the session's access token takes
precedence over the static key, and is **auto-refreshed** 10 seconds before
`access_expires_at`. Refresh tokens rotate; reusing a rotated token surfaces
as `E_REVOKED_TOKEN`.

`sign_out()` is **local-only**: the server exposes no sign-out / token-revoke
route today (no `/auth/v1/signout` endpoint exists server-side).

## Error handling

Every non-2xx response raises `BasinApiError(code, message, status)`,
mirroring the server envelope `{"code": "E_...", "message": "..."}`.
Match on `code`, never on `message`:

```python
from basin import BasinApiError

try:
    basin.table("orders").delete()
except BasinApiError as e:
    if e.code == "E_ENGINE_UNSUPPORTED":
        print("DELETE not supported on this table")
    elif e.code == "E_UNAUTHENTICATED":
        basin.auth.refresh_session()
    else:
        raise
```

Known codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from a newer server pass
through as plain strings.

`BasinNetworkError` (subclass of `BasinError`) is raised when the transport
fails before a server response arrives (connection refused, timeout, etc.).

## Arrow transport notes

The server does **not** expose a native Arrow IPC endpoint (confirmed by
inspecting `crates/basin-rest/src/routes/data.rs` — no
`application/vnd.apache.arrow.stream` content-type negotiation exists).

`to_arrow()` performs a client-side conversion: it fetches the JSON result set
and converts via `pyarrow.Table.from_pylist()`. This works correctly for
typical column types but may lose numeric precision for very large integers
(which become Python `int` via JSON). A native server-side Arrow IPC stream
would preserve the declared schema — see follow-ups below.

## Route bindings (method → verified server route)

| SDK method | Route | Source |
|---|---|---|
| `auth.sign_up` | `POST /auth/v1/signup` | `server.rs:250` |
| `auth.sign_in` | `POST /auth/v1/signin` | `server.rs:251` |
| `auth.refresh_session` (+auto-refresh) | `POST /auth/v1/refresh` | `server.rs:252` |
| `auth.verify_email` | `POST /auth/v1/verify-email` | `server.rs:253` |
| `auth.reset_password` | `POST /auth/v1/reset-password` | `server.rs:254` |
| `auth.request_password_reset` | `POST /auth/v1/request-password-reset` | `server.rs:255` |
| `auth.request_magic_link` | `POST /auth/v1/magic-link` (204) | `server.rs:262` |
| `auth.consume_magic_link` | `POST /auth/v1/magic-link/consume` | `server.rs:263` |
| `auth.create_api_key` / `list_api_keys` | `POST/GET /auth/v1/api-keys` | `server.rs:267-270` |
| `auth.delete_api_key` | `DELETE /auth/v1/api-keys/:id` | `server.rs:271` |
| `auth.get_oauth_authorize_url` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:277-279` |
| (server-side only — browser redirect) | `GET /auth/v1/oauth/:provider/callback` | `server.rs:281-283` |
| `auth.enroll_factor` | `POST /auth/v1/factors` (201) | `server.rs:286-287` |
| `auth.list_factors` | `GET /auth/v1/factors` | `server.rs:286-287` |
| `auth.verify_factor` | `POST /auth/v1/factors/:id/verify` | `server.rs:290-291` |
| `auth.challenge_factor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:294-296` |
| `auth.verify_challenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:298-300` |
| `auth.unenroll_factor` | `DELETE /auth/v1/factors/:id` | `server.rs:302-303` |
| `table(t).run()` (select/eq/.../order/limit/offset/cursor/stream) | `GET /rest/v1/:table` | `server.rs:243-249`, `parser.rs` |
| `table(t).insert` | `POST /rest/v1/:table` (201) | `server.rs:246` |
| `table(t).update` | `PATCH /rest/v1/:table?filters` | `server.rs:247` |
| `table(t).delete` | `DELETE /rest/v1/:table?filters` (may 501) | `server.rs:248`, `data.rs` |
| `rpc` / `functions.rpc` | `POST /rest/v1/rpc/:fn_name` | `server.rs:236`, `routes/rpc.rs` |
| `functions.invoke` | `ANY /fn/v1/:name` | `server.rs:238`, `routes/fn_handler.rs` |
| `storage.create_bucket` | `POST /storage/v1/bucket` | `server.rs:373` |
| `storage.get_bucket` / `delete_bucket` | `GET/DELETE /storage/v1/bucket/:name` | `server.rs:377` |
| `storage.from_bucket(b).upload/download/remove` | `POST/GET/DELETE /storage/v1/object/:bucket/*path` | `server.rs:409` |
| `storage.from_bucket(b).list` | `POST /storage/v1/object/list/:bucket` | `server.rs:417` |
| `storage.from_bucket(b).remove_by_prefixes` | `DELETE /storage/v1/object/:bucket` | `server.rs:421` |
| `storage.from_bucket(b).get_public_url` | `GET /storage/v1/object/public/:project/:bucket/*path` | `server.rs:384` |
| `storage.from_bucket(b).create_signed_url` | `POST /storage/v1/object/sign/upload/:bucket/*path` | `server.rs:397`, `storage_sign.rs` |
| `health` | `GET /health` | `server.rs:368` |
| `realtime.listen` / `subscribe` / presence | `GET /realtime/v1/ws/:project` | `basin-realtime/src/ws.rs:191` |

## Realtime (WebSocket)

Receive INSERT / UPDATE / DELETE events as they happen via
`GET /realtime/v1/ws/:project`.

Requires the optional `realtime` extra:

```sh
pip install "basin-sdk[realtime]"   # adds websockets>=10
```

### Async generator (recommended)

```python
import asyncio
from basin import create_async_client

async def main():
    async with create_async_client("http://localhost:8080", "my-key",
                                    project_id="01J...") as basin:
        # Listen for all changes on "orders".
        async for event in basin.realtime.listen("orders"):
            print(event.op, event.table, event.after)

        # Filter server-side — only events where NEW.status = 'paid'.
        async for event in basin.realtime.listen(
            "orders", filter="NEW.status = 'paid'"
        ):
            print(event.after)

        # Reconnect-resume: pass the last seq you processed; the server
        # replays missed events from its ring buffer (or sends a
        # RealtimeGapFrame if the ring has been evicted).
        async for frame in basin.realtime.listen("orders", last_event_id=42):
            from basin import RealtimeGapFrame
            if isinstance(frame, RealtimeGapFrame):
                print("gap — cold re-sync needed", frame.oldest_in_ring)
            else:
                print(frame.seq, frame.op)

asyncio.run(main())
```

### Callback API

```python
async def on_event(ev):
    print(ev)

handle = await basin.realtime.subscribe("orders", on_event)
# … later:
await handle.unsubscribe()
```

### Presence (Phoenix Channels shape)

```python
# Listen for presence frames on a channel.
asyncio.ensure_future(
    basin.realtime.presence_track("room:1", client_id="user-abc",
                                   metadata={"name": "Alice"})
)
async for frame in basin.realtime.listen_presence("room:1"):
    from basin import PresenceStateFrame, PresenceDiffFrame
    if isinstance(frame, PresenceStateFrame):
        print("snapshot", frame.presences)
    elif isinstance(frame, PresenceDiffFrame):
        print("joins", frame.joins, "leaves", frame.leaves)
```

### Event types

| Type | When |
|---|---|
| `RealtimeEvent` | INSERT / UPDATE / DELETE row; fields: `op`, `table`, `project`, `seq`, `before?`, `after?` |
| `RealtimeErrorFrame` | Protocol error for a table: `code="lag"` (missed events) or `code="invalid_filter"` |
| `RealtimeGapFrame` | Reconnect cursor predates the replay ring; client must cold re-sync |
| `PresenceStateFrame` | Snapshot of current members on join |
| `PresenceDiffFrame` | Incremental joins / leaves |
| `PresenceErrorFrame` | Rejected presence op (e.g. identity mismatch) |

### Reconnect behaviour

On unexpected disconnect, `RealtimeClient` reconnects with exponential
backoff (0.5 s, 1 s, 2 s … capped at 30 s) and automatically re-issues all
active subscriptions.  Pass `last_event_id` to `listen()` to request
server-side replay of events missed during the gap.

## OAuth

Basin supports OAuth 2.0 / OIDC via preset providers (Google, GitHub, Apple,
Discord, Slack, etc.) or a custom OIDC endpoint configured server-side.

The SDK wraps the authorize URL builder only.  The browser redirect dance and
provider code exchange happen server-side — the SDK cannot open a browser tab.

```python
basin = create_client("http://localhost:8080", project_id="01J...")

# 1. Get the provider authorize URL.
result = basin.auth.get_oauth_authorize_url(
    "google",
    redirect_to="https://myapp.example.com/auth/callback",
)
# 2. Redirect the user's browser to result.redirect_url.
#    The server's GET /auth/v1/oauth/google/callback endpoint handles the code
#    exchange and issues Basin JWT + refresh tokens.
print(result.redirect_url)  # https://accounts.google.com/o/oauth2/v2/auth?...
print(result.state)         # CSRF state value embedded in redirect_url
```

After the flow completes, your app receives a Basin token pair via your
`redirect_to` URL (query parameters or fragment, depending on your
server-side setup).  Create a session from those tokens:

```python
from basin import Session
session = Session(
    access_token="...",
    refresh_token="...",
    access_expires_at="...",
    refresh_expires_at="...",
)
basin.auth.set_session(session)
```

**Supported preset providers:** google, github, apple, bitbucket, discord,
figma, gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify,
twitch, twitter_x (twitter).  Custom OIDC providers are registered
server-side and passed by name.

## MFA (TOTP and WebAuthn)

Basin supports TOTP (RFC 6238, 6-digit, 30-second step) and WebAuthn /
FIDO2 passkeys.  A successful challenge elevates the session to AAL2 — the
access token's JWT claims reflect this, and RLS policies can gate on `aal2`.

### Factor lifecycle

```python
import asyncio
from basin import create_async_client

async def mfa_demo():
    async with create_async_client("http://localhost:8080", "jwt-token",
                                    project_id="01J...") as basin:

        # 1. Enroll a TOTP factor (JWT required).
        enroll = await basin.auth.enroll_factor("totp", friendly_name="My Authenticator")
        # enroll.secret_b32 → display as QR code via enroll.otpauth_uri
        # enroll.factor_id  → save for later calls
        print(enroll.otpauth_uri)

        # 2. Confirm enrollment with the first OTP code.
        result = await basin.auth.verify_factor(enroll.factor_id, code="123456")
        if result.recovery_codes:
            print("Save these codes:", result.recovery_codes)  # shown once

        # 3. List factors.
        factors = await basin.auth.list_factors()
        for f in factors:
            print(f.id, f.factor_type, f.status)

        # 4. Step-up: begin a challenge.
        challenge = await basin.auth.challenge_factor(enroll.factor_id)
        # challenge.challenge_id → pass to verify_challenge

        # 5. Complete the challenge → aal2 session.
        aal2_session = await basin.auth.verify_challenge(
            enroll.factor_id,
            challenge.challenge_id,
            code="654321",
        )
        # aal2_session is now stored as the live session.

        # 6. Unenroll (requires aal2 token).
        await basin.auth.unenroll_factor(enroll.factor_id)

asyncio.run(mfa_demo())
```

### WebAuthn

```python
# Enroll
enroll = await basin.auth.enroll_factor("webauthn", friendly_name="YubiKey")
# Pass enroll.creation_options_json to navigator.credentials.create() in JS.
# Then call verify_factor with the attestation response:
await basin.auth.verify_factor(
    enroll.factor_id,
    attestation='<json from navigator.credentials.create()>',
    challenge_id=enroll.challenge_id,
)

# Step-up
challenge = await basin.auth.challenge_factor(factor_id)
# Pass challenge.request_options_json to navigator.credentials.get() in JS.
session = await basin.auth.verify_challenge(
    factor_id,
    challenge.challenge_id,
    assertion='<json from navigator.credentials.get()>',
)
```

### Sync variant

All MFA methods have identical sync signatures:

```python
basin = create_client("http://localhost:8080", "jwt-token", project_id="01J...")
enroll = basin.auth.enroll_factor("totp")
result = basin.auth.verify_factor(enroll.factor_id, code="123456")
challenge = basin.auth.challenge_factor(enroll.factor_id)
session = basin.auth.verify_challenge(enroll.factor_id, challenge.challenge_id, code="654321")
factors = basin.auth.list_factors()
basin.auth.unenroll_factor(enroll.factor_id)
```

## Not bound yet (gap list)

- **Realtime WebSocket** — now wrapped (see above).
- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — SSE variant of
  the realtime surface.
- **Admin surface** (`/admin/v1/*`) — operator-grade; use `client.request()`
  as an escape hatch.
- **Sign-out / token revocation** — no server route exists; `sign_out()` is
  local-only by design.
- **Native Arrow IPC** — no `application/vnd.apache.arrow.stream` endpoint
  exists server-side; `to_arrow()` is a client-side fallback today.

## Development

```sh
cd sdk/basin-python
pip install -e ".[dev]"
pytest tests/ -v                              # offline suite (all fast, no server)
BASIN_LIVE_URL=http://localhost:8080 \
BASIN_LIVE_KEY=<key> \
BASIN_LIVE_PROJECT=<ulid> pytest tests/test_live.py -v   # live integration
```
