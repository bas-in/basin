# basin-sdk (Ruby)

Official Ruby client for [Basin](../../README.md)'s HTTP surfaces: REST data
API, auth, functions, and object storage. Bindings are derived from the server
source, not invented — every method cites the Rust route it talks to.

- **Stdlib-only HTTP** — uses `net/http` + `json` (no runtime gem deps for
  core).
- **Thread-safe auth** — Mutex-guarded session + auto-refresh 10 s before
  `access_expires_at`.
- **Chainable query builder** — `client.from("orders").select("id,total").eq("status","paid").limit(50).execute`.
- **Full auth surface** — email/password, magic links, OAuth authorize URL,
  full MFA (TOTP + WebAuthn), API keys, server-side sign-out.
- **Storage** — bucket CRUD, object upload/download/delete/list, signed URLs,
  public URLs.
- **Realtime WebSocket** — subscribe to table changes via block callbacks or
  Enumerator; presence; exponential-backoff reconnect.
- **Functions** — HTTP-handler functions (`/fn/v1/:name`) and catalog RPC
  (`/rest/v1/rpc/:fn`).

## Install

```ruby
# Gemfile
gem "basin-sdk"

# Realtime WebSocket support (optional extra):
gem "websocket-client-simple"
```

```sh
gem install basin-sdk
```

## Quickstart

```ruby
require "basin"

client = Basin::Client.new(
  url:        "https://api.basin.run",  # or http://localhost:8080 for local dev
  token:      "my-api-key",            # JWT or raw API key
  project_id: "01J..."                 # optional when token is a JWT with project_id claim
)

# Health check
client.health  # => "ok"

# Query builder
result = client.from("orders")
               .select("id,total,status")
               .eq("status", "paid")
               .gte("total", 100)
               .order("total", ascending: false)
               .limit(50)
               .execute

result.rows.each { |row| puts row }

# Keyset pagination
page      = client.from("orders").limit(100).page
next_page = client.from("orders").cursor(page.next_cursor).limit(100).page

# NDJSON streaming (large result sets)
client.from("events").stream { |row| process(row) }

# Writes
client.from("orders").insert({ "total" => 12, "status" => "new" })
client.from("orders").eq("id", 7).update({ "status" => "paid" })
client.from("orders").eq("id", 7).delete  # may raise E_ENGINE_UNSUPPORTED

# RPC / functions
total = client.rpc("add", { "a" => 40, "b" => 2 })
res   = client.functions.invoke("resize", body: { "width" => 100 })
```

## Auth (email/password, per-project)

```ruby
client = Basin::Client.new(url: "http://localhost:8080", project_id: "01J...")

# Sign up / sign in
client.auth.sign_up(email: "alice@example.com", password: "secret")
session = client.auth.sign_in(email: "alice@example.com", password: "secret")
# session stored; access token auto-refreshes before expiry

# API keys (JWT-gated)
key = client.auth.create_api_key("ci-pipeline")
puts key.secret   # shown exactly once
client.auth.delete_api_key(key.id)

# Magic links
client.auth.request_magic_link("alice@example.com")
session = client.auth.consume_magic_link("token-from-email")

# Sign out — revokes refresh token server-side, clears local session
client.auth.sign_out
```

## Storage

```ruby
# Create a public bucket
client.storage.create_bucket("avatars", public: true)

# Upload / download
bucket = client.storage.from_bucket("avatars")
bucket.upload("users/alice.png", File.binread("alice.png"), content_type: "image/png")
result = bucket.download("users/alice.png")
puts result.data.bytesize, result.content_type

# List objects
objects = bucket.list(prefix: "users/")

# Signed URL (time-boxed download, no JWT needed by caller)
signed = bucket.create_signed_url("users/alice.png", expires_in: 3600)
puts signed.absolute_url

# Public URL (bucket must be created with public: true)
url = bucket.get_public_url("users/alice.png")
```

## Realtime (WebSocket)

Requires the optional `websocket-client-simple` gem:

```ruby
gem "websocket-client-simple"  # Gemfile
```

```ruby
require "basin"
require "basin/realtime"

client = Basin::Client.new(
  url:        "http://localhost:8080",
  token:      "my-key",
  project_id: "01J..."
)

# Block/callback API (recommended for background threads)
client.realtime.subscribe("orders") do |event|
  puts "#{event.op} #{event.table} #{event.after}"
end

# Filter server-side
client.realtime.subscribe("orders", filter: "NEW.status='paid'") do |event|
  puts event.after
end

# Reconnect-resume: pass last_event_id to replay missed events
client.realtime.subscribe("orders", last_event_id: 42) do |frame|
  case frame
  when Basin::RealtimeGapFrame
    puts "gap — cold re-sync needed; oldest_in_ring=#{frame.oldest_in_ring}"
  when Basin::RealtimeEvent
    puts frame.seq, frame.op
  end
end

# Block the current thread running the receive loop (with auto-reconnect)
client.realtime.run_loop

# Enumerator API (pull-based)
enum = client.realtime.listen("orders")
loop { puts enum.next }

# Presence (Phoenix Channels shape)
client.realtime.presence_track("room:1", "user-abc", metadata: { name: "Alice" })
client.realtime.listen_presence("room:1") # Enumerator of PresenceStateFrame / PresenceDiffFrame
```

### Realtime event types

| Type | When |
|---|---|
| `Basin::RealtimeEvent` | INSERT / UPDATE / DELETE row; fields: `op`, `table`, `project`, `seq`, `before?`, `after?` |
| `Basin::RealtimeErrorFrame` | Protocol error for a table: `code="lag"` (missed events) or `code="invalid_filter"` |
| `Basin::RealtimeGapFrame` | Reconnect cursor predates the replay ring; cold re-sync needed |
| `Basin::PresenceStateFrame` | Snapshot of current members on join |
| `Basin::PresenceDiffFrame` | Incremental joins / leaves |
| `Basin::PresenceErrorFrame` | Rejected presence op (e.g. identity mismatch); wire type `"presenceerror"` (no underscore) |

### Reconnect behaviour

On unexpected disconnect, `RealtimeClient` reconnects with exponential backoff
(0.5 s, 1 s, 2 s … capped at 30 s) and re-issues all active subscriptions.
Pass `last_event_id:` to replay missed events from the server ring.

## OAuth

```ruby
client = Basin::Client.new(url: "http://localhost:8080", project_id: "01J...")

# 1. Get the provider authorize URL (server-built, includes PKCE + CSRF state).
result = client.auth.get_oauth_authorize_url(
  "google",
  redirect_to: "https://myapp.example.com/auth/callback"
)
# 2. Redirect the user's browser to result.redirect_url.
puts result.redirect_url  # https://accounts.google.com/o/oauth2/v2/auth?...
puts result.state         # CSRF state embedded in redirect_url
```

Supported preset providers: google, github, apple, bitbucket, discord, figma,
gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
twitter_x. Custom OIDC providers are registered server-side.

After the flow completes, restore the session from the tokens your app receives:

```ruby
session = Basin::Session.new(
  access_token:       "...",
  refresh_token:      "...",
  access_expires_at:  "...",
  refresh_expires_at: "..."
)
client.auth.set_session(session)
```

## MFA (TOTP and WebAuthn)

```ruby
# Enroll a TOTP factor (JWT required)
enroll = client.auth.enroll_factor("totp", friendly_name: "My Authenticator")
puts enroll.otpauth_uri   # display as QR code
puts enroll.secret_b32    # raw TOTP secret

# Confirm enrollment with the first OTP code
result = client.auth.verify_factor(enroll.factor_id, code: "123456")
puts result.recovery_codes  # shown once — store securely

# List factors
factors = client.auth.list_factors
factors.each { |f| puts "#{f.id} #{f.factor_type} #{f.status}" }

# Step-up: begin a challenge
challenge = client.auth.challenge_factor(enroll.factor_id)

# Complete the challenge → aal2 session
aal2_session = client.auth.verify_challenge(
  enroll.factor_id,
  challenge.challenge_id,
  code: "654321"
)

# Unenroll (requires aal2 token)
client.auth.unenroll_factor(enroll.factor_id)
```

WebAuthn flow:

```ruby
enroll = client.auth.enroll_factor("webauthn", friendly_name: "YubiKey")
# Pass enroll.creation_options_json to navigator.credentials.create() in JS.
client.auth.verify_factor(
  enroll.factor_id,
  attestation: "<json from navigator.credentials.create()>",
  challenge_id: enroll.challenge_id
)

challenge = client.auth.challenge_factor(factor_id)
# Pass challenge.request_options_json to navigator.credentials.get() in JS.
session = client.auth.verify_challenge(
  factor_id, challenge.challenge_id,
  assertion: "<json from navigator.credentials.get()>"
)
```

## Auth model

Everything is `Authorization: Bearer <token>`. The server tries JWT
verification first, then falls back to API-key lookup
(`crates/basin-rest/src/server.rs` `authorize`) — so `Basin::Client.new` accepts
either. After `auth.sign_in`, the session's access token takes precedence over
the static key and is **auto-refreshed** 10 seconds before `access_expires_at`.
A `Mutex` ensures only one refresh call is in-flight at once, so concurrent
threads safely share a client.

`sign_out` calls `POST /auth/v1/signout` to write a server-side revocation row,
then clears the local session regardless of the server response. Refresh tokens
rotate; reusing a rotated token surfaces as `E_REVOKED_TOKEN`.

## Error handling

Every non-2xx response raises `Basin::ApiError(code, message, status)`,
mirroring the server envelope `{"code": "E_...", "message": "..."}`.
Match on `code`, never on `message`:

```ruby
require "basin"

begin
  client.from("orders").delete
rescue Basin::ApiError => e
  case e.code
  when "E_ENGINE_UNSUPPORTED"
    puts "DELETE not supported on this table"
  when "E_UNAUTHENTICATED"
    client.auth.refresh_session
  else
    raise
  end
end
```

Known codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from a newer server pass
through as plain strings.

`Basin::NetworkError` (subclass of `Basin::Error`) is raised when the transport
fails before a server response arrives (connection refused, timeout, etc.).

## Rails integration

The SDK works cleanly in a Rails initializer alongside ActiveRecord (pgwire
raw connection is a separate surface):

```ruby
# config/initializers/basin.rb
BASIN = Basin::Client.new(
  url:        ENV.fetch("BASIN_URL"),
  token:      ENV.fetch("BASIN_KEY"),
  project_id: ENV.fetch("BASIN_PROJECT_ID")
)
```

Use `BASIN` from any controller or service object. The client is thread-safe
(one Mutex guards session state; the net/http layer creates a new `Net::HTTP`
per request — no shared socket).

## Route bindings (method → verified server route)

| SDK method | Route | Source |
|---|---|---|
| `auth.sign_up` | `POST /auth/v1/signup` | `server.rs:250` |
| `auth.sign_in` | `POST /auth/v1/signin` | `server.rs:251` |
| `auth.refresh_session` (+auto-refresh) | `POST /auth/v1/refresh` | `server.rs:252` |
| `auth.sign_out` | `POST /auth/v1/signout` | `server.rs:253` |
| `auth.verify_email` | `POST /auth/v1/verify-email` | `server.rs:254` |
| `auth.reset_password` | `POST /auth/v1/reset-password` | `server.rs:255` |
| `auth.request_password_reset` | `POST /auth/v1/request-password-reset` | `server.rs:256` |
| `auth.request_magic_link` | `POST /auth/v1/magic-link` (204) | `server.rs:262` |
| `auth.consume_magic_link` | `POST /auth/v1/magic-link/consume` | `server.rs:263` |
| `auth.create_api_key` / `list_api_keys` | `POST/GET /auth/v1/api-keys` | `server.rs:267-270` |
| `auth.delete_api_key` | `DELETE /auth/v1/api-keys/:id` | `server.rs:271` |
| `auth.get_oauth_authorize_url` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:277` |
| `auth.enroll_factor` | `POST /auth/v1/factors` | `server.rs:286` |
| `auth.list_factors` | `GET /auth/v1/factors` | `server.rs:287` |
| `auth.verify_factor` | `POST /auth/v1/factors/:id/verify` | `server.rs:290` |
| `auth.challenge_factor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:294` |
| `auth.verify_challenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:298` |
| `auth.unenroll_factor` | `DELETE /auth/v1/factors/:id` | `server.rs:302` |
| `from(t).execute` (select/eq/neq/gt/gte/lt/lte/in_/is_/order/limit/offset/cursor) | `GET /rest/v1/:table` | `server.rs:243-249`, `parser.rs` |
| `from(t).stream` | `GET /rest/v1/:table?stream=true` (NDJSON) | `server.rs:243`, `data.rs` |
| `from(t).insert` | `POST /rest/v1/:table` (201) | `server.rs:246` |
| `from(t).update` | `PATCH /rest/v1/:table?filters` | `server.rs:247` |
| `from(t).delete` | `DELETE /rest/v1/:table?filters` (may 501) | `server.rs:248`, `data.rs` |
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
| `realtime.subscribe` / `listen` / presence | `GET /realtime/v1/ws/:project` | `basin-realtime/src/ws.rs` |

## Not bound yet

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — SSE variant of
  the realtime surface.
- **Admin surface** (`/admin/v1/*`) — operator-grade; use `client.request()`
  as an escape hatch.
- **Native Arrow IPC** — no `application/vnd.apache.arrow.stream` endpoint
  exposed; use downstream `arrow` gem for client-side conversion if needed.
- **Cursor pagination helper** — iterate pages automatically; easy to build
  on top of `#page`.

## Development

```sh
cd sdk/basin-ruby
bundle install
bundle exec rspec spec/ -f doc      # offline suite (no server required)

# Live integration (optional, requires a running Basin server)
BASIN_LIVE_URL=http://localhost:8080 \
BASIN_LIVE_KEY=<key> \
BASIN_LIVE_PROJECT=<ulid> bundle exec rspec spec/live_spec.rb
```
