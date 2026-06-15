# basin-sdk (Ruby)

Official Ruby client for [Basin](https://basin.run) — a Postgres-native BaaS
with an open-source Rust engine. Speaks directly to a deployed
[`basin-engine`](https://github.com/bas-in/basin) over its HTTP surfaces:
REST data API, auth, object storage, realtime WebSocket subscriptions, and
serverless functions.

- **Stdlib-only HTTP** — `net/http` + `json`; zero runtime gem dependencies
  for the core path.
- **Thread-safe** — a `Mutex`-guarded session with automatic token refresh
  (10 s before `access_expires_at`), safe to share across threads and Puma workers.
- **Chainable query builder** — fluent, filter-and-execute style.
- **Full auth surface** — email/password, magic links, OAuth, TOTP + WebAuthn
  MFA, API-key management, server-side sign-out, auto-refresh.
- **NDJSON streaming** — iterate large result sets row-by-row without buffering.
- **Arrow IPC** — optional `red-arrow` integration for columnar analytics.
- **Realtime** — block-callback and Enumerator APIs over WebSocket, with
  presence tracking and exponential-backoff reconnect.
- **Idiomatic Ruby** — keyword arguments, `Struct`-backed value objects,
  no `method_missing` magic.

Part of the Basin SDK family (`basin-js`, `basin-py`, `basin-ruby`, …). The
cloud dashboard at [basin.run](https://basin.run) is the control plane — once
you have a project URL and key, every SDK call lands on `basin-engine` directly.

## Install

```sh
gem install basin-sdk
```

Or in a `Gemfile`:

```ruby
gem "basin-sdk"

# Optional: realtime WebSocket support
gem "websocket-client-simple"

# Optional: Arrow IPC transport (requires the native Apache Arrow GLib library)
gem "red-arrow", ">= 14.0"
```

Then:

```sh
bundle install
```

### Self-hosting

`basin-sdk` works against any Basin engine — the managed regional deployments
at `https://<region>.basin.run`, or a self-hosted engine you run yourself
(`cargo run -p basin-server` or the published container). The engine runs
`basin-auth` on loopback over its own pgwire listener — no external Postgres
needed. Point `Basin::Client.new` at the engine's HTTP base URL and the SDK
behaves identically to the managed cloud.

Mint your own anon key via `POST /auth/v1/api-keys` (requires an admin JWT),
or use the Basin dashboard at `https://basin.run/app/project/<ref>/api-keys`.

## Quickstart

```ruby
require "basin"

# BASIN_URL is the engine base URL (NOT the cloud dashboard URL).
# BASIN_KEY is a JWT or raw API key.
client = Basin::Client.new(
  url:        ENV.fetch("BASIN_URL"),        # e.g. "https://basin-engine.fly.dev"
  token:      ENV.fetch("BASIN_KEY"),
  project_id: ENV.fetch("BASIN_PROJECT_ID")  # ULID; optional when token is a JWT
)

# Health check
client.health  # => "ok"

# Sign in — session is auto-refreshed before expiry
session = client.auth.sign_in(email: "alice@example.com", password: "secret")

# Query builder
result = client.from("orders")
               .select("id,total,status")
               .eq("status", "paid")
               .gte("total", 100)
               .order("total", ascending: false)
               .limit(50)
               .execute

result.rows       # => [{"id" => 1, "total" => 120, "status" => "paid"}, ...]
result.next_cursor  # => "opaque-token" or nil

# Writes
client.from("orders").insert({ "total" => 12, "status" => "new" })
client.from("orders").eq("id", 7).update({ "status" => "paid" })
client.from("orders").eq("id", 7).delete

# RPC / functions
total = client.rpc("add", { "a" => 40, "b" => 2 })
res   = client.functions.invoke("resize", body: { "width" => 100 })
```

### Rails initializer

```ruby
# config/initializers/basin.rb
BASIN = Basin::Client.new(
  url:        ENV.fetch("BASIN_URL"),
  token:      ENV.fetch("BASIN_KEY"),
  project_id: ENV.fetch("BASIN_PROJECT_ID")
)
```

The client is thread-safe. One `Mutex` guards session state; `net/http` opens
a fresh connection per request, so there are no shared sockets across threads.

## Auth

### Email / password

```ruby
client.auth.sign_up(email: "alice@example.com", password: "secret")
session = client.auth.sign_in(email: "alice@example.com", password: "secret")
# Session is stored on the client; access token auto-refreshes before expiry.

client.auth.sign_out  # revokes refresh token server-side, clears local session
```

### Magic links

```ruby
client.auth.request_magic_link("alice@example.com")  # 204 always
session = client.auth.consume_magic_link("token-from-email")
```

### OAuth

The engine builds the provider URL server-side (with PKCE + signed CSRF state).
Redirect the user's browser to `result.redirect_url`.

```ruby
result = client.auth.get_oauth_authorize_url(
  "google",
  redirect_to: "https://myapp.example.com/auth/callback"
)
# Redirect browser → result.redirect_url
# After the flow, restore the session from the tokens your app receives:
client.auth.set_session(
  Basin::Session.new(
    access_token:       "...",
    refresh_token:      "...",
    access_expires_at:  "...",
    refresh_expires_at: "..."
  )
)
```

Supported preset providers: google, github, apple, bitbucket, discord, figma,
gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
twitter_x. Custom OIDC providers are registered server-side.

### MFA — TOTP

```ruby
# Enroll
enroll = client.auth.enroll_factor("totp", friendly_name: "My Authenticator")
puts enroll.otpauth_uri   # display as QR code in your UI
puts enroll.secret_b32    # raw TOTP secret for manual entry

# Verify enrollment with the first OTP code
result = client.auth.verify_factor(enroll.factor_id, code: "123456")
puts result.recovery_codes  # shown once — store securely

# Step-up challenge
challenge = client.auth.challenge_factor(enroll.factor_id)
aal2_session = client.auth.verify_challenge(
  enroll.factor_id, challenge.challenge_id,
  code: "654321"
)

# Unenroll (requires aal2 access token)
client.auth.unenroll_factor(enroll.factor_id)
```

### MFA — WebAuthn / passkeys

```ruby
enroll = client.auth.enroll_factor("webauthn", friendly_name: "YubiKey")
# Pass enroll.creation_options_json to navigator.credentials.create() in JS.
client.auth.verify_factor(
  enroll.factor_id,
  attestation: "<json from navigator.credentials.create()>",
  challenge_id: enroll.challenge_id
)

challenge = client.auth.challenge_factor(enroll.factor_id)
# Pass challenge.request_options_json to navigator.credentials.get() in JS.
session = client.auth.verify_challenge(
  enroll.factor_id, challenge.challenge_id,
  assertion: "<json from navigator.credentials.get()>"
)
```

### API keys

```ruby
key = client.auth.create_api_key("ci-pipeline")
puts key.secret    # shown exactly once

keys = client.auth.list_api_keys
client.auth.delete_api_key(key.id)
```

### Auth model

All requests use `Authorization: Bearer <token>`. The server tries JWT
verification first and falls back to API-key lookup — so `Basin::Client.new`
accepts either. After `auth.sign_in`, the session's access token takes
precedence over the static key and is auto-refreshed 10 s before
`access_expires_at`. A `Mutex` serialises concurrent refresh calls.

`sign_out` calls `POST /auth/v1/signout` to write a server-side revocation row,
then clears the local session regardless of the server response. Refresh tokens
rotate on use; reusing a rotated token raises `ApiError` with
`code = "E_REVOKED_TOKEN"`.

## Query builder

```ruby
# Filter operators: eq, neq, gt, gte, lt, lte, in_, is_
client.from("products")
      .select("id,name,price")
      .eq("category", "electronics")
      .gte("price", 100)
      .lte("price", 1000)
      .in_("status", ["available", "preorder"])
      .is_("deleted_at", "null")
      .order("price", ascending: true)
      .order("name")           # multi-column sort (repeatable)
      .limit(25)
      .offset(50)
      .execute

# Keyset cursor pagination
page1 = client.from("events").limit(100).page  # Basin::Page
if page1.next_cursor
  page2 = client.from("events").cursor(page1.next_cursor).limit(100).page
end

# Rows shorthand (no cursor)
client.from("products").select("name").rows  # => [{"name" => "..."}, ...]
```

The filter grammar is AND-only. `or=` / `not.` / `like` / `ilike` / embedded
resources (foreign-key joins) are not yet supported by the engine's REST parser.

## NDJSON streaming

Stream large result sets row-by-row — the engine sends `stream=true` as
newline-delimited JSON. No intermediate buffering; suitable for millions of rows.

```ruby
# Block form — recommended
next_cursor = client.from("events").select("id,ts,payload").stream do |row|
  process(row)  # row is a Hash
end

# Enumerator form (lazy pull)
enum = client.from("events").stream
enum.each { |row| process(row) }
```

`stream` returns the `next_cursor` string when there are more pages (or `nil`).
The trailing sentinel line `{"_basin_next_cursor": "..."}` is consumed
internally and never yielded to the block.

## Arrow IPC transport

For columnar analytics, the query builder can request Arrow IPC instead of JSON:

```ruby
# Requires: gem "red-arrow" and the Apache Arrow GLib native library.
# macOS: brew install apache-arrow-glib
table, next_cursor = client.from("metrics").limit(10_000).to_arrow
# table is an Arrow::Table — full i64 / timestamp fidelity, zero JSON round-trip.

# Page through large tables
loop do
  table, cursor = client.from("metrics").limit(10_000).cursor(cursor).to_arrow
  process(table)
  break unless cursor
end
```

`to_arrow` raises a descriptive `LoadError` at call-time if `red-arrow` is not
installed. If the server falls back to JSON (older engine), the rows are
converted to an `Arrow::Table` transparently — same return shape.

## Storage

```ruby
# Bucket management
client.storage.create_bucket("avatars", public: true, file_size_limit: 10 * 1024 * 1024)
client.storage.get_bucket("avatars")
client.storage.delete_bucket("avatars")  # also purges all objects

# Object operations
bucket = client.storage.from_bucket("avatars")

bucket.upload("users/alice.png", File.binread("alice.png"), content_type: "image/png")

result = bucket.download("users/alice.png")
result.data          # => binary String
result.content_type  # => "image/png"
result.etag          # => "\"abc123\""

# List
objects = bucket.list(prefix: "users/", limit: 100)
objects.first.path   # => "users/alice.png"

# Delete
bucket.remove("users/alice.png")
bucket.remove_by_prefixes(["tmp/", "cache/"])  # bulk, prefix-based

# Signed download URL (time-boxed, no JWT needed by downloader)
signed = bucket.create_signed_url("users/alice.png", expires_in: 3600)
signed.absolute_url  # full URL for browser download

# Public URL (bucket must have public: true)
url = bucket.get_public_url("users/alice.png")
```

## Realtime

Realtime requires the optional `websocket-client-simple` gem. Add it to your
`Gemfile` and call `client.realtime` — the dependency is loaded lazily.

```ruby
require "basin"

client = Basin::Client.new(
  url: ENV.fetch("BASIN_URL"),
  token: ENV.fetch("BASIN_KEY"),
  project_id: ENV.fetch("BASIN_PROJECT_ID")
)

# Block/callback API — receives RealtimeEvent, RealtimeErrorFrame, RealtimeGapFrame
client.realtime.subscribe("orders") do |frame|
  case frame
  when Basin::RealtimeEvent
    puts "#{frame.op} on orders: #{frame.after}"
  when Basin::RealtimeErrorFrame
    warn "realtime error: #{frame.code} (missed #{frame.missed})"
  when Basin::RealtimeGapFrame
    warn "replay gap — resync from oldest_in_ring=#{frame.oldest_in_ring}"
  end
end

# Server-side filter (only rows matching the SQL predicate are sent)
client.realtime.subscribe("orders", filter: "NEW.status='paid'") do |frame|
  puts frame.after
end

# Reconnect-resume: replays missed events from the server ring
client.realtime.subscribe("orders", last_event_id: 42) { |frame| process(frame) }

# Block the current thread running the receive loop (with auto-reconnect)
client.realtime.run_loop

# Enumerator / pull-based API
enum = client.realtime.listen("orders")
loop { puts enum.next }

# Presence
client.realtime.presence_track("room:lobby", "user-abc", metadata: { name: "Alice" })
client.realtime.presence_heartbeat("room:lobby", "user-abc")  # every ~30 s

enum = client.realtime.listen_presence("room:lobby")
loop do
  frame = enum.next
  case frame
  when Basin::PresenceStateFrame then puts "snapshot: #{frame.presences.map(&:client_id)}"
  when Basin::PresenceDiffFrame  then puts "joins=#{frame.joins.size} leaves=#{frame.leaves.size}"
  end
end

client.realtime.unsubscribe("orders")
client.realtime.disconnect
```

### Realtime frame types

| Type | When |
|---|---|
| `Basin::RealtimeEvent` | INSERT / UPDATE / DELETE. Fields: `op`, `table`, `project`, `seq`, `before`, `after` |
| `Basin::RealtimeErrorFrame` | Protocol error for a table: `code="lag"` (missed events) or `code="invalid_filter"` |
| `Basin::RealtimeGapFrame` | Reconnect cursor predates the replay ring; cold re-sync needed |
| `Basin::SubscribedFrame` | Acknowledged server-side subscription |
| `Basin::PresenceStateFrame` | Snapshot of current members on join |
| `Basin::PresenceDiffFrame` | Incremental joins / leaves |
| `Basin::PresenceErrorFrame` | Rejected presence operation (wire type `"presenceerror"`, no underscore) |

### Reconnect behaviour

On unexpected disconnect, `RealtimeClient` reconnects with exponential backoff
(0.5 s, 1 s, 2 s … capped at 30 s) and re-issues all active subscriptions. Pass
`last_event_id:` to replay missed events from the server ring on reconnect.

## Functions

```ruby
# POST /rest/v1/rpc/:fn_name — catalog SQL or Wasm UDF
total = client.rpc("add", { "a" => 3, "b" => 4 })  # => 7 (bare scalar)
rows  = client.rpc("active_users", { "min_logins" => 5 })  # => [{"id"=>...}, ...]

# ANY /fn/v1/:name — HTTP-handler Wasm function (response proxied verbatim)
result = client.functions.invoke("resize", body: { "width" => 100 })
result.status   # => 200
result.data     # => parsed JSON or raw string
result.headers  # => Hash of response headers

# Custom HTTP method or headers
result = client.functions.invoke("webhook", method: "GET",
                                            headers: { "X-Hook-Secret" => "..." })
```

The active session JWT is forwarded automatically on both call paths.

## Error handling

Every non-2xx response raises `Basin::ApiError`. Match on `code`, never on
`message` — `code` is the stable contract; `message` is human-readable detail
that may change.

```ruby
begin
  client.from("orders").delete
rescue Basin::ApiError => e
  case e.code
  when "E_ENGINE_UNSUPPORTED"
    puts "DELETE is not supported on this table (#{e.status})"
  when "E_UNAUTHENTICATED"
    client.auth.refresh_session
    retry
  when "E_FORBIDDEN"
    puts "RLS denied access"
  else
    raise
  end
end

# SQLSTATE is present for SQL-layer errors (e.g. "23505" = unique violation)
rescue Basin::ApiError => e
  if e.sqlstate == "23505"
    puts "unique constraint violated"
  end
```

Known codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from a newer server pass
through as plain strings — `rescue Basin::ApiError` always catches them.

`Basin::NetworkError` (subclass of `Basin::Error`) is raised when the transport
fails before a server response arrives (connection refused, timeout, etc.).

## Raw requests

For routes not yet wrapped (e.g. `/admin/v1/*`):

```ruby
data = client.request("GET", "/admin/v1/projects/#{project_id}/credentials")
```

## pgwire connections

Direct Postgres-wire connections (psql, DBeaver, migration tools) use the
engine's pgwire listener (default port 5433):

```sh
# JWT / session auth — access token as username:
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API-key auth:
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

After connecting, `auth.uid()` / `auth.role()` / `auth.jwt()` are available
as SQL functions — the same RLS policies that apply to REST calls apply here.

## Development

```sh
bundle install
bundle exec rspec spec/ -f doc          # offline suite (no server required)

# Live integration (optional — requires a running Basin engine)
BASIN_LIVE_URL=http://localhost:8080 \
BASIN_LIVE_KEY=<key> \
BASIN_LIVE_PROJECT=<ulid> \
  bundle exec rspec spec/live_spec.rb
```

## License

MIT — see [`LICENSE`](./LICENSE).
