# basin-swift

Swift client for [Basin](../../README.md)'s HTTP surfaces: REST data API, auth,
functions, object storage, and realtime WebSocket.

- **Swift 5.9+ / SwiftPM** — iOS 15+, macOS 12+, tvOS 15+, watchOS 8+.
- **Zero external dependencies** — URLSession for HTTP, URLSessionWebSocketTask
  for realtime, Foundation's Codable for JSON. All built-in.
- **Actor-isolated** — `BasinClient` uses Swift actors throughout; concurrent
  calls to `accessToken()` coalesce safely.
- **async/await throughout** — idiomatic Swift concurrency, no callbacks.
- **Derived from the server code** — every binding cites the Rust route it
  talks to (see route table below).

## Install

### Swift Package Manager

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/basin-db/basin-swift", from: "0.1.0")
],
targets: [
    .target(name: "MyApp", dependencies: ["Basin"])
]
```

Or in Xcode: **File → Add Package Dependencies**, enter the repo URL.

## Quickstart

```swift
import Basin

let client = BasinClient(url: "https://project.basin.run", key: "my-api-key",
                          projectID: "01J...")   // optional if key is a JWT

// Health check
let status = try await client.health()  // → "ok"

// Query builder — generic over any Decodable
struct Order: Decodable {
    let id: Int; let total: Int; let status: String
}

let orders: [Order] = try await client.table("orders")
    .select("id,total,status")
    .eq("status", .string("paid"))
    .gte("total", .int(100))
    .order("total", ascending: false)
    .limit(50)
    .run()

// Keyset pagination
let page: QueryPage<Order> = try await client.table("orders").limit(100).page()
let nextPage: QueryPage<Order> = try await client.table("orders")
    .cursor(page.nextCursor!)
    .limit(100)
    .page()

// NDJSON streaming — rows arrive incrementally, no full-buffer in memory
for try await order in client.table("orders").stream() as AsyncThrowingStream<Order, Error> {
    process(order)
}

// Streaming with pagination cursor
let streamResult = client.table("orders").limit(1000).streamPage() as StreamPage<Order>
for try await order in streamResult.rows { process(order) }
let nextCursor = await streamResult.nextCursor  // available after loop

// Writes
struct NewOrder: Encodable { let total: Int; let status: String }
_ = try await client.table("orders").insert(NewOrder(total: 12, status: "new"))
_ = try await client.table("orders").eq("id", .int(7)).update(["status": "paid"])
_ = try await client.table("orders").eq("id", .int(7)).delete()

// RPC / functions
let result = try await client.rpc("add", args: ["a": 40, "b": 2])
let fnResult = try await client.functions.invoke("resize",
                                                  body: #"{"width":100}"#.data(using: .utf8))
```

## NDJSON streaming

`stream()` and `streamPage()` send `?stream=true` to the server and iterate
rows line-by-line via `URLSession.bytes`, avoiding buffering the full result
set in memory.  Useful for large exports or long-running queries.

Route: `GET /rest/v1/:table?stream=true&…`
Response: newline-delimited JSON rows; optional trailing
`{"_basin_next_cursor":"…"}` line when paginating.

```swift
struct Event: Decodable { let id: Int; let type: String; let ts: String }

// Simple streaming — rows only, cursor is discarded
for try await event in client.table("events")
    .eq("type", .string("click"))
    .stream() as AsyncThrowingStream<Event, Error> {
    process(event)
}

// Streaming with cursor capture — for paginating over large tables
let page = client.table("events").limit(5000).streamPage() as StreamPage<Event>
for try await event in page.rows {
    process(event)
}
// Cursor is set once the for-loop above finishes (stream is exhausted).
if let cursor = await page.nextCursor {
    let nextPage = client.table("events").cursor(cursor).limit(5000).streamPage() as StreamPage<Event>
    // …
}
```

`stream()` / `streamPage()` respect all query builder filters, ordering,
`select()`, `limit()`, and `cursor()` — the same filter chain works for
both buffered (`run()`) and streaming variants.

## Auth (email/password, per-project)

```swift
let client = BasinClient(url: "https://project.basin.run", projectID: "01J...")

// Sign up / sign in
try await client.auth.signUp(email: "alice@example.com", password: "secret")
let session = try await client.auth.signIn(email: "alice@example.com", password: "secret")
// Session is stored automatically; access token auto-refreshes before expiry.

// API keys (JWT-gated)
let key = try await client.auth.createApiKey(name: "ci-pipeline")
print(key.secret)   // shown exactly once
try await client.auth.deleteApiKey(key.id)

// Magic links
try await client.auth.requestMagicLink(email: "alice@example.com")
let session = try await client.auth.consumeMagicLink(token: "token-from-email")

// Sign out — revokes refresh token server-side, clears local session.
// Local session is always cleared even on network error.
await client.auth.signOut()
```

### iOS Keychain persistence

The SDK stores the session in memory only. For iOS apps, persist the session
across launches using the Keychain:

```swift
import Security

func saveSession(_ session: Session) {
    guard let data = try? JSONEncoder().encode(session) else { return }
    let query: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrAccount as String: "basin-session",
        kSecValueData as String: data
    ]
    SecItemDelete(query as CFDictionary)
    SecItemAdd(query as CFDictionary, nil)
}

func loadSession() -> Session? {
    let query: [String: Any] = [
        kSecClass as String: kSecClassGenericPassword,
        kSecAttrAccount as String: "basin-session",
        kSecReturnData as String: true
    ]
    var result: AnyObject?
    guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
          let data = result as? Data else { return nil }
    return try? JSONDecoder().decode(Session.self, from: data)
}

// Restore session on app launch:
if let saved = loadSession() {
    await client.auth.setSession(saved)
}
// After sign-in, persist:
let session = try await client.auth.signIn(email: email, password: password)
saveSession(session)
```

## OAuth

```swift
// 1. Get the provider authorize URL (no JWT required for this call).
let result = try await client.auth.getOAuthAuthorizeURL(
    provider: "google",
    redirectTo: "https://myapp.example.com/auth/callback"
)
// 2. Open result.redirectUrl in a browser or ASWebAuthenticationSession.
//    The server's callback endpoint exchanges the code and issues tokens.
print(result.redirectUrl)
print(result.state)  // CSRF state value embedded in redirectUrl

// 3. After the flow, restore the session from the tokens your server receives:
let session = Session(
    accessToken: "...", refreshToken: "...",
    accessExpiresAt: "...", refreshExpiresAt: "..."
)
await client.auth.setSession(session)
```

Supported preset providers: google, github, apple, bitbucket, discord, figma,
gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify, twitch,
twitter_x (twitter). Custom OIDC providers are registered server-side and
passed by name.

## MFA (TOTP and WebAuthn)

```swift
// 1. Enroll a TOTP factor (JWT required).
let enroll = try await client.auth.enrollFactor(factorType: "totp",
                                                 friendlyName: "My Authenticator")
if case .totp(let t) = enroll {
    print(t.otpauthUri)  // render as QR code
    // 2. Confirm enrollment with first OTP code.
    let verified = try await client.auth.verifyFactor(t.factorId, code: "123456")
    if let codes = verified.recoveryCodes {
        print("Save these:", codes)  // shown exactly once
    }
    // 3. Step-up: begin a challenge.
    let challenge = try await client.auth.challengeFactor(t.factorId)
    // 4. Complete challenge → aal2 session.
    let aal2 = try await client.auth.verifyChallenge(
        t.factorId, challengeID: challenge.challengeId, code: "654321"
    )
    // 5. Unenroll (requires aal2 token).
    try await client.auth.unenrollFactor(t.factorId)
}

// WebAuthn
let enroll = try await client.auth.enrollFactor(factorType: "webauthn")
if case .webAuthn(let w) = enroll {
    // Pass w.creationOptionsJson to navigator.credentials.create() via JS bridge.
    try await client.auth.verifyFactor(w.factorId,
                                        attestation: "<attestation JSON>",
                                        challengeID: w.challengeId)
}
```

## Storage

```swift
// Create a public bucket
try await client.storage.createBucket("avatars", public: true)

// Upload / download
let bucket = await client.storage.fromBucket("avatars")
let obj = try await bucket.upload("users/alice.png",
                                   data: imageData,
                                   contentType: "image/png")
let result = try await bucket.download("users/alice.png")
print(result.data, result.contentType as Any)

// List objects
let objects = try await bucket.list(prefix: "users/")

// Signed URL (time-boxed download, no JWT needed by caller)
let signed = try await bucket.createSignedURL("users/alice.png", expiresIn: 3600)
print(signed.absoluteUrl)

// Public URL (bucket must have public=true)
let url = try await bucket.getPublicURL("users/alice.png")
```

## Realtime (WebSocket)

Receive INSERT / UPDATE / DELETE events as they happen via
`GET /realtime/v1/ws/:project`.

Auth is performed via the `Sec-WebSocket-Protocol: basin-v1, <token>`
subprotocol header — no custom header needed.

```swift
let client = BasinClient(url: "https://project.basin.run",
                          key: "my-key", projectID: "01J...")

// Async stream (recommended)
let stream = await client.realtime.listen(table: "orders")
for try await frame in stream {
    if case .event(let e) = frame {
        print(e.op, e.table, e.after as Any)
    }
}

// With server-side filter
let filtered = await client.realtime.listen(
    table: "orders",
    filter: "NEW.status = 'paid'"
)
for try await frame in filtered {
    if case .event(let e) = frame { print(e.after as Any) }
}

// Reconnect-resume: pass the last seq you processed.
// Server replays missed events from its ring buffer, or sends a gap frame.
let resume = await client.realtime.listen(table: "orders", lastEventID: 42)
for try await frame in resume {
    switch frame {
    case .gap(let g):
        print("gap — cold re-sync needed, oldest:", g.oldestInRing)
    case .event(let e):
        print(e.seq, e.op)
    default:
        break
    }
}
```

### Presence (Phoenix Channels shape)

```swift
// Track presence in a channel.
try await client.realtime.presenceTrack(
    channel: "room:1",
    clientID: "user-abc",
    metadata: ["name": "Alice"]
)

// Listen for presence frames.
let presenceStream = await client.realtime.listenPresence(channel: "room:1")
for try await frame in presenceStream {
    switch frame {
    case .presenceState(let s):
        print("snapshot:", s.presences.map { $0.clientId })
    case .presenceDiff(let d):
        print("joins:", d.joins.map { $0.clientId })
        print("leaves:", d.leaves.map { $0.clientId })
    default:
        break
    }
}
```

### Reconnect behaviour

On unexpected disconnect, `RealtimeClient` reconnects with exponential backoff
(0.5 s, 1 s, 2 s … capped at 30 s) and automatically re-issues all active
subscriptions. Pass `lastEventID` to request server-side replay of events
missed during the gap.

## Error handling

Every non-2xx response throws `BasinApiError`. Match on `code`, never on
`message`:

```swift
do {
    let orders: [Order] = try await client.table("orders").delete().run()
} catch let e as BasinApiError {
    switch e.code {
    case .engineUnsupported:
        print("DELETE not supported on this table")
    case .unauthenticated:
        try await client.auth.refreshSession()
    default:
        throw e
    }
} catch let e as BasinNetworkError {
    print("network error:", e)
}
```

Known codes: `.unauthenticated`, `.forbidden`, `.notFound`, `.invalidRequest`,
`.rateLimited`, `.engineUnsupported`, `.internal`, `.emailDisabled`,
`.revokedToken`. Unknown codes from a newer server come through as `.unknown`
(with the raw string in `rawCode`).

## Route bindings (method → verified server route)

| SDK method | Route | Source |
|---|---|---|
| `auth.signUp` | `POST /auth/v1/signup` | `server.rs:250` |
| `auth.signIn` | `POST /auth/v1/signin` | `server.rs:251` |
| `auth.refreshSession` (+auto-refresh) | `POST /auth/v1/refresh` | `server.rs:252` |
| `auth.signOut` | `POST /auth/v1/signout` | `server.rs:253` |
| `auth.verifyEmail` | `POST /auth/v1/verify-email` | `server.rs:254` |
| `auth.resetPassword` | `POST /auth/v1/reset-password` | `server.rs:255` |
| `auth.requestPasswordReset` | `POST /auth/v1/request-password-reset` | `server.rs:256` |
| `auth.requestMagicLink` | `POST /auth/v1/magic-link` (204) | `server.rs:262` |
| `auth.consumeMagicLink` | `POST /auth/v1/magic-link/consume` | `server.rs:263` |
| `auth.createApiKey` / `listApiKeys` | `POST/GET /auth/v1/api-keys` | `server.rs:267-270` |
| `auth.deleteApiKey` | `DELETE /auth/v1/api-keys/:id` | `server.rs:271` |
| `auth.getOAuthAuthorizeURL` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:277-279` |
| `auth.enrollFactor` | `POST /auth/v1/factors` (201) | `server.rs:286-287` |
| `auth.listFactors` | `GET /auth/v1/factors` | `server.rs:286-287` |
| `auth.verifyFactor` | `POST /auth/v1/factors/:id/verify` | `server.rs:290-291` |
| `auth.challengeFactor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:294-296` |
| `auth.verifyChallenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:298-300` |
| `auth.unenrollFactor` | `DELETE /auth/v1/factors/:id` | `server.rs:302-303` |
| `table(t).run()` (select/eq/.../order/limit/offset/cursor) | `GET /rest/v1/:table` | `server.rs:243-249`, `parser.rs` |
| `table(t).page()` | `GET /rest/v1/:table` → `{ rows, next_cursor }` | `server.rs:243`, `data.rs` |
| `table(t).stream()` | `GET /rest/v1/:table?stream=true` → NDJSON rows | `server.rs:243`, `data.rs` |
| `table(t).streamPage()` | `GET /rest/v1/:table?stream=true` → NDJSON rows + cursor | `server.rs:243`, `data.rs` |
| `table(t).insert` | `POST /rest/v1/:table` (201) | `server.rs:246` |
| `table(t).update` | `PATCH /rest/v1/:table?filters` | `server.rs:247` |
| `table(t).delete` | `DELETE /rest/v1/:table?filters` (may 501) | `server.rs:248`, `data.rs` |
| `rpc` / `functions.rpc` | `POST /rest/v1/rpc/:fn_name` | `server.rs:236`, `routes/rpc.rs` |
| `functions.invoke` | `ANY /fn/v1/:name` | `server.rs:238`, `routes/fn_handler.rs` |
| `storage.createBucket` | `POST /storage/v1/bucket` | `server.rs:373` |
| `storage.getBucket` / `deleteBucket` | `GET/DELETE /storage/v1/bucket/:name` | `server.rs:377` |
| `storage.fromBucket(b).upload/download/remove` | `POST/GET/DELETE /storage/v1/object/:bucket/*path` | `server.rs:409` |
| `storage.fromBucket(b).list` | `POST /storage/v1/object/list/:bucket` | `server.rs:417` |
| `storage.fromBucket(b).removeByPrefixes` | `DELETE /storage/v1/object/:bucket` | `server.rs:421` |
| `storage.fromBucket(b).getPublicURL` | `GET /storage/v1/object/public/:project/:bucket/*path` | `server.rs:384` |
| `storage.fromBucket(b).createSignedURL` | `POST /storage/v1/object/sign/upload/:bucket/*path` | `server.rs:397`, `storage_sign.rs` |
| `health` | `GET /health` | `server.rs:368` |
| `realtime.listen` / `listenPresence` / `presenceTrack` | `GET /realtime/v1/ws/:project` | `basin-realtime/src/ws.rs` |

## Not bound yet (gap list)

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — SSE variant of the realtime surface.
- **Admin surface** (`/admin/v1/*`) — operator-grade; use `client.request(_:path:)` as an escape hatch.
- **Arrow IPC** — the server exposes an Arrow IPC endpoint (`Accept: application/vnd.apache.arrow.stream`
  on `GET /rest/v1/:table`), but the Swift SDK does not yet implement Arrow IPC decoding.
  The `apache/arrow-swift` package is available via SwiftPM but pulls in FlatBuffers and
  swift-atomics as transitive dependencies, which conflicts with this SDK's zero-dependency
  design goal.  Use row-level `Decodable` structs or `[String: AnyCodable]` rows in the
  meantime; Arrow support can be added as an opt-in sub-library in a future release.

## Testing

```swift
// Use MockURLProtocol (included in BasinTests) to stub HTTP:
MockURLProtocol.push { req in
    mockResponse(url: req.url!, status: 200,
                 json: [["id": 1, "status": "paid"]])
}
let session = mockSession()
let client = BasinClient(url: "http://localhost", key: "key", session: session)
```

Run the test suite:

```sh
cd sdk/basin-swift
swift test
```
