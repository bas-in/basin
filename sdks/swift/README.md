# basin-swift

Swift client SDK for [Basin](https://basin.run) — speaks directly to a deployed
[`basin-engine`](https://github.com/vul-os/basin) (the open-source Rust core)
over REST and WebSocket. Covers auth, PostgREST-style table queries, NDJSON
streaming, object storage, realtime subscriptions, and RPC functions.

- **Swift 5.9+ / SwiftPM** — iOS 15+, macOS 12+, tvOS 15+, watchOS 8+
- **Zero external dependencies** — URLSession for HTTP, URLSessionWebSocketTask
  for realtime, Foundation's Codable for JSON
- **Actor-isolated** — `AuthClient`, `StorageClient`, `FunctionsClient`, and
  `RealtimeClient` are Swift actors; concurrent calls are safe without extra locking
- **async/await throughout** — idiomatic Swift concurrency, no callbacks
- **Part of the Basin SDK family** — one engine, consistent surface across JS,
  Python, Swift, Go, Rust, Dart, and more

## Install

Add to your `Package.swift`:

```swift
// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "MyApp",
    dependencies: [
        .package(url: "https://github.com/bas-in/basin-swift", from: "0.1.0"),
    ],
    targets: [
        .target(name: "MyApp", dependencies: [
            .product(name: "Basin", package: "basin-swift"),
        ]),
    ]
)
```

Or in Xcode: **File → Add Package Dependencies**, paste the repo URL.

## Architecture

[Basin Cloud](https://basin.run) is the control plane — dashboard, billing,
and the place you mint your anon key. Once you have a URL and key, Basin Cloud
is off the data path. Every `auth.*`, `table(...)`, and `storage.*` call lands
on `basin-engine` directly. The engine is open-source and self-hostable; point
`BasinClient` at any engine URL and the SDK behaves identically.

**Engine routes used by this SDK:**
`/auth/v1/*`, `/rest/v1/:table`, `/rest/v1/rpc/:fn_name`, `/fn/v1/:name`,
`/storage/v1/*`, `/realtime/v1/ws/:project`, `/health`.

## Quickstart

```swift
import Basin

// BASIN_URL points at a deployed basin-engine.
// Mint your API key at https://basin.run/app/project/<ref>/api-keys.
let client = BasinClient(
    url: "https://project.basin.run",
    key: "basin_01J..._...",
    projectID: "01J..."   // optional when key is a JWT carrying project_id
)

// Health check
let status = try await client.health()  // → "ok"

// Query builder — generic over any Decodable
struct Order: Decodable {
    let id: Int
    let total: Int
    let status: String
}

let orders: [Order] = try await client.table("orders")
    .select("id,total,status")
    .eq("status", .string("paid"))
    .gte("total", .int(100))
    .order("total", ascending: false)
    .limit(50)
    .run()
```

## Auth

Auth calls hit `basin-engine` directly — no cloud round-trip. The session is
stored inside the actor; every subsequent `table(...)` or `storage.*` call
automatically attaches the access token. The token is refreshed transparently
before expiry.

```swift
// Sign up
try await client.auth.signUp(email: "alice@example.com", password: "secret")

// Sign in — session is stored and auto-refreshes
let session = try await client.auth.signIn(email: "alice@example.com",
                                            password: "secret")

// The JWT sets auth.uid() / auth.role() for RLS policies on the engine:
//   CREATE POLICY "own rows" ON orders FOR ALL USING (owner_id = auth.uid());
// After sign-in, client.table("orders").run() returns only the signed-in user's rows.

// Sign out — revokes refresh token server-side, clears local session
await client.auth.signOut()
```

### Magic links

```swift
try await client.auth.requestMagicLink(email: "alice@example.com")
let session = try await client.auth.consumeMagicLink(token: "token-from-email")
```

### OAuth

```swift
// 1. Get the provider authorize URL (no JWT required).
let result = try await client.auth.getOAuthAuthorizeURL(
    provider: "google",
    redirectTo: "https://myapp.example.com/auth/callback"
)
// 2. Open result.redirectUrl in ASWebAuthenticationSession or a browser.
//    The engine callback handler exchanges the code and issues tokens.

// 3. After the flow, adopt the tokens your server receives:
await client.auth.setSession(Session(
    accessToken: "...", refreshToken: "...",
    accessExpiresAt: "...", refreshExpiresAt: "..."
))
```

Supported providers: google, github, apple, bitbucket, discord, figma, gitlab,
linkedin, microsoft (azure_ad), notion, slack, spotify, twitch, twitter_x.
Custom OIDC providers are registered server-side and passed by name.

### MFA (TOTP and WebAuthn)

```swift
// Enroll TOTP
let enroll = try await client.auth.enrollFactor(factorType: "totp",
                                                 friendlyName: "My Authenticator")
if case .totp(let t) = enroll {
    print(t.otpauthUri)  // render as QR code for an authenticator app
    let verified = try await client.auth.verifyFactor(t.factorId, code: "123456")
    if let codes = verified.recoveryCodes {
        print("Store these securely:", codes)  // shown exactly once
    }
    // Step-up challenge
    let challenge = try await client.auth.challengeFactor(t.factorId)
    let aal2Session = try await client.auth.verifyChallenge(
        t.factorId, challengeID: challenge.challengeId, code: "654321"
    )
}
```

### iOS Keychain persistence

The SDK stores sessions in memory only. Persist across launches with Keychain:

```swift
import Security

func saveSession(_ session: Session) {
    guard let data = try? JSONEncoder().encode(session) else { return }
    let q: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
                             kSecAttrAccount as String: "basin-session",
                             kSecValueData as String: data]
    SecItemDelete(q as CFDictionary)
    SecItemAdd(q as CFDictionary, nil)
}

func loadSession() -> Session? {
    let q: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
                             kSecAttrAccount as String: "basin-session",
                             kSecReturnData as String: true]
    var result: AnyObject?
    guard SecItemCopyMatching(q as CFDictionary, &result) == errSecSuccess,
          let data = result as? Data else { return nil }
    return try? JSONDecoder().decode(Session.self, from: data)
}

// On app launch:
if let saved = loadSession() { await client.auth.setSession(saved) }
// After sign-in:
let session = try await client.auth.signIn(email: email, password: password)
saveSession(session)
```

### API keys

```swift
let key = try await client.auth.createApiKey(name: "ci-pipeline")
print(key.secret)   // shown exactly once — store it now
let all = try await client.auth.listApiKeys()
try await client.auth.deleteApiKey(key.id)
```

## Query builder

`client.table(_:)` returns a fluent `QueryBuilder`. All filter methods return
`self` for chaining. Call `.run()` to execute and decode rows, or `.page()` to
also get the pagination cursor.

```swift
// Projection and filters
let rows: [Order] = try await client.table("orders")
    .select("id,total,status")
    .eq("status", .string("paid"))
    .gte("total", .int(100))
    .in("region", [.string("eu"), .string("us")])
    .order("total", ascending: false)
    .limit(50)
    .run()

// Keyset cursor pagination
let page: QueryPage<Order> = try await client.table("orders").limit(100).page()
// page.rows, page.nextCursor

if let cursor = page.nextCursor {
    let next: QueryPage<Order> = try await client.table("orders")
        .limit(100)
        .cursor(cursor)
        .page()
}

// Writes
struct NewOrder: Encodable { let total: Int; let status: String }
_ = try await client.table("orders").insert(NewOrder(total: 49, status: "new"))
_ = try await client.table("orders").eq("id", .int(7)).update(["status": "paid"])
_ = try await client.table("orders").eq("id", .int(7)).delete()
```

Supported filter ops: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is`
(null / notnull). Modifiers: `select`, `order`, `limit`, `offset`, `cursor`.

## NDJSON streaming

`stream()` and `streamPage()` send `?stream=true` to the server and deliver
rows line-by-line via `URLSession.bytes` — no full buffer in memory. Use this
for large exports or long-running queries.

```swift
struct Event: Decodable { let id: Int; let type: String; let ts: String }

// Rows only
for try await event in client.table("events")
    .eq("type", .string("click"))
    .stream() as AsyncThrowingStream<Event, Error> {
    process(event)
}

// Rows + pagination cursor
let page = client.table("events").limit(5000).streamPage() as StreamPage<Event>
for try await event in page.rows { process(event) }
if let cursor = await page.nextCursor {
    // Continue with the next page
}
```

All filter, order, select, limit, and cursor modifiers work identically on
streaming and buffered paths.

## Functions and RPC

```swift
// POST /rest/v1/rpc/:fn_name — SQL or Wasm UDF, named args
let sum = try await client.rpc("add", args: ["a": 40, "b": 2])   // → 7 (Any)

// ANY /fn/v1/:name — HTTP-handler function, response proxied verbatim
let result = try await client.functions.invoke(
    "resize",
    method: "POST",
    body: #"{"width":100}"#.data(using: .utf8),
    headers: ["Content-Type": "application/json"]
)
// result.status, result.headers, result.data (decoded JSON or raw Data)
```

## Storage

```swift
// Bucket management
try await client.storage.createBucket("avatars", public: true)
let meta = try await client.storage.getBucket("avatars")

// Object operations
let bucket = await client.storage.fromBucket("avatars")

let obj = try await bucket.upload("users/alice.png",
                                   data: imageData,
                                   contentType: "image/png")

let download = try await bucket.download("users/alice.png")
// download.data, download.contentType, download.etag

let objects = try await bucket.list(prefix: "users/")

// Delete one object or a set of paths
try await bucket.remove("users/old.png")
try await bucket.removeByPrefixes(["users/alice.png", "users/bob.png"])

// Signed URL (time-limited, no JWT required by recipient)
let signed = try await bucket.createSignedURL("users/alice.png", expiresIn: 3600)
print(signed.absoluteUrl)

// Public URL (requires bucket public=true)
let url = try await bucket.getPublicURL("users/alice.png")
```

## Realtime

Subscribe to live table changes via `GET /realtime/v1/ws/:project`. Auth is
delivered in the `Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol header.
The client reconnects on unexpected disconnect with exponential backoff (0.5 s →
1 s → 2 s … capped at 30 s) and re-issues all active subscriptions automatically.

```swift
let client = BasinClient(url: "https://project.basin.run",
                          key: "my-key", projectID: "01J...")

// Listen for INSERT / UPDATE / DELETE on a table
let stream = await client.realtime.listen(table: "orders")
for try await frame in stream {
    if case .event(let e) = frame {
        print(e.op, e.table, e.after as Any)  // e.seq for ordering
    }
}

// Server-side filter — only matched events reach the client
let paid = await client.realtime.listen(
    table: "orders",
    filter: "NEW.status = 'paid'"
)
for try await frame in paid {
    if case .event(let e) = frame { print(e.after as Any) }
}

// Reconnect-resume: pass the last seq you processed
// Server replays events from its ring buffer, or sends a .gap frame
let resume = await client.realtime.listen(table: "orders", lastEventID: 42)
for try await frame in resume {
    switch frame {
    case .gap(let g):
        print("missed events — cold re-sync from", g.oldestInRing)
    case .event(let e):
        print(e.seq, e.op)
    default: break
    }
}
```

### Presence

```swift
// Register in a channel
try await client.realtime.presenceTrack(
    channel: "room:1",
    clientID: "user-abc",
    metadata: ["name": "Alice", "color": "#f00"]
)

// Send periodic heartbeats (server evicts after 90 s of silence)
try await client.realtime.presenceHeartbeat(channel: "room:1", clientID: "user-abc")

// Listen for presence frames
let presenceStream = await client.realtime.listenPresence(channel: "room:1")
for try await frame in presenceStream {
    switch frame {
    case .presenceState(let s):
        print("snapshot:", s.presences.map { $0.clientId })
    case .presenceDiff(let d):
        print("joins:", d.joins.map { $0.clientId })
        print("leaves:", d.leaves.map { $0.clientId })
    default: break
    }
}

// Unregister
try await client.realtime.presenceUntrack(channel: "room:1", clientID: "user-abc")
// Disconnect when done
await client.realtime.disconnect()
```

## Error handling

Every non-2xx response throws `BasinApiError`. Match on the stable `.code`
enum — never on `.message`, which is human-readable and may change.

```swift
do {
    let rows: [Order] = try await client.table("orders").run()
} catch let e as BasinApiError {
    switch e.code {
    case .unauthenticated:
        try await client.auth.refreshSession()
    case .forbidden:
        print("RLS policy denied access")
    case .notFound:
        print("table or row not found")
    case .rateLimited:
        print("back off and retry")
    case .engineUnsupported:
        print("operation not supported on this engine version")
    default:
        print("error:", e.rawCode, e.message)  // e.rawCode preserved for unknown codes
    }
    // SQL-layer errors (constraint violations, etc.) carry a Postgres SQLSTATE:
    if let state = e.sqlState {
        print("SQLSTATE:", state)  // e.g. "23505" for unique violation
    }
} catch let e as BasinNetworkError {
    print("transport error:", e.underlying)
}
```

Known codes: `.unauthenticated`, `.forbidden`, `.notFound`, `.invalidRequest`,
`.rateLimited`, `.engineUnsupported`, `.internal`, `.emailDisabled`,
`.revokedToken`. Unknown codes from a newer server arrive as `.unknown`
(raw string preserved in `rawCode`).

## Escape hatch

Routes not yet wrapped (e.g. `/admin/v1/*`) are reachable via:

```swift
let (data, response) = try await client.request(
    "GET",
    path: "/admin/v1/projects",
    headers: ["Accept": "application/json"]
)
```

## SDK family

basin-swift is part of the Basin SDK family. All SDKs speak directly to
`basin-engine` over the same REST + WebSocket surface — the engine is the
server, Basin Cloud is only the control plane.

| SDK | Repo |
|---|---|
| JavaScript / TypeScript | `basin-js` |
| Python | `basin-py` |
| Swift | **this repo** |
| Go | `basin-go` |
| Rust | `basin-rs` |
| Dart / Flutter | `basin-dart` |
| Kotlin / Android | `basin-kotlin` |
| .NET | `basin-dotnet` |
| Ruby | `basin-ruby` |
| PHP | `basin-php` |

Full API reference: <https://basin.run/docs/swift-sdk>

## License

MIT — see [`LICENSE`](./LICENSE).
