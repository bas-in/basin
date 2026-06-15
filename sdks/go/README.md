# basin-go

Go client for [Basin](https://basin.run) — a cloud-native HTAP database with
a PostgreSQL wire-compatible interface and an HTTP REST API.

Speaks **directly** to a deployed
[`basin-engine`](https://github.com/bas-in/basin) (the open-source Rust core,
Apache-2.0) over HTTP: auth, PostgREST-style table queries, object storage,
realtime WebSocket change streams, Arrow IPC, and RPC functions. No cloud
intermediary on the data path.

## Install

```sh
go get github.com/bas-in/basin/sdk/basin-go
```

Requires Go 1.21+. Two optional external dependencies are pulled in only when
you use the corresponding surface:

| Dep | Used by |
|---|---|
| `nhooyr.io/websocket` v1.8.11 | `client.Realtime` (WebSocket change streams) |
| `github.com/apache/arrow-go/v18` | `QueryBuilder.Arrow(ctx)` |

All other surfaces — auth, query, storage, functions — use only the Go
standard library.

## Quickstart

```go
import (
    "context"
    "fmt"

    basin "github.com/bas-in/basin/sdk/basin-go"
)

func main() {
    client := basin.New(
        "https://your-project.basin.run", // basin-engine base URL
        "your-api-key",                   // JWT or raw API key
        basin.WithProjectID("01JWXXX..."), // omit if the JWT carries project_id
    )

    ctx := context.Background()

    // Sign in — session stored and auto-refreshed from here on.
    _, err := client.Auth.SignIn(ctx, "you@example.com", "password", "")
    if err != nil {
        panic(err)
    }

    // SELECT id, total FROM orders WHERE total >= 100 ORDER BY total DESC LIMIT 10
    result, err := client.Table("orders").
        Select("id", "total").
        Gte("total", 100).
        Order("total", false).
        Limit(10).
        Run(ctx)
    if err != nil {
        panic(err)
    }

    for _, row := range result.Rows {
        fmt.Println(row["id"], row["total"])
    }
}
```

## Architecture

[Basin Cloud](https://basin.run) is the control plane — dashboard, billing,
project management, and the place you mint the API key. Once you have a URL
and key, the cloud is **off the data path**: every `client.Auth.*`,
`client.Table(...)`, `client.Storage.*`, and `client.Realtime.*` call lands on
`basin-engine` directly. The engine is open source and can be self-hosted.

Engine routes used by this SDK:

| Surface | Routes |
|---|---|
| Auth | `/auth/v1/{signup,signin,refresh,verify-email,reset-password,request-password-reset,magic-link,magic-link/consume,oauth/:provider/authorize,api-keys,factors/*}` |
| Query | `GET|POST|PATCH|DELETE /rest/v1/:table` |
| RPC | `POST /rest/v1/rpc/:fn_name` |
| Functions | `ANY /fn/v1/:name` |
| Storage | `/storage/v1/bucket/*`, `/storage/v1/object/*` |
| Realtime | `GET /realtime/v1/ws/:project` (WebSocket) |
| Health | `GET /health` |

## Auth

```go
ctx := context.Background()

// Register a new user
result, err := client.Auth.SignUp(ctx, "alice@example.com", "password", "")
// result.UserID is the new user's UUID

// Sign in — tokens stored and auto-refreshed on every request
sess, err := client.Auth.SignIn(ctx, "alice@example.com", "password", "")

// Restore an existing session (e.g. after process restart)
client.Auth.SetSession(&basin.Session{
    AccessToken:      "...",
    RefreshToken:     "...",
    AccessExpiresAt:  "2099-01-01T00:00:00Z",
    RefreshExpiresAt: "2099-01-01T00:00:00Z",
})

// Sign out (local only — clears the stored session)
client.Auth.SignOut(ctx)

// Magic-link
err = client.Auth.RequestMagicLink(ctx, "alice@example.com")
sess, err = client.Auth.ConsumeMagicLink(ctx, tokenFromEmail)

// OAuth — get the provider redirect URL, then send the browser there
auth, err := client.Auth.GetOAuthAuthorizeURL(ctx, "github", "https://app/callback", "")
// redirect user to auth.RedirectURL

// Password reset
err = client.Auth.RequestPasswordReset(ctx, "alice@example.com", "")
err = client.Auth.ResetPassword(ctx, tokenFromEmail, "new-password", "")

// Email verification
err = client.Auth.VerifyEmail(ctx, tokenFromEmail, "")
```

The access token auto-refreshes within 10 seconds of expiry. The refresh is
mutex-guarded; concurrent requests share a single refresh call. The refreshed
token is injected into every subsequent HTTP request — no user code required.

### API keys

```go
// Create a named API key (requires a signed-in session)
key, err := client.Auth.CreateAPIKey(ctx, "my-service")
fmt.Println(key.Secret) // shown exactly once — store immediately

// List keys (metadata only — no secrets)
keys, err := client.Auth.ListAPIKeys(ctx)

// Revoke a key
err = client.Auth.DeleteAPIKey(ctx, key.ID)
```

### MFA (TOTP and WebAuthn)

```go
// Enroll a TOTP factor
res, err := client.Auth.EnrollFactor(ctx, "totp", "Authenticator App")
if totp, ok := res.(*basin.TotpEnrollResult); ok {
    // Display totp.OtpauthURI as a QR code, or share totp.SecretB32
}

// Verify enrollment (recovery codes shown exactly once)
vr, err := client.Auth.VerifyFactor(ctx, factorID, "123456", "", "")
// Store vr.RecoveryCodes securely

// Step-up challenge (raises session to aal2)
challenge, err := client.Auth.ChallengeFactor(ctx, factorID)
if tc, ok := challenge.(*basin.TotpChallengeResult); ok {
    sess, err := client.Auth.VerifyChallenge(ctx, factorID, tc.ChallengeID, "654321", "")
    _ = sess // aal2 session
}

// Unenroll (requires aal2 session)
err = client.Auth.UnenrollFactor(ctx, factorID)
```

WebAuthn factors follow the same flow: `EnrollFactor(ctx, "webauthn", "YubiKey")`
returns `*WebAuthnEnrollResult` whose `CreationOptionsJSON` you pass to
`navigator.credentials.create()`, then `VerifyFactor` with the attestation.

## Query builder

`client.Table(name)` and its alias `client.From(name)` return a `*QueryBuilder`.
Each filter method returns a new builder — the original is reusable.

```go
// Filters: Eq, Neq, Gt, Gte, Lt, Lte, In, Is
q := client.Table("products").
    Select("id", "name", "price").
    Eq("active", true).
    Gte("price", 10.0).
    Lt("price", 500).
    In("category", []any{"books", "music"}).
    Order("price", true). // ascending
    Limit(20)

result, err := q.Run(ctx)
for _, row := range result.Rows {
    fmt.Println(row["name"], row["price"])
}

// Decode into a typed slice via JSON round-trip
type Product struct {
    ID    int     `json:"id"`
    Name  string  `json:"name"`
    Price float64 `json:"price"`
}
var products []Product
err = result.Into(&products)

// Keyset cursor pagination
page2, err := client.Table("products").
    Limit(20).
    Cursor(result.NextCursor).
    Run(ctx)
```

Filter values accept any scalar type (`string`, `int`, `int64`, `float64`,
`bool`, `nil`). `nil` serialises as `"null"` for `Is` checks.

### Writes

```go
// Insert one row
_, err = client.Table("orders").Insert(ctx, map[string]any{
    "total":  49.99,
    "status": "new",
})

// Batch insert
_, err = client.Table("orders").Insert(ctx, []map[string]any{
    {"total": 10.0, "status": "new"},
    {"total": 20.0, "status": "new"},
})

// Update rows matching the current filters
_, err = client.Table("orders").
    Eq("status", "new").
    Update(ctx, map[string]any{"status": "processing"})

// Delete
_, err = client.Table("orders").Eq("id", 5).Delete(ctx)
```

### NDJSON streaming

`Stream` returns a pull-based iterator (`iter.Seq2`, Go 1.23+) that yields
rows as they arrive line-by-line, keeping memory proportional to one row:

```go
for row, err := range client.Table("events").Select("id", "ts").Stream(ctx) {
    if err != nil {
        break
    }
    fmt.Println(row["id"])
}
```

To also capture the next-page cursor from the trailing sentinel line:

```go
var sr basin.StreamResult
for row, err := range client.Table("events").Limit(1000).StreamWithCursor(ctx, &sr) {
    if err != nil {
        break
    }
    process(row)
}
fmt.Println("next:", sr.NextCursor)
```

### Arrow IPC

For analytics workloads, request Arrow record batches directly — zero JSON
round-trip, full `int64`/timestamp fidelity:

```go
import "github.com/apache/arrow-go/v18/arrow/array"

result, err := client.Table("events").
    Eq("status", "active").
    Limit(10000).
    Arrow(ctx)
if err != nil {
    panic(err)
}
defer result.Release()

fmt.Println("rows:", result.Records[0].NumRows())
fmt.Println("next cursor:", result.NextCursor)

// Typed column access
ids := result.Records[0].Column(0).(*array.Int64)
for i := 0; i < int(result.Records[0].NumRows()); i++ {
    fmt.Println(ids.Value(i))
}

// Paginate
if result.NextCursor != "" {
    page2, err := client.Table("events").
        Eq("status", "active").
        Limit(10000).
        Cursor(result.NextCursor).
        Arrow(ctx)
    defer page2.Release()
}
```

When the server returns JSON instead of Arrow IPC (older engine, or 406),
`result.Records` is nil and `result.FallbackRows` contains the rows as
`[]map[string]any` — the same fallback is transparent.

## Storage

```go
// Bucket management
bucket, err := client.Storage.CreateBucket(ctx, "avatars",
    basin.WithPublic(true),
    basin.WithFileSizeLimit(5*1024*1024),
    basin.WithAllowedMimeTypes([]string{"image/png", "image/jpeg"}),
)
bucket, err = client.Storage.GetBucket(ctx, "avatars")
err = client.Storage.DeleteBucket(ctx, "avatars")

// Object operations — scoped to one bucket
bkt := client.Storage.FromBucket("avatars")

// Upload
data, _ := os.ReadFile("profile.png")
obj, err := bkt.Upload(ctx, "alice/profile.png", data,
    basin.WithContentType("image/png"),
)

// Download (authenticated)
dl, err := bkt.Download(ctx, "alice/profile.png")
// dl.Data []byte, dl.ContentType, dl.ETag

// List with prefix
objects, err := bkt.List(ctx, "alice/", 100, 0)

// Delete single object
err = bkt.Remove(ctx, "alice/profile.png")

// Bulk delete by prefix
err = bkt.RemoveByPrefixes(ctx, []string{"alice/", "tmp/"})

// Public URL (bucket must be Public=true; requires WithProjectID)
publicURL, err := bkt.GetPublicURL("alice/profile.png")

// Mint a time-limited signed download URL (default 3600 s; max 7 days)
signed, err := bkt.CreateSignedURL(ctx, "alice/profile.png", 3600)
fmt.Println(signed.AbsoluteURL) // use directly, no JWT needed
```

## Functions

Two function surfaces, one client:

```go
// RPC — POST /rest/v1/rpc/:fn_name (catalog SQL / Wasm UDFs)
// Returns a bare scalar for single-value functions, or []any for RETURNS TABLE.
res, err := client.Functions.RPC(ctx, "calculate_total", map[string]any{
    "order_id": 7,
})

// Convenience alias on the top-level client
res, err = client.RPC(ctx, "calculate_total", map[string]any{"order_id": 7})

// Invoke — ANY /fn/v1/:name (HTTP-handler-shape Wasm functions; response proxied verbatim)
resp, err := client.Functions.Invoke(ctx, "my-handler",
    map[string]any{"key": "val"},
    basin.WithInvokeMethod("POST"),
    basin.WithInvokeHeaders(map[string]string{"X-Custom": "header"}),
)
fmt.Println(resp.Status, resp.Data)
```

## Realtime

Change events over WebSocket. A single connection is opened lazily on the
first `Subscribe` call and shared across all subscriptions. The client
reconnects with exponential backoff (500 ms → 30 s) and resubscribes all
active channels automatically.

Requires a JWT with a `project_id` claim (or `WithProjectID` at construction).

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Subscribe — blocks until the "subscribed" ack is received (30 s timeout)
events, err := client.Realtime.Subscribe(ctx, "orders", basin.SubscribeOptions{
    Filter:      "NEW.status = 'paid'", // optional server-side SQL predicate
    LastEventID: 42,                    // replay from seq 42 on reconnect
})
if err != nil {
    panic(err)
}

for frame := range events {
    switch frame.Type {
    case "event":
        ev := frame.Event
        fmt.Println(ev.Op, ev.Table, ev.Seq, ev.After)
    case "error":
        // Code "lag" means the server dropped events; "invalid_filter" means
        // the filter predicate was rejected.
        fmt.Println("realtime error:", frame.Error.Code, frame.Error.Missed)
    case "gap":
        // The replay ring was evicted — a cold re-sync from storage is needed.
        fmt.Println("gap: oldest in ring:", frame.Gap.OldestInRing)
    }
}

// Unsubscribe explicitly (cancelling ctx also works)
_ = client.Realtime.Unsubscribe(ctx, "orders")
client.Realtime.Disconnect()
```

### Presence

```go
// Register this client in a presence channel (sends presence_track frame)
err = client.Realtime.PresenceTrack(ctx, "room:lobby", "user-c1",
    map[string]any{"name": "Alice", "avatar": "..."},
)

// Refresh the presence TTL (send every < 90 s; server evicts after 90 s silence)
err = client.Realtime.PresenceHeartbeat(ctx, "room:lobby", "user-c1")

// Receive presence_state and presence_diff frames on the subscription channel
// by subscribing to the same channel name:
frames, err := client.Realtime.Subscribe(ctx, "room:lobby", basin.SubscribeOptions{})
for frame := range frames {
    switch frame.Type {
    case "presence_state":
        fmt.Println("current members:", frame.PresenceState.Presences)
    case "presence_diff":
        fmt.Println("joined:", frame.PresenceDiff.Joins)
        fmt.Println("left:", frame.PresenceDiff.Leaves)
    }
}

// Remove from presence
err = client.Realtime.PresenceUntrack(ctx, "room:lobby", "user-c1")
```

## Error handling

All API errors are returned as `*BasinError`. Use `errors.As` to extract it:

```go
import "errors"

var be *basin.BasinError
if errors.As(err, &be) {
    switch be.Code {
    case "E_UNAUTHENTICATED":
        // re-authenticate
    case "E_NOT_FOUND":
        // resource does not exist
    case "E_RATE_LIMITED":
        // back off and retry
    case "E_ENGINE_UNSUPPORTED":
        // server does not support this operation yet
    }
    fmt.Printf("code=%s status=%d message=%s\n", be.Code, be.Status, be.Message)
    if be.SQLState != "" {
        // SQLSTATE is present for SQL-layer errors (e.g. "23505" unique violation)
        fmt.Println("SQLSTATE:", be.SQLState)
    }
}
```

Stable error codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from newer server versions
pass through as-is — do not match on `Message`.

## Context and timeouts

Every method accepts `context.Context` as its first argument:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
result, err := client.Table("events").Run(ctx)
```

The default client-wide HTTP timeout is 30 seconds. Override at construction:

```go
client := basin.New(url, key, basin.WithTimeout(10*time.Second))
```

## Functional options reference

| Option | Effect |
|---|---|
| `WithToken(key)` | Bearer token (JWT or API key); same as the `key` argument to `New` |
| `WithBaseURL(url)` | Override server base URL |
| `WithProjectID(id)` | Default project ID for auth and storage public URLs |
| `WithTimeout(d)` | Per-request HTTP timeout (default: 30 s) |
| `WithHTTPClient(hc)` | Custom `*http.Client` for testing or proxy configuration |

## Basin SDK family

basin-go is part of the Basin SDK family. All SDKs target the same
`basin-engine` REST surface (pgwire + HTTP REST), speak directly to the engine
on every data call, and carry no cloud intermediary on the data path. They
are tested against the same engine binary and follow a shared feature parity
matrix across Go, TypeScript, Python, Rust, Java, Dart, .NET, Ruby, Swift,
PHP, and Kotlin.

Full reference: <https://basin.run/docs/go-sdk>.

## License

MIT — see [`LICENSE`](./LICENSE).
