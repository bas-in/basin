# basin-go

Official Go SDK for [Basin](https://basin.run) — a cloud-native HTAP database
with a PostgreSQL wire-compatible interface and an HTTP REST API.

**Zero external dependencies.** The core SDK uses only the Go standard library
(`net/http`, `encoding/json`, `context`, `sync`, `time`). No transitive module
graph is imposed on users.

## Installation

```
go get github.com/bas-in/basin/sdk/basin-go
```

Requires Go 1.21+.

## Quickstart

### Create a client

```go
import "github.com/bas-in/basin/sdk/basin-go"

client := basin.New(
    "https://your-project.basin.run",
    "your-api-key",                   // JWT or raw API key
    basin.WithProjectID("01JWXXX..."), // optional when JWT carries project_id
)
```

### Authentication

```go
ctx := context.Background()

// Sign up a new user
result, err := client.Auth.SignUp(ctx, "alice@example.com", "password", "")

// Sign in — session stored automatically; tokens auto-refresh before expiry
sess, err := client.Auth.SignIn(ctx, "alice@example.com", "password", "")

// Sign out (local only — no server route; token expires naturally)
client.Auth.SignOut(ctx)

// Adopt an existing session (e.g. restored from persistent storage)
client.Auth.SetSession(&basin.Session{
    AccessToken:      "...",
    RefreshToken:     "...",
    AccessExpiresAt:  "2099-01-01T00:00:00Z",
    RefreshExpiresAt: "2099-01-01T00:00:00Z",
})
```

### Query

```go
// SELECT id, total FROM orders WHERE total >= 100 ORDER BY total DESC LIMIT 10
result, err := client.Table("orders").
    Select("id", "total").
    Gte("total", "100").
    Order("total", false).
    Limit(10).
    Run(ctx)

for _, row := range result.Rows {
    fmt.Println(row["id"], row["total"])
}

// Decode rows into a typed slice
type Order struct {
    ID    int     `json:"id"`
    Total float64 `json:"total"`
}
var orders []Order
if err := result.Into(&orders); err != nil { ... }

// Pagination — continue from a cursor
page2, err := client.Table("orders").
    Limit(10).
    Cursor(result.NextCursor).
    Run(ctx)
```

### Insert / Update / Delete

```go
// Insert
_, err = client.Table("orders").Insert(ctx, map[string]any{
    "total": 49.99, "status": "new",
})

// Batch insert
_, err = client.Table("orders").Insert(ctx, []map[string]any{
    {"total": 10}, {"total": 20},
})

// Update rows matching a filter
_, err = client.Table("orders").
    Eq("status", "new").
    Update(ctx, map[string]any{"status": "processing"})

// Delete
_, err = client.Table("orders").Eq("id", "5").Delete(ctx)
```

### RPC (catalog functions / Wasm UDFs)

```go
// POST /rest/v1/rpc/my_fn
result, err := client.RPC(ctx, "my_fn", map[string]any{"x": 42})

// Or via Functions client
result, err = client.Functions.RPC(ctx, "calculate_total", map[string]any{"order_id": 7})
```

### HTTP-handler functions

```go
// ANY /fn/v1/:name — proxied verbatim
resp, err := client.Functions.Invoke(ctx, "my-handler", map[string]any{"key": "val"},
    basin.WithInvokeMethod("POST"),
)
fmt.Println(resp.Status, resp.Data)
```

### Storage

```go
// Bucket management
bucket, err := client.Storage.CreateBucket(ctx, "avatars",
    basin.WithPublic(true),
    basin.WithFileSizeLimit(5*1024*1024),
)
bucket, err = client.Storage.GetBucket(ctx, "avatars")
err = client.Storage.DeleteBucket(ctx, "avatars")

// Object operations
bkt := client.Storage.FromBucket("avatars")

// Upload
data, _ := os.ReadFile("profile.png")
obj, err := bkt.Upload(ctx, "alice/profile.png", data,
    basin.WithContentType("image/png"),
)

// Download
dl, err := bkt.Download(ctx, "alice/profile.png")
// dl.Data []byte, dl.ContentType, dl.ETag

// List
objects, err := bkt.List(ctx, "alice/", 100, 0)

// Delete single object
err = bkt.Remove(ctx, "alice/profile.png")

// Bulk delete by prefix
err = bkt.RemoveByPrefixes(ctx, []string{"alice/", "bob/"})

// Public URL (bucket must have Public=true)
url, err := bkt.GetPublicURL("alice/profile.png")

// Signed URL (time-limited, no JWT required for download)
signed, err := bkt.CreateSignedURL(ctx, "alice/profile.png", 3600)
fmt.Println(signed.AbsoluteURL) // full URL ready to use
```

## Error handling

All API errors are returned as `*BasinError`. The `Code` field is the stable
contract; `Message` is human-readable and may change between server versions.

```go
var be *basin.BasinError
if errors.As(err, &be) {
    switch be.Code {
    case "E_UNAUTHENTICATED":
        // re-authenticate
    case "E_NOT_FOUND":
        // resource missing
    case "E_RATE_LIMITED":
        // back off and retry
    case "E_ENGINE_UNSUPPORTED":
        // server does not support this operation (e.g. DELETE on some engines)
    }
    fmt.Println(be.Code, be.Status, be.Message)
    if be.SQLState != "" {
        fmt.Println("SQLSTATE:", be.SQLState) // e.g. "23505" unique violation
    }
}
```

Stable error codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`.

## Context and timeouts

Every method accepts `context.Context` as its first argument. Use it to set
per-request timeouts or cancel in-flight requests:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
result, err := client.Table("events").Run(ctx)
```

The default client-wide timeout is 30 seconds. Override it at construction:

```go
client := basin.New(url, key, basin.WithTimeout(10*time.Second))
```

## Session auto-refresh

Once `Auth.SignIn` has been called, the SDK stores the session and
auto-refreshes the access token when it is within 10 seconds of expiry.
The refresh is mutex-guarded; concurrent requests share a single refresh
call. Refreshed tokens are injected into every subsequent HTTP request
automatically — no user code is required.

## MFA

```go
// Enroll a TOTP factor
res, err := client.Auth.EnrollFactor(ctx, "totp", "My Authenticator")
if totp, ok := res.(*basin.TotpEnrollResult); ok {
    // display totp.OtpauthURI as QR code
    fmt.Println(totp.SecretB32)
}

// Enroll a WebAuthn factor
res, err = client.Auth.EnrollFactor(ctx, "webauthn", "YubiKey")
if wa, ok := res.(*basin.WebAuthnEnrollResult); ok {
    // pass wa.CreationOptionsJSON to navigator.credentials.create()
}

// Verify factor enrollment (stores recovery codes; shown exactly once)
vr, err := client.Auth.VerifyFactor(ctx, factorID, "123456", "", "")
fmt.Println(vr.RecoveryCodes) // save securely

// Step-up challenge (raises session to aal2)
challenge, err := client.Auth.ChallengeFactor(ctx, factorID)
if tc, ok := challenge.(*basin.TotpChallengeResult); ok {
    sess, err := client.Auth.VerifyChallenge(ctx, factorID, tc.ChallengeID, "654321", "")
    _ = sess // now aal2
}

// Unenroll (requires aal2 session)
err = client.Auth.UnenrollFactor(ctx, factorID)
```

## Functional options reference

| Option | Effect |
|---|---|
| `WithToken(key)` | Set bearer token (JWT or API key) |
| `WithBaseURL(url)` | Override server base URL |
| `WithProjectID(id)` | Set default project ID |
| `WithTimeout(d)` | Per-request HTTP timeout (default: 30s) |
| `WithHTTPClient(hc)` | Supply a custom `*http.Client` |

## Deliberate omissions

- **Arrow IPC transport**: the Basin server does not expose an Arrow/IPC
  endpoint. JSON is the only supported transport. Client-side Arrow conversion
  is not provided; use `result.Into(&dest)` for typed decoding.
- **Realtime WebSocket**: `GET /realtime/v1/ws/:project` is not wrapped in v1
  of this SDK. It is on the roadmap.
- **Admin routes** (`/admin/v1/*`): operator-only; use the escape hatch below
  if needed.

## Escape hatch

For routes not yet wrapped:

```go
raw, err := client.Auth.(*basin.AuthClient) // not needed — use direct method calls
// or via direct HTTP:
resp, err := client.Health(ctx)
```

The `*http.Client` is not directly exposed; construct a separate `http.Client`
for admin routes or use any HTTP library against the same `baseURL`.
