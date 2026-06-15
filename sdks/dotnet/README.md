# basin-dotnet

Official .NET SDK for [Basin](https://basin.run) — a cloud-native HTAP database
with a PostgreSQL wire-compatible interface and an HTTP REST API.

**Zero heavy dependencies:** `System.Net.Http` (HttpClient) for transport,
`System.Net.WebSockets` (ClientWebSocket) for realtime, and
`System.Text.Json` for serialisation — all built into .NET 8.
Apache Arrow IPC is an opt-in NuGet package (see [Arrow section](#arrow-ipc)).

## Installation

```
dotnet add package Basin.Sdk
```

Requires .NET 8+.

## Quickstart

### Create a client

```csharp
using Basin;

using var client = new BasinClient.Builder()
    .WithUrl("https://your-project.basin.run")
    .WithApiKey("your-api-key")          // JWT or raw API key
    .WithProjectId("01JWXXX...")         // optional when JWT carries project_id
    .Build();
```

### Authentication

```csharp
// Sign up a new user
var result = await client.Auth.SignUpAsync("alice@example.com", "password");
Console.WriteLine(result.UserId);

// Sign in — session stored automatically; access token auto-refreshes before expiry
var session = await client.Auth.SignInAsync("alice@example.com", "password");

// Sign out (revokes refresh token server-side via POST /auth/v1/signout)
await client.Auth.SignOutAsync();

// Adopt an existing session (e.g. restored from persistent storage)
client.Auth.SetSession(new Session
{
    AccessToken     = "...",
    RefreshToken    = "...",
    AccessExpiresAt = "2099-01-01T00:00:00Z",
    RefreshExpiresAt = "2099-01-01T00:00:00Z",
});

// Magic link
await client.Auth.RequestMagicLinkAsync("alice@example.com");
var magicSession = await client.Auth.ConsumeMagicLinkAsync("token-from-email");

// API keys
var issued = await client.Auth.CreateApiKeyAsync("ci-key"); // secret shown once
var keys   = await client.Auth.ListApiKeysAsync();
await client.Auth.DeleteApiKeyAsync(issued.Id);

// OAuth — redirect the user's browser to result.RedirectUrl
var oauth = await client.Auth.GetOAuthAuthorizeUrlAsync("google",
    redirectTo: "https://app.example.com/callback");
Console.WriteLine(oauth.RedirectUrl);
```

### Query

```csharp
// SELECT id, total FROM orders WHERE total >= 100 ORDER BY total DESC LIMIT 10
var result = await client.Table("orders")
    .Select("id,total")
    .Gte("total", "100")
    .Order("total", ascending: false)
    .Limit(10)
    .RunAsync();

foreach (var row in result.Rows)
    Console.WriteLine(row["id"]);

// Decode rows into typed objects
record OrderDto(
    [property: JsonPropertyName("id")]    int    Id,
    [property: JsonPropertyName("total")] double Total);

var orders = result.Into<OrderDto>();

// Pagination — continue from a cursor
var page2 = await client.Table("orders")
    .Limit(10)
    .Cursor(result.NextCursor!)
    .RunAsync();
```

### Supported filter operators

| Method          | PostgREST wire form    |
|-----------------|------------------------|
| `.Eq(col, val)` | `col=eq.val`           |
| `.Neq(col, val)` | `col=neq.val`         |
| `.Gt(col, val)` | `col=gt.val`           |
| `.Gte(col, val)` | `col=gte.val`         |
| `.Lt(col, val)` | `col=lt.val`           |
| `.Lte(col, val)` | `col=lte.val`         |
| `.In(col, values)` | `col=in.(a,b,c)`   |
| `.Is(col, "null")` | `col=is.null`      |
| `.Order(col, asc)` | `order=col.asc`    |
| `.Limit(n)` | `limit=N`                   |
| `.Offset(n)` | `offset=N`                 |
| `.Cursor(tok)` | `cursor=token`          |

### Insert / Update / Delete

```csharp
// Insert one row
await client.Table("orders")
    .InsertAsync(new { total = 49.99, status = "new" });

// Batch insert
await client.Table("orders")
    .InsertAsync(new[] {
        new { total = 10.0, status = "new" },
        new { total = 20.0, status = "new" },
    });

// Update rows matching filters
await client.Table("orders")
    .Eq("id", "7")
    .UpdateAsync(new { status = "shipped" });

// Delete
await client.Table("orders")
    .Eq("id", "5")
    .DeleteAsync();
```

### RPC (catalog functions / Wasm UDFs)

```csharp
// POST /rest/v1/rpc/my_fn
var result = await client.RpcAsync("my_fn",
    new Dictionary<string, object?> { ["x"] = 42 });

// Or via Functions client
var r = await client.Functions.RpcAsync("calculate_total",
    new Dictionary<string, object?> { ["order_id"] = 7 });
```

### HTTP-handler functions

```csharp
// ANY /fn/v1/:name — response proxied verbatim
var resp = await client.Functions.InvokeAsync("my-handler",
    body: new { key = "value" },
    method: "POST");

Console.WriteLine(resp.Status);
Console.WriteLine(resp.Data);
```

### Storage

```csharp
// Bucket management
var bucket = await client.Storage.CreateBucketAsync("avatars", @public: true);
bucket = await client.Storage.GetBucketAsync("avatars");
await client.Storage.DeleteBucketAsync("avatars");

// Object operations
var bkt = client.Storage.FromBucket("avatars");

// Upload
var bytes = await File.ReadAllBytesAsync("profile.png");
var obj = await bkt.UploadAsync("alice/profile.png", bytes, "image/png");

// Download
var dl = await bkt.DownloadAsync("alice/profile.png");
// dl.Data byte[], dl.ContentType, dl.ETag

// List
var objects = await bkt.ListAsync(prefix: "alice/", limit: 100);

// Delete single object
await bkt.RemoveAsync("alice/profile.png");

// Bulk delete by prefix
await bkt.RemoveByPrefixesAsync(new[] { "alice/", "bob/" });

// Public URL (bucket must have Public=true)
var url = bkt.GetPublicUrl("alice/profile.png");

// Signed URL (time-limited, no JWT required for download)
var signed = await bkt.CreateSignedUrlAsync("alice/profile.png", expiresIn: 3600);
Console.WriteLine(signed.AbsoluteUrl); // full URL ready to use
```

### Realtime (WebSocket change streams)

```csharp
using var cts = new CancellationTokenSource();

// Subscribe to change events for "orders"
await foreach (var frame in client.Realtime.ListenAsync("orders",
    new SubscribeOptions
    {
        Filter      = "NEW.status = 'paid'", // optional server-side SQL predicate
        LastEventId = 42,                    // optional reconnect cursor (replay from seq 42)
    },
    cancellationToken: cts.Token))
{
    switch (frame.Type)
    {
        case "event":
            Console.WriteLine($"{frame.Event!.Op} {frame.Event.Table} seq={frame.Event.Seq}");
            Console.WriteLine(frame.Event.After?["id"]);
            break;
        case "error":
            Console.WriteLine($"realtime error: {frame.Error!.Code}");
            break;
        case "gap":
            Console.WriteLine($"replay ring evicted — cold re-sync needed; oldest={frame.Gap!.OldestInRing}");
            break;
    }
}

// Cancel to stop listening
cts.Cancel();
```

**Presence:**

```csharp
// Register in a presence channel
await client.Realtime.PresenceTrackAsync("room:1", "user-c1",
    new { name = "Alice" });

// Refresh presence TTL
await client.Realtime.PresenceHeartbeatAsync("room:1", "user-c1");

// Remove from presence
await client.Realtime.PresenceUntrackAsync("room:1", "user-c1");

// Listen for presence frames on the same IAsyncEnumerable
await foreach (var frame in client.Realtime.ListenAsync("room:1", ct: cts.Token))
{
    if (frame.Type == "presence_state")
    {
        foreach (var entry in frame.PresenceState!.Presences)
            Console.WriteLine(entry.ClientId);
    }
    else if (frame.Type == "presence_diff")
    {
        foreach (var join in frame.PresenceDiff!.Joins)
            Console.WriteLine($"joined: {join.ClientId}");
    }
    else if (frame.Type == "presenceerror")
    {
        // Note: wire type is "presenceerror" (no underscore) per Rust serialisation.
        Console.WriteLine(frame.PresenceError!.Code);
    }
}
```

Realtime protocol notes:
- URL: `wss://host/realtime/v1/ws/:project`
- Auth: `Sec-WebSocket-Protocol: basin-v1,<token>` subprotocol header
- Server close codes: `4001` unauthorized, `4003` forbidden, `4008` project deleted
- Reconnects automatically with exponential backoff (0.5 s → … → 30 s), resubscribing all active channels
- Requires a JWT with a `project_id` claim (raw API keys don't carry a project ID; use `.WithProjectId(...)`)

### Arrow IPC

The query builder can return raw Arrow IPC bytes for zero-JSON columnar reads:

```csharp
// Returns native Arrow IPC bytes — decode with Apache.Arrow NuGet package.
var (arrowBytes, nextCursor) = await client.Table("events")
    .Eq("status", "active")
    .Limit(1000)
    .ToArrowBytesAsync();

// Decode with Apache.Arrow (install: dotnet add package Apache.Arrow)
if (arrowBytes.Length > 0)
{
    using var ms = new System.IO.MemoryStream(arrowBytes);
    using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(ms);
    var recordBatch = await reader.ReadNextRecordBatchAsync();
    Console.WriteLine($"rows: {recordBatch?.Length}");
    Console.WriteLine($"next cursor: {nextCursor}");
}
```

When the server doesn't serve Arrow IPC (older server), `arrowBytes` is empty.

## Error handling

All API errors are thrown as `BasinApiException`. The `Code` property is the stable
contract — match on it, not `Message` (which is human-readable and may change).

```csharp
try
{
    await client.Table("orders").RunAsync();
}
catch (BasinApiException ex)
{
    switch (ex.Code)
    {
        case "E_UNAUTHENTICATED":
            // re-authenticate
            break;
        case "E_NOT_FOUND":
            // resource missing
            break;
        case "E_RATE_LIMITED":
            // back off and retry
            break;
        case "E_ENGINE_UNSUPPORTED":
            // server does not support this operation (e.g. DELETE on some engines)
            break;
    }

    Console.WriteLine($"{ex.Code} (HTTP {ex.Status}): {ex.Message}");

    if (ex.SqlState is not null)
        Console.WriteLine($"SQLSTATE: {ex.SqlState}"); // e.g. "23505" unique violation
}
catch (BasinNetworkException ex)
{
    // Transport-layer failure (connection refused, timeout, etc.)
    Console.WriteLine($"network error: {ex.Message}");
}
```

Stable error codes:
`E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`, `E_INVALID_REQUEST`,
`E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`, `E_EMAIL_DISABLED`,
`E_REVOKED_TOKEN`.

## Session auto-refresh

Once `Auth.SignInAsync` has been called, the SDK stores the session and
auto-refreshes the access token when it is within 10 seconds of expiry.
Refresh is `SemaphoreSlim(1,1)`-guarded so concurrent requests share a single
refresh call. Refreshed tokens are injected into every subsequent HTTP request
automatically.

## MFA

```csharp
// Enroll a TOTP factor
var res = await client.Auth.EnrollFactorAsync("totp", "My Authenticator");
if (res is TotpEnrollResult totp)
{
    // Display totp.OtpauthUri as a QR code
    Console.WriteLine(totp.SecretB32);
}

// Enroll a WebAuthn factor
var wares = await client.Auth.EnrollFactorAsync("webauthn", "YubiKey");
if (wares is WebAuthnEnrollResult wa)
{
    // Pass wa.CreationOptionsJson to navigator.credentials.create()
}

// Verify factor enrollment (RecoveryCodes shown exactly once — store securely)
var vr = await client.Auth.VerifyFactorAsync(factorId, code: "123456");
Console.WriteLine(vr.RecoveryCodes); // save securely

// Step-up challenge (raises session to aal2)
var challenge = await client.Auth.ChallengeFactorAsync(factorId);
if (challenge is TotpChallengeResult tc)
{
    var sess = await client.Auth.VerifyChallengeAsync(factorId, tc.ChallengeId, code: "654321");
    // sess now has aal2 access token
}

// Unenroll (requires aal2 session)
await client.Auth.UnenrollFactorAsync(factorId);
```

## Builder reference

| Method | Effect |
|---|---|
| `.WithUrl(url)` | Basin server base URL (required) |
| `.WithApiKey(key)` | JWT or raw API key |
| `.WithProjectId(id)` | Default project ULID |
| `.WithTimeout(ts)` | Per-request HTTP timeout (default: 30 s) |
| `.WithHttpClient(hc)` | Custom `HttpClient` for testing or advanced config |

## Deliberate omissions

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`): WebSocket covers the same
  events. SSE is left for environments without WebSocket.
- **Admin routes** (`/admin/v1/*`): operator-only. Use the escape hatch below.

## Escape hatch

For routes not yet wrapped:

```csharp
var raw = await client.RequestAsync("GET", "/admin/v1/projects");
```
