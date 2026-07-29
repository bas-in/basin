# Basin.Sdk

Official .NET client for [Basin](https://basin.run) — a cloud-native HTAP
database with a PostgreSQL wire-compatible interface and an HTTP REST API.

Speaks **directly** to a deployed
[`basin-engine`](https://github.com/vul-os/basin) (the open-source Rust core,
Apache-2.0). Auth, PostgREST-shaped table queries, object storage, realtime
change streams, RPC functions, and Arrow IPC transport are all first-class.

**Zero heavy dependencies.** The core library uses only what ships with .NET 8:
`System.Net.Http` for transport, `System.Net.WebSockets` for realtime, and
`System.Text.Json` for serialisation. Apache Arrow IPC is a separate
conditional NuGet package you opt into only when you need it.

Part of the Basin SDK family — identical surface across
[basin-js](https://github.com/bas-in/basin-js),
[basin-py](https://github.com/bas-in/basin-py),
basin-dotnet, and [eight more languages](https://basin.run/docs/sdks).

## Install

```sh
dotnet add package Basin.Sdk
```

Requires .NET 8 or later.

## Quickstart

```csharp
using Basin;

// BASIN_URL points at a deployed basin-engine — not basin-cloud.
// Mint your API key at https://basin.run/app/project/<ref>/api-keys
using var client = new BasinClient.Builder()
    .WithUrl("https://your-project.basin.run")
    .WithApiKey("basin_…")
    .Build();

// Sign in — session is stored and auto-refreshed before expiry
var session = await client.Auth.SignInAsync("you@example.com", "hunter2");

// Query — hits /rest/v1/products
var result = await client.Table("products")
    .Select("id, name, price")
    .Eq("active", "true")
    .Order("price", ascending: true)
    .Limit(10)
    .RunAsync();

foreach (var row in result.Rows)
    Console.WriteLine(row["id"]);
```

## Architecture

[Basin Cloud](https://basin.run) is the control plane — dashboard, billing,
project management, and the place you mint the anon-key JWT that the SDK
forwards to the engine. Once you have a URL + key, basin-cloud is **off the
data path**: every `client.Auth.*`, `client.Table(…)`, and `client.Realtime.*`
call lands on `basin-engine` directly. The engine is open source and
deployable to Fly with `./deploy.sh -t engine` from the `basin-cloud` repo
root, or runnable locally via `cargo run -p basin-server`.

Engine routes the SDK covers: `/auth/v1/{signup,signin,signout,refresh,
verify-email,reset-password,request-password-reset,magic-link,
magic-link/consume,api-keys,oauth/:provider/authorize,factors/*}`,
`/rest/v1/:table`, `/rest/v1/rpc/:fn`, `/fn/v1/:name`,
`/storage/v1/{bucket,object}/*`, `/realtime/v1/ws/:project`, `/health`.

## Auth

```csharp
// Sign up a new user
var signup = await client.Auth.SignUpAsync("alice@example.com", "password");
Console.WriteLine(signup.UserId);

// Sign in — access token stored, auto-refreshed 10 s before expiry
var session = await client.Auth.SignInAsync("alice@example.com", "password");

// Sign out (revokes refresh token server-side)
await client.Auth.SignOutAsync();

// Restore a previously serialised session
client.Auth.SetSession(new Session
{
    AccessToken      = "...",
    RefreshToken     = "...",
    AccessExpiresAt  = "2099-01-01T00:00:00Z",
    RefreshExpiresAt = "2099-01-01T00:00:00Z",
});

// Magic link
await client.Auth.RequestMagicLinkAsync("alice@example.com");
var magicSession = await client.Auth.ConsumeMagicLinkAsync("token-from-email");

// Password reset
await client.Auth.RequestPasswordResetAsync("alice@example.com");
await client.Auth.ResetPasswordAsync("token-from-email", "new-password");

// Email verification
await client.Auth.VerifyEmailAsync("token-from-email");

// OAuth — redirect the user's browser to result.RedirectUrl
var oauth = await client.Auth.GetOAuthAuthorizeUrlAsync("google",
    redirectTo: "https://app.example.com/callback");
Console.WriteLine(oauth.RedirectUrl);

// API keys
var issued = await client.Auth.CreateApiKeyAsync("ci-runner"); // secret shown once
var keys   = await client.Auth.ListApiKeysAsync();
await client.Auth.DeleteApiKeyAsync(issued.Id);
```

### Session auto-refresh

After `SignInAsync`, the SDK keeps the session in memory and auto-refreshes the
access token when it is within 10 seconds of expiry. Refresh is guarded by a
`SemaphoreSlim(1,1)` so concurrent requests share a single refresh call. The
refreshed token is injected into every subsequent HTTP and WebSocket request
automatically.

### Row Level Security

After signing in, every query builder call attaches the JWT as
`Authorization: Bearer <at>`. Use Basin's auth functions in RLS policies:

```sql
ALTER TABLE items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "users see own rows" ON items
  FOR ALL USING (owner_id = auth.uid());
```

After that, `client.Table("items").Select("*").RunAsync()` returns only the
signed-in user's rows — no extra code needed.

| SQL function | Returns | Description |
|---|---|---|
| `auth.uid()` | `uuid` | UUID of the signed-in user |
| `auth.role()` | `text` | `'authenticated'` or `'anon'` |
| `auth.jwt()` | `jsonb` | Full JWT claims |

## Query builder

```csharp
// SELECT id, total FROM orders WHERE total >= 100 ORDER BY total DESC LIMIT 10
var result = await client.Table("orders")
    .Select("id, total")
    .Gte("total", "100")
    .Order("total", ascending: false)
    .Limit(10)
    .RunAsync();

// Decode into typed records
record OrderDto(
    [property: JsonPropertyName("id")]    int    Id,
    [property: JsonPropertyName("total")] double Total);

var orders = result.Into<OrderDto>();

// Keyset pagination — continue from a cursor
var page2 = await client.Table("orders")
    .Order("id", ascending: true)
    .Limit(100)
    .Cursor(result.NextCursor!)
    .RunAsync();
```

### Filter operators

| Method | PostgREST wire form |
|---|---|
| `.Eq(col, val)` | `col=eq.val` |
| `.Neq(col, val)` | `col=neq.val` |
| `.Gt(col, val)` | `col=gt.val` |
| `.Gte(col, val)` | `col=gte.val` |
| `.Lt(col, val)` | `col=lt.val` |
| `.Lte(col, val)` | `col=lte.val` |
| `.In(col, values)` | `col=in.(a,b,c)` |
| `.Is(col, "null")` | `col=is.null` |
| `.Order(col, asc)` | `order=col.asc\|desc` |
| `.Limit(n)` | `limit=N` |
| `.Offset(n)` | `offset=N` |
| `.Cursor(token)` | `cursor=token` |

### Insert / Update / Delete

```csharp
// Insert one row (anonymous object or serialisable type)
await client.Table("orders")
    .InsertAsync(new { total = 49.99, status = "new" });

// Batch insert
await client.Table("orders")
    .InsertAsync(new[] {
        new { total = 10.0, status = "new" },
        new { total = 20.0, status = "new" },
    });

// Update rows matching the active filters
await client.Table("orders")
    .Eq("id", "7")
    .UpdateAsync(new { status = "shipped" });

// Delete
await client.Table("orders")
    .Eq("id", "5")
    .DeleteAsync();
```

## Arrow IPC

The query builder can request Arrow IPC format for zero-JSON columnar reads.
Decode the bytes with the `Apache.Arrow` NuGet package:

```sh
dotnet add package Apache.Arrow
```

```csharp
var (arrowBytes, nextCursor) = await client.Table("events")
    .Eq("status", "active")
    .Limit(1000)
    .ToArrowBytesAsync();

if (arrowBytes.Length > 0)
{
    using var ms = new System.IO.MemoryStream(arrowBytes);
    using var reader = new Apache.Arrow.Ipc.ArrowStreamReader(ms);
    var batch = await reader.ReadNextRecordBatchAsync();
    Console.WriteLine($"rows: {batch?.Length}, next cursor: {nextCursor}");
}
```

The SDK sends `Accept: application/vnd.apache.arrow.stream` and falls back to
an empty byte array when the server returns JSON — call `RunAsync()` in that
case.

## RPC / functions

```csharp
// POST /rest/v1/rpc/my_fn — SQL function or Wasm UDF
var r = await client.RpcAsync("calculate_total",
    new Dictionary<string, object?> { ["order_id"] = 7 });

// Via Functions client (same route)
var r2 = await client.Functions.RpcAsync("add",
    new Dictionary<string, object?> { ["x"] = 3, ["y"] = 4 });

// ANY /fn/v1/:name — HTTP-handler function, response proxied verbatim
var resp = await client.Functions.InvokeAsync("send-welcome-email",
    body: new { user_id = "abc" },
    method: "POST");

Console.WriteLine(resp.Status);
Console.WriteLine(resp.Data);
```

## Storage

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
// dl.Data (byte[]), dl.ContentType, dl.ETag

// List objects
var objects = await bkt.ListAsync(prefix: "alice/", limit: 100);

// Delete a single object
await bkt.RemoveAsync("alice/profile.png");

// Bulk delete by prefix
await bkt.RemoveByPrefixesAsync(new[] { "alice/", "bob/" });

// Public URL — bucket must have Public = true
var url = bkt.GetPublicUrl("alice/profile.png");

// Signed URL — time-limited, no JWT required for download
var signed = await bkt.CreateSignedUrlAsync("alice/profile.png", expiresIn: 3600);
Console.WriteLine(signed.AbsoluteUrl);
```

## Realtime

The realtime client speaks WebSocket (`/realtime/v1/ws/:project`) and yields
`ServerFrame` values via `IAsyncEnumerable<ServerFrame>`. Auth is carried as a
`Sec-WebSocket-Protocol: basin-v1,<token>` subprotocol header. Reconnects
happen automatically with exponential backoff (0.5 s → 30 s), resubscribing
all active channels after each reconnect.

```csharp
using var cts = new CancellationTokenSource();

// Subscribe to INSERT/UPDATE/DELETE events for "orders"
await foreach (var frame in client.Realtime.ListenAsync("orders",
    new SubscribeOptions
    {
        Filter      = "NEW.status = 'paid'", // optional server-side SQL predicate
        LastEventId = 42,                    // optional replay cursor (resume from seq 42)
    },
    cancellationToken: cts.Token))
{
    switch (frame.Type)
    {
        case "event":
            Console.WriteLine($"{frame.Event!.Op} on {frame.Event.Table} seq={frame.Event.Seq}");
            Console.WriteLine(frame.Event.After?["id"]);
            break;

        case "error":
            Console.WriteLine($"realtime error: {frame.Error!.Code}");
            break;

        case "gap":
            // Ring evicted — missed events; issue a cold re-sync
            Console.WriteLine($"gap detected; oldest seq still in ring: {frame.Gap!.OldestInRing}");
            break;
    }
}

cts.Cancel();
```

### Presence

```csharp
// Register in a presence channel (server enforces client_id matches JWT identity)
await client.Realtime.PresenceTrackAsync("room:lobby", clientId: "user-alice",
    new { name = "Alice", avatar = "…" });

// Refresh presence TTL (call at least every 90 s or the server evicts the entry)
await client.Realtime.PresenceHeartbeatAsync("room:lobby", clientId: "user-alice");

// Remove from presence
await client.Realtime.PresenceUntrackAsync("room:lobby", clientId: "user-alice");

// Listen for presence frames on the same IAsyncEnumerable
await foreach (var frame in client.Realtime.ListenAsync("room:lobby", ct: cts.Token))
{
    if (frame.Type == "presence_state")
    {
        foreach (var entry in frame.PresenceState!.Presences)
            Console.WriteLine($"online: {entry.ClientId}");
    }
    else if (frame.Type == "presence_diff")
    {
        foreach (var join in frame.PresenceDiff!.Joins)
            Console.WriteLine($"joined: {join.ClientId}");
        foreach (var leave in frame.PresenceDiff.Leaves)
            Console.WriteLine($"left: {leave.ClientId}");
    }
    else if (frame.Type == "presenceerror")
    {
        // Note: wire type is "presenceerror" (no underscore) per Rust serde
        Console.WriteLine($"presence error: {frame.PresenceError!.Code}");
    }
}
```

Realtime protocol notes:
- URL: `wss://host/realtime/v1/ws/:project`
- Server close codes: `4001` unauthorized, `4003` forbidden, `4008` project deleted
- A project ID is required. Pass it with `.WithProjectId(…)` on the builder if
  your key is not a JWT that carries a `project_id` claim.

## MFA

```csharp
// Enroll TOTP
var res = await client.Auth.EnrollFactorAsync("totp", "My Authenticator App");
if (res is TotpEnrollResult totp)
{
    // Display totp.OtpauthUri as a QR code
    Console.WriteLine(totp.SecretB32);
}

// Enroll WebAuthn / passkey
var wa = await client.Auth.EnrollFactorAsync("webauthn", "YubiKey");
if (wa is WebAuthnEnrollResult webauthn)
{
    // Pass webauthn.CreationOptionsJson to navigator.credentials.create()
}

// Verify enrollment (RecoveryCodes present only on the first verified factor)
var vr = await client.Auth.VerifyFactorAsync(factorId, code: "123456");
if (vr.RecoveryCodes is { } codes)
    Console.WriteLine("Save these: " + string.Join(", ", codes));

// Step-up challenge (raises session to aal2)
var challenge = await client.Auth.ChallengeFactorAsync(factorId);
if (challenge is TotpChallengeResult tc)
{
    var sess = await client.Auth.VerifyChallengeAsync(
        factorId, tc.ChallengeId, code: "654321");
    // sess now has aal2 access token
}

// Unenroll (requires aal2 JWT)
await client.Auth.UnenrollFactorAsync(factorId);

// List enrolled factors
var factors = await client.Auth.ListFactorsAsync();
```

## Error handling

All API errors surface as `BasinApiException`. The `Code` property is the
stable contract — match on it, not `Message`.

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
            // Call SignInAsync and retry
            break;
        case "E_NOT_FOUND":
            break;
        case "E_RATE_LIMITED":
            // Back off and retry
            break;
        case "E_ENGINE_UNSUPPORTED":
            // Server does not yet support this operation
            break;
    }

    Console.WriteLine($"{ex.Code} (HTTP {ex.Status}): {ex.Message}");

    // SQLSTATE is present when the error originated from the SQL engine
    if (ex.SqlState is not null)
        Console.WriteLine($"SQLSTATE: {ex.SqlState}"); // e.g. "23505" unique violation
}
catch (BasinNetworkException ex)
{
    // Transport failure — connection refused, timeout, DNS, etc.
    Console.WriteLine($"network: {ex.Message}");
}
```

**Stable error codes:** `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`.

## Builder reference

```csharp
using var client = new BasinClient.Builder()
    .WithUrl("https://your-project.basin.run")   // required
    .WithApiKey("basin_…")                        // JWT or raw API key
    .WithProjectId("01JWXXX…")                   // required for realtime + public storage URLs
                                                  // when the key is not a JWT
    .WithTimeout(TimeSpan.FromSeconds(60))        // per-request HTTP timeout (default: 30 s)
    .WithHttpClient(myHttpClient)                 // custom HttpClient for testing or proxying
    .Build();
```

## Self-hosting basin

`BasinClient` works against any basin engine — the managed regional deployments
at `https://<region>.basin.run` or a self-hosted engine you run yourself
(`cargo run -p basin-server`, or the published container). Point
`.WithUrl(…)` at the engine's HTTP base URL (`http://localhost:5434` by
default) and the SDK behaves identically to talking to the managed cloud.

Mint your own anon key via `POST /auth/v1/api-keys` on your engine (requires an
admin JWT). The returned key is the value you pass to `.WithApiKey(…)`.

## pgwire connections (advanced)

For psql, DBeaver, migration tools, and other clients that speak Postgres wire
protocol, use the engine's pgwire listener (default port 5433):

```
# Session/JWT auth — pass the access token as the username:
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API-key auth — username is {tenant_id}_{hex}, password is the full key:
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

After connecting via pgwire, `auth.uid()` / `auth.role()` / `auth.jwt()` work
identically to the REST path — the same RLS policies apply.

## Escape hatch

For routes not yet wrapped by the SDK (e.g. `/admin/v1/*`):

```csharp
var raw = await client.RequestAsync("GET", "/admin/v1/projects");
```

## License

MIT — see [LICENSE](./LICENSE).
