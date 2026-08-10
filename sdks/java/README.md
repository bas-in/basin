# basin-java

Java client SDK for [Basin](https://basin.run) — speaks directly to a deployed
[`basin-engine`](https://github.com/vul-os/basin) (the open-source Rust core,
Apache-2.0). Covers auth, PostgREST-style table queries, Apache Arrow IPC,
NDJSON streaming, object storage, realtime WebSocket, and RPC functions.

Part of the Basin SDK family alongside `basin-js` and `basin-py`. All three bind
the same `basin-rest` route set: engine-direct over pgwire + REST, no intermediary.

- **Java 17+** — uses `java.net.http.HttpClient` (built-in); no OkHttp or Retrofit.
- **Zero heavy deps for the core** — only `jackson-databind` at runtime.
- **Arrow IPC** (`toArrow()`) — zero JSON round-trip, full i64/timestamp fidelity;
  Arrow jars are `compileOnly` so they don't bloat projects that don't need them.
- **NDJSON streaming** (`stream()`) — `Iterable<Map<String,Object>>`; cursor-aware.
- **Dual API** — async (`CompletableFuture`) plus blocking (`*Blocking(…)`) wrappers
  for every method.
- **Kotlin-friendly** — suspend-function extension wrappers in `BasinExtensions.kt`.
- **Typed exceptions** — `BasinApiException` carries `code`, `status`, `sqlState`;
  `BasinNetworkException` for transport failures.
- **Auto-refresh** — synchronized access-token refresh 10 s before expiry.

## Install

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.basin:basin-java:0.1.0")

    // Optional: Arrow IPC support (QueryBuilder.toArrow()).
    // Add only if you call .toArrow() — keeps the default footprint small.
    runtimeOnly("org.apache.arrow:arrow-vector:19.0.0")
    runtimeOnly("org.apache.arrow:arrow-memory-unsafe:19.0.0")
}
```

### Maven

```xml
<dependency>
    <groupId>io.basin</groupId>
    <artifactId>basin-java</artifactId>
    <version>0.1.0</version>
</dependency>

<!-- Optional: Arrow IPC. Add only if you use QueryBuilder.toArrow(). -->
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>19.0.0</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-memory-unsafe</artifactId>
    <version>19.0.0</version>
    <scope>runtime</scope>
</dependency>
```

### Self-hosting

`basin-java` works against any Basin engine — the cloud-managed regional
deployments at `https://<region>.basin.run`, or a self-hosted engine. Point
`.url(...)` at the engine's HTTP base URL (e.g. `http://localhost:5434`).
Mint an anon key via `POST /auth/v1/api-keys` on your engine (requires an
admin JWT) and pass it as `.apiKey(...)`.

## Quickstart

```java
import io.basin.sdk.BasinClient;
import io.basin.sdk.QueryResult;
import io.basin.sdk.Session;

// BASIN_URL points at a deployed basin-engine — NOT the control plane.
// Mint BASIN_API_KEY at https://basin.run/app/project/<ref>/api-keys
BasinClient client = BasinClient.builder()
    .url(System.getenv("BASIN_URL"))       // e.g. https://basin-engine.fly.dev
    .apiKey(System.getenv("BASIN_API_KEY"))
    .projectId("01JWXXX...")               // optional when key is a JWT
    .build();

// Health check
String status = client.healthBlocking(); // "ok"

// Sign in
Session session = client.auth.signIn("alice@example.com", "hunter2", null);

// Query — hits /rest/v1/orders
QueryResult result = client.table("orders")
    .select("id,total,status")
    .eq("status", "paid")
    .gte("total", "100")
    .order("total", false)   // descending
    .limit(50)
    .execute()               // returns CompletableFuture<QueryResult>
    .join();

result.rows().forEach(row -> System.out.println(row));

// Insert
client.table("orders")
    .insertBlocking(Map.of("total", 50, "status", "new"));

// Update
client.table("orders")
    .eq("id", 7)
    .updateBlocking(Map.of("status", "shipped"));

// Delete
client.table("orders")
    .eq("id", 7)
    .deleteBlocking();

// RPC
JsonNode total = client.rpcBlocking("add_two", Map.of("a", 1, "b", 41));

client.close(); // or use try-with-resources
```

Every async method has a `*Blocking(…)` convenience wrapper. The async form
returns `CompletableFuture<T>` — compose with `thenApply`, `thenCompose`, or
call `.join()` for blocking.

## Architecture

[Basin Cloud](https://basin.run) is the **control plane** — dashboard, billing,
project management, and the place you mint the API key. Once you have a URL and
key, the control plane is **off the data path**: every `auth.*`, `table(...)`, and
`storage.*` call lands on `basin-engine` directly. The engine is open source and
deployable anywhere via `cargo run -p basin-server`.

Engine routes used by this SDK: `/auth/v1/*`, `/rest/v1/:table`,
`/rest/v1/rpc/:fn_name`, `/fn/v1/:name`, `/storage/v1/*`,
`/realtime/v1/ws/:project`, `/health`.

## Auth

```java
AuthClient auth = client.auth;

// Sign up
auth.signUp("alice@example.com", "hunter2", null);

// Sign in — stores session; access token auto-refreshed 10 s before expiry
Session s = auth.signIn("alice@example.com", "hunter2", null);

// Restore an existing session (e.g. from secure storage)
auth.setSession(storedSession);

// Sign out — revokes the refresh token server-side, then clears locally
auth.signOut();

// API keys (JWT-gated)
ApiKeyIssued key = auth.createApiKey("ci-pipeline");
System.out.println(key.secret);  // shown exactly once
List<ApiKeyDescriptor> keys = auth.listApiKeys();
auth.deleteApiKey(key.id);

// Magic links
auth.requestMagicLink("alice@example.com");
Session fromMagicLink = auth.consumeMagicLink("token-from-email");

// Email / password reset
auth.requestPasswordReset("alice@example.com", null);
auth.resetPassword("token-from-email", "newPassword", null);
auth.verifyEmail("token-from-email", null);
```

After `signIn(...)`, subsequent calls automatically attach the session's access
token as `Authorization: Bearer <at>`. The static API key is the fallback when
there is no session. Refresh tokens rotate on each use; reusing a rotated token
throws `BasinApiException` with `code = "E_REVOKED_TOKEN"`.

## OAuth

```java
// 1. Get the provider authorize URL (built server-side with PKCE + CSRF state).
OAuthAuthorizeResult r = auth.getOAuthAuthorizeUrl(
    "google",
    "https://myapp.example.com/auth/callback",
    null);

// 2. Redirect the user's browser to r.redirectUrl.
//    After the flow completes your app receives Basin tokens via the redirect URL.
System.out.println(r.redirectUrl);  // https://accounts.google.com/…
System.out.println(r.state);        // CSRF state
```

Supported preset providers: `google`, `github`, `apple`, `bitbucket`, `discord`,
`figma`, `gitlab`, `linkedin`, `microsoft` (azure_ad), `notion`, `slack`,
`spotify`, `twitch`, `twitter_x`. Custom OIDC providers are registered
server-side and referenced by name.

## MFA (TOTP and WebAuthn)

```java
// Enroll a TOTP factor
TotpEnrollResult enroll = (TotpEnrollResult) auth.enrollFactor("totp", "Authenticator");
System.out.println(enroll.otpauthUri);  // display as QR code

// Confirm enrollment
VerifyFactorResult verified = auth.verifyFactor(enroll.factorId, "123456", null, null);
// verified.recoveryCodes — save these; shown once

// Begin a step-up challenge
TotpChallengeResult challenge = (TotpChallengeResult) auth.challengeFactor(enroll.factorId);

// Complete the challenge — returns an aal2 session
Session aal2 = auth.verifyChallenge(enroll.factorId, challenge.challengeId, "654321", null);

// Unenroll (requires aal2 token)
auth.unenrollFactor(enroll.factorId);
```

WebAuthn passkeys follow the same flow — `enrollFactor("webauthn", "YubiKey")`
returns `WebAuthnEnrollResult` whose `creationOptionsJson` is passed to
`navigator.credentials.create()` in your JS bridge. Challenge and verify steps
are identical, swapping the TOTP code for the assertion JSON.

## Row Level Security

The engine exposes three SQL session functions usable in RLS policies:

| Function | Returns | Description |
|---|---|---|
| `auth.uid()` | `uuid` | UUID of the signed-in user |
| `auth.role()` | `text` | `'authenticated'` or `'anon'` |
| `auth.jwt()` | `jsonb` | Full JWT claims |

After `signIn(...)`, queries automatically carry the session JWT, so RLS policies
filter rows without extra client code.

## Query builder

Supported filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is`.
Filters AND together. Not supported: `or=`, `not.`, `like`/`ilike`, embedded
resource selects, `Prefer` headers.

```java
QueryResult result = client.table("orders")
    .select("id,total,status")
    .eq("status", "paid")
    .neq("archived", "true")
    .gte("total", "100")
    .in("category", List.of("a", "b", "c"))
    .is("deleted_at", "null")
    .order("total", false)   // false = descending
    .limit(50)
    .offset(0)
    .executeBlocking();

// Typed decode via Jackson-annotated POJO
List<Order> orders = result.into(new ObjectMapper(), Order.class);

// Batch insert
client.table("orders").insertBlocking(List.of(
    Map.of("total", 10, "status", "new"),
    Map.of("total", 20, "status", "new")));

// Keyset pagination
QueryResult page1 = client.table("events").limit(500).executeBlocking();
if (page1.nextCursor != null) {
    QueryResult page2 = client.table("events")
        .cursor(page1.nextCursor).limit(500).executeBlocking();
}
```

## NDJSON streaming

For large result sets, `stream()` issues `?stream=true` and returns an
`Iterable<Map<String,Object>>` backed by NDJSON. The server auto-promotes
responses over ~1 MiB or 10 000 rows to NDJSON regardless; `stream()` makes
this the explicit path.

```java
QueryBuilder.StreamResult sr = client.table("events")
    .gte("id", "1000")
    .stream();

for (Map<String, Object> row : sr) {
    process(row);
}

// Cursor from the trailing _basin_next_cursor sentinel line
String next = sr.nextCursor();
```

## Arrow IPC

`toArrow()` sends `Accept: application/vnd.apache.arrow.stream` and decodes
the response into a `VectorSchemaRoot` — zero JSON overhead, full columnar
access, and full i64/timestamp/decimal fidelity. Falls back to a JSON-to-Arrow
conversion when talking to an older server that doesn't support the IPC path.

Requires the optional Arrow runtime jars (see [Install](#install)).

```java
String cursor = null;
do {
    QueryBuilder qb = client.table("orders").limit(1000);
    if (cursor != null) qb = qb.cursor(cursor);

    try (ArrowResult ar = qb.toArrow()) {
        VectorSchemaRoot root = ar.root;
        // columnar access — no per-row boxing
        BigIntVector ids = (BigIntVector) root.getVector("id");
        for (int i = 0; i < root.getRowCount(); i++) {
            process(ids.get(i));
        }
        cursor = ar.nextCursor;
    }
} while (cursor != null);
```

## Functions and RPC

```java
// HTTP-handler function at ANY /fn/v1/:name
FunctionInvokeResult r = client.functions
    .invokeBlocking("resize", "POST", Map.of("width", 100), null);
System.out.println(r.status);   // function's own HTTP status
System.out.println(r.data);     // parsed body (JsonNode or String)

// Catalog UDF at POST /rest/v1/rpc/:fn_name
// Scalar: returns bare value
JsonNode sum = client.rpcBlocking("add_two", Map.of("a", 3, "b", 4));
// sum.asInt() == 7

// RETURNS TABLE: returns array of row objects
JsonNode users = client.rpcBlocking("active_users", Map.of("min_logins", 5));
```

## Storage

```java
StorageClient storage = client.storage;

// Bucket management
storage.createBucketBlocking("avatars", /* public= */ true, null, null);
StorageBucket meta = storage.getBucketBlocking("avatars");
storage.deleteBucketBlocking("avatars");

// Object operations
StorageBucketClient bucket = storage.fromBucket("avatars");

bucket.uploadBlocking("users/alice.png",
    Files.readAllBytes(Path.of("alice.png")), "image/png");

DownloadResult dl = bucket.downloadBlocking("users/alice.png");
System.out.println(dl.contentType);
byte[] bytes = dl.bytes;

List<StorageObject> objects = bucket.listBlocking("users/", 100, null);
bucket.removeBlocking("users/alice.png");
bucket.removeByPrefixesBlocking(List.of("users/", "tmp/"));

// URLs
String publicUrl = bucket.getPublicUrl("users/alice.png");  // bucket must be public
SignedUrl signed  = bucket.createSignedUrlBlocking("users/alice.png", 3600);
System.out.println(signed.absoluteUrl);
```

## Realtime

Subscribe to live INSERT / UPDATE / DELETE events over
`GET /realtime/v1/ws/:project`. Auth is via the
`Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol header — no extra
dependency; `java.net.http.WebSocket` handles this natively.

The client reconnects with exponential backoff (0.5 s → 30 s) on unexpected
disconnect and automatically re-issues all active subscriptions. Pass
`lastEventId` to replay events missed during the gap.

```java
RealtimeClient realtime = client.realtime();

ChannelHandle handle = realtime.subscribe(
    "orders",
    event -> {
        if (event instanceof RealtimeEvent e) {
            System.out.printf("%s %s -> %s%n", e.op, e.table, e.after);
        } else if (event instanceof RealtimeFrame.Error err) {
            System.err.println("lag error: " + err.code);
        } else if (event instanceof RealtimeFrame.Gap g) {
            System.err.println("gap detected on " + g.table);
        }
    },
    null,  // filter: optional server-side SQL predicate, e.g. "NEW.status='paid'"
    null   // lastEventId: reconnect cursor (Long); null to start fresh
).join();

// Presence
realtime.presenceTrack("room:lobby", "user-abc", Map.of("name", "Alice")).join();
realtime.presenceHeartbeat("room:lobby", "user-abc").join();
realtime.presenceUntrack("room:lobby", "user-abc").join();

// Clean up
handle.unsubscribe();
realtime.close();
```

### Event types

| Java type | When |
|---|---|
| `RealtimeEvent` | INSERT / UPDATE / DELETE — fields: `op`, `table`, `project`, `seq`, `before`, `after` |
| `RealtimeFrame.Error` | Protocol error for a table subscription; `code = "lag"` means missed events |
| `RealtimeFrame.Gap` | Reconnect cursor predates the replay ring; cold re-sync needed |
| `RealtimeFrame.PresenceState` | Snapshot of current members on join |
| `RealtimeFrame.PresenceDiff` | Incremental joins / leaves |
| `RealtimeFrame.PresenceError` | Rejected presence op (e.g. identity mismatch) |

## Error handling

Every non-2xx response throws `BasinApiException`. Match on `code` — the message
is human-readable and may change across server versions.

```java
import io.basin.sdk.BasinApiException;
import io.basin.sdk.BasinNetworkException;

try {
    client.table("orders").deleteBlocking();
} catch (BasinApiException e) {
    switch (e.code) {
        case "E_ENGINE_UNSUPPORTED" ->
            System.out.println("DELETE not supported on this table");
        case "E_UNAUTHENTICATED" ->
            client.auth.refreshSession();
        default -> throw e;
    }
} catch (BasinNetworkException e) {
    // connection refused, timeout, TLS error
    System.err.println("transport failure: " + e.getMessage());
}

// SQLSTATE for SQL-layer errors (e.g. unique-constraint violations)
try {
    client.table("orders").insertBlocking(Map.of("id", 1));
} catch (BasinApiException e) {
    if ("23505".equals(e.sqlState)) {
        System.out.println("unique constraint violation");
    }
}
```

Known stable error codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from a newer server pass
through as plain strings on `e.code`.

## pgwire (direct database connections)

For psql, DBeaver, Flyway, and other tools that speak the Postgres wire protocol:

```
# Session / JWT auth — access token as the username
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API-key auth — username is {tenant_id}_{hex}, password is the full key
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

`auth.uid()`, `auth.role()`, and `auth.jwt()` work identically over pgwire;
RLS policies apply the same way as over REST.

## Kotlin coroutines

Add `kotlinx-coroutines-jdk8` to your build, then use the suspend-function
extension wrappers from `BasinExtensions.kt`:

```kotlin
import io.basin.sdk.*

val client = BasinClient.builder()
    .url(System.getenv("BASIN_URL"))
    .apiKey(System.getenv("BASIN_API_KEY"))
    .build()

coroutineScope {
    val result  = client.table("orders").select("id,total").awaitExecute()
    val session = client.auth.awaitSignIn("alice@example.com", "hunter2", null)
    val bucket  = client.storage.awaitCreateBucket("avatars", public = true)
}

client.close()
```

## Escape hatch

For routes not yet wrapped by the SDK (e.g. `/admin/v1/*`):

```java
JsonNode projects = client.request("GET", "/admin/v1/projects", null).join();
```

## Development

No running Basin server is required for the test suite.

```sh
gradle test        # run the JUnit 5 suite
gradle build       # compile + test + produce the JAR
```

## License

MIT — see [`LICENSE`](./LICENSE).
