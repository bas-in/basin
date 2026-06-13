# basin-java

Java/Kotlin client for [Basin](../../README.md)'s HTTP surfaces: REST data API,
auth, functions, object storage, and realtime WebSocket.

- **Java 17+** — uses `java.net.http.HttpClient` (built-in); no OkHttp or Retrofit.
- **Zero heavy deps for core** — only Jackson for JSON (`jackson-databind`).
- **CompletableFuture async API** — the JVM idiom; blocking convenience wrappers
  (`*Blocking(…)`) included for every method.
- **Kotlin-friendly** — suspend-function extension wrappers in `BasinExtensions.kt`.
- **Typed exceptions** — `BasinApiException` decodes the server error envelope
  (`code`, `message`, `sqlstate`); `BasinNetworkException` for transport failures.
- **Auto-refresh** — synchronized access-token refresh, 10 s before expiry.

## Install

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.basin:basin-java:0.1.0")
}
```

### Maven

```xml
<dependency>
    <groupId>io.basin</groupId>
    <artifactId>basin-java</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Quickstart

### Async (CompletableFuture)

```java
import io.basin.sdk.BasinClient;
import io.basin.sdk.QueryResult;

BasinClient client = BasinClient.builder()
    .url("https://your-project.basin.run")
    .apiKey("your-api-key")
    .projectId("01JWXXX...")  // optional if key is a JWT
    .build();

// Health check
String status = client.healthBlocking(); // "ok"

// Query builder
QueryResult result = client.table("orders")
    .select("id,total,status")
    .eq("status", "paid")
    .gte("total", "100")
    .order("total", false)  // descending
    .limit(50)
    .execute()              // returns CompletableFuture<QueryResult>
    .join();

result.rows().forEach(row -> System.out.println(row));

// Keyset pagination
QueryResult page1 = client.table("orders").limit(100).executeBlocking();
QueryResult page2 = client.table("orders")
    .cursor(page1.nextCursor).limit(100).executeBlocking();

// Writes
client.table("orders")
    .insertBlocking(Map.of("total", 50, "status", "new"));

client.table("orders")
    .eq("id", 7)
    .updateBlocking(Map.of("status", "paid"));

client.table("orders")
    .eq("id", 7)
    .deleteBlocking();  // may throw E_ENGINE_UNSUPPORTED

// RPC
JsonNode total = client.rpcBlocking("add_two", Map.of("a", 1, "b", 41));

// Functions (HTTP-handler shape)
FunctionInvokeResult r = client.functions()
    .invokeBlocking("resize", "POST", Map.of("width", 100), null);

client.close(); // or use try-with-resources
```

### Blocking convenience

Every async method has a `*Blocking(…)` variant that calls `.join()` internally:

```java
Session s = client.auth().signIn("alice@example.com", "secret", null);  // throws on error
```

### Kotlin (suspend functions)

Add `kotlinx-coroutines-jdk8` to your project, then use the extension wrappers
from `BasinExtensions.kt`:

```kotlin
import io.basin.sdk.*

val client = BasinClient.builder()
    .url("https://your-project.basin.run")
    .apiKey("your-api-key")
    .build()

coroutineScope {
    val result  = client.table("orders").select("id,total").awaitExecute()
    val session = client.auth.awaitSignIn("a@b.com", "secret", null)
    val bucket  = client.storage.awaitCreateBucket("avatars", public = true)
}

client.close()
```

## Auth

Everything is `Authorization: Bearer <token>`. The server tries JWT verification
first, then falls back to API-key lookup — so `apiKey("k")` accepts either.

After `auth.signIn(…)`, the session's access token takes precedence over the
static key and is **auto-refreshed** 10 seconds before `access_expires_at`.
Refresh tokens rotate; reusing a rotated token surfaces as `E_REVOKED_TOKEN`.

```java
AuthClient auth = client.auth();

// Sign up / sign in
auth.signUp("alice@example.com", "secret", null);
Session s = auth.signIn("alice@example.com", "secret", null);
// Session stored; access token auto-refreshed before expiry.

// Restore an existing session (e.g. from secure storage)
auth.setSession(storedSession);

// Sign out — revokes the refresh token server-side, then clears locally
auth.signOut();  // 404 from older servers tolerated silently

// API keys (JWT-gated)
ApiKeyIssued key = auth.createApiKey("ci-pipeline");
System.out.println(key.secret);   // shown exactly once
auth.deleteApiKey(key.id);

// Magic links
auth.requestMagicLink("alice@example.com");
Session s = auth.consumeMagicLink("token-from-email");
```

## OAuth

```java
// 1. Get the provider authorize URL.
OAuthAuthorizeResult r = auth.getOAuthAuthorizeUrl(
    "google",
    "https://myapp.example.com/auth/callback",
    null);

// 2. Redirect the user's browser to r.redirectUrl.
//    After the OAuth flow, your app receives Basin tokens via the redirect URL.
//    Decode them and restore the session:
System.out.println(r.redirectUrl);  // https://accounts.google.com/…
System.out.println(r.state);        // CSRF state
```

Supported preset providers: `google`, `github`, `apple`, `bitbucket`, `discord`,
`figma`, `gitlab`, `linkedin`, `microsoft` (azure_ad), `notion`, `slack`,
`spotify`, `twitch`, `twitter_x` (twitter). Custom OIDC providers are registered
server-side and passed by name.

## MFA (TOTP and WebAuthn)

```java
// 1. Enroll a TOTP factor (JWT required).
Object enroll = auth.enrollFactor("totp", "My Authenticator");
TotpEnrollResult r = (TotpEnrollResult) enroll;
System.out.println(r.otpauthUri);   // display as QR code

// 2. Confirm enrollment with the first OTP code.
VerifyFactorResult verified = auth.verifyFactor(r.factorId, "123456", null, null);
if (verified.recoveryCodes != null) {
    System.out.println("Save these: " + verified.recoveryCodes);  // shown once
}

// 3. List factors.
List<FactorDescriptor> factors = auth.listFactors();

// 4. Step-up: begin a challenge.
TotpChallengeResult challenge = (TotpChallengeResult) auth.challengeFactor(r.factorId);

// 5. Complete the challenge → aal2 session.
Session aal2 = auth.verifyChallenge(r.factorId, challenge.challengeId, "654321", null);

// 6. Unenroll (requires aal2 token).
auth.unenrollFactor(r.factorId);
```

### WebAuthn

```java
// Enroll
Object enroll = auth.enrollFactor("webauthn", "YubiKey");
WebAuthnEnrollResult we = (WebAuthnEnrollResult) enroll;
// Pass we.creationOptionsJson to navigator.credentials.create() in your JS bridge.

// Verify enrollment
auth.verifyFactor(we.factorId, null, attestationJsonFromBrowser, we.challengeId);

// Step-up
WebAuthnChallengeResult ch = (WebAuthnChallengeResult) auth.challengeFactor(factorId);
// Pass ch.requestOptionsJson to navigator.credentials.get().
Session aal2 = auth.verifyChallenge(factorId, ch.challengeId, null, assertionJsonFromBrowser);
```

## Query builder

The query builder mirrors the Python and Go SDKs. Filters AND together.

```java
// Projection + filters + ordering + pagination
QueryResult result = client.table("orders")
    .select("id,total,status")      // select=id,total,status
    .eq("status", "paid")           // status=eq.paid
    .neq("archived", "true")        // archived=neq.true
    .gte("total", "100")            // total=gte.100
    .in("category", List.of("a","b","c"))  // category=in.(a,b,c)
    .is("deleted_at", "null")       // deleted_at=is.null
    .order("total", false)          // order=total.desc
    .limit(50)
    .offset(0)
    .executeBlocking();

// Typed decode (requires Jackson-annotated POJO)
List<Order> orders = result.into(new ObjectMapper(), Order.class);

// Insert
client.table("orders").insertBlocking(Map.of("total", 12, "status", "new"));

// Batch insert
client.table("orders").insertBlocking(List.of(
    Map.of("total", 10), Map.of("total", 20)));

// Update
client.table("orders").eq("id", 7).updateBlocking(Map.of("status", "paid"));

// Delete
client.table("orders").eq("id", 7).deleteBlocking();

// RPC (catalog UDF)
JsonNode val = client.rpc("my_function", Map.of("arg1", "value")).join();
```

Supported filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is`.

NOT supported (not in Basin's parser): `or=`, `not.`, `like`/`ilike`, embedded
resource selects, Prefer headers.

## Storage

```java
StorageClient storage = client.storage();

// Create a bucket
storage.createBucketBlocking("avatars", true, null, null);

// Object operations
StorageBucketClient bucket = storage.fromBucket("avatars");

// Upload
bucket.uploadBlocking("users/alice.png",
    Files.readAllBytes(Path.of("alice.png")), "image/png");

// Download
DownloadResult dl = bucket.downloadBlocking("users/alice.png");
System.out.println(dl.contentType);

// List
List<StorageObject> objects = bucket.listBlocking("users/", 100, null);

// Delete
bucket.removeBlocking("users/alice.png");

// Bulk delete by prefix
bucket.removeByPrefixesBlocking(List.of("users/", "tmp/"));

// Public URL (bucket must have public=true)
String url = bucket.getPublicUrl("users/alice.png");

// Signed URL (time-boxed download, no JWT needed by caller)
SignedUrl signed = bucket.createSignedUrlBlocking("users/alice.png", 3600);
System.out.println(signed.absoluteUrl);
```

## Realtime (WebSocket)

Receive INSERT / UPDATE / DELETE events as they happen via
`GET /realtime/v1/ws/:project`.

Auth is via the `Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol header.
The `java.net.http.WebSocket` built-in handles this natively — no extra dependency.

```java
RealtimeClient realtime = client.realtime();

// Subscribe with a callback
ChannelHandle handle = realtime.subscribe(
    "orders",
    event -> {
        if (event instanceof RealtimeEvent e) {
            System.out.println(e.op + " " + e.table + " " + e.after);
        } else if (event instanceof RealtimeFrame.Error err) {
            System.err.println("error: " + err.code);
        }
    },
    null,   // filter: optional server-side SQL predicate
    null    // lastEventId: reconnect cursor
).join();

// Reconnect-resume: pass the last seq you processed
handle.unsubscribe();
ChannelHandle h2 = realtime.subscribe("orders", ev -> {}, null, 42L).join();

// Presence
realtime.presenceTrack("room:1", "user-abc", Map.of("name", "Alice")).join();
realtime.presenceHeartbeat("room:1", "user-abc").join();
realtime.presenceUntrack("room:1", "user-abc").join();

// Clean up
handle.unsubscribe();
realtime.close();
```

### Event types

| Java type | When |
|---|---|
| `RealtimeEvent` | INSERT / UPDATE / DELETE; fields: `op`, `table`, `project`, `seq`, `before`, `after` |
| `RealtimeFrame.Error` | Protocol error for a table: `code="lag"` (missed events) |
| `RealtimeFrame.Gap` | Reconnect cursor predates the replay ring; cold re-sync needed |
| `RealtimeFrame.PresenceState` | Snapshot of current members on join |
| `RealtimeFrame.PresenceDiff` | Incremental joins / leaves |
| `RealtimeFrame.PresenceError` | Rejected presence op (e.g. identity mismatch) |

### Reconnect

On unexpected disconnect, the client reconnects with exponential backoff
(0.5 s → 1 s → 2 s … capped at 30 s) and automatically re-issues all active
subscriptions. Pass `lastEventId` to `subscribe()` to request server-side
replay of events missed during the gap.

## Error handling

Every non-2xx response throws `BasinApiException`. Match on `code`, never
on `getMessage()`:

```java
import io.basin.sdk.BasinApiException;

try {
    client.table("orders").deleteBlocking();
} catch (BasinApiException e) {
    switch (e.code) {
        case "E_ENGINE_UNSUPPORTED" ->
            System.out.println("DELETE not supported on this table");
        case "E_UNAUTHENTICATED" ->
            client.auth().refreshSession();
        default -> throw e;
    }
}

// SQLSTATE for SQL-layer errors
try {
    client.table("orders").insertBlocking(Map.of("id", 1));
} catch (BasinApiException e) {
    if ("23505".equals(e.sqlState)) {
        System.out.println("unique constraint violation");
    }
}
```

Transport failures (connection refused, timeout, TLS error) throw
`BasinNetworkException` (a subclass of `BasinException`).

Known stable error codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`. Unknown codes from a newer server pass
through as plain strings.

## Route bindings

| SDK method | Route | Source |
|---|---|---|
| `auth.signUp` | `POST /auth/v1/signup` | `server.rs:302` |
| `auth.signIn` | `POST /auth/v1/signin` | `server.rs:303` |
| `auth.refreshSession` (+ auto-refresh) | `POST /auth/v1/refresh` | `server.rs:304` |
| `auth.signOut` | `POST /auth/v1/signout` | `server.rs:305` |
| `auth.verifyEmail` | `POST /auth/v1/verify-email` | `server.rs:306` |
| `auth.resetPassword` | `POST /auth/v1/reset-password` | `server.rs:307` |
| `auth.requestPasswordReset` | `POST /auth/v1/request-password-reset` | `server.rs:308` |
| `auth.requestMagicLink` | `POST /auth/v1/magic-link` (204) | `server.rs:315` |
| `auth.consumeMagicLink` | `POST /auth/v1/magic-link/consume` | `server.rs:316` |
| `auth.createApiKey` / `listApiKeys` | `POST/GET /auth/v1/api-keys` | `server.rs:322-323` |
| `auth.deleteApiKey` | `DELETE /auth/v1/api-keys/:id` | `server.rs:325-327` |
| `auth.getOAuthAuthorizeUrl` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:329-332` |
| `auth.enrollFactor` | `POST /auth/v1/factors` | `server.rs:339-342` |
| `auth.listFactors` | `GET /auth/v1/factors` | `server.rs:339-342` |
| `auth.verifyFactor` | `POST /auth/v1/factors/:id/verify` | `server.rs:343-345` |
| `auth.challengeFactor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:347-349` |
| `auth.verifyChallenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:351-353` |
| `auth.unenrollFactor` | `DELETE /auth/v1/factors/:id` | `server.rs:354-356` |
| `table(t).execute()` | `GET /rest/v1/:table` | `server.rs:296-300` |
| `table(t).insert` | `POST /rest/v1/:table` (201) | `server.rs:297` |
| `table(t).update` | `PATCH /rest/v1/:table?filters` | `server.rs:298` |
| `table(t).delete` | `DELETE /rest/v1/:table?filters` (may 501) | `server.rs:299` |
| `functions.rpc` | `POST /rest/v1/rpc/:fn_name` | `server.rs:288` |
| `functions.invoke` | `ANY /fn/v1/:name` | `server.rs:290` |
| `storage.createBucket` | `POST /storage/v1/bucket` | `server.rs:373+` |
| `storage.getBucket` / `deleteBucket` | `GET/DELETE /storage/v1/bucket/:name` | `server.rs:377+` |
| `fromBucket(b).upload/download/remove` | `POST/GET/DELETE /storage/v1/object/:bucket/*path` | `server.rs:409+` |
| `fromBucket(b).list` | `POST /storage/v1/object/list/:bucket` | `server.rs:417+` |
| `fromBucket(b).removeByPrefixes` | `DELETE /storage/v1/object/:bucket` | `server.rs:421+` |
| `fromBucket(b).getPublicUrl` | `GET /storage/v1/object/public/:project/:bucket/*path` | `server.rs:384+` |
| `fromBucket(b).createSignedUrl` | `POST /storage/v1/object/sign/upload/:bucket/*path` | `server.rs:397+` |
| `health` | `GET /health` | `server.rs:368` |
| `realtime().subscribe` / presence | `GET /realtime/v1/ws/:project` | `basin-realtime/src/ws.rs` |

## Not bound (gap list)

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — SSE variant.
- **Admin surface** (`/admin/v1/*`) — use `client.request("GET", "/admin/v1/…", null)`.
- **Native Arrow IPC** — `application/vnd.apache.arrow.stream` (server supports
  it; Java SDK sends JSON for now; Arrow Java library can be wired in by callers).
- **CDC stream** (`GET /v1/cdc/:project/stream`) — not yet wrapped.

## Development

```sh
cd sdk/basin-java
./gradlew test    # offline suite via JUnit 5 + com.sun.net.httpserver
```

No running Basin server is required for the test suite.
