# basin_sdk

Dart / Flutter client for [Basin](../../README.md)'s HTTP surfaces: REST data
API, auth, functions, object storage, and realtime WebSocket.

Works on **Flutter** (iOS, Android, Web, desktop) and pure **Dart** (CLI,
server) with a single package — `package:http` and `package:web_socket_channel`
abstract the platform-level differences.

## Install

```yaml
# pubspec.yaml
dependencies:
  basin_sdk: ^0.1.0
```

```sh
dart pub get
# or for Flutter:
flutter pub add basin_sdk
```

## Quickstart

```dart
import 'package:basin_sdk/basin_sdk.dart';

void main() async {
  final basin = BasinClient.create(
    url: 'https://your-project.basin.run',
    key: 'your-api-key',
    projectId: '01J...',   // optional when key is a JWT with project_id claim
  );

  // Health check
  print(await basin.health()); // 'ok'

  // Query builder
  final result = await basin
      .table('orders')
      .select('id,total,status')
      .eq('status', 'paid')
      .gte('total', 100)
      .order('total', ascending: false)
      .limit(50)
      .get();

  for (final row in result.rows) {
    print(row);
  }

  // Keyset pagination
  final page = await basin.table('orders').limit(100).page();
  final next = await basin.table('orders').cursor(page.nextCursor!).limit(100).page();

  // Writes
  await basin.table('orders').insert({'total': 12, 'status': 'new'});
  await basin.table('orders').eq('id', 7).update({'status': 'paid'});
  await basin.table('orders').eq('id', 7).delete(); // may throw E_ENGINE_UNSUPPORTED

  // RPC / functions
  final total = await basin.rpc('add', {'a': 40, 'b': 2});
  final res = await basin.functions.invoke('resize', body: {'width': 100});

  basin.close();
}
```

## Auth (email / password, per-project)

```dart
final basin = BasinClient.create(
  url: 'https://your-project.basin.run',
  projectId: '01J...',
);

// Sign up
await basin.auth.signUp(email: 'alice@example.com', password: 'secret');

// Sign in — session stored; access token auto-refreshes 10 s before expiry
final session = await basin.auth.signIn(
  email: 'alice@example.com',
  password: 'secret',
);

// API keys (JWT-gated)
final key = await basin.auth.createApiKey('ci-pipeline');
print(key.secret);   // shown exactly once
await basin.auth.deleteApiKey(key.id);

// Magic links
await basin.auth.requestMagicLink('alice@example.com');
final magicSession = await basin.auth.consumeMagicLink('token-from-email');

// Sign out — revokes the refresh token server-side, clears local session
await basin.auth.signOut();
```

### Token persistence (Flutter)

`BasinClient` stores the session in memory only. For mobile apps that must
survive process restarts, persist the session yourself:

```dart
import 'package:shared_preferences/shared_preferences.dart';
import 'dart:convert';

// Save after signIn / consumeMagicLink / verifyChallenge:
Future<void> saveSession(Session session) async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString('basin_session', jsonEncode(session.toJson()));
}

// Restore on app start:
Future<void> restoreSession(BasinClient client) async {
  final prefs = await SharedPreferences.getInstance();
  final raw = prefs.getString('basin_session');
  if (raw != null) {
    client.auth.setSession(Session.fromJson(jsonDecode(raw) as Map<String, dynamic>));
  }
}
```

## Storage

```dart
// Create a bucket
await basin.storage.createBucket('avatars', public: true);

// Upload / download
final bucket = basin.storage.fromBucket('avatars');
await bucket.upload(
  'users/alice.png',
  await File('alice.png').readAsBytes(),
  contentType: 'image/png',
);
final download = await bucket.download('users/alice.png');
print(download.contentType); // 'image/png'

// List objects
final objects = await bucket.list(prefix: 'users/');

// Signed URL (time-boxed download, no JWT needed by caller)
final signed = await bucket.createSignedUrl('users/alice.png', expiresIn: 3600);
print(signed.absoluteUrl); // https://...

// Public URL (bucket must have public: true)
final url = bucket.getPublicUrl('users/alice.png');
```

## Realtime (WebSocket)

Receive INSERT / UPDATE / DELETE events as they happen via
`GET /realtime/v1/ws/:project`.

### Stream-based (recommended)

```dart
import 'package:basin_sdk/basin_sdk.dart';

Future<void> main() async {
  final basin = BasinClient.create(
    url: 'https://your-project.basin.run',
    key: 'your-api-key',
    projectId: '01J...',
  );

  await for (final frame in basin.realtime.listen('orders')) {
    if (frame is RealtimeEvent) {
      print('${frame.op} ${frame.table} seq=${frame.seq}');
      print('after: ${frame.after}');
    } else if (frame is RealtimeGapFrame) {
      print('gap — cold re-sync needed (oldestInRing: ${frame.oldestInRing})');
    } else if (frame is RealtimeErrorFrame) {
      print('error: ${frame.code}');
    }
  }
}
```

### With filter

```dart
// Only events where NEW.status = 'paid'
await for (final frame in basin.realtime.listen(
  'orders',
  filter: "NEW.status = 'paid'",
)) {
  if (frame is RealtimeEvent) print(frame.after);
}
```

### Reconnect resume

```dart
int lastSeq = 0;

await for (final frame in basin.realtime.listen(
  'orders',
  lastEventId: lastSeq,  // server replays from this seq
)) {
  if (frame is RealtimeEvent) {
    lastSeq = frame.seq;
    process(frame);
  }
}
```

### Callback API

```dart
final handle = await basin.realtime.subscribe(
  'orders',
  (frame) {
    if (frame is RealtimeEvent) print(frame.op);
  },
);
// Later:
await handle.unsubscribe();
```

### Presence (Phoenix Channels shape)

```dart
// Track presence
await basin.realtime.presenceTrack(
  'room:1',
  'user-alice',
  metadata: {'name': 'Alice'},
);

// Listen for presence frames
await for (final frame in basin.realtime.listenPresence('room:1')) {
  if (frame is PresenceStateFrame) {
    print('snapshot: ${frame.presences.map((p) => p.clientId)}');
  } else if (frame is PresenceDiffFrame) {
    print('joins: ${frame.joins}, leaves: ${frame.leaves}');
  }
}

// Heartbeat (refresh TTL)
await basin.realtime.presenceHeartbeat('room:1', 'user-alice');

// Untrack
await basin.realtime.presenceUntrack('room:1', 'user-alice');
```

### Reconnect behaviour

On unexpected disconnect, `RealtimeClient` reconnects with exponential
backoff (0.5 s, 1 s, 2 s … capped at 30 s) and automatically re-issues all
active subscriptions. Pass `lastEventId` to `listen()` to request server-side
replay of events missed during the gap.

### Flutter Web vs. native

`package:web_socket_channel` is cross-platform:

- **Flutter Web** — uses `dart:html` WebSocket under the hood.
- **Flutter native / Dart CLI** — uses `dart:io` WebSocket.

Both support the `Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol
form used by this SDK. This is the correct choice for browser compatibility
(browsers cannot set arbitrary HTTP headers on WebSocket upgrade requests).

## OAuth

```dart
// Get the authorize URL — redirect the user's browser here.
final result = await basin.auth.getOAuthAuthorizeUrl(
  'google',
  redirectTo: 'https://myapp.com/auth/callback',
);
// Redirect to result.redirectUrl in your app's browser/webview.

// After the OAuth flow completes, restore the session from the tokens
// your redirect_to URL receives:
basin.auth.setSession(Session(
  accessToken: '...',
  refreshToken: '...',
  accessExpiresAt: '...',
  refreshExpiresAt: '...',
));
```

Supported preset providers: google, github, apple, bitbucket, discord,
figma, gitlab, linkedin, microsoft (azure_ad), notion, slack, spotify,
twitch, twitter_x.

## MFA (TOTP and WebAuthn)

### TOTP

```dart
// 1. Enroll
final enroll = await basin.auth.enrollFactor('totp',
    friendlyName: 'My Authenticator') as TotpEnrollResult;
// Display enroll.otpauthUri as QR code; user scans with authenticator app.

// 2. Verify enrollment
final verify = await basin.auth.verifyFactor(
  enroll.factorId,
  code: '123456',  // from the authenticator app
);
if (verify.recoveryCodes != null) {
  // Save these — shown exactly once.
  print(verify.recoveryCodes);
}

// 3. Step-up: challenge + verify → aal2 session
final challenge = await basin.auth.challengeFactor(enroll.factorId)
    as TotpChallengeResult;
final aal2Session = await basin.auth.verifyChallenge(
  enroll.factorId,
  challenge.challengeId,
  code: '654321',
);

// 4. Unenroll (requires aal2 token)
await basin.auth.unenrollFactor(enroll.factorId);
```

### WebAuthn

```dart
final enroll = await basin.auth.enrollFactor('webauthn',
    friendlyName: 'YubiKey') as WebAuthnEnrollResult;
// Pass enroll.creationOptionsJson to navigator.credentials.create() in JS.
await basin.auth.verifyFactor(
  enroll.factorId,
  attestation: '<json from navigator.credentials.create()>',
  challengeId: enroll.challengeId,
);
```

## Error handling

Every non-2xx response throws `BasinApiError(code, message, status)`,
mirroring the server envelope `{"code": "E_...", "message": "..."}`.
Match on `code`, never on `message`:

```dart
import 'package:basin_sdk/basin_sdk.dart';

try {
  await basin.table('orders').eq('id', 7).delete();
} on BasinApiError catch (e) {
  switch (e.code) {
    case 'E_ENGINE_UNSUPPORTED':
      print('DELETE not supported on this table');
    case 'E_UNAUTHENTICATED':
      await basin.auth.refreshSession();
    default:
      rethrow;
  }
} on BasinNetworkError catch (e) {
  print('Network failure: ${e.message}');
}
```

Known codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`.

`BasinNetworkError` is thrown when the transport fails before a server
response arrives (connection refused, timeout, etc.).

## Auth model

Everything is `Authorization: Bearer <token>`. The server tries JWT
verification first, then falls back to API-key lookup — so `BasinClient.create`
accepts either. After `auth.signIn(...)`, the session's access token takes
precedence over the static key and is **auto-refreshed** 10 seconds before
`accessExpiresAt`. Refresh tokens rotate; reusing a rotated token surfaces as
`E_REVOKED_TOKEN`.

`signOut()` calls `POST /auth/v1/signout` to revoke the refresh token
server-side, then clears the local session. The local session is always
cleared even if the server call fails.

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
| `auth.getOAuthAuthorizeUrl` | `GET /auth/v1/oauth/:provider/authorize` | `server.rs:277-279` |
| `auth.enrollFactor` | `POST /auth/v1/factors` (201) | `server.rs:286-287` |
| `auth.listFactors` | `GET /auth/v1/factors` | `server.rs:286-287` |
| `auth.verifyFactor` | `POST /auth/v1/factors/:id/verify` | `server.rs:290-291` |
| `auth.challengeFactor` | `POST /auth/v1/factors/:id/challenge` | `server.rs:294-296` |
| `auth.verifyChallenge` | `POST /auth/v1/factors/:id/challenge/verify` | `server.rs:298-300` |
| `auth.unenrollFactor` | `DELETE /auth/v1/factors/:id` | `server.rs:302-303` |
| `table(t).get()` (select/eq/.../order/limit/offset/cursor) | `GET /rest/v1/:table` | `server.rs:243-249`, `parser.rs` |
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
| `storage.fromBucket(b).getPublicUrl` | `GET /storage/v1/object/public/:project/:bucket/*path` | `server.rs:384` |
| `storage.fromBucket(b).createSignedUrl` | `POST /storage/v1/object/sign/upload/:bucket/*path` | `server.rs:397`, `storage_sign.rs` |
| `health` | `GET /health` | `server.rs:368` |
| `realtime.listen` / `subscribe` / presence | `GET /realtime/v1/ws/:project` | `basin-realtime/src/ws.rs:191` |

## Not bound yet

- **SSE realtime** (`GET /realtime/v1/sse/:project/:table`) — use
  `client.request(...)` as an escape hatch.
- **Admin surface** (`/admin/v1/*`) — operator-grade; use
  `client.request(...)`.
- **CDC stream** (`GET /v1/cdc/:project/stream`) — use `client.request(...)`.
- **Arrow IPC transport** — the server accepts
  `Accept: application/vnd.apache.arrow.stream` on any `GET /rest/v1/:table`
  request and returns a native Arrow IPC stream (see
  `crates/basin-rest/src/arrow_ipc.rs`). A `toArrow()` method on
  `QueryBuilder` is not yet implemented because no general-purpose Arrow IPC
  decoder exists for Dart. The only pub.dev package that decodes Arrow IPC
  (`meshagent_dart_arrow`) is a vendor-specific library tied to the Meshagent
  agent platform and is not suitable as a general dependency. When an
  official Apache Arrow Dart package ships, a `toArrow()` method should be
  added to `QueryBuilder` following the pattern in `sdk/basin-js/src/query.ts`
  and `sdk/basin-python/basin/query.py`.

## Development

```sh
cd sdk/basin-dart
dart pub get
dart test test/          # offline suite (no server required)
```

There is no live integration test runner in this package. For end-to-end
testing, point `BasinClient.create(url: ...)` at a running Basin instance.
