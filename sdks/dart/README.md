# basin_sdk

Official Dart / Flutter client for [Basin](https://basin.run) — the
open-source Postgres-compatible data platform.

Speaks directly to a deployed
[`basin-engine`](https://github.com/vul-os/basin) (Apache-2.0 Rust core):
auth, PostgREST-style table queries, object storage, SQL/Wasm functions, and
realtime WebSocket subscriptions. Works on **Flutter** (iOS, Android, Web,
desktop) and pure **Dart** (CLI, server) from a single package —
`package:http` and `package:web_socket_channel` handle the platform
differences.

basin_sdk is part of the Basin SDK family alongside
[basin-js](https://github.com/bas-in/basin-js) (TypeScript) and
[basin-py](https://github.com/bas-in/basin-py) (Python). All SDKs bind the
same engine routes (pgwire + REST) so behaviour is consistent across
languages.

## Install

```sh
dart pub add basin_sdk
# Flutter:
flutter pub add basin_sdk
```

Or add manually to `pubspec.yaml`:

```yaml
dependencies:
  basin_sdk: ^0.1.0
```

## Quickstart

```dart
import 'package:basin_sdk/basin_sdk.dart';

void main() async {
  final basin = BasinClient.create(
    url: 'https://your-project.basin.run',
    key: 'your-api-key',         // JWT or raw API key
    projectId: '01J...',         // optional when key is a JWT with project_id
  );

  // Health check
  print(await basin.health()); // 'ok'

  // Query — GET /rest/v1/orders
  final result = await basin
      .from('orders')
      .select('id,total,status')
      .eq('status', 'paid')
      .gte('total', 100)
      .order('total', ascending: false)
      .limit(50)
      .get();

  for (final row in result.rows) {
    print(row);
  }

  basin.close();
}
```

`BasinClient.from(table)` and `BasinClient.table(table)` are aliases; the
`from` form matches the JS/Python SDK style.

## Auth

### Sign up / sign in

```dart
final basin = BasinClient.create(
  url: 'https://your-project.basin.run',
  projectId: '01J...',
);

await basin.auth.signUp(email: 'alice@example.com', password: 'secret');

// signIn stores the session; access token is auto-refreshed 10 s before expiry.
final session = await basin.auth.signIn(
  email: 'alice@example.com',
  password: 'secret',
);

// Sign out — revokes refresh token server-side, clears local session.
await basin.auth.signOut();
```

### Password reset

```dart
await basin.auth.requestPasswordReset(email: 'alice@example.com');
// User receives an email; extract the token and call:
await basin.auth.resetPassword(token: '<token>', newPassword: 'newpass');
```

### Email verification

```dart
await basin.auth.verifyEmail(token: '<token-from-email>');
```

### Magic link

```dart
await basin.auth.requestMagicLink('alice@example.com'); // 204 always
final session = await basin.auth.consumeMagicLink('<token-from-email>');
```

### OAuth

```dart
final result = await basin.auth.getOAuthAuthorizeUrl(
  'google',
  redirectTo: 'https://myapp.com/auth/callback',
);
// Redirect the user's browser to result.redirectUrl.
// After the callback, restore the session from the returned tokens:
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

### API keys

```dart
// Issue a named key (JWT-gated; secret shown exactly once).
final issued = await basin.auth.createApiKey('ci-pipeline');
print(issued.secret);

final keys = await basin.auth.listApiKeys();
await basin.auth.deleteApiKey(issued.id);
```

### MFA — TOTP

```dart
// 1. Enroll
final enroll = await basin.auth.enrollFactor('totp',
    friendlyName: 'My Authenticator') as TotpEnrollResult;
// Show enroll.otpauthUri as a QR code; the user scans with an authenticator app.

// 2. Confirm enrollment
final verify = await basin.auth.verifyFactor(
  enroll.factorId,
  code: '123456',
);
if (verify.recoveryCodes != null) print(verify.recoveryCodes); // save these

// 3. Step-up challenge → aal2 session
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

### MFA — WebAuthn

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

### Session persistence (Flutter)

`BasinClient` keeps the session in memory only. For mobile apps that need to
survive process restarts, persist it yourself:

```dart
import 'package:shared_preferences/shared_preferences.dart';
import 'dart:convert';

Future<void> saveSession(Session s) async {
  final prefs = await SharedPreferences.getInstance();
  await prefs.setString('basin_session', jsonEncode(s.toJson()));
}

Future<void> restoreSession(BasinClient client) async {
  final prefs = await SharedPreferences.getInstance();
  final raw = prefs.getString('basin_session');
  if (raw != null) {
    client.auth.setSession(
      Session.fromJson(jsonDecode(raw) as Map<String, dynamic>));
  }
}
```

## Query builder

```dart
// Filter, project, order, limit
final result = await basin
    .from('products')
    .select('id,name,price')
    .eq('active', true)
    .gte('price', 10)
    .lt('price', 500)
    .order('price')           // ascending by default
    .limit(20)
    .get();

// .rows() is shorthand for .get() and discarding the cursor.
final rows = await basin.from('products').rows();

// in-list filter
await basin.from('orders').inFilter('status', ['paid', 'shipped']).rows();

// NULL check
await basin.from('events').is_('deleted_at', 'null').rows();

// Insert / update / delete
await basin.from('orders').insert({'total': 42, 'status': 'new'});
await basin.from('orders').eq('id', 7).update({'status': 'paid'});
await basin.from('orders').eq('id', 7).delete();
```

### Keyset cursor pagination

The engine returns `{rows, next_cursor}` for requests that include `limit`.
Walk pages with `.page()` and `.cursor()`:

```dart
var page = await basin.from('events').select('*').limit(100).page();
while (page.rows.isNotEmpty) {
  process(page.rows);
  if (page.nextCursor == null) break;
  page = await basin
      .from('events')
      .select('*')
      .limit(100)
      .cursor(page.nextCursor!)
      .page();
}
```

### NDJSON streaming

For large result sets the engine supports `?stream=true`, returning rows
as newline-delimited JSON. `streamCollect()` reads the stream lazily and
returns a `QueryResult` with all rows and the final cursor:

```dart
final result = await basin.from('events').select('*').streamCollect();
for (final row in result.rows) {
  process(row);
}
```

The engine auto-promotes responses above ~1 MiB or 10 000 rows to NDJSON
even without the flag; `.get()` handles both response shapes transparently.

## Storage

```dart
// Create a bucket
await basin.storage.createBucket('avatars', public: true);

// Upload bytes
final bucket = basin.storage.fromBucket('avatars');
await bucket.upload(
  'users/alice.png',
  await File('alice.png').readAsBytes(),
  contentType: 'image/png',
);

// Download
final download = await bucket.download('users/alice.png');
print(download.contentType); // 'image/png'

// List objects
final objects = await bucket.list(prefix: 'users/');

// Signed URL (time-boxed, no auth needed by the recipient)
final signed = await bucket.createSignedUrl('users/alice.png', expiresIn: 3600);
print(signed.absoluteUrl);

// Public URL (bucket must be public)
final url = bucket.getPublicUrl('users/alice.png');

// Remove
await bucket.remove('users/alice.png');

// Bulk remove by prefix
await bucket.removeByPrefixes(['users/']);
```

## Functions

### HTTP-handler functions (`/fn/v1/:name`)

```dart
// POST by default; any method supported
final result = await basin.functions.invoke(
  'resize',
  body: {'width': 100, 'height': 100},
);
print(result.status); // function's own HTTP status
print(result.data);   // decoded response body
```

### SQL / Wasm UDFs (`/rest/v1/rpc/:fn_name`)

```dart
// Scalar UDF: add(a int, b int) RETURNS int
final total = await basin.rpc('add', {'a': 40, 'b': 2});
// total == 42

// RETURNS TABLE UDF
final rows = await basin.rpc('active_users', {'min_logins': 5}) as List;
```

The active session JWT is forwarded automatically on both paths.

## Realtime

All realtime goes through `GET /realtime/v1/ws/:project` (WebSocket).
`RealtimeClient` reconnects automatically with exponential backoff (0.5 s,
1 s, 2 s … capped at 30 s) and re-issues active subscriptions on reconnect.

### Stream-based (recommended)

```dart
await for (final frame in basin.realtime.listen('orders')) {
  if (frame is RealtimeEvent) {
    print('${frame.op.name} on ${frame.table}, seq=${frame.seq}');
    print('row: ${frame.after}');
  } else if (frame is RealtimeGapFrame) {
    print('gap detected — consider a cold re-sync');
  } else if (frame is RealtimeErrorFrame) {
    print('server error: ${frame.code}');
  }
}
```

### Server-side change filter

```dart
await for (final frame in basin.realtime.listen(
  'orders',
  filter: "NEW.status = 'paid'",
)) {
  if (frame is RealtimeEvent) print(frame.after);
}
```

### Reconnect with replay

```dart
int lastSeq = 0;

await for (final frame in basin.realtime.listen(
  'orders',
  lastEventId: lastSeq,   // server replays events missed since this seq
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

// Stop listening:
await handle.unsubscribe();
```

### Presence

```dart
// Track this client in a named channel
await basin.realtime.presenceTrack(
  'room:lobby',
  'user-alice',
  metadata: {'name': 'Alice', 'avatar': 'https://...'},
);

// Listen for presence state and diffs
await for (final frame in basin.realtime.listenPresence('room:lobby')) {
  if (frame is PresenceStateFrame) {
    print('online: ${frame.presences.map((p) => p.clientId)}');
  } else if (frame is PresenceDiffFrame) {
    print('joined: ${frame.joins}, left: ${frame.leaves}');
  }
}

// Refresh TTL (call every ~30 s)
await basin.realtime.presenceHeartbeat('room:lobby', 'user-alice');

// Leave
await basin.realtime.presenceUntrack('room:lobby', 'user-alice');
```

### Flutter Web vs. native

`package:web_socket_channel` is cross-platform. On Flutter Web the
transport uses `dart:html` WebSocket; on native (iOS, Android, desktop,
server) it uses `dart:io` WebSocket. Auth is passed via the
`Sec-WebSocket-Protocol: basin-v1, <token>` subprotocol — the correct
approach for browsers, which cannot set arbitrary HTTP headers on WebSocket
upgrade requests.

## Error handling

Non-2xx responses from the engine throw `BasinApiError`. Match on `code`,
not `message` — `message` is human-readable and not a stable contract.

```dart
import 'package:basin_sdk/basin_sdk.dart';

try {
  await basin.from('orders').eq('id', 7).delete();
} on BasinApiError catch (e) {
  switch (e.code) {
    case 'E_ENGINE_UNSUPPORTED':
      print('DELETE not yet supported on this table');
    case 'E_UNAUTHENTICATED':
      await basin.auth.refreshSession();
    case 'E_RATE_LIMITED':
      print('rate limited — retry after a moment');
    default:
      rethrow;
  }
} on BasinNetworkError catch (e) {
  print('transport failure: ${e.message}');
}
```

When a SQL-layer error occurs the `sqlstate` field carries the 5-character
Postgres SQLSTATE code (e.g. `23505` for a unique-key violation):

```dart
} on BasinApiError catch (e) {
  if (e.sqlstate == '23505') {
    print('duplicate key — record already exists');
  }
}
```

Known stable codes: `E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`,
`E_INVALID_REQUEST`, `E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`,
`E_EMAIL_DISABLED`, `E_REVOKED_TOKEN`.

`BasinNetworkError` is thrown when the transport fails before a server
response arrives (connection refused, timeout, etc.).

## Architecture

[Basin Cloud](https://basin.run) is the control plane — dashboard, billing,
project management, and where you mint the anon-key JWT. Once you have a
URL + key, the cloud is off the data path: every SDK call hits `basin-engine`
directly. The engine is open source and self-hostable:

```sh
cargo run -p basin-server   # default pgwire :5433, REST :5434
```

Point `BasinClient.create(url: 'http://localhost:5434', ...)` at a local
engine and the SDK behaves identically to the managed cloud.

**Direct pgwire connections** (psql, DBeaver, migration tools):

```
# JWT / session token:
psql "postgres://<access_token>@<engine-host>:5433/basin"

# API key:
psql "postgres://{tenant_id}_{hex}:<api_key>@<engine-host>:5433/basin"
```

After connecting, `auth.uid()` / `auth.role()` / `auth.jwt()` SQL functions
work identically to the REST path — the same RLS policies apply.

## Row Level Security

After `auth.signIn(...)` the query builder attaches the JWT as
`Authorization: Bearer <token>` automatically. Enable RLS and a policy in
schema setup:

```sql
ALTER TABLE items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "users see own rows" ON items
  FOR ALL USING (owner_id = auth.uid());
```

Then `basin.from('items').rows()` returns only the signed-in user's rows —
no extra client code needed.

## Testing / injection

Both `http.Client` and the WebSocket factory are injectable for offline
testing:

```dart
import 'package:http/testing.dart';

final client = MockClient((request) async {
  return http.Response('[{"id":1}]', 200,
      headers: {'content-type': 'application/json'});
});

final basin = BasinClient.create(
  url: 'http://localhost',
  key: 'test-key',
  httpClient: client,
);
```

## Development

```sh
dart pub get
dart analyze
dart test test/
```

There is no live integration test runner in this package. For end-to-end
testing point `BasinClient.create(url: ...)` at a running Basin engine.

## License

MIT — see [LICENSE](./LICENSE).
