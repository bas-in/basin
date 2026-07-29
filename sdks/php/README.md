# basin-php

Official PHP client for [Basin](https://basin.run) — a Postgres-protocol
database engine with built-in auth, object storage, serverless functions, and
realtime change subscriptions.

Talks **directly** to a deployed
[`basin-engine`](https://github.com/bas-in/basin) (the open-source Rust core,
Apache-2.0) over REST + WebSocket — auth, PostgREST-style table queries,
object storage, RPC functions, and live table events. Basin Cloud is the
control plane (dashboard, billing, anon-key minting); once you have a URL and
key, every SDK call lands on the engine directly.

Part of the Basin SDK family alongside
[basin-js](https://github.com/bas-in/basin-js),
[basin-py](https://github.com/bas-in/basin-py), and others.

**Requirements:** PHP 8.1+, [Guzzle](https://docs.guzzlephp.org/) 7.x
(auto-installed).

## Install

```bash
composer require basin/basin-php
```

For realtime WebSocket support, the SDK delegates to `textalk/websocket`
(synchronous — suitable for CLI workers and queue processors) or `ratchet/pawl`
(async, ReactPHP / event-loop environments):

```bash
# Synchronous (default, CLI workers)
composer require textalk/websocket

# Async (ReactPHP / event-loop)
composer require ratchet/pawl
```

## Quickstart

```php
<?php

use Basin\Client;

$basin = new Client([
    'url'        => 'https://api.basin.run',
    'token'      => 'basin_01J..._<base64>',  // anon key or JWT
    'project_id' => '01JXXXXXXXXXXXXXXXXXXXXXXXXX',
]);

// Query
$result = $basin->from('orders')
    ->select('id,total,status')
    ->eq('status', 'paid')
    ->gte('total', 100)
    ->order('created_at', ascending: false)
    ->limit(50)
    ->execute();

foreach ($result->rows as $row) {
    echo $row['id'] . ': $' . $row['total'] . PHP_EOL;
}

// Cursor pagination
if ($result->nextCursor !== null) {
    $nextPage = $basin->from('orders')
        ->cursor($result->nextCursor)
        ->execute();
}
```

Anon keys are minted at
`https://basin.run/app/project/<ref>/api-keys`. For self-hosted engines, mint
a key via `POST /auth/v1/api-keys` (requires an admin JWT) and point `url` at
the engine's base URL (default: `http://localhost:5434`).

## Auth

```php
// Sign up
$signup = $basin->auth->signUp('user@example.com', 'password123');
echo $signup->userId;  // usr_01J...

// Sign in — stores session, auto-refreshes tokens before expiry
$session = $basin->auth->signIn('user@example.com', 'password123');
echo $session->accessToken;

// Sign out — revokes refresh token server-side, clears local session
$basin->auth->signOut();

// Magic link
$basin->auth->requestMagicLink('user@example.com');
$session = $basin->auth->consumeMagicLink($tokenFromEmail);

// OAuth — redirect the browser to the returned URL
$oauth = $basin->auth->getOAuthAuthorizeUrl('google', redirectTo: 'https://yourapp.com/callback');
header('Location: ' . $oauth->redirectUrl);

// Password reset
$basin->auth->requestPasswordReset('user@example.com');
$basin->auth->resetPassword($tokenFromEmail, 'new-password');

// Email verification
$basin->auth->verifyEmail($tokenFromEmail);

// API keys
$issued  = $basin->auth->createApiKey('ci-deploy');
echo $issued->secret;  // shown exactly once
$keys    = $basin->auth->listApiKeys();
$basin->auth->deleteApiKey($keys[0]->id);

// MFA — TOTP enrollment
$enroll = $basin->auth->enrollFactor('totp', friendlyName: 'Authenticator App');
// Display $enroll->otpauthUri as a QR code, then confirm:
$basin->auth->verifyFactor($enroll->factorId, code: '123456');

// MFA — step-up challenge
$challenge   = $basin->auth->challengeFactor($factorId);
$aal2Session = $basin->auth->verifyChallenge($factorId, $challenge->challengeId, code: '654321');

// MFA — WebAuthn enrollment
$enroll = $basin->auth->enrollFactor('webauthn', friendlyName: 'Security Key');
// Pass $enroll->registrationOptions to the browser WebAuthn API, then:
$basin->auth->verifyFactor($enroll->factorId, attestation: $attestationJson);

// Unenroll (requires aal2 session)
$basin->auth->unenrollFactor($factorId);
```

The session token auto-refreshes 10 seconds before expiry; every query and
storage call automatically uses the freshest token. Session persistence
(across HTTP requests) is your responsibility — store the `Session` object and
restore it with `$basin->auth->setSession($session)`.

## Query Builder

All filter methods chain and return `$this`. Call `execute()` to dispatch.

```php
$result = $basin->from('products')
    ->select(['id', 'name', 'price'])       // array or CSV string
    ->eq('category', 'electronics')         // exact match
    ->neq('status', 'discontinued')         // not equal
    ->gte('price', 10.00)                   // >=
    ->lte('price', 999.99)                  // <=
    ->gt('stock', 0)                        // >
    ->lt('weight', 5.0)                     // <
    ->in('brand', ['Apple', 'Samsung'])     // IN list
    ->is('deleted_at', 'null')              // IS NULL / IS NOTNULL
    ->order('price', ascending: true)       // sort (repeatable for multi-column)
    ->limit(20)
    ->offset(40)
    ->execute();                            // returns QueryResult

$rows = $result->rows;        // array<int, array<string, mixed>>
$next = $result->nextCursor;  // string|null — pass to ->cursor()

// Shorthand — rows only
$rows = $basin->from('orders')->eq('status', 'paid')->get();

// NDJSON streaming — low memory, any result size
// stream() returns a Generator; iterate it directly
$gen = $basin->from('events')->select('id,type,payload')->stream();
foreach ($gen as $row) {
    process($row);
}
// The generator's return value is the next_cursor string (if any)
$nextCursor = $gen->getReturn();

// Writes
$basin->from('orders')->insert(['total' => 50, 'status' => 'new']);
$basin->from('orders')->insert([
    ['total' => 10, 'status' => 'new'],
    ['total' => 20, 'status' => 'new'],
]);
$basin->from('orders')->eq('id', 1)->update(['status' => 'paid']);
$basin->from('orders')->eq('id', 1)->delete();
```

The engine returns `{rows, next_cursor}` for any `GET` with `limit` or
`cursor`. For unbounded queries the response is a plain JSON array unless it
exceeds ~1 MiB or 10 000 rows, in which case the engine auto-promotes to
NDJSON — use `stream()` when you expect large result sets.

## Storage

```php
// Bucket management
$basin->storage()->createBucket('avatars', public: true);
$meta = $basin->storage()->getBucket('avatars');
$basin->storage()->deleteBucket('old-bucket');

// Object operations
$bucket = $basin->storage()->fromBucket('avatars');

$obj      = $bucket->upload('user/photo.jpg', file_get_contents('/tmp/photo.jpg'), 'image/jpeg');
$download = $bucket->download('user/photo.jpg');
file_put_contents('/tmp/out.jpg', $download->data);

$bucket->remove('user/old.jpg');
$bucket->removeByPrefixes(['temp/', 'drafts/']);

// List objects
$objects = $bucket->list(prefix: 'user/', limit: 100);

// Public URL (bucket must have public: true)
$url = $bucket->getPublicUrl('user/photo.jpg');

// Signed download URL (TTL up to 7 days)
$signed = $bucket->createSignedUrl('user/photo.jpg', expiresIn: 3600);
echo $signed->absoluteUrl;
echo $signed->expiresAt;
```

## Functions

```php
// POST /rest/v1/rpc/:fn — SQL or Wasm UDFs registered in the catalog
$total = $basin->functions->rpc('calculate_total', ['order_id' => 42]);
$basin->rpc('my_function', ['arg' => 'value']);  // convenience alias on Client

// ANY /fn/v1/:name — HTTP-handler functions, proxied verbatim
$result = $basin->functions->invoke('send-email', body: ['to' => 'user@example.com']);
echo $result->status;  // HTTP status from the function
echo $result->data;    // decoded JSON or raw string body
```

## Realtime

Requires `composer require textalk/websocket` (or `ratchet/pawl` for async).

```php
$rt = $basin->realtime();

// Subscribe with a callback — called for every incoming event frame
$rt->subscribe('orders', function (array $event): void {
    // $event['type'] — "event" | "error" | "gap" | "subscribed" | ...
    // $event['op']   — "INSERT" | "UPDATE" | "DELETE"
    // $event['after'] — new row as associative array
    echo $event['op'] . ': ' . json_encode($event['after']) . PHP_EOL;
});

// Server-side filter — only events matching the predicate are sent
$rt->subscribe('orders', $callback, filter: "NEW.status = 'paid'");

// Resume from a known sequence number to replay missed events
$rt->subscribe('orders', $callback, lastEventId: $lastSeenSeq);

// Block and dispatch incoming frames until limit or timeout
$rt->listen(maxMessages: 100, timeoutMs: 5000);

// Presence
$rt->presenceTrack('room:lobby', 'client-id-1', metadata: ['name' => 'Alice']);
$rt->presenceHeartbeat('room:lobby', 'client-id-1');  // keep-alive, every ~30 s
$rt->presenceUntrack('room:lobby', 'client-id-1');

// Tear down
$rt->unsubscribe('orders');
$rt->disconnect();
```

The WebSocket transport reconnects automatically on drop with exponential
backoff (0.5 s base, 30 s ceiling). All active subscriptions are reinstated
after reconnect.

Frame `$event['type']` values: `event`, `error`, `gap`, `subscribed`,
`unsubscribed`, `presence_state`, `presence_diff`, `presenceerror`.

## Error Handling

```php
use Basin\Exception\BasinApiException;
use Basin\Exception\BasinNetworkException;

try {
    $basin->from('orders')->execute();
} catch (BasinApiException $e) {
    // Match on the stable error code, not the human-readable message
    match ($e->getErrorCode()) {
        'E_UNAUTHENTICATED' => redirect('/login'),
        'E_NOT_FOUND'       => abort(404),
        'E_RATE_LIMITED'    => sleep(1),
        default             => throw $e,
    };
    // $e->getHttpStatus()  — HTTP status code (0 for client-synthesised errors)
    // $e->getSqlState()    — 5-char Postgres SQLSTATE, e.g. "23505"; null if not a SQL error
    // $e->getMessage()     — human-readable detail (not stable; do not match on it)
} catch (BasinNetworkException $e) {
    // Transport failure — connection refused, timeout, DNS, TLS
    error_log('Basin connection failed: ' . $e->getMessage());
}
```

Stable error codes (from `basin-rest/src/errors.rs`):
`E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`, `E_INVALID_REQUEST`,
`E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`, `E_EMAIL_DISABLED`,
`E_REVOKED_TOKEN`.

Unknown codes from a newer server are surfaced as plain strings so an older
SDK version never silently drops information.

## Laravel

The package ships a service provider and facade that Laravel auto-discovers.

### Environment variables

```dotenv
BASIN_URL=https://api.basin.run
BASIN_TOKEN=basin_01J..._<base64>
BASIN_PROJECT_ID=01JXXXXXXXXXXXXXXXXXXXXXXXXX
BASIN_TIMEOUT=30
```

### Publish config (optional)

```bash
php artisan vendor:publish --provider="Basin\Laravel\BasinServiceProvider"
```

### Usage

```php
// Via facade
use Basin\Laravel\BasinFacade as Basin;

$result = Basin::from('orders')->eq('status', 'paid')->execute();

// Via dependency injection
use Basin\Client;

class OrderController extends Controller
{
    public function __construct(private readonly Client $basin) {}

    public function index(): JsonResponse
    {
        $result = $this->basin->from('orders')->limit(20)->execute();
        return response()->json($result->rows);
    }
}
```

### Per-request auth (user-facing apps)

```php
// In middleware — sign in and persist the session
$session = $basin->auth->signIn($request->email, $request->password);
session(['basin_session' => $session]);

// In subsequent requests — restore from session store
$basin->auth->setSession(session('basin_session'));
```

## Testing

The SDK uses Guzzle's `MockHandler` for unit testing — no live server needed.

```php
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;
use Basin\Client;

$mock  = new MockHandler([
    new Response(200, ['Content-Type' => 'application/json'],
        json_encode([['id' => 1, 'total' => 50]])),
]);
$stack = HandlerStack::create($mock);

$client = new Client([
    'url'     => 'http://localhost:8080',
    'token'   => 'test-token',
    'handler' => $stack,
]);

$result = $client->from('orders')->execute();
assert(count($result->rows) === 1);
```

Run the SDK's own test suite:

```bash
composer install
vendor/bin/phpunit
```

## Architecture

[Basin Cloud](https://basin.run) is the control plane — dashboard, billing,
project management, and the place you mint the anon-key JWT. Once you have a
URL and key, Basin Cloud is **off the data path**: every `auth.*`, `from()`,
`storage()`, `functions.*`, and `realtime()` call lands on `basin-engine`
directly. The engine is open-source (Apache-2.0) and self-hostable:
`cargo run -p basin-server` starts a single process that includes the REST
layer, pgwire listener, auth service, object storage, and realtime stack — no
external Postgres required.

Engine REST routes used by this SDK:
`/auth/v1/{signup,signin,refresh,signout,verify-email,reset-password,
request-password-reset,magic-link,magic-link/consume,api-keys,
oauth/:provider/authorize,factors/*}`,
`/rest/v1/:table`, `/rest/v1/rpc/:fn`,
`/fn/v1/:name`,
`/storage/v1/{bucket,object}/*`,
`/realtime/v1/ws/:project`,
`/health`.

Full reference: <https://basin.run/docs/php-sdk>.

## License

MIT — see [`LICENSE`](./LICENSE).
