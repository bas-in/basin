# Basin PHP SDK

Official PHP client for the [Basin](https://basin.run) HTTP API.

Covers: REST data queries, auth (email/password, magic links, OAuth, MFA TOTP/WebAuthn, API keys), object storage, serverless functions, and realtime WebSocket change-data subscriptions.

**Requirements:** PHP 8.1+, [Guzzle](https://docs.guzzlephp.org/) 7.x (auto-installed).

---

## Installation

```bash
composer require basin/basin-php
```

For realtime WebSocket support, add the optional WebSocket client:

```bash
# Synchronous (CLI workers, long-running scripts)
composer require textalk/websocket

# Async (ReactPHP / event-loop environments)
composer require ratchet/pawl
```

---

## Quickstart

```php
use Basin\Client;

$basin = new Client([
    'url'        => 'https://api.basin.run',
    'token'      => 'your-api-key-or-jwt',
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

---

## Auth

```php
// Sign up
$signup = $basin->auth->signUp('user@example.com', 'password123');
echo $signup->userId; // usr_01J...

// Sign in — stores session, auto-refreshes tokens on expiry
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

// API keys
$issued = $basin->auth->createApiKey('ci-deploy');
echo $issued->secret; // shown exactly once

$keys = $basin->auth->listApiKeys();
$basin->auth->deleteApiKey($keys[0]->id);

// MFA — TOTP
$enroll = $basin->auth->enrollFactor('totp', friendlyName: 'Authenticator App');
// Display $enroll->otpauthUri as a QR code, then verify:
$basin->auth->verifyFactor($enroll->factorId, code: '123456');

// MFA — step-up challenge
$challenge = $basin->auth->challengeFactor($factorId);
$aal2Session = $basin->auth->verifyChallenge($factorId, $challenge->challengeId, code: '654321');
```

---

## Query Builder

All filter methods chain and return `$this`. Call `execute()` to run.

```php
$result = $basin->from('products')
    ->select(['id', 'name', 'price'])      // array or CSV string
    ->eq('category', 'electronics')        // exact match
    ->neq('status', 'discontinued')        // not equal
    ->gte('price', 10.00)                  // >=
    ->lte('price', 999.99)                 // <=
    ->gt('stock', 0)                       // >
    ->lt('weight', 5.0)                    // <
    ->in('brand', ['Apple', 'Samsung'])    // IN list
    ->is('deleted_at', 'null')             // IS NULL / IS NOTNULL
    ->order('price', ascending: true)      // sort (repeatable)
    ->limit(20)
    ->offset(40)
    ->execute();                           // QueryResult

$rows  = $result->rows;       // array<int, array<string, mixed>>
$next  = $result->nextCursor; // string|null — use with ->cursor()

// Shorthand — rows only
$rows = $basin->from('orders')->eq('status', 'paid')->get();

// NDJSON streaming (large result sets)
$gen = $basin->from('events')->stream();
foreach ($gen as $row) {
    process($row);
}

// Writes
$basin->from('orders')->insert(['total' => 50, 'status' => 'new']);
$basin->from('orders')->eq('id', 1)->update(['status' => 'paid']);
$basin->from('orders')->eq('id', 1)->delete();
```

---

## Storage

```php
// Bucket management
$bucket = $basin->storage()->createBucket('avatars', public: true);
$meta   = $basin->storage()->getBucket('avatars');
$basin->storage()->deleteBucket('old-bucket');

// Object operations
$bucket = $basin->storage()->fromBucket('avatars');

$obj = $bucket->upload('user/photo.jpg', file_get_contents('/tmp/photo.jpg'), 'image/jpeg');
echo $obj->path;

$download = $bucket->download('user/photo.jpg');
file_put_contents('/tmp/out.jpg', $download->data);

$bucket->remove('user/old.jpg');

// List objects
$objects = $bucket->list(prefix: 'user/', limit: 100);

// Bulk delete
$bucket->removeByPrefixes(['temp/', 'drafts/']);

// Public URL (bucket must have public=true)
$url = $bucket->getPublicUrl('user/photo.jpg');

// Signed download URL (TTL max 7 days)
$signed = $bucket->createSignedUrl('user/photo.jpg', expiresIn: 3600);
echo $signed->absoluteUrl;
echo $signed->expiresAt;
```

---

## Functions

```php
// POST /rest/v1/rpc/:fn — catalog SQL / Wasm UDFs
$result = $basin->functions->rpc('calculate_total', ['order_id' => 42]);
$basin->rpc('my_function', ['arg' => 'value']); // convenience alias

// ANY /fn/v1/:name — HTTP-handler functions (Wasm/JS, proxied verbatim)
$result = $basin->functions->invoke('send-email', body: ['to' => 'user@example.com']);
echo $result->status; // HTTP status from the function
echo $result->data;   // decoded JSON or raw string
```

---

## Realtime

Requires `composer require textalk/websocket`.

```php
$rt = $basin->realtime();

// Subscribe to a table with a callback
$rt->subscribe('orders', function (array $event): void {
    echo $event['type'] . ': ' . $event['op'] . PHP_EOL;  // INSERT / UPDATE / DELETE
    echo json_encode($event['after']) . PHP_EOL;
});

// Subscribe with a server-side filter
$rt->subscribe('orders', $callback, filter: "NEW.status = 'paid'");

// Subscribe with a resume cursor (replay missed events)
$rt->subscribe('orders', $callback, lastEventId: $lastSeenSeq);

// Listen for incoming frames (blocks until $maxMessages or $timeoutMs)
$rt->listen(maxMessages: 100, timeoutMs: 5000);

// Presence
$rt->presenceTrack('room:lobby', 'client-id-1', metadata: ['name' => 'Alice']);
$rt->presenceHeartbeat('room:lobby', 'client-id-1');
$rt->presenceUntrack('room:lobby', 'client-id-1');

// Unsubscribe
$rt->unsubscribe('orders');
$rt->disconnect();
```

Frame `$event['type']` values: `event`, `error`, `gap`, `subscribed`, `unsubscribed`,
`presence_state`, `presence_diff`, `presenceerror` (no underscore — server serialisation).

---

## Error Handling

```php
use Basin\Exception\BasinApiException;
use Basin\Exception\BasinNetworkException;
use Basin\Exception\BasinException;

try {
    $basin->from('orders')->execute();
} catch (BasinApiException $e) {
    // Match on the stable error code, not the human-readable message.
    match ($e->getErrorCode()) {
        'E_UNAUTHENTICATED' => redirect('/login'),
        'E_NOT_FOUND'       => abort(404),
        'E_RATE_LIMITED'    => sleep(1),
        default             => throw $e,
    };
    // $e->getHttpStatus() — HTTP status code (0 for client-synthesised errors)
    // $e->getMessage()    — human-readable detail (not stable, do not match on)
} catch (BasinNetworkException $e) {
    // Transport failure (connection refused, timeout, etc.)
    logger()->error('Basin connection failed', ['error' => $e->getMessage()]);
}
```

Stable error codes (basin-rest/src/errors.rs):
`E_UNAUTHENTICATED`, `E_FORBIDDEN`, `E_NOT_FOUND`, `E_INVALID_REQUEST`,
`E_RATE_LIMITED`, `E_ENGINE_UNSUPPORTED`, `E_INTERNAL`, `E_EMAIL_DISABLED`,
`E_REVOKED_TOKEN`.

---

## Laravel Integration

The package ships a service provider and facade that are auto-discovered.

### Environment variables

```dotenv
BASIN_URL=https://api.basin.run
BASIN_TOKEN=your-api-key
BASIN_PROJECT_ID=01JXXXXXXXXXXXXXXXXXXXXXXXXX
BASIN_TIMEOUT=30
```

### Publish config

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

### Per-request auth

For user-facing apps, sign in per request and let the SDK auto-refresh:

```php
// In middleware:
$session = $basin->auth->signIn($request->email, $request->password);
session(['basin_session' => $session]);

// In subsequent requests:
$basin->auth->setSession(session('basin_session'));
```

---

## Testing

The SDK uses Guzzle's `MockHandler` for unit testing — no live server needed.

```php
use GuzzleHttp\Handler\MockHandler;
use GuzzleHttp\HandlerStack;
use GuzzleHttp\Psr7\Response;

$mock  = new MockHandler([
    new Response(200, ['Content-Type' => 'application/json'],
        json_encode([['id' => 1, 'total' => 50]])),
]);
$stack = HandlerStack::create($mock);

$client = new Basin\Client([
    'url'     => 'http://localhost:8080',
    'token'   => 'test-token',
    'handler' => $stack,
]);

$result = $client->from('orders')->execute();
assert(count($result->rows) === 1);
```

Run the SDK's own test suite:

```bash
cd sdk/basin-php
composer install
./vendor/bin/phpunit
```

---

## HTTP client choice

The SDK uses **Guzzle 7** (`guzzlehttp/guzzle`).

Rationale: Guzzle is the de-facto standard PHP HTTP client, ships PSR-7 + PSR-18
support, provides a well-tested `MockHandler` stack used by every major PHP framework
for SDK testing, and is already a transitive dependency in virtually all Laravel /
Symfony projects. `symfony/http-client` was considered but Guzzle's `MockHandler`
and `HandlerStack` middleware ecosystem is more ergonomic for SDK-level unit testing
without a live server.

## WebSocket choice

For realtime, **textalk/websocket** (`^1.6`) is the suggested package. It is
lightweight, synchronous (no event loop required), and works well in CLI workers
and queue processors. For async / event-loop environments (ReactPHP, Amp),
**ratchet/pawl** is suggested instead. Neither is a hard dependency — both are in
`suggest` in `composer.json`; a clear `RuntimeException` is thrown on first use if
neither is installed.
