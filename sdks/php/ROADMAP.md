# basin-php — Roadmap

The shape this SDK is reaching for: a first-class PHP client for Basin that
covers the full REST surface — auth, queries, storage, functions, realtime —
with idiomatic PHP ergonomics and the streaming capabilities that matter most
for backend workloads. PHP 8.1+ throughout; Laravel auto-discovery included.

v0.1 (Client, QueryBuilder, AuthClient, StorageClient, FunctionsClient,
RealtimeClient, typed exceptions, Laravel service provider) shipped and is
preserved in git history. Everything below is forward work.

---

## 0.2 — Close the known parity gaps

Three gaps are identified in the SDK parity matrix relative to the reference
surface (Rust + Python). These are the highest-leverage items.

### 0.2.1 Realtime out of the box

**Current state:** `RealtimeClient` implements the full WebSocket protocol
(subscribe/unsubscribe, presence track/untrack/heartbeat, exponential backoff,
reconnect-with-replay) but delegates the actual WebSocket transport to
`textalk/websocket`, which must be `composer require`d separately. A
`RuntimeException` is thrown on first use if neither `textalk/websocket` nor
`ratchet/pawl` is installed. This is a parity gap: the SDK appears complete at
compile time and breaks at runtime.

Outcome:
- Promote `textalk/websocket:^1.6` from `suggest` to `require` in
  `composer.json`. It is a pure-PHP library, adds no C extension requirement,
  and is already tested against the SDK's `RealtimeClient`. This one-line
  change closes the gap.
- Document `ratchet/pawl` as the async-loop alternative in README + docblock
  but keep it in `suggest` (ReactPHP users install it knowingly).
- Add a `RealtimeClientTest` that exercises subscribe, presence_track, and
  reconnect using a mock WebSocket handler.

### 0.2.2 Arrow IPC transport

**Current state:** the server accepts `Accept: application/vnd.apache.arrow.stream`
on any `GET /rest/v1/:table` and returns a native Arrow IPC stream with
`X-Basin-Next-Cursor` and `X-Basin-Row-Count` headers. No PHP Arrow decoder
exists today; `flow-php/arrow-ext` is a compiled C extension, not a Composer
library.

Outcome:
- Track the PHP Arrow ecosystem. When a pure-PHP or FFI-backed Arrow IPC
  decoder becomes available on Packagist, add a `QueryBuilder::toArrow()`
  method following the pattern in `sdk/basin-js/src/query.ts` and
  `sdk/basin-python/basin/query.py`. Until then, `stream()` (NDJSON) is the
  recommended path for large result sets.
- Ship a `QueryBuilder::toArrowRaw(): string` escape hatch in the meantime:
  sets the `Accept` header, returns the raw response body, and lets callers
  who already have an Arrow library hand-decode it. No additional dependency;
  purely a transport convenience.

### 0.2.3 SQLSTATE on errors

**Current state:** `BasinApiException` already parses and stores the `sqlstate`
field from the server's error envelope and exposes it via `getSqlState()`.
The parity matrix entry is stale — the field is present. Verify in
`BasinApiException::fromResponseBody` and close the matrix row.

Outcome:
- Confirm `getSqlState()` surfaces `23505` for a unique-constraint violation
  end-to-end; add a test asserting `getSqlState() === '23505'` against a
  mock response.
- Update the parity matrix entry for `php` from `❌` to `✅`.

---

## 0.3 — Richer PostgREST filter surface

**Current state:** the query builder supports `eq/neq/gt/gte/lt/lte/in/is`
and `order/limit/offset/cursor`. The parity matrix marks `or/not/like/ilike`
and embedded resource selects as absent from all ten SDKs — these are
server/contract gaps, not SDK gaps. But the `Prefer` header pass-through and
compound `or=` filter syntax that PostgREST supports and Basin honours are
within reach.

Outcome:
- `QueryBuilder::prefer(string $value): static` — appends a `Prefer: <value>`
  header to the underlying request, enabling callers to use PostgREST
  conventions (`return=representation`, `count=exact`, etc.) without a raw
  `Client::request()` escape hatch.
- `QueryBuilder::textSearch(string $column, string $query): static` — maps
  to `<col>=fts.<query>` when the server's full-text search operator ships.
- Document the `filter` parameter on `RealtimeClient::subscribe()` more
  fully — it accepts the same `<col>=<op>.<val>` grammar.

---

## 0.4 — Packagist publish + distribution

**Current state:** the package name is `basin/basin-php` and `composer.json`
is production-ready, but the package has not been submitted to Packagist.

Outcome:
- Submit `basin/basin-php` to Packagist linked to this repository.
- Add a `CHANGELOG.md` tracking releases.
- Tag `v0.1.0` and verify `composer require basin/basin-php` resolves from
  Packagist in a clean project.
- Add Packagist download badge to README.

---

## 0.5 — DX polish

The smaller things that compound into "this SDK feels considered."

- Retry + backoff on transient failures. `Transport` currently raises
  `BasinNetworkException` on the first Guzzle connect error. Add configurable
  retry (default: 3 attempts, exponential 0.5 s base) with opt-out per
  `Client` config key (`'retry' => false`).
- Async support via Guzzle's async pool for callers on ReactPHP / Amp who
  want concurrent Basin calls without multiple client instances. Document
  the `ratchet/pawl` path for realtime in that environment.
- PHP 8.4 matrix slot in the CI matrix as soon as 8.4 goes GA.
- Example scripts under `examples/` — one for a Laravel queue worker that
  subscribes to table events, one for a CLI bulk-importer that uses
  `QueryBuilder::stream()`.

---

## 1.0 — Stable API contract

When 0.2 + 0.3 land and the public shape is tested against a live engine:
- Declare `Basin\Client`, `Basin\Query\QueryBuilder`, `Basin\Auth\AuthClient`,
  `Basin\Storage\StorageClient`, `Basin\Functions\FunctionsClient`, and
  `Basin\Realtime\RealtimeClient` as stable, semver-governed surfaces.
- The `Basin\Laravel\*` namespace follows the same stability contract.
- All other `Basin\Http\*` and `Basin\Types\*` internals remain `@internal`.

---

## Priority ordering

1. **Realtime out of the box** (0.2.1) — the gap most likely to surprise a
   new user. A one-line `composer.json` change; do this first.
2. **SQLSTATE verification** (0.2.3) — likely already done; confirm with a
   test and close the matrix row.
3. **Packagist publish** (0.4) — needed before any external user can install
   the SDK at all.
4. **Arrow IPC escape hatch** (0.2.2 `toArrowRaw()`) — low effort, unblocks
   users who already have an Arrow library.
5. **Richer filters** (0.3) — incremental; `prefer()` first, then `textSearch`.
6. **DX polish** (0.5) and **1.0 stable** — after the surface is exercised.
