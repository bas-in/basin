# Basin SDK Feature-Parity Matrix

This matrix compares the public surface of all ten Basin client SDKs in `sdk/`.
Every cell was derived by reading the SDK source (not READMEs, not tests). The
SDKs are unusually uniform: all ten are hand-derived from the same `basin-rest`
route set, so the **query builder, auth, storage, functions, realtime, and the
typed error envelope are at full parity across every language**. The genuine
differentiators are a small set of optional/advanced capabilities — Arrow IPC
transport, NDJSON result streaming, the SQLSTATE field on errors, and the
sync/async execution model — which is where this document focuses.

Legend: ✅ full · ➖ partial / conditional · ❌ absent. Columns are
dart / dotnet / go / java / js / php / python / ruby / rust / swift.

| Capability | dart | dotnet | go | java | js | php | python | ruby | rust | swift |
|---|---|---|---|---|---|---|---|---|---|---|
| **Query: filter ops** (eq/neq/gt/gte/lt/lte/in/is) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Query: order/limit/offset** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Keyset cursor pagination** (`next_cursor`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Insert / Update / Delete** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Richer PostgREST filters** (or/not/like/ilike, embeds) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Parameterized values** (typed scalar literals) | ✅ | ✅ | ➖ string-only | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **NDJSON streaming results** (`stream=true`) | ✅ | ❌ | ❌ | ❌ | ✅ async-gen | ✅ Generator | ✅ generator | ✅ | ❌ | ❌ |
| **Arrow IPC transport** (`to_arrow`) | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ✅ pyarrow | ❌ | ✅ feature-gated | ❌ |
| **RPC** (`/rest/v1/rpc/:fn`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **HTTP-handler invoke** (`/fn/v1/:name`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auth: signup/signin/refresh/signout** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auth: verify-email / reset-password** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auth: magic-link** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auth: OAuth authorize-URL** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auth: MFA factors** (enroll/challenge/verify, TOTP+WebAuthn) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **API-key issue/list/revoke** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Auto-refresh session token** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Bearer auth (JWT or raw API key)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Realtime: change events** (INSERT/UPDATE/DELETE) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ feature-gated | ✅ |
| **Realtime: presence track/untrack** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖ types only* | ✅ |
| **Realtime: reconnect + backoff** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Realtime: replay (`last_event_id`/gap)** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Realtime transport** | ✅ | ✅ | ✅ | ✅ | ✅ | ➖ needs opt. dep† | ✅ | ✅ | ✅ | ✅ |
| **Server-side change filter** (subscribe `filter`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Storage: buckets + object CRUD** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Storage: signed + public URLs** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Typed error model** (code + status + message) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Forward-compatible unknown codes** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **SQLSTATE on errors** | ❌ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Typed error-code enum** | ➖ const list | ➖ const | ➖ const | ➖ | ➖ union type | ➖ doc only | ➖ tuple | ➖ | ➖ const | ✅ enum |
| **Async API** | ✅ | ✅ | ✅ ctx | ✅ Future | ✅ Promise | ➖ blocking | ✅ AsyncBasinClient | ❌ blocking | ✅ | ✅ |
| **Sync API** | ❌ | ❌ | ✅ | ✅ Blocking | ❌ | ✅ | ✅ BasinClient | ✅ | ❌ | ❌ |
| **Transactions / BEGIN-COMMIT** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **COPY / bulk-insert path** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Vector / similarity search** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Explicit connection pooling knob** | ➖ inj. client | ➖ HttpClient | ➖ http.Client | ➖ | ➖ fetch | ➖ | ➖ inj. httpx | ➖ | ✅ reqwest builder | ➖ URLSession |

\* **Rust presence**: the realtime module fully parses presence frames
(`presence_state`/`presence_diff`/`presenceerror`) and exposes a
`PresenceMetadata` type and `SubscribeOptions`, but the client-side `listen`
API surfaces a read-only event `Stream`; there is no `presence_track` /
`presence_untrack` send method like the other nine SDKs have
(`sdk/basin-rust/src/realtime.rs`).

† **PHP realtime**: the `RealtimeClient` implements the full protocol but the
actual WebSocket transport is delegated to the optional `textalk/websocket`
(or `ratchet/pawl`) package, which must be installed separately
(`sdk/basin-php/src/Basin/Realtime/RealtimeClient.php`).

### Notes on "uniform absences"

Transactions, COPY/bulk-insert, vector search, and a raw-SQL `execute()`
endpoint are absent from **all ten** SDKs — they are not exposed by the
`basin-rest` surface these clients bind to (data access is the PostgREST-style
query builder plus `rpc()`). These are server/contract gaps, not per-SDK gaps,
so they are not "recommendations" against any single SDK.

## Gaps & recommendations

Ranked per SDK by impact (most impactful first). Only gaps relative to the
de-facto reference surface (Rust + Python, the richest) are listed.

### basin-rust
1. **Presence send API missing.** Parses presence frames but cannot
   `presence_track` / `presence_untrack`; it is the only SDK that can observe
   presence but not participate. Add send methods to `RealtimeClient`.
2. **No NDJSON streaming terminal.** Has Arrow IPC but not the `stream=true`
   NDJSON iterator the dart/js/python/ruby/php builders expose.

### basin-swift
1. **No Arrow IPC.** Largest analytics gap; Apple-platform analytics clients
   fall back to JSON-row decoding only.
2. **No NDJSON streaming.** Large result sets are buffered whole.

### basin-java
1. **No Arrow IPC.** Same analytics gap as Swift; notable for a JVM client
   where Arrow tooling is mature and expected.
2. **No NDJSON streaming.**

### basin-dotnet
1. **No NDJSON streaming.** Has Arrow IPC, so this is the remaining streaming
   gap for non-Arrow consumers.

### basin-go
1. **String-only filter values.** `Eq/Gt/...` take `string`, unlike the typed
   `Scalar`/union elsewhere — callers must pre-stringify ints/bools/null,
   risking malformed filter literals.
2. **No NDJSON streaming.** Has Arrow IPC; lacks the row-streaming iterator.

### basin-dart
1. **No Arrow IPC.** Has NDJSON streaming, so columnar analytics is the gap.
2. **No SQLSTATE** on `BasinApiError` (only dotnet/go/java/rust carry it).

### basin-php
1. **Realtime needs an optional dependency.** WebSocket transport is not
   built-in; document/bundle `textalk/websocket` to reach out-of-box parity.
2. **No Arrow IPC**, and **no SQLSTATE** on errors.

### basin-ruby
1. **Sync-only.** No async/fiber client; long realtime/streaming calls block
   the thread. (Acceptable for Ruby, but a gap vs python's dual model.)
2. **No Arrow IPC**, **no SQLSTATE**.

### basin-js
1. **No Arrow IPC?** — js *does* ship `toArrow()`, so the only residual gaps
   are **no SQLSTATE** field on `BasinApiError` and no sync mode (N/A for JS).

### basin-python
- Effectively the reference: dual sync+async, Arrow IPC (via pyarrow), NDJSON
  streaming, full auth/realtime/storage. Only minor gap: **no SQLSTATE** on
  `BasinApiError` despite the server providing it.

## Top 5 cross-SDK parity gaps (ranked by impact)

1. **SQLSTATE is dropped by 6 of 10 SDKs.** dart, js, php, python, ruby, swift
   parse the error envelope but discard the Postgres `sqlstate` field that
   dotnet/go/java/rust expose. This is the single most impactful inconsistency:
   it silently breaks programmatic handling of constraint violations
   (e.g. unique-violation `23505`) in the majority of languages. Cheap to fix —
   add one field to the existing error class.
2. **Arrow IPC transport in only 5 of 10** (dotnet, go, js, python, rust).
   Missing from dart, java, php, ruby, swift — meaning analytics/large-result
   workloads on the JVM, Apple, and PHP/Ruby web stacks pay a full JSON
   round-trip with i64/timestamp fidelity loss.
3. **NDJSON result streaming in only 5 of 10** (dart, js, php, python, ruby).
   Missing from dotnet, go, java, rust, swift, which must buffer entire result
   sets in memory. Note dotnet/go/rust have Arrow instead, but java and swift
   have *neither* streaming path.
4. **Rust cannot send presence.** The only SDK that can observe but not
   participate in presence channels — a functional hole in an otherwise
   complete realtime story.
5. **PHP realtime depends on an unbundled package.** Out-of-the-box, a PHP
   consumer gets compile-time-complete realtime code that throws at runtime
   until `textalk/websocket` is installed — a parity gap disguised as a soft
   dependency.
