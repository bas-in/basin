# basin_sdk (Dart) — Roadmap

The SDK implements the full Basin REST surface: query builder, auth (including
MFA and OAuth), storage, SQL/Wasm functions, and realtime WebSocket with
presence. The gap list below is grounded in the Basin
[SDK parity matrix](https://github.com/vul-os/basin/blob/main/docs/sdk-parity.md).
Items are ordered by impact.

---

## 0.2 — Parity gaps

### 0.2.1 Arrow IPC transport

The engine accepts `Accept: application/vnd.apache.arrow.stream` on any
`GET /rest/v1/:table` request and returns a native Arrow IPC stream
(`crates/basin-rest/src/arrow_ipc.rs`). This is absent from the Dart SDK
because no general-purpose Arrow IPC decoder exists for Dart today — the only
pub.dev package that decodes Arrow IPC is vendor-specific.

Outcome:
- When an official Apache Arrow Dart package (or a production-quality
  community package) ships on pub.dev, add a `toArrow()` terminal method to
  `QueryBuilder` following the pattern in `basin-js` and `basin-py`.
- The method should return a typed columnar object (rows as typed lists per
  column) rather than raw bytes, preserving i64 / timestamp fidelity that
  JSON encoding loses.
- Gate it behind a separate import (`package:basin_sdk/arrow.dart`) so
  consumers who do not need Arrow do not pull in the dep.

This is the most impactful cross-SDK gap for the Dart client. Analytics and
large-result workloads currently pay a full JSON round-trip with numeric
precision loss on 64-bit integers.

### 0.2.2 SQLSTATE on errors — done (0.1)

`BasinApiError` already carries `sqlstate` (the 5-character Postgres
SQLSTATE code, e.g. `23505` for unique violations). This closes the gap
noted in the parity matrix for dart. No further work needed.

### 0.2.3 SSE realtime transport

The engine also ships a lighter-weight SSE transport at
`GET /realtime/v1/sse/:project/:table` for read-only, single-table
subscriptions. The Dart SDK currently uses WebSocket for all realtime. Adding
an SSE path would:
- Reduce connection overhead for simple INSERT/UPDATE/DELETE listeners.
- Align with the JS SDK's transport-selection heuristic (SSE for single-table
  no-presence channels, WebSocket otherwise).

This requires reading a streaming HTTP response body line-by-line using
`package:http`'s `send()` / `ByteStream` API. The CDC stream endpoint
(`GET /v1/cdc/:project/stream`) follows the same pattern.

---

## 0.3 — DX and maturity

### 0.3.1 Publish to pub.dev

The package is not yet published. Steps:
- Verify `pubspec.yaml` metadata (description, homepage, repository,
  topics, screenshots for Flutter pub.dev page).
- Add `example/` directory with a short self-contained runnable example.
- Run `dart pub publish --dry-run` and resolve any warnings.
- Set up CI secret + `dart pub publish` step gated on tags.

### 0.3.2 Richer PostgREST filter surface

The current query builder covers `eq`, `neq`, `gt`, `gte`, `lt`, `lte`,
`in`, `is`, `order`, `limit`, `offset`, `cursor`. The engine's parser also
supports `or`, `not`, `like`, `ilike`, and embedded resource selects — none
of which are exposed yet. These are pure SDK work; the engine already handles
them.

Outcome:
- `or(List<String> filters)` — `or=(col1.eq.a,col2.gt.b)` conjunction.
- `not(String column, String op, Object? value)` — `not.` prefix.
- `like(String column, String pattern)` / `ilike(...)` — pattern match.

### 0.3.3 Admin surface

The engine exposes operator-grade routes under `/admin/v1/*` for provisioning
per-project pgwire credentials and rotating them. Today these are reachable
only via `client.request(...)` (the raw escape hatch). A typed `admin`
namespace would unblock SaaS builders:

- `basin.admin.projects.provision(projectId)` → `{ connectionString }`.
- `basin.admin.projects.rotateCredentials(pgwireUser)`.
- `basin.admin.projects.listCredentials(projectId)`.

### 0.3.4 Configurable retry + backoff on REST calls

The WebSocket transport already does exponential reconnect. The HTTP client
(`BasinHttpClient`) does not retry transient failures (network errors, 5xx,
429 with `Retry-After`). Adding a retry layer with sane defaults (3 attempts,
backoff, opt-out per-call) would make the SDK more resilient in Flutter mobile
apps with flaky connectivity.

### 0.3.5 Streaming row iteration (lazy NDJSON `Stream<Row>`)

`streamCollect()` buffers all rows before returning. For very large result
sets a `Stream<Row>` backed by line-by-line NDJSON parsing would allow
constant-memory consumption. This requires the same `ByteStream` approach as
the SSE work above.

---

## 0.4 — Example app and documentation

- Add `example/main.dart` — a standalone CLI example (no Flutter dep)
  demonstrating auth → query → streaming → storage → realtime.
- Add `example/flutter/` — a minimal Flutter counter app backed by a Basin
  realtime subscription.
- Add `doc/` API documentation hooks (`dart doc`) and host on pub.dev.

---

## Priority ordering

1. **Arrow IPC** (0.2.1) — biggest analytics gap; unblocks float/int64
   fidelity for columnar workloads. Blocked on ecosystem, but worth tracking.
2. **pub.dev publish** (0.3.1) — package is not discoverable until published.
3. **Lazy NDJSON streaming** (0.3.5) — closes constant-memory gap for large
   result sets without waiting for Arrow.
4. **SSE realtime** (0.2.3) — lighter transport for simple subscribers.
5. **Richer filters** (0.3.2) — `or`/`not`/`like`/`ilike` pure SDK work.
6. **Admin surface** (0.3.3) — unblocks multi-tenant SaaS operators.
7. **HTTP retry** (0.3.4) — DX polish for mobile.
8. **Examples + docs** (0.4) — last, after the surface stabilises.
