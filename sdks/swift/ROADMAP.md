# basin-swift — Roadmap

The shape this SDK is reaching for: a Basin-native Swift client that is
idiomatic for Apple-platform and server-side Swift developers, exposes
Basin's distinctive capabilities (NDJSON streaming, keyset cursor
pagination, presence) as first-class async/await APIs, and covers the
full engine surface without dragging in external dependencies.

v0.1 (BasinClient, query builder, auth, storage, functions, realtime
WebSocket, NDJSON streaming, keyset cursor pagination, typed error model,
CI) shipped and is preserved in git history. Everything below is forward
work.

---

## 0.2 — Parity gaps from the SDK matrix

Two capabilities are absent from basin-swift that are present in several
peer SDKs (per `docs/sdk-parity.md`). These are the highest-leverage
items because they affect workloads that are genuinely common on Apple
platforms.

### 0.2.1 Arrow IPC transport

The engine returns `application/vnd.apache.arrow.stream` when the
`Accept` header requests it, enabling zero-copy columnar transfer for
analytics workloads. basin-swift currently falls back to JSON row
decoding for every query — meaning large result sets pay full JSON
parse cost and lose i64/timestamp precision.

The [`apache/arrow-swift`](https://github.com/apache/arrow-swift)
package provides IPC decoding but pulls in FlatBuffers and
swift-atomics as transitive dependencies, which conflicts with the
SDK's zero-dependency goal. Options:

- Ship Arrow IPC as an **opt-in sub-library** (`BasinArrow` product in
  `Package.swift`). Consumers who don't need columnar access take no
  extra dependency; those who do opt in via a second product dependency.
- Implement the subset of Arrow IPC framing we need (schema + record
  batches) in pure Swift, avoiding the full `arrow-swift` dependency tree.

Outcome: `QueryBuilder.runArrow()` returning `RecordBatch` values (or a
Swift wrapper thereof); `.streamArrow()` for incremental columnar pages.

### 0.2.2 SQLSTATE on `BasinApiError`

`BasinApiError` already carries a `sqlState: String?` field and the
wire decoder populates it from the engine's `sqlstate` envelope field —
this is already done. The remaining work is confirming test coverage:
one unit test that exercises a unique-violation (`23505`) response and
asserts `sqlState == "23505"`.

---

## 0.3 — Maturation

### 0.3.1 Swift Package Index registration

Register with [Swift Package Index](https://swiftpackageindex.com) to
surface compatibility badges and searchability. Requires:
- A valid `Package.swift` with explicit platform declarations (already
  present).
- A tagged release (`0.1.0`).
- Adding the repo to SPI's tracked list.

Once registered, add `.compatible(with: .swiftpackageindex)` metadata
and document the SPI badge in the README.

### 0.3.2 Linux / server-side Swift support

The current platform list is Apple-only (iOS/macOS/tvOS/watchOS).
`URLSessionWebSocketTask` is available on Linux via `swift-corelibs-foundation`
in Swift 5.9+, so realtime should work. Blockers to verify:

- Foundation's `ISO8601DateFormatter` on Linux (used in `accessToken()`).
- `URLSession.bytes` streaming on Linux (used by `stream()` /
  `streamPage()`).
- CI: add an `ubuntu-latest` matrix leg once both are confirmed working.

### 0.3.3 Configurable retry and backoff

Network errors on `run()` / `page()` / `insert()` / `upload()` are
currently propagated immediately as `BasinNetworkError`. Add opt-in
retry with exponential backoff (mirroring what the realtime client
already does internally) on the HTTP transport layer, configurable per
`BasinClient`:

```swift
let client = BasinClient(
    url: "...", key: "...",
    retryPolicy: .exponential(maxAttempts: 3, base: 0.5)
)
```

Retry should apply to transient failures (connection error, 429 with
`Retry-After`, 503) and not to 4xx client errors.

### 0.3.4 SSE realtime transport

The engine exposes `GET /realtime/v1/sse/:project/:table` as a
lighter-weight alternative to WebSocket for read-only single-table
listeners (no presence, no multi-table multiplexing). The Swift SDK
currently binds only the WebSocket path. Adding SSE:

- Use `URLSession.bytes` (same as NDJSON streaming) to consume the
  `text/event-stream` response.
- Parse `data:` lines and `Last-Event-Id` for reconnect-resume.
- Route selection: expose `.listenSSE(table:)` as an explicit
  lower-overhead alternative; keep `.listen(table:)` on WebSocket for
  full feature parity.

### 0.3.5 Admin surface

`/admin/v1/*` routes for provisioning per-project pgwire credentials
and rotating them are reachable today via `client.request(...)` as an
escape hatch. Wrapping them as first-class methods:

```swift
let cred = try await client.admin.provision(projectID: "01J...")
let rotated = try await client.admin.rotateCredentials(pgwireUser: cred.username)
let list = try await client.admin.listCredentials(projectID: "01J...")
```

---

## 0.4 — DX polish

- **Examples target** — a `Sources/Examples/` executable that demonstrates
  the full surface (auth → query → stream → realtime) against a local
  engine, runnable with `swift run basin-swift-examples`.
- **DocC documentation** — add a `docc` plugin target so `swift package
  generate-documentation` produces a browsable symbol reference. Host
  on GitHub Pages from the `gh-pages` branch.
- **Concurrency audit** — verify `@Sendable` conformance is complete
  across all public types; address any `Strict Concurrency` warnings
  under `swift build -Xswiftc -strict-concurrency=complete`.
- **Coverage audit** — `streamPage()` and `listenPresence()` are covered
  by unit tests; `createSignedURL()` and `getPublicURL()` are not. Add
  one integration-stub test each.

---

## Priority ordering

1. **SQLSTATE test coverage** (0.2.2) — one test, closes a parity gap
   documented in the cross-SDK matrix.
2. **Linux support** (0.3.2) — unblocks Vapor/Hummingbird server-side use.
3. **Arrow IPC opt-in** (0.2.1) — largest analytics gap; explore pure-Swift
   IPC framing before taking the full `arrow-swift` dependency.
4. **SPI registration + tagged release** (0.3.1) — discoverability.
5. **Retry / backoff** (0.3.3) — correctness for production use.
6. **SSE transport** (0.3.4) — lighter option for read-only listeners.
7. **Admin surface** (0.3.5) — unblocks operator tooling.
8. **DX polish** (0.4) — examples, DocC, concurrency audit.
