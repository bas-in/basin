# basin-java — Roadmap

The shape this SDK is reaching for: a Basin-native JVM client that's at home
in server-side Java and Android/KMP Kotlin, exposes Basin's distinctive
capabilities — Arrow IPC, cursor pagination, NDJSON streaming, the realtime
WebSocket — as first-class features rather than thin HTTP wrappers, and is
publishable to Maven Central so teams can adopt it without a local build.

v0.1 (async + blocking query builder, full auth including MFA/OAuth/magic-link,
Arrow IPC, NDJSON streaming, realtime WebSocket + presence, storage CRUD,
functions/RPC, typed error envelope with SQLSTATE) shipped and is preserved in
git history. Everything below is forward work.

---

## 0.2 — Distribution and discoverability

The SDK is functionally complete at v0.1 but lives outside any public registry.
This is the top-priority gap: a library that can't be pulled from `mavenCentral()`
or `maven { url "..." }` has near-zero adoption in the Java ecosystem.

### 0.2.1 Publish to Maven Central

`build.gradle.kts` already configures the `maven-publish` plugin and the POM.
What remains:

- Register `io.basin` as a Maven Central namespace (Sonatype OSSRH or Central
  Portal).
- Add signing (`signing {}` block, GPG key in CI secrets).
- Wire a `publishMavenJavaPublicationToMavenCentral` step in CI that fires on
  version-tag pushes.
- Ship `javadoc` and `sources` JARs (already gated behind `withJavadocJar()` and
  `withSourcesJar()` in the build — just needs the publication target).
- Write the OSSRH checklist to `CONTRIBUTING.md`.

Until this lands, the only install path is a local `./gradlew publishToMavenLocal`
or a GitHub Packages publication, both of which are friction for new users.

### 0.2.2 Gradle wrapper

The repo has no `gradlew`. CI currently relies on whatever Gradle version
`gradle/actions/setup-gradle@v4` installs. Adding a wrapper (`gradle wrapper
--gradle-version 8.8`) pins the build toolchain version, enables `./gradlew`
for contributors, and removes the implicit dependency on the CI action's default.
Low-effort, high-confidence improvement.

### 0.2.3 Example module

A runnable example in `examples/quickstart/` demonstrating the happy path
(sign-in → query → stream → realtime subscribe) would dramatically lower the
time-to-first-success for new adopters. The test suite covers correctness but
not the "run this and see it work against a real engine" experience.

---

## 0.3 — Streaming completeness

The two remaining streaming gaps relative to the parity matrix (dart, js,
python, ruby all have them):

### 0.3.1 Incremental NDJSON via HTTP/2 push

`QueryBuilder.stream()` currently reads the full response body before
iterating. This is correct but memory-bounded: a 10 M-row NDJSON stream is
fully buffered before the caller's first `for` iteration. The fix:

- Use `HttpResponse.BodyHandlers.ofLines()` (Java 11+) to get a `Stream<String>`
  backed by the response body's line-by-line arrival.
- Wrap as an `Iterator<Map<String,Object>>` that parses each line on demand —
  true incremental processing, bounded memory regardless of result size.
- Surface a `streamAsync()` returning `Flow.Publisher<Map<String,Object>>` for
  callers on a reactive pipeline.

The server side is already correct. This is a pure SDK change.

### 0.3.2 Cursor-pagination iterator

Expose a `paginate()` method on `QueryBuilder` that returns an
`Iterable<Map<String,Object>>` (or `Stream<Map<String,Object>>`) that walks
`next_cursor` transparently across multiple HTTP requests, analogous to the JS
SDK's `.paginate()` async-generator. Callers iterate rows without knowing page
boundaries exist.

---

## 0.4 — DX polish

### 0.4.1 Richer filter operators

The current filter set (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is`)
covers the most common cases. Missing vs. the JS SDK: `like`, `ilike`,
`contains`, `containedBy`, `overlaps`, `textSearch`, `not`, `or`. Check which
of these Basin's `parser.rs` actually supports and add the ones that are live
rather than adding dead stubs.

### 0.4.2 Configurable retry and backoff

The HTTP transport has no retry logic. Add configurable retry on network errors,
`5xx`, and `429` (with `Retry-After` header awareness) with exponential backoff.
Default off, opt-in via `BasinClient.Builder.retryPolicy(...)`. The realtime
client already has reconnect backoff; this fills the same gap on the REST path.

### 0.4.3 Typed error-code constants

`BasinApiException.code` is a plain `String`. Every caller either hard-codes
string literals or writes their own `switch`. Expose the known stable codes as
`public static final String` constants on `BasinApiException` (or a companion
`ErrorCode` class) so IDEs can autocomplete them and refactoring tools track them.
This is a non-breaking addition.

### 0.4.4 SSE realtime transport

`GET /realtime/v1/sse/:project/:table` is not wrapped. For read-only
single-table subscriptions it's cheaper than opening a WebSocket. Add
`RealtimeClient.subscribeSSE(table, callback)` that uses
`HttpResponse.BodyHandlers.ofLines()` to consume the event stream and reconnects
with `Last-Event-Id` on drop. The subscription model is identical to the WS
path from the caller's perspective.

---

## 0.5 — Admin surface

`/admin/v1/*` is reachable today via `client.request("GET", "/admin/v1/…", null)`
but there's no typed wrapper. Add an `AdminClient` (accessible as `client.admin`)
with:

- `projects.provision(projectId)` → `{ connectionString }`
- `projects.rotateCredentials(pgwireUser)` → `{ connectionString }`
- `projects.listCredentials(projectId)` → `List<CredentialDescriptor>`

This unblocks SaaS operators who provision per-tenant Basin projects from Java
server code.

---

## 0.6 — Iceberg catalog client

The engine ships a Lakekeeper-compatible Iceberg REST catalog at
`/iceberg/v1/:warehouse/*`. Spark, Trino, and Flink can already talk to it.
Adding a `basin-java-iceberg` sub-module (or a separate artifact) would let
Java analytics pipelines discover and read Basin tables without a separate
catalog service. Arrow IPC already being present in the SDK makes this a
natural fit for the JVM analytics space.

Decision depends on demand. Not blocking 0.2–0.5.

---

## Priority ordering

1. **Maven Central publication** (0.2.1) — blocks all real-world adoption.
2. **Gradle wrapper** (0.2.2) — contributor hygiene, low effort.
3. **Incremental NDJSON** (0.3.1) — correctness for large streams.
4. **Cursor-pagination iterator** (0.3.2) — completes the streaming story.
5. **Example module** (0.2.3) — time-to-first-success for new users.
6. **Retry / backoff** (0.4.2) and **SSE transport** (0.4.4) — production polish.
7. **Filter operators** (0.4.1) and **typed error constants** (0.4.3) — DX fills.
8. **Admin surface** (0.5) — unblocks operators.
9. **Iceberg** (0.6) — analytics audience, demand-gated.
