# basin-sdk (Rust) — Roadmap

The SDK is derived from the same `basin-rest` route set as all ten Basin SDKs,
and currently sits at or above the reference surface in most areas. This
document records the known gaps plus the maturation work needed to treat this
crate as a production-grade published library.

v0.1 (the initial split from the basin monorepo) covers the full REST query
builder, auth (password, magic-link, OAuth, MFA), storage CRUD, RPC + HTTP-handler
functions, realtime WebSocket streams with reconnect/backoff, and the Arrow IPC
transport. Everything below is forward work.

---

## 0.2 — Realtime presence sends

The parity matrix (`docs/sdk-parity.md`) records basin-sdk as the **only SDK
that can observe presence but not participate in it**. `ServerFrame::PresenceState`
and `PresenceDiff` are fully parsed and delivered via the `listen()` stream; the
client-to-server `presence_track` / `presence_untrack` / `heartbeat` send path
is absent.

Work needed:

- Add `RealtimeClient::track(channel, client_id, metadata)` and `untrack(…)`
  that send `{"type":"presence_track",…}` / `{"type":"presence_untrack",…}`
  over the active WebSocket sink.
- The heartbeat (every 30 s; the server evicts after 90 s of silence) should
  run automatically on a background task when any presence subscription is
  active.
- `SubscribeOptions` gains an optional `presence_channel` field to combine a
  table change subscription and a presence channel over the same socket.

This is a purely additive change to `realtime.rs` with no protocol or API
surface break.

---

## 0.3 — NDJSON streaming iterator

Basin's REST layer auto-promotes large responses to NDJSON (one JSON object per
line, trailing `{"_basin_next_cursor":"…"}` sentinel) past ~1 MiB or 10 000
rows. The parity matrix marks NDJSON streaming as absent from basin-sdk — the
SDK has Arrow IPC for columnar analytics but no row-level streaming path for
consumers who cannot or do not want Arrow.

Work needed:

- Add `QueryBuilder::stream()` that sends `?stream=true`, reads the response as
  a byte stream, parses lines, and returns `impl Stream<Item = Result<Value,
  BasinError>>`.
- The terminal detects the trailing cursor sentinel and surfaces it (or exposes
  it via a wrapper type) so callers can resume.
- Existing `run()` path should additionally detect `Content-Type:
  application/x-ndjson` responses and parse them correctly even when the caller
  did not explicitly request streaming (matching how the JS/Python SDKs handle
  the server's auto-promotion).

---

## 0.4 — Richer PostgREST filter grammar

The query builder currently supports: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`,
`in`, `is`. The parity matrix marks `or`/`not`/`like`/`ilike` and embedded
resource selects as absent from **all ten** SDKs — these are server grammar gaps
as much as SDK gaps, but as the basin REST parser matures this SDK should be
ready to expose them.

Planned additions (implement incrementally as the engine ships support):

- `.like(col, pattern)` / `.ilike(col, pattern)` — `LIKE` / `ILIKE` filter ops.
- `.or(clause)` — compound OR predicate, e.g. `.or("status=eq.paid,status=eq.refunded")`.
- `.not(col, op, value)` — negation, e.g. `.not("status", "eq", "cancelled")`.
- Embedded selects (foreign key traversal), e.g. `select("*, customer(name)")`.

None of these require a wire-protocol change — they map to existing query
parameter conventions; the server just needs to parse them.

---

## 0.5 — Crates.io publish and docs.rs

The crate is not yet published to crates.io. Before publishing:

- **License field mismatch:** `Cargo.toml` currently declares `license =
  "Apache-2.0"` but the `LICENSE` file in this repository is MIT (matching
  basin-js). These must agree before publishing. Update `Cargo.toml` to
  `license = "MIT"` once the team has confirmed the final choice.
- **`repository` field:** `Cargo.toml` points at `github.com/basin-db/basin`
  (the engine monorepo). Update to the canonical URL for this standalone repo
  once it is established.
- **`docs.rs` all-features:** `[package.metadata.docs.rs] all-features = true`
  is already set, so docs.rs will build with both `realtime` and `arrow`
  enabled. Verify the rendered docs before publishing.
- **Semver stability:** audit the public API surface (especially `ArrowResult`,
  `ServerFrame`, `SubscribeOptions`, and `QueryResult`) and mark anything
  intentionally unstable with `#[doc(hidden)]` or a `#[non_exhaustive]`
  attribute. `BasinError` already carries `#[non_exhaustive]`.
- **Integration test infrastructure:** the existing unit tests use `wiremock`
  for HTTP mocking. Add a thin integration-test harness that can run against a
  real basin-engine binary (gated behind a `#[ignore]` or `BASIN_TEST_URL` env
  check) to catch regressions before each publish.

---

## 0.6 — Examples directory

Add `examples/` with runnable single-file programs:

- `examples/quickstart.rs` — sign in, query, insert, paginate.
- `examples/realtime.rs` — subscribe to table changes with reconnect.
- `examples/arrow_query.rs` — IPC download + RecordBatch iteration.
- `examples/storage.rs` — bucket create, upload, signed URL.

These examples will be linked from docs.rs automatically and serve as the
primary copy-paste surface for new users.

---

## Priority ordering

1. **Presence sends (0.2)** — the only functional gap vs. the rest of the SDK
   family; unblocks multiplayer / collaborative features.
2. **crates.io publish (0.5)** — required before `cargo add basin-sdk` works;
   fixing the license field is the immediate prerequisite.
3. **NDJSON streaming (0.3)** — unlocks low-memory large-result paths for
   callers who don't want Arrow; also closes the auto-promotion correctness gap.
4. **Examples (0.6)** — pure DX; needed for a credible docs.rs page.
5. **Richer filters (0.4)** — best done in lock-step with the engine grammar
   as each op lands.
