# basin-sdk (Ruby) — Roadmap

The shape this SDK is reaching for: a complete, idiomatic Ruby client for
Basin that can be used in Rails, Sinatra, Sidekiq workers, and plain scripts
without surprises — production-grade auth and data access today, async and
columnar analytics as the Ruby ecosystem matures.

v0.1 (query builder, auth, storage, functions, realtime, NDJSON streaming,
Arrow IPC, RubyGems gemspec, rspec suite) shipped and is preserved in git
history. Everything below is forward work.

---

## 0.2 — Publish to RubyGems + hardened packaging

The gem source is ready but has never been published. Before any user can
`gem install basin-sdk`, a few packaging gates need to pass.

### 0.2.1 RubyGems release

- Run `gem build basin.gemspec` and `gem push basin-sdk-0.1.0.gem` from CI on
  tag (GitHub Actions `push: tags: ['v*']` trigger).
- Pin a GPG signing key in the gemspec (`spec.signing_key` /
  `spec.cert_chain`) so the gem is verifiable.
- Add a `CHANGELOG.md` with the 0.1.0 surface; keep it up to date per release.

### 0.2.2 CI matrix hardening

The `test.yml` workflow runs rspec on Ruby 3.1 / 3.2 / 3.3. Outstanding gaps:

- Add a `ruby-head` slot with `continue-on-error: true` to catch breakage early.
- Run `gem build basin.gemspec` in CI to catch gemspec regressions.
- Add WebMock-based integration tests for every public method — current
  coverage is partial (auth + query well-covered; storage signed-URL path and
  all `FunctionsClient` verbs under-tested).

### 0.2.3 Bundled examples

`examples/` directory with three runnable scripts:

- `examples/quickstart.rb` — Client.new, sign_in, query, insert.
- `examples/realtime.rb` — subscribe, run_loop, presence.
- `examples/streaming.rb` — 1M-row NDJSON stream with a progress counter.

These serve as both documentation and smoke tests against a live engine.

---

## 0.3 — Async / Fiber client

Per the SDK parity matrix, `basin-ruby` is sync-only (blocking `net/http`).
That is acceptable for most Rails apps (Puma multi-thread), but it means:

- Long-running `realtime.run_loop` calls block the thread entirely.
- Streaming large NDJSON responses on a Fiber scheduler (Ractor, Falcon,
  Async gem) is not possible without wrapping in a thread.

The Python SDK ships both `BasinClient` (sync) and `AsyncBasinClient` (async).
Ruby has the pieces — `Async::HTTP` from the `async-http` gem, Ruby 3.x Fiber
scheduler, or `concurrent-ruby` `Future` — but the right choice depends on
which server stack Basin's Ruby users actually run.

Outcome:
- Evaluate `async-http` (Falcon-native, `Async::Task`) vs a thread-pool
  executor approach (safer for Puma/Sidekiq).
- Ship `Basin::AsyncClient` as a separate optional entry point so the sync
  `Basin::Client` surface is unchanged.
- Realtime `run_loop` becomes a supervised `Async::Task` on the async path
  instead of blocking a thread.

This is not blocking 0.2 — decide based on user demand.

---

## 0.4 — SQLSTATE on errors

The SDK parses and surfaces the `sqlstate` field from the error envelope on
`Basin::ApiError#sqlstate` (code already present in `errors.rb`). However,
this field is documented only in the source; it is absent from the README
error-handling section and from the rspec suite.

Outstanding work:
- Add a rspec example covering `e.sqlstate == "23505"` for unique-violation
  handling (WebMock a 409 response with `sqlstate` in the body).
- Surface `sqlstate` in `ApiError#to_s` only when non-nil (already done) —
  verify the output format matches what operators expect in log aggregators.
- Document common SQLSTATE codes in the README error-handling section
  (`23505` unique, `23503` FK, `40001` serialization failure).

---

## 0.5 — SSE realtime transport

The engine exposes a second realtime transport:
`GET /realtime/v1/sse/:project/:table` — server-sent events for read-only
single-table subscriptions. It is lighter than WebSocket (no optional gem
dependency, standard HTTP chunked response) and is the right default for
simple change-feed use cases.

The JS SDK picks SSE automatically for single-table listeners with no presence
and no dynamic filters. Ruby should do the same:

- Implement `Basin::SseRealtimeClient` using `net/http` chunked reads (no
  extra gem required).
- Wire `Basin::RealtimeClient` to use SSE when the channel only subscribes to
  one table with no presence; escalate to WebSocket otherwise.
- Handle `Last-Event-Id` reconnect header for replay.

---

## 0.6 — DX polish

The smaller things that compound into "this gem feels considered."

- **Retry + backoff on transient failures.** The HTTP layer currently does not
  retry. Add configurable retry logic (network errors, 5xx, 429 with
  `Retry-After`) with sensible defaults; opt-out per-request via `retries: 0`.
- **Connection reuse.** `net/http` opens a new connection per request. Add a
  `Net::HTTP.start` persistent-connection mode (opt-in on `Basin::Client.new`
  via `persistent: true`) for environments where per-request connection
  overhead is measurable.
- **Cursor pagination helper.** `QueryBuilder#each_page` — an `Enumerator`
  that walks `next_cursor` transparently, so callers never manage the cursor
  token by hand.
- **Admin namespace.** Wrap `/admin/v1/projects/*` for provisioning and
  rotating pgwire credentials (`provision`, `rotate_credentials`,
  `list_credentials`). Currently reachable via `client.request(...)` only.

---

## Priority ordering

1. **RubyGems publish** (0.2.1) — nothing else matters until users can
   `gem install basin-sdk`.
2. **CI hardening + examples** (0.2.2 / 0.2.3) — gate on green before publish.
3. **SQLSTATE docs + tests** (0.4) — cheap, high impact for PostgreSQL-aware
   callers handling constraint violations.
4. **SSE transport** (0.5) — removes the `websocket-client-simple` dependency
   for the common single-table subscribe case.
5. **Async client** (0.3) — deliver when Falcon / Async gem adoption warrants.
6. **DX polish** (0.6) — fill in around the edges as the surface stabilises.
