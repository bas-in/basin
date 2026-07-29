# basin-go — Roadmap

The shape this SDK is reaching for: an idiomatic Go client that exposes
basin-engine's full REST surface in a way that feels native to the language —
context-propagation everywhere, functional options, typed errors with
SQLSTATE, range-over-func streaming, and Arrow IPC for analytics without a
JSON round-trip. Go programs should be able to treat Basin as a first-class
dependency with the same confidence they'd give `database/sql`.

v0.1 (client construction, auth, PostgREST query builder, Arrow IPC, NDJSON
streaming, storage, realtime WebSocket, functions, typed error model, CI
scaffolding) shipped and is preserved in git history. Everything below is
forward work.

---

## 0.2 — Parity completions and DX polish

Small gaps relative to the richer SDKs (Rust, Python) that are pure SDK work
with no engine changes needed.

### 0.2.1 Richer PostgREST filter operators

The query builder today covers `eq/neq/gt/gte/lt/lte/in/is` — the core set.
Basin's PostgREST-style parser accepts several more operators that are absent
from this SDK and all other SDKs in the family:

- `like` / `ilike` — pattern matching (`%` wildcard)
- `not` — filter negation
- `or` — OR-group of filters
- Resource embedding via dot-notation selects (e.g. `select=id,orders(total)`)

These are the `or/not/like/ilike` gap marked `❌` across all ten SDKs in the
parity matrix. Adding them here, in Go where static typing makes the API
easiest to express, creates a template the other SDKs can follow. None require
an engine change.

### 0.2.2 Publish to pkg.go.dev and the module proxy

The module path `github.com/vul-os/basin/sdks/go` is correct, but the
module has not been tagged and pushed through `proxy.golang.org` yet. Until a
`v0.1.0` tag exists and the module is indexed, `go get` works only against
the git HEAD — not a stable release.

Outcome:
- Tag `v0.1.0`, verify `pkg.go.dev` picks it up within a few minutes of the
  tag push.
- Add a `CHANGELOG.md` that records what shipped in each tag.
- CI: add a step that runs `go list -m github.com/vul-os/basin/sdks/go`
  to smoke-test that the module can be fetched from the proxy.

### 0.2.3 Typed error-code constants

Today error codes are checked by string comparison: `be.Code == "E_NOT_FOUND"`.
That is fine, but discoverable typed constants eliminate typos and enable IDE
auto-complete:

```go
if be.Code == basin.ErrNotFound { ... }
```

The existing `KnownErrorCodes` slice in `errors.go` is the list to promote to
typed `const` values. The `Code string` field on `BasinError` stays as-is for
forward compat with unknown codes from newer engine versions.

---

## 0.3 — Streaming iterator for paginated Arrow

Arrow IPC pagination today requires manual `Cursor` chaining across multiple
`.Arrow(ctx)` calls. A higher-level iterator would walk `X-Basin-Next-Cursor`
transparently:

```go
for result, err := range client.Table("events").Limit(10000).Pages(ctx) {
    if err != nil { break }
    // result is an *ArrowResult covering one page
    process(result.Records)
    result.Release()
}
```

The `Stream` iterator on the query builder already does this for NDJSON rows.
The Arrow equivalent is the remaining gap: the parity matrix shows Go carries
Arrow IPC (`✅`) but lacks NDJSON streaming (`❌`). In practice Go has both —
`Stream` covers NDJSON and `Arrow` covers columnar — but a page iterator for
Arrow would make large analytical scans idiomatic.

---

## 0.4 — Retry and resilience

- Configurable retry with exponential backoff on transient failures: network
  errors, 5xx responses, 429 with `Retry-After`. Default: 3 attempts with
  jittered backoff. Opt-out per-call via a `WithNoRetry()` option.
- The realtime reconnect already does this; the REST path does not.
- A `RetryPolicy` functional option on `New(...)` covers the common cases;
  callers who need more control pass a custom `*http.Client` with a round-trip
  wrapper.

---

## 0.5 — Examples directory and integration tests

- `examples/` subdirectory with runnable `main.go` programs for the most
  common patterns: query + stream, storage upload/download, realtime
  subscribe, Arrow analytics.
- An integration test (`_test.go` with a build tag `//go:build integration`)
  that runs against a local basin-engine binary. The CI matrix already has the
  `go test ./...` step; adding `-tags integration` in a separate job (with a
  `services:` or `needs:` that starts the engine) would give end-to-end
  coverage without polluting the unit test run.

---

## 1.0 — Stable API

Once 0.2–0.4 land, the public surface is frozen under a `v1.0.0` tag. The
only remaining pre-1.0 risks:

- `QueryBuilder.Stream` uses `iter.Seq2` (Go 1.23 range-over-func). If Go 1.21
  support must be kept, a callback form needs to be offered alongside or the
  minimum version bumped to 1.23 before the tag.
- The `EnrollFactor` / `ChallengeFactor` return type is `any` — convenient but
  untyped. A `Factor` interface with a `FactorType() string` method would be a
  cleaner contract.
- The module path (`github.com/vul-os/basin/sdks/go`) may move to a
  standalone repo (`github.com/bas-in/basin-go`) to match the other SDK repos.
  That is a breaking import path change; it must happen before `v1.0.0`.
