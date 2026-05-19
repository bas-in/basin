# basin-js — Roadmap

The shape this SDK is reaching for: a basin-native JS/TS client that's familiar
to anyone who's used a modern BaaS SDK, but exposes basin's distinctive
capabilities — cursor pagination, NDJSON streaming, OpenAPI introspection,
per-project credential admin, the Iceberg catalog — as first-class features
rather than ports of someone else's surface.

v0.1 (createClient, password + magic-link auth, `from()` query builder, error
model, npm/JSR/CI scaffolding) shipped and is preserved in git history.
Everything below is forward work.

---

## 0.2 — basin-distinctive surface

Three pillars where basin can lead instead of follow. All pure SDK work; no
engine changes needed. These are the highest-leverage things we can do today.

### 0.2.1 Cursor pagination + streaming on the query builder

The engine returns `{rows, next_cursor}` and accepts `?cursor=…` for O(1) seek
on subsequent pages. It also auto-promotes large responses to NDJSON (one row
per line, trailing `{"_basin_next_cursor":"…"}` sentinel) past ~1 MiB or
10,000 rows — even if the caller never asked for streaming. The current
builder ignores both, which means large queries silently break or paginate
wrong.

Outcome:
- `.cursor(token?)` modifier on the query builder.
- `await basin.from(t).select().paginate()` returns an `AsyncIterable<Row>`
  that walks `next_cursor` transparently.
- `await basin.from(t).select().stream()` returns an `AsyncIterable<Row>`
  backed by `?stream=true` and reads NDJSON line-by-line.
- The builder's existing `then()` path detects NDJSON content-type and parses
  it correctly even when the caller didn't ask — large `.select()` calls just
  work.

### 0.2.2 OpenAPI-driven types

The engine ships `GET /rest/v1/_openapi.json` — a per-project OpenAPI 3.0.3
document auto-generated from the Arrow schema of every table. We can turn
that single fetch into typed table rows without a separate CLI codegen step.

Outcome:
- `bin/basin-js-gen` (or `npx @bas-in/basin-js gen-types`): fetches the
  OpenAPI doc with a project's anon key and emits `database.types.ts` with
  per-table `Row` / `Insert` / `Update` shapes.
- README quickstart updated so the typed flow is the documented default:
  `import type { Database } from './database.types'`,
  `createClient<Database>(url, key)`, then `basin.from('users')` infers
  `User` automatically.

### 0.2.3 `basin.admin.*` namespace

The engine exposes operator-grade routes under `/admin/v1/*` for provisioning
per-project pgwire credentials and rotating them. SaaS operators building on
basin need a JS path to these; today there isn't one.

Outcome:
- `basin.admin.projects.provision({ projectId })` →
  `{ connectionString }`.
- `basin.admin.projects.rotateCredentials(pgwireUser)` →
  `{ connectionString }`.
- `basin.admin.projects.listCredentials(projectId)` →
  `Credential[]` (metadata only — no plaintext hashes).
- Calls 401 cleanly when the session's claims lack `is_admin`; surface a
  typed `BasinError("unauthorized", …)` so callers can route to a
  permissions-gate UI.

---

## 0.3 — Server-route follow-on (waiting on basin v0.2 engine routes)

These methods exist in the SDK today as `not_implemented` stubs. They flip
to live the moment the engine ships the underlying route. The SDK work is
mostly threading bodies through and converting stubs into real network
calls — none of it should be blocked on architectural decisions.

- `signInWithOAuth` → `/auth/v1/oauth/:provider/authorize` + callback handler
- `auth.mfa.{enroll, verify, unenroll}` → `/auth/v1/mfa/*` (TOTP first;
  WebAuthn is a follow-on)
- `storage.from(bucket).{upload, download, list, remove, createSignedUrl}` →
  `/object/*` (single-shot first; multipart + TUS resumable behind feature
  flags that flip when the engine ships them)
- `realtime.channel().on().subscribe()` → WebSocket transport against the
  engine's broadcast surface
- `functions.invoke()` → `/v1/projects/:ref/functions/:slug/invoke`
- `from().delete()` — engine currently returns 501; either fix engine-side
  or have the SDK throw a clearer "not yet" error than the raw 501

---

## 0.4 — DX polish

The smaller things that compound into "this SDK feels considered."

- Configurable retry + exponential backoff on transient failures (network
  errors, 5xx, 429 with `Retry-After`). Defaults sensible, opt-out per-call.
- `@bas-in/ssr` companion package for cookie-based session helpers in
  Next.js (App Router) / SvelteKit / Nuxt / Remix. Pure client-side; no
  engine changes.
- Stop shipping stubs that throw. The current `not_implemented` methods are
  worse DX than "the method doesn't exist." Either gate the types behind a
  feature flag so unimplemented methods don't appear in autocomplete, or
  remove the surface entirely until 0.3 lights them up.
- Per-request `Prefer` header pass-through audit — verify callers can set
  arbitrary `Prefer:` headers (PostgREST convention basin honours).
- Coverage hole audit on the postgrest builder — `.csv()`, `.geojson()`,
  `.explain()` get one unit test each (not just type-level).

---

## 0.5 — Iceberg catalog client

The engine ships a Lakekeeper-compatible Iceberg REST catalog at
`/iceberg/v1/:warehouse/*`. Spark / Trino / DuckDB / pyiceberg can already
talk to it. There's no JS story yet.

Options to decide between when this comes up:
- Ship `basin.iceberg.*` inside the main SDK (heavier dep but one install).
- Ship `@bas-in/basin-iceberg` as a separate package (cleaner but two
  installs; the people who want this will know).
- Skip the JS client entirely and document "use pyiceberg / use Spark, here's
  the catalog URL."

Decide based on demand. Not blocking 0.2–0.4.

---

## 1.0 — Sibling SDKs

Once 0.2 + 0.3 land and the public shape is stable, the same template clones
into other languages. Each gets its own repo + task list:

- `basin-py` — Python (httpx async)
- `basin-rs` — Rust (reqwest + serde)
- `basin-go` — Go (stdlib net/http; can share types with basin-cli)
- `basin-dart` — Dart (Flutter)
- `basin-swift` — Swift (iOS / macOS)
- `basin-kotlin` — Kotlin (Android / KMP)

Don't start any of these until the JS surface is settled — every drift
between SDKs is a docs + support tax later.

---

## Priority ordering

Within 0.2–0.4, the work is ordered:

1. **Streaming correctness** (0.2.1 stream-detection in `then()`) — this is a
   bug, not a feature. Large queries break today.
2. **Cursor + paginate iterator** (rest of 0.2.1) — biggest "basin feels
   different and better" win on the data path.
3. **OpenAPI codegen** (0.2.2) — one fetch unlocks typed tables without a CLI
   dance. Massive DX moment.
4. **`basin.admin.*`** (0.2.3) — unblocks SaaS builders and operators.
5. **0.3 stubs** — opportunistically, as each engine route lights up.
6. **DX polish** (0.4) — fill in around the edges.
7. **Iceberg** (0.5) and **sibling SDKs** (1.0) — only when the JS surface is
   stable.
