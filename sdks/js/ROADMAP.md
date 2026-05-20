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

## 0.3 — Server-route follow-on

### Now unblocked — engine routes shipped (basin OSS, 2026-05-20)

The basin engine landed the realtime stack, the RPC mount, and engine
`DELETE` this cycle. These SDK methods are no longer waiting on
architectural decisions — the route shapes are final and documented
below. See **0.6 — Realtime** for the full protocol.

- `from().delete()` — **engine `DELETE` shipped** (Iceberg copy-on-write).
  The 501 is gone; the SDK `delete()` path is live (T-027 ✅).
- `functions.invoke()` → **`POST /rest/v1/rpc/:fn_name`** (basin 5.11.L).
  Body is a JSON object of named args; response is the function result
  (scalar bare value, or array of row objects for `RETURNS TABLE`). Both
  `LANGUAGE sql` and `LANGUAGE wasm` functions dispatch through the same
  route. **Not** the old `/v1/projects/:ref/functions/:slug/invoke` shape.
- `realtime.channel().on('postgres_changes', …).subscribe()` →
  **SSE `GET /realtime/v1/sse/:project/:table`** for read-only single-table
  subscriptions, **WebSocket `GET /realtime/v1/ws/:project`** for
  multi-table multiplexing + presence + mid-stream filter changes.
  Full protocol in 0.6.

### Still waiting — engine routes not yet shipped

- `signInWithOAuth` → `/auth/v1/oauth/:provider/authorize` + callback handler
- `auth.mfa.{enroll, verify, unenroll}` → `/auth/v1/mfa/*` (TOTP first;
  WebAuthn is a follow-on)
- `storage.from(bucket).{upload, download, list, remove, createSignedUrl}` →
  `/object/*` (engine has no object-storage HTTP surface yet; keep stubbed)

---

## 0.6 — Realtime (engine shipped; SDK is the remaining work)

The basin engine ships a complete realtime stack (basin 5.11.R1–R7). The
SDK mirrors Supabase's `channel()` ergonomics on top of it. Two transports;
the SDK picks based on what the channel needs.

**SSE — read-only, single table.** `GET /realtime/v1/sse/:project/:table`,
`Authorization: Bearer <jwt>`. Server streams one JSON event per committed
`INSERT`/`UPDATE`/`DELETE`; RLS-filtered per the caller's policies; 15s
heartbeat comment frames; `Last-Event-Id` request header replays missed
events from the durable log on reconnect. Use this when the channel only
listens to one table and never needs presence.

**WebSocket — multiplexed, bidirectional.**
`GET /realtime/v1/ws/:project`. One socket carries many table
subscriptions. JSON control plane (`tag = "type"`):

- Client → server:
  - `{"type":"subscribe","table":"orders"}` — optional
    `"filter":"NEW.status = 'paid'"` for server-side predicate pushdown
  - `{"type":"unsubscribe","table":"orders"}` (socket stays open)
  - `{"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{…}}`
  - `{"type":"presence_untrack","channel":"room:1","client_id":"c1"}`
  - `{"type":"heartbeat","channel":"room:1","client_id":"c1"}` (every 30s;
    server evicts after 90s of silence)
- Server → client:
  - `{"type":"event","project":"…","table":"orders","op":"INSERT","after":{…},"seq":42}`
  - `{"type":"subscribed","table":"orders"}` / `{"type":"unsubscribed","table":"orders"}`
  - `{"type":"error","code":"lag","table":"orders","missed":5}`
  - `{"type":"presence_state","channel":"room:1","presences":[…]}`
  - `{"type":"presence_diff","channel":"room:1","joins":[…],"leaves":[…]}`

SDK routing rule: a channel that only does `postgres_changes` on one table
with no presence and no mid-stream filter change → SSE. Anything with
presence, multiple tables, or dynamic filters → WS. Reconnect-with-replay
uses `Last-Event-Id` (SSE) or re-subscribe + `seq` gap detection (WS).

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
