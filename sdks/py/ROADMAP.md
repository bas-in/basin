# basin-py — Roadmap

The shape this SDK is reaching for: a basin-native Python client that's
familiar to anyone who's used a modern BaaS SDK (Supabase-shaped surface),
but exposes basin's distinctive capabilities — cursor pagination, NDJSON
streaming, OpenAPI introspection, per-project credential admin, the Iceberg
catalog — as first-class features rather than ports of someone else's surface.

This repo is the Python sibling of [`basin-js`](../basin-js). The public shape
is already settled in basin-js; **basin-py mirrors it 1:1** so docs and support
don't fork. Where the languages differ, we follow Python idiom (async-first on
`httpx`, an optional sync facade, `async for` instead of `AsyncIterable`,
`pyiceberg` interop instead of a hand-rolled catalog client) — but never the
*method names* or *route shapes*. Every drift between SDKs is a docs + support
tax later.

Nothing is built yet. v0.1 (Phase 0.1 below) is the immediate foundation;
everything after mirrors basin-js's shipped 0.2–0.5.

---

## Design constraints (decided up front, see `decisions.md`)

- **Async-first.** The core client is `httpx.AsyncClient`-backed. A thin
  **sync facade** (`basin.SyncClient`) wraps every coroutine via a private
  event loop so synchronous callers (scripts, notebooks, Django views) aren't
  excluded. The async client is the source of truth; the sync facade is
  generated/delegated, never a parallel implementation.
- **One runtime dep: `httpx`.** Typed row models via `pydantic` v2 are an
  **optional extra** (`pip install basin[pydantic]`), never required. Realtime
  WebSocket support pulls `websockets` only under the `[realtime]` extra; SSE
  rides on `httpx` (no extra).
- **Typed.** Ship a `py.typed` marker. Public API fully annotated; `mypy
  --strict` clean. Generated `database` types target both plain `TypedDict`
  (zero-dep) and `pydantic` models (under the extra).
- **Python 3.9+.** Use `from __future__ import annotations`; no 3.10-only
  syntax in public modules.
- **Tooling:** `pyproject.toml` (hatchling build backend), `uv` for dev/CI,
  `ruff` (lint + format), `mypy --strict`, `pytest` + `pytest-asyncio` +
  `respx` (httpx mock transport).
- **Package name:** import package `basin`; PyPI distribution `basin` if
  available, else `basin-sdk` (resolve in T-001).

---

## 0.1 — Foundation (not yet built — the immediate work)

The parity floor: everything basin-js shipped in its own v0.1. Until this
lands there is no SDK.

- **Packaging + CI scaffold.** `pyproject.toml`, `src/basin/` layout,
  `py.typed`, ruff/mypy/pytest config, GitHub Actions matrix (3.9–3.13),
  MIT `LICENSE`.
- **`create_client(url, key) -> AsyncClient`** + the `Client` class holding
  the base URL, anon/service key, and a shared `httpx.AsyncClient`. Sets the
  `apikey` + `Authorization: Bearer` headers basin expects.
- **Error model.** `BasinError(code, message, *, status=None, details=None)`
  with a stable `code` taxonomy matching basin-js (`network`,
  `invalid_response`, `unauthorized`, `not_found`, `conflict`,
  `rate_limited`, `server_error`, `not_implemented`). One error type; callers
  branch on `.code`.
- **Auth — password + magic-link.** `client.auth.sign_up`,
  `sign_in_with_password`, `sign_in_with_otp` (magic link),
  `verify_otp`, `sign_out`, `get_session` / `refresh_session`. Session stored
  on the client; bearer swaps to the user token after sign-in.
- **Query builder — `client.from_(table).select()`** with the PostgREST
  filter surface basin honours: `eq/neq/gt/gte/lt/lte/like/ilike/is_/in_/
  contains`, `order`, `limit`, `range`, `single`/`maybe_single`, plus
  `insert`/`update`/`upsert`/`delete` terminals. `await` a builder to execute
  (it's an awaitable). Returns `APIResponse(data, count, ...)`.
- **Sync facade** for all of the above (`SyncClient`, `client.from_(...)`
  awaitable-free).

---

## 0.2 — basin-distinctive surface

Three pillars where basin can lead instead of follow. Pure SDK work; no engine
changes needed. Highest-leverage things we can do once 0.1 exists. Mirrors
basin-js 0.2.

### 0.2.1 Cursor pagination + streaming on the query builder

The engine returns `{rows, next_cursor}` and accepts `?cursor=…` for O(1) seek.
It also auto-promotes large responses to NDJSON (one row per line, trailing
`{"_basin_next_cursor":"…"}` sentinel) past ~1 MiB or 10,000 rows — even if the
caller never asked. A naive builder silently breaks on large queries.

Outcome:
- `.cursor(token)` modifier.
- `client.from_(t).select().paginate()` returns an `AsyncIterator[Row]` that
  walks `next_cursor` transparently (`async for row in ...`).
- `client.from_(t).select().stream()` returns an `AsyncIterator[Row]` backed
  by `?stream=true`, reading NDJSON line-by-line off `httpx`'s
  `aiter_lines()`.
- The execute path **auto-detects** NDJSON by content-type and parses it even
  when the caller didn't ask — large `.select()` calls just work.

### 0.2.2 OpenAPI-driven types

The engine ships `GET /rest/v1/_openapi.json` — a per-project OpenAPI 3.0.3
doc auto-generated from each table's Arrow schema. One fetch → typed rows, no
separate codegen dance.

Outcome:
- `fetch_openapi(url, anon_key)` helper returning the parsed document.
- `python -m basin.codegen --url … --key … --out database.py` (also exposed
  as a `basin-gen-types` console script): emits per-table `TypedDict`s
  (`{Table}Row` / `{Table}Insert` / `{Table}Update`) and, under
  `--pydantic`, pydantic v2 models.
- README quickstart documents the typed flow as the default.

### 0.2.3 `client.admin.*` namespace

The engine exposes operator routes under `/admin/v1/*` for provisioning +
rotating per-project pgwire credentials. SaaS operators on basin need a Python
path.

Outcome:
- `client.admin.projects.provision(project_id) -> {connection_string}`.
- `client.admin.projects.rotate_credentials(pgwire_user) -> {connection_string}`.
- `client.admin.projects.list_credentials(project_id) -> list[Credential]`
  (metadata only — no plaintext hashes).
- 401 → typed `BasinError("unauthorized", …)` when claims lack `is_admin`.

---

## 0.3 — Server-route follow-on (engine routes already shipped)

basin's engine landed realtime, the RPC mount, engine `DELETE`, auth v2
(ADR 0020), and object storage (ADR 0021). Route shapes are final and match
basin-js — build straight against them.

- `from_(t).delete()` — engine `DELETE` (Iceberg copy-on-write) is live.
- `client.functions.invoke(name, body)` → **`POST /rest/v1/rpc/:fn_name`**
  (basin 5.11.L). Body is a JSON object of named args; both `LANGUAGE sql`
  and `LANGUAGE wasm` dispatch through this route.
- `sign_in_with_oauth(provider, redirect_to)` →
  **`GET /auth/v1/authorize?provider=…&redirect_to=…`** (presets + generic
  OIDC, ADR 0020; PKCE + signed `state` server-side; `GET /auth/v1/callback`
  completes).
- `client.auth.mfa.*` → **`POST /auth/v1/factors`** (enroll),
  **`/factors/:id/verify`**, **`/factors/:id/challenge`**,
  **`/factors/:id/challenge/verify`**, **`DELETE /factors/:id`**. TOTP +
  WebAuthn/passkeys ship together; JWT carries `aal` + `amr`.
- `client.storage.from_(bucket).*` →
  **`/storage/v1/object/:bucket/:path`** (upload/download/delete),
  **`/storage/v1/object/list/:bucket`**,
  **`/storage/v1/object/sign/:bucket/:path`**; `.remove(paths)` → bulk
  `DELETE /storage/v1/object/:bucket` with a `{prefixes}` body. Catalog-backed
  (`storage.objects` + RLS).

---

## 0.6 — Realtime (engine shipped; SDK is the remaining work)

Mirror basin-js's `channel()` ergonomics. Two transports; the SDK picks based
on what the channel needs.

**SSE — read-only, single table.** `GET /realtime/v1/sse/:project/:table`,
bearer auth. One JSON event per committed `INSERT`/`UPDATE`/`DELETE`,
RLS-filtered; 15s heartbeat comments; `Last-Event-Id` replays missed events on
reconnect. Implemented on `httpx`'s streaming response (no extra dep). Use
when a channel listens to one table and never needs presence.

**WebSocket — multiplexed, bidirectional.** `GET /realtime/v1/ws/:project`.
One socket carries many table subscriptions + presence + mid-stream filter
changes. JSON control plane (`type`-tagged): `subscribe`/`unsubscribe`
(optional `filter` for predicate pushdown), `presence_track`/`untrack`/
`heartbeat`; server emits `event`/`subscribed`/`error`/`presence_state`/
`presence_diff`. Pulls the `websockets` lib under the `[realtime]` extra.

SDK routing rule (identical to basin-js): single table, `postgres_changes`
only, no presence, no dynamic filter → SSE. Anything with presence, multiple
tables, or dynamic filters → WS. Reconnect-with-replay via `Last-Event-Id`
(SSE) or re-subscribe + `seq` gap detection (WS).

---

## 0.4 — DX polish

- **Sync facade hardening.** `SyncClient` covers the entire async surface
  (including `paginate`/`stream` as plain generators, and `channel()` via a
  background thread). This is the Python-specific headline feature — many
  Python users are sync-first.
- Configurable retry + exponential backoff on transient failures (network,
  5xx, 429 with `Retry-After`). Sensible defaults, per-call opt-out.
- No throwing stubs. Unimplemented surface raises `BasinError("not_implemented")`
  with a clear message *or* isn't exported — never a bare `NotImplementedError`
  with no guidance.
- Per-request `Prefer:` header pass-through (PostgREST convention basin honours).
- `py.typed` shipped + `mypy --strict` green across the public API; a
  `tests/typing/` set of `reveal_type` assertions guards inference.

---

## 0.5 — Iceberg catalog client (Python's natural advantage)

The engine ships a Lakekeeper-compatible Iceberg REST catalog at
`/iceberg/v1/:warehouse/*`. **Unlike JS, Python already has `pyiceberg`** — so
basin-py's story is interop, not a hand-rolled client.

Outcome:
- `client.iceberg.catalog_url(warehouse)` + a documented recipe:
  `pyiceberg.catalog.load_catalog("basin", uri=…, token=…)` against the
  engine's REST catalog, authenticated with a project token.
- A `basin.iceberg.load_catalog(client, warehouse)` convenience that hands
  back a configured `pyiceberg` `Catalog` when the `[iceberg]` extra is
  installed; clear error pointing to `pip install basin[iceberg]` otherwise.
- Decide (when demand is real) whether to vendor more than the convenience.

---

## 1.0 — Parity + release

Once 0.2 + 0.3 land and match basin-js method-for-method:
- Published to PyPI (and the public surface frozen against basin-js's).
- Parity test: a shared fixture matrix asserting basin-py and basin-js
  produce identical request shapes for the same calls.
- Docs site section + quickstart parity with basin-js.

Sibling SDKs after this (`basin-rs`, `basin-go`, `basin-dart`, …) clone the
same template, per the basin-js roadmap.

---

## Priority ordering

1. **0.1 foundation** — nothing exists; this is the gate. Within it:
   packaging → client/errors → query builder → auth → sync facade.
2. **Streaming correctness** (0.2.1 NDJSON auto-detect) — a bug class, not a
   feature: large queries break without it.
3. **Cursor + `paginate()`/`stream()`** — biggest "basin feels different and
   better" win on the data path.
4. **OpenAPI codegen** (0.2.2) — one fetch → typed tables; big DX moment.
5. **`client.admin.*`** (0.2.3) — unblocks SaaS builders.
6. **0.3 server routes** — opportunistically; engine routes are live.
7. **Realtime** (0.6), **DX polish** (0.4), **Iceberg** (0.5) — once the data
   + auth surface is stable.
