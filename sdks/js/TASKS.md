# basin-js — Tasks

Forward-looking task list for v0.2+. v0.1 completion log is in git history
(see `git log -- TASKS.md` around 2026-05-11).

Tasks are sized for one Sonnet agent each (~30–60 min of focused work).
Every task names the file(s) it touches, the acceptance criteria, and any
references it needs. No hidden context — an agent reading the task in
isolation should be able to start.

Conventions:
- **Status:** `[ ]` pending, `[~]` in-progress (agent claimed), `[x]` done
- **Files:** absolute paths relative to repo root
- **Tests:** add to the existing `*.test.ts` next to the file being changed
- **Style:** TS-first; no comments unless WHY is non-obvious; match existing
  patterns in the file
- **No "I added X for Y reason" comments in code** — that belongs in the
  commit message, not the source

---

## Phase 0.2 — basin-distinctive surface

### T-001 — Fix NDJSON auto-streaming in `then()` (bug, highest priority) ✅

**Status:** done 2026-05-19. NDJSON branch added to `#execute()` after the
204 check; `nextCursor` threaded onto `PostgrestResponse`. 3 new tests in
`src/postgrest/builder.test.ts`, all 55 builder tests green.

**Why this is a bug, not a feature:** the engine auto-promotes any response
larger than ~1 MiB or 10,000 rows to NDJSON, even when the caller didn't
pass `?stream=true`. The current builder assumes a JSON body. Large queries
break today.

**Files:** `src/postgrest/builder.ts`, `src/postgrest/builder.test.ts`

**Scope:**
- In `#execute()` / the `then()` path, after the fetch resolves, branch on
  the response `Content-Type` header.
- If content-type contains `application/x-ndjson` or `application/jsonl`,
  read the body as text, split on newlines, parse each line as JSON.
- The final line is `{"_basin_next_cursor":"…"}` — peel it off, expose the
  cursor via the response (extend the `PostgrestResponse` shape to carry an
  optional `nextCursor: string | null` field).
- All other rows go into `data` as before.
- Keep the existing JSON path untouched for non-NDJSON responses.

**Acceptance criteria:**
- Unit test: fetch stub returns NDJSON with 3 rows + cursor sentinel →
  `data` is the 3 rows, `nextCursor` is the cursor string, no errors.
- Unit test: fetch stub returns NDJSON with 0 rows + sentinel → `data: []`,
  `nextCursor` set.
- Unit test: fetch stub returns NDJSON with rows and NO sentinel →
  `nextCursor: null`.
- Existing JSON / envelope / 204 tests still pass.
- `npm run typecheck` clean, `npm test` green.

**Reference:** server-side behaviour documented in
`basin/crates/basin-rest/src/lib.rs:50-55` (the `?stream=true` and auto-promotion comment).

---

### T-002 — `.stream()` modifier returning `AsyncIterable<Row>` ✅

**Status:** done 2026-05-19. `stream()` method ships on `PostgrestQueryBuilder`,
returns `AsyncIterable<T>` (NOT PromiseLike). 4 new tests in
`src/postgrest/builder.test.ts`. Merged cleanly with T-001's `#execute()`
NDJSON branch despite worktree base divergence.

**Files:** `src/postgrest/builder.ts`, `src/postgrest/builder.test.ts`,
`src/postgrest/types.ts`

**Depends on:** T-001 (NDJSON detection must work first).

**Scope:**
- Add `.stream()` method on `PostgrestQueryBuilder<T>` that returns an
  `AsyncIterable<T>` (NOT a Promise). It sets `?stream=true` on the URL
  and reads the response body as a `ReadableStream`, yielding rows as they
  arrive line-by-line. Skip the trailing `_basin_next_cursor` sentinel.
- Usage shape: `for await (const row of basin.from('events').select().stream())`.
- Implement via a small `TextDecoderStream` + line-buffering routine — no
  external deps.
- The method changes the return type away from `PromiseLike`; expose it as
  a separate terminal so the type system reflects this (it's `stream()`
  XOR `then()`, not both).

**Acceptance criteria:**
- Unit test: mock fetch returns a `ReadableStream` of 3 NDJSON lines +
  cursor sentinel → `for await` yields 3 rows in order, then completes.
- Unit test: stream errors mid-flight (mock stream throws) → caller's
  `for await` rejects with a `BasinError("network", …)`.
- Unit test: NDJSON line that fails to parse → `BasinError("invalid_response", …)`
  surfaced inside the iterator.
- Tree-shakeability: importing `basin.from(t).select()` without `.stream()`
  doesn't pull TextDecoderStream into the bundle.

---

### T-003 — `.cursor(token?)` modifier ✅

**Status:** done 2026-05-19. `cursor(token)` ships next to `.range()`.
3 new tests in `builder.test.ts`.

**Files:** `src/postgrest/builder.ts`, `src/postgrest/builder.test.ts`

**Scope:**
- Add `.cursor(token: string)` that sets `?cursor=<token>` on the URL —
  used for the next page after reading `nextCursor` from a previous
  response.
- `.cursor()` (no arg) is a no-op convenience; document that as such.
- Pairs with the existing `.limit()` — caller can do
  `.select().limit(100).cursor(nextCursor)` to advance pages.

**Acceptance criteria:**
- Unit test: `.cursor("abc")` puts `cursor=abc` on the rendered URL.
- Unit test: chained `.eq().limit().cursor()` preserves all three params.
- README in src/postgrest gets a one-line usage block (only if a README
  already exists there; do not create new docs files otherwise).

---

### T-004 — `.paginate()` AsyncIterable walker ✅

**Status:** done 2026-05-19. `paginate()` walks `next_cursor` through JSON pages.
5 tests green. Default page size 1000 when no `.limit()` set.

**Files:** `src/postgrest/builder.ts`, `src/postgrest/builder.test.ts`

**Depends on:** T-001, T-003.

**Scope:**
- Add `.paginate()` returning `AsyncIterable<T>` that walks `next_cursor`
  transparently: page 1 → read `nextCursor` → fetch page 2 with
  `?cursor=…` → repeat until `nextCursor` is `null`.
- Uses the wrapped `{rows, next_cursor}` shape, not NDJSON. (Callers who
  want NDJSON use `.stream()` instead.)
- Respects `.limit()` as page size; defaults to 1000 if unset.

**Acceptance criteria:**
- Unit test: mock fetch returns 3 pages (with cursors), `for await` yields
  all rows across pages in order.
- Unit test: mock fetch returns 1 page with `nextCursor: null` → iterator
  completes after that page.
- Unit test: mid-pagination fetch error → iterator throws with the same
  error shape `.then()` would produce.

---

### T-005 — OpenAPI fetch helper ✅

**Status:** done 2026-05-19. `fetchOpenAPI(url, anonKey, opts?)` ships in
`src/openapi/`. 7 tests green; `./openapi` sub-path export wired.

**Files:** new `src/openapi/index.ts`, `src/openapi/fetch.ts`,
`src/openapi/fetch.test.ts`; add `./openapi` sub-path export in
`package.json` and `tsup.config.ts`.

**Scope:**
- New module exposing `fetchOpenAPI(url, anonKey, opts?)` that fetches
  `GET ${url}/rest/v1/_openapi.json` with the anon-key header and returns
  the parsed OpenAPI 3.0.3 document as a typed object.
- Define a minimal TS type for the bits we care about: `paths`, `components.schemas`.
- Surface `BasinError("network", …)` / `BasinError("invalid_response", …)`
  on the usual failure modes.

**Acceptance criteria:**
- Unit test: fetch stub returns a valid OpenAPI doc → parsed correctly.
- Unit test: 404 → typed error.
- Unit test: malformed JSON → typed error.
- Sub-path import works: `import { fetchOpenAPI } from '@bas-in/basin-js/openapi'`.

**Reference:** server route at `basin/crates/basin-rest/src/routes/openapi.rs`.

---

### T-006 — `database.types.ts` codegen from OpenAPI ✅

**Status:** done 2026-05-19. `npx basin-js-gen-types --url --key --out` CLI ships;
pure `openAPIDocToTypes()` function in `src/codegen/`. 14 tests green.

**Files:** new `bin/gen-types.ts` (CLI entrypoint), wire into
`package.json` `bin` field as `basin-js-gen-types`, plus
`bin/gen-types.test.ts`.

**Depends on:** T-005.

**Scope:**
- CLI: `npx @bas-in/basin-js gen-types --url <basin-url> --key <anon> --out database.types.ts`.
- Pulls the OpenAPI doc via the T-005 helper, walks `components.schemas`,
  and emits a TS module exporting `interface Database { public: { tables: { [tableName]: { Row: …; Insert: …; Update: … } } } }`.
- `Row` = required props from the schema; `Insert` = `Row` with
  nullable/default columns made optional; `Update` = `Partial<Row>`.
- Map Arrow / OpenAPI primitives to TS: `integer` → `number`, `string` →
  `string`, `boolean` → `boolean`, `number` → `number`, formatted
  `date-time` → `string`, arrays → `T[]`, nullable → `T | null`.
- README quickstart gets an updated section demonstrating the typed flow:
  `createClient<Database>(...)` + `basin.from('users')` infers row type.

**Acceptance criteria:**
- Unit test: feed a small OpenAPI doc fixture → emits expected TS string.
- Manual smoke: running against a real basin-engine OpenAPI doc produces
  a compilable `database.types.ts`.
- Generated file is `prettier`-clean (run prettier programmatically on the
  output before writing).

---

### T-007 — `basin.admin` namespace scaffold ✅

**Status:** done 2026-05-19. `AdminClient` + `AdminProjectsClient` + types
shipped; `./admin` sub-path exports wired; root barrel re-exports added;
3 stub-shape tests green. Agent also fixed T-100 in the same commit.

**Files:** new `src/admin/index.ts`, `src/admin/client.ts`,
`src/admin/types.ts`, `src/admin/client.test.ts`; wire `admin: AdminClient`
into `src/client.ts`; add `./admin` sub-path export in `package.json` +
`tsup.config.ts`; re-export from `src/index.ts`.

**Scope:**
- New `AdminClient` class with the shape `basin.admin.projects.<method>`.
- Methods are stubs in this task — they just need correct signatures +
  type definitions + a "this returns `not_implemented` if you don't have
  is_admin claims" comment. The bodies land in T-008, T-009, T-010.
- Types: `Credential { id, project_id, pgwire_user, created_at, last_used_at, revoked_at }`,
  `ProvisionResult { connectionString }`.
- Follow the existing namespace pattern (look at `src/storage/client.ts`
  for the `{url, headers, fetch, auth}` adapter pattern).

**Acceptance criteria:**
- `basin.admin.projects.provision`, `rotateCredentials`, `listCredentials`
  exist as typed stubs that return `not_implemented`.
- Sub-path import: `import { AdminClient } from '@bas-in/basin-js/admin'`.
- `npm run typecheck` clean.

---

### T-008 — `basin.admin.projects.provision()` ✅

**Status:** done 2026-05-19. POST `/admin/v1/projects`; 401/403 → `unauthorized`. Tested.

**Files:** `src/admin/client.ts`, `src/admin/client.test.ts`

**Depends on:** T-007.

**Scope:**
- POST `${url}/admin/v1/projects` with `{project_id}` body.
- Returns `{connectionString: string}` (server response is
  `{"postgres://<user>:<pass>@host:5433/db"}` — extract / type it cleanly).
- Auth: bearer from the active session. If no session or claims lack
  `is_admin`, the server returns 401/403 — surface as
  `BasinError("unauthorized", …)` with status preserved.

**Acceptance criteria:**
- Unit test: happy path with mock fetch returning the connection string.
- Unit test: 401 → typed `unauthorized` error.
- Unit test: network failure → typed `network` error.

---

### T-009 — `basin.admin.projects.rotateCredentials()` ✅

**Status:** done 2026-05-19. POST `/admin/v1/projects/:user/rotate`; 404 → `not_found`.

**Files:** `src/admin/client.ts`, `src/admin/client.test.ts`

**Depends on:** T-007.

**Scope:**
- POST `${url}/admin/v1/projects/${pgwireUser}/rotate` with no body.
- Returns the new `{connectionString}`.
- Same error model as T-008.

**Acceptance criteria:**
- Unit test: happy path.
- Unit test: 401 unauthorized.
- Unit test: 404 unknown pgwire user → typed `not_found`.

---

### T-010 — `basin.admin.projects.listCredentials()` ✅

**Status:** done 2026-05-19. GET `/admin/v1/projects/:id/credentials`; empty list returns `[]`.

**Files:** `src/admin/client.ts`, `src/admin/client.test.ts`

**Depends on:** T-007.

**Scope:**
- GET `${url}/admin/v1/projects/${projectId}/credentials`.
- Returns `Credential[]` with metadata only — no plaintext hashes.
- Same error model as T-008.

**Acceptance criteria:**
- Unit test: happy path with 2 credentials in the response.
- Unit test: empty list `[]`.
- Unit test: 401 unauthorized.

---

## Phase 0.3 — Server-route follow-on (each blocked on engine v0.2+)

Each of these is currently a `not_implemented` stub. Task is to flip the
stub to a real network call. **Do not start these until the engine route
exists** — check `basin/crates/basin-rest/src/server.rs` for the route
table. If the route isn't there, leave the stub and move on.

### T-020 — Wire `signInWithOAuth` 🔒

**Status:** blocked 2026-05-19. Engine `/auth/v1/oauth/*` routes do not exist
in `basin-rest/src/server.rs`. Stub remains in place. Flip when engine ships.

**Files:** `src/auth/client.ts`, `src/auth/client.test.ts`

**Engine prerequisite:** `/auth/v1/oauth/:provider/authorize` route in
basin-rest.

**Scope:**
- Build the authorize URL: `${url}/auth/v1/oauth/${provider}/authorize?redirect_to=…&state=…`.
- Return `{data: {url, provider}, error: null}` — the caller redirects the
  browser to this URL; the engine handles PKCE state server-side.
- Drop the current `not_implemented` short-circuit.

**Acceptance criteria:**
- Existing `not_implemented` test deleted; replaced with URL-shape tests
  for the 14 providers.
- No fetch is attempted — this is pure URL construction.

---

### T-021 — Wire `auth.mfa.{enroll, verify, unenroll}` 🔒

**Status:** blocked 2026-05-19. Engine `/auth/v1/mfa/*` routes do not exist.

**Files:** `src/auth/mfa.ts`, `src/auth/client.test.ts`

**Engine prerequisite:** `/auth/v1/mfa/totp/{enable,confirm,disable,challenge}`
routes in basin-rest.

**Scope:**
- TOTP enroll → POST `/auth/v1/mfa/totp/enable` → returns
  `{secret, qrCode}`.
- TOTP verify → POST `/auth/v1/mfa/totp/confirm` with `{code}`.
- TOTP unenroll → POST `/auth/v1/mfa/totp/disable`.
- TOTP challenge mid-signin → POST `/auth/v1/mfa/totp/challenge` with
  `{partial_token, code}` → full session.
- WebAuthn paths stay stubbed for now (separate task once engine ships).

**Acceptance criteria:**
- All four TOTP methods have happy-path + error-path unit tests.
- `signInWithPassword` `mfa_required` branch routes the partial token
  correctly to the challenge endpoint when the caller invokes it.

---

### T-022 — Wire storage `.upload()` + `.download()` 🔒

**Status:** blocked 2026-05-19. Engine `/object/*` routes do not exist.

**Files:** `src/storage/client.ts`, `src/storage/client.test.ts`

**Engine prerequisite:** `/object/*` upload + download routes.

**Scope:**
- `.upload(path, file, opts)` → POST multipart/form-data to
  `/object/${bucket}/${path}`. Body: `Blob | ArrayBuffer | Uint8Array | string`.
  Headers: `Content-Type` from `opts.contentType` or sniff from `Blob.type`.
- `.download(path)` → GET `/object/${bucket}/${path}` → returns `Blob`.
- Both stay synchronous about returning `{data, error}` envelope.

**Acceptance criteria:**
- Upload happy path with `Blob`, `ArrayBuffer`, `Uint8Array`, `string`.
- Download returns a `Blob` matching the response body.
- 404 on download → `not_found` error.

---

### T-023 — Wire storage `.list()` + `.remove()` 🔒

**Status:** blocked 2026-05-19. Engine `/object/*` routes do not exist.

**Files:** `src/storage/client.ts`, `src/storage/client.test.ts`

**Engine prerequisite:** `/object/list` + `/object/remove` routes.

**Scope:**
- `.list(prefix, opts)` → POST `/object/list/${bucket}` with
  `{prefix, limit, offset, sortBy}` → returns `ObjectInfo[]`.
- `.remove(paths)` → POST `/object/remove/${bucket}` with `{paths}`.

**Acceptance criteria:**
- Happy path + 401 unauthorized for both methods.
- `.list()` empty result returns `[]` not `null`.

---

### T-024 — Wire storage `.createSignedUrl()` 🔒

**Status:** blocked 2026-05-19. Engine `/object/sign/*` route does not exist.

**Files:** `src/storage/client.ts`, `src/storage/client.test.ts`

**Engine prerequisite:** `/object/sign/*` route.

**Scope:**
- POST `/object/sign/${bucket}/${path}` with `{expiresIn}` → returns
  `{signedUrl: string}`.
- Server may return a relative path; resolve against `storageUrl`.

**Acceptance criteria:**
- Happy path returns a `URL`-parseable string.
- 404 unknown path → `not_found` error.
- Negative `expiresIn` → `invalid_request` error before any fetch.

---

### T-025 — Realtime: SSE transport (single-table, read-only) ✅ done

**Status:** done 2026-05-20. `SseSubscription` + `subscribe()` in `src/realtime/sse.ts`; `backoff()` helper in `src/realtime/backoff.ts`; 8 tests green. Engine shipped the realtime stack (basin
5.11.R1–R7). Route shapes are final — see ROADMAP 0.6. This task was the
old "WebSocket transport 🔒" placeholder; it is now split across T-025
(SSE), T-028 (WS multiplex), T-029 (presence), T-030 (channel-API routing
+ reconnect/replay). Do them in that order.

**Files:** `src/realtime/sse.ts` (new), `src/realtime/sse.test.ts` (new)

**Engine route:** `GET /realtime/v1/sse/:project/:table`, header
`Authorization: Bearer <jwt>`. Streams one JSON event per committed
mutation: `{"project","table","op":"INSERT|UPDATE|DELETE","after":{…},"seq":N}`.
15s heartbeat comment frames (`:\n\n`). `Last-Event-Id` request header
replays missed events on reconnect.

**Scope:**
- An `SseSubscription` class using `globalThis.EventSource` if available,
  else `fetch()` + `ReadableStream` line reader (Node 18+ has no
  `EventSource`). No external deps.
- `subscribe(project, table, { jwt }, onEvent)` opens the stream, parses
  each `data:` line as JSON, invokes `onEvent` with the parsed event.
- Track the last `seq`; on reconnect send `Last-Event-Id: <seq>`.
- Auto-reconnect with exponential backoff (1s, 2s, 4s … cap 30s).
- Ignore heartbeat/comment frames.

**Acceptance criteria:**
- Unit tests stub the stream (mock `EventSource` or a `ReadableStream` of
  NDJSON-ish `data:` frames).
- Event frame → `onEvent` fires with `{op, after, seq}`.
- Heartbeat frame → no callback.
- Disconnect → reconnect attempt observed; `Last-Event-Id` sent with last seq.
- `npm run typecheck` clean, `npm test` green.

**Reference:** `basin/crates/basin-realtime/src/sse.rs` (handler + frame
shape); ROADMAP 0.6.

---

### T-026 — Wire `functions.invoke()` → `/rest/v1/rpc/:fn` ✅

**Status:** done 2026-05-20. `invoke()` POSTs to `/rest/v1/rpc/:fn_name`; `enabled=true`; 9 tests green. Engine shipped the RPC mount (basin 5.11.L,
commit 183c315). Route is **`POST /rest/v1/rpc/:fn_name`** — NOT the old
`/v1/projects/:ref/functions/:slug/invoke` shape the stub assumed.

**Files:** `src/functions/client.ts`, `src/functions/client.test.ts`

**Engine route:** `POST /rest/v1/rpc/:fn_name`, `Authorization: Bearer`,
body = JSON object of named args (e.g. `{"x":3,"y":4}`). Response: bare
scalar for single-row/single-column results; array of row objects for
`RETURNS TABLE` functions. Both `LANGUAGE sql` and `LANGUAGE wasm`
functions dispatch identically.

**Scope:**
- Flip the `enabled` flag; remove the `c8 ignore` block.
- `functions.invoke(fnName, { body, headers })` → POST
  `${url}/rest/v1/rpc/${fnName}` with the JSON args body + bearer auth.
- Return type-generic `{ data: T, error }` matching the existing surface;
  pass through the bare-scalar vs array distinction without reshaping.
- Rename the public method/alias if the SDK currently exposes
  `functions.invoke(slug)` — keep `invoke` but document it maps to RPC.

**Acceptance criteria:**
- Happy path: scalar function `add(x,y)` with `{x:3,y:4}` → `data === 7`.
- Happy path: `RETURNS TABLE` function → `data` is an array of objects.
- No auth token → propagates the engine 401 as `BasinError("unauthorized")`.
- 5xx → propagates `BasinError`.
- `npm run typecheck` clean, `npm test` green.

**Reference:** `basin/crates/basin-rest/src/routes/rpc.rs`; ROADMAP 0.3.

---

### T-028 — Realtime: WebSocket multiplex transport ✅ done

**Status:** done 2026-05-20. `WsConnection` ships in `src/realtime/ws.ts`;
6 tests green. Engine route `GET /realtime/v1/ws/:project`.

**Files:** `src/realtime/ws.ts` (new), `src/realtime/ws.test.ts` (new)

**Protocol (JSON, `tag = "type"`):**
- send `{"type":"subscribe","table":"orders"}` (optional
  `"filter":"NEW.status = 'paid'"`), `{"type":"unsubscribe","table":"orders"}`
- recv `{"type":"subscribed","table":…}`, `{"type":"event","table","op","after","seq"}`,
  `{"type":"unsubscribed",…}`, `{"type":"error","code":"lag","missed":N}`

**Scope:**
- `WsConnection` over `globalThis.WebSocket` (no deps). One socket,
  many table subscriptions multiplexed.
- `subscribe(table, { filter? }, onEvent)` sends the frame, resolves on
  `subscribed` ack, routes `event` frames to the right per-table callback.
- `unsubscribe(table)` keeps the socket open.
- Auto-reconnect (reuse T-025 backoff); on reconnect, re-send all active
  subscribe frames; detect `seq` gaps and surface a `lag` event.

**Acceptance criteria:**
- Mock `WebSocket`. Subscribe two tables → both acks → interleaved events
  route to the correct callbacks.
- Unsubscribe one mid-stream → its events stop; the other continues; socket
  stays open.
- Disconnect → reconnect → both subscriptions re-established.
- `npm run typecheck` clean, `npm test` green.

**Reference:** `basin/crates/basin-realtime/src/ws.rs` (ClientMsg/ServerMsg
enums, lines ~200–270); ROADMAP 0.6.

---

### T-029 — Realtime: presence over WebSocket ✅ done

**Status:** done 2026-05-20. `PresenceChannel` in `src/realtime/presence.ts`; presence routing added to `WsConnection` in `src/realtime/ws.ts`; 8 tests green.

**Files:** `src/realtime/presence.ts` (new), `src/realtime/presence.test.ts`
(new), small additions to `src/realtime/ws.ts`

**Protocol:** send `{"type":"presence_track","channel":"room:1","client_id":"c1","metadata":{…}}`,
`{"type":"presence_untrack",…}`, `{"type":"heartbeat",…}` (every 30s).
Recv `{"type":"presence_state","channel":"room:1","presences":[…]}` (full
snapshot on join) and `{"type":"presence_diff","channel","joins":[…],"leaves":[…]}`.

**Scope:**
- `channel.track(metadata)` / `channel.untrack()` send the frames.
- Maintain a local presence map from `presence_state` + `presence_diff`;
  expose `channel.presenceState()`.
- Start a 30s heartbeat timer while tracked; clear on untrack/close.
- `on('presence', { event: 'sync'|'join'|'leave' }, cb)` bindings
  (Supabase-shaped).

**Acceptance criteria:**
- Mock WS. `track()` → server `presence_state` → `presenceState()` reflects
  members; `sync` callback fires.
- `presence_diff` with a join → `join` callback + map updated.
- `presence_diff` with a leave → `leave` callback + map updated.
- Heartbeat frames sent on a fake timer at 30s cadence.
- `npm run typecheck` clean, `npm test` green.

**Reference:** `basin/crates/basin-realtime/src/presence.rs`; ROADMAP 0.6.

---

### T-030 — Realtime: `channel()` API + SSE/WS routing + replay ✅ done

**Status:** done 2026-05-20. **Depends on T-025, T-028, T-029.** This is
the public Supabase-shaped surface that picks a transport.

**Files:** `src/realtime/client.ts`, `src/realtime/client.test.ts`,
`src/realtime/index.ts`

**Scope:**
- `basin.channel(name)` returns a `RealtimeChannel`.
- `.on('postgres_changes', { event: 'INSERT'|'UPDATE'|'DELETE'|'*', table }, cb)`
  registers a binding.
- `.on('presence', { event }, cb)` registers presence bindings.
- `.subscribe()` decides transport: **SSE** when the channel has exactly
  one `postgres_changes` table binding, no presence, no per-binding filter;
  **WS** otherwise (presence, multiple tables, or a filter). Reuse T-025/028/029.
- Remove the old `enabled = false` flag and any throwing stubs.
- Reconnect-with-replay: SSE via `Last-Event-Id`; WS via re-subscribe +
  `seq` gap detection.

**Acceptance criteria:**
- One-table changes-only channel → SSE transport selected (assert via the
  mock that opened SSE not WS).
- Channel with presence → WS transport selected.
- Channel with two table bindings → WS transport selected.
- End-to-end (mocked transport): subscribe → event → binding callback fires
  with the right `{op, new}` shape.
- `npm run typecheck` clean, `npm test` green.

**Reference:** ROADMAP 0.6; Supabase `channel()` API for ergonomic parity.

---

### T-027 — Engine DELETE 501 — fix or wrap ✅

**Status:** done 2026-05-19 (SDK-side wrap). `.delete()` on 501 surfaces
`BasinError("not_implemented", "DELETE in basin REST v0.1 ...", 501)`.

**Files:** EITHER `basin/crates/basin-rest/src/...` (engine side, route the
DELETE) OR `src/postgrest/builder.ts` (clearer client-side error).

**Decision needed before starting:** is DELETE supposed to ship in 0.2 or
not? Check with the engine team / TASK.md in the basin repo.

**Scope (engine path):** implement the DELETE handler; remove the 501.
**Scope (SDK path):** detect 501 in `.delete()` and replace it with
`BasinError("not_implemented", "DELETE lands in basin v0.2 — see RELEASE.md")`.

**Acceptance criteria:**
- Either DELETE works end-to-end, or the error message is actionable.

---

## Phase 0.4 — DX polish

### T-040 — Configurable retry + exponential backoff ✅

**Status:** done 2026-05-19. `withRetry(fetch, opts)` wraps the user fetch; defaults
3 attempts, 250ms base, 5s cap, retries network + 5xx + 429 (honours `Retry-After`).
7 tests with fake timers. `retry: false` opt-out wired via `BasinClientOptions`.

**Files:** new `src/internal/retry.ts`, `src/client.ts` (wire into fetch
adapter), `src/internal/retry.test.ts`.

**Scope:**
- `RetryOptions { maxAttempts: number; baseMs: number; maxMs: number; retryOn: (response, attempt) => boolean }`.
- Default: 3 attempts, 250ms base, 5000ms cap, retry on network errors +
  5xx + 429 (respect `Retry-After` header if present).
- Wire as a wrapper around the user-provided `fetch` so per-call opt-out
  is possible: `basin.from('t').select().options({ retry: { maxAttempts: 1 } })`.
- Opt-out per createClient via `options.retry: false`.

**Acceptance criteria:**
- 3-attempt retry on a flaky fetch stub.
- `Retry-After: 5` header → second attempt waits ~5s (test with fake timers).
- `retry: false` → single attempt, no retries.

---

### T-041 — `@bas-in/ssr` cookie helpers for Next.js / SvelteKit ⏸

**Status:** deferred 2026-05-19. Needs structural decision: monorepo vs
sub-path vs separate repo. Not blocking v0.2 ship. Re-litigate when a user
files a Next.js / SvelteKit issue.

**Files:** consider a new package directory `packages/ssr/` if the
current repo is single-package, OR a sub-path `src/ssr/`. Decide
based on `package.json` shape — if it's already a single-package repo
and we want to avoid restructuring, ship as `src/ssr/` first and split
later.

**Scope:**
- `createServerClient(url, key, { cookies: { get, set, remove } })` for
  Next.js App Router (cookies()) + SvelteKit (cookies.get/set).
- Session reads from cookies instead of localStorage.
- `getSession()` reads the cookie; `signInWithPassword` sets the cookie.

**Acceptance criteria:**
- Unit test with a mock cookie store.
- README quickstart updated with a Next.js + SvelteKit snippet.

**Note:** this is a meaty task. Likely needs to be split once we decide
mono-repo vs sub-path. Sonnet agent should write a short
`decisions.md` entry first describing what they chose.

---

### T-042 — Stop shipping stubs that throw — decision + execution ⏸

**Status:** deferred 2026-05-19. Needs paired decision with the Phase 0.3
engine-route ramp: if 0.3 routes are landing soon, leaving the stubs in
place is correct (so app code can be written against the future shape). If
0.3 slips, remove the stubs and re-add when engine routes ship. Coupled to
basin engine roadmap, not standalone SDK work.

**Files:** all `*/client.ts` files with `not_implemented` stubs.

**Scope:**
- Decide between (a) gating types behind a feature flag so unimplemented
  methods don't appear in autocomplete, vs (b) removing the surface
  entirely until the engine route lands.
- Write the decision into `decisions.md` with the trade-off.
- Implement the chosen path.

**Acceptance criteria:**
- Either: autocomplete on `basin.storage.from('x').` doesn't show
  `.upload` until 0.3, OR: `basin.storage` namespace doesn't exist
  in the types yet.
- README updated to match.

---

### T-043 — Per-request `Prefer` header pass-through audit ✅

**Status:** done 2026-05-19. Added `.headers(extra)` modifier on the builder; `Prefer`
values merge (comma-join) with SDK-set Prefer; other keys overwrite. 3 tests.

**Files:** `src/postgrest/builder.ts`, `src/postgrest/builder.test.ts`

**Scope:**
- Verify the builder accepts arbitrary `Prefer:` headers via a `.headers()`
  modifier or per-call option.
- If missing, add `.headers({ Prefer: 'tx=rollback' })` style passthrough.

**Acceptance criteria:**
- Test: caller-provided `Prefer` header reaches the fetch.
- Test: SDK-set Prefer (e.g. `return=representation`) is preserved when
  caller adds their own.

---

### T-044 — Coverage hole audit: `.csv()` / `.geojson()` / `.explain()` ✅

**Status:** done 2026-05-19. Runtime tests for all three terminals + `explain({format:'json'})`.
Existing terminals had runtime tests; added the missing `application/vnd.pgrst.plan+json` case.

**Files:** `src/postgrest/builder.test.ts`

**Scope:**
- Each of the three terminals gets one runtime test that doesn't exist
  today: mock fetch returns the appropriate content-type, response body
  is the raw text the caller asked for, no JSON parse attempted.

**Acceptance criteria:**
- Coverage report shows all three terminals exercised at runtime, not
  just at type level.

---

## Phase 0.5 — Iceberg catalog client

### T-050 — Iceberg client: decide scope ✅

**Status:** decided 2026-05-19. JS Iceberg client deferred — point users at
pyiceberg / Spark / Trino / DuckDB against `/iceberg/v1/:warehouse/*`. Full
reasoning in `decisions.md`.

**Files:** new `decisions.md` entry.

**Scope:**
- Write a 1-page decision doc: (a) ship inside `@bas-in/basin-js`, (b)
  separate `@bas-in/basin-iceberg`, (c) skip the JS client and document
  pyiceberg / Spark instead.
- No code in this task — just the decision.

**Acceptance criteria:**
- `decisions.md` updated with the choice + the reasoning.

---

## Phase 0.0 — Pre-existing hygiene (discovered during T-001)

### T-100 — Fix pre-existing typecheck + lint failures ✅

**Status:** done 2026-05-19 as part of T-007's acceptance gate.
`src/functions/client.ts:106` fetch-body `undefined → null` for
`exactOptionalPropertyTypes`; `src/auth/client.test.ts` unused-`phase`
renames; new flat `eslint.config.js` shipped; `@typescript-eslint` devDeps
added; `package-lock.json` regenerated.

**Files:** `src/functions/client.ts`, repo root.

**Scope:**
- `src/functions/client.ts` has a TS2769 type mismatch (caught by T-001
  agent during acceptance gate). Read the error, fix the signature.
- `eslint.config.js` is missing — `npm run lint` errors out before linting
  anything. Either add a minimal flat config (`tseslint.config(…)` style)
  or remove the `lint` script if we're not enforcing it yet.
- After fixing, `npm run typecheck` and `npm run lint` both run clean.

**Acceptance criteria:**
- `npm run typecheck` exits 0.
- `npm run lint` exits 0 (or the script is intentionally removed and the
  CI workflow updated to match).

**Why this matters:** every future task's acceptance gate hits these
failures. Fixing once unblocks every subsequent agent.

---

## Out of scope for now

- Sibling SDKs (basin-py, basin-rs, basin-go, basin-dart, basin-swift,
  basin-kotlin) — defer until v0.2 JS is shipped and stable.
- Browser smoke tests via `vitest --browser` — wait for the @vitest/browser
  install gate to clear.
