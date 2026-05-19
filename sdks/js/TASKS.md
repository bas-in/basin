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

### T-002 — `.stream()` modifier returning `AsyncIterable<Row>`

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

### T-003 — `.cursor(token?)` modifier

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

### T-004 — `.paginate()` AsyncIterable walker

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

### T-005 — OpenAPI fetch helper

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

### T-006 — `database.types.ts` codegen from OpenAPI

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

### T-008 — `basin.admin.projects.provision()`

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

### T-009 — `basin.admin.projects.rotateCredentials()`

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

### T-010 — `basin.admin.projects.listCredentials()`

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

### T-020 — Wire `signInWithOAuth`

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

### T-021 — Wire `auth.mfa.{enroll, verify, unenroll}`

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

### T-022 — Wire storage `.upload()` + `.download()`

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

### T-023 — Wire storage `.list()` + `.remove()`

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

### T-024 — Wire storage `.createSignedUrl()`

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

### T-025 — Realtime WebSocket transport

**Files:** `src/realtime/client.ts`, `src/realtime/client.test.ts`

**Engine prerequisite:** broadcast surface on basin-rest (TBD route shape).

**Scope:**
- Replace the `enabled = false` flag with a real WebSocket client using
  `globalThis.WebSocket` (no external deps).
- `RealtimeChannel.subscribe()` opens (or reuses) the socket, sends a
  subscribe frame, and resolves once the server ack arrives.
- Postgres-changes bindings receive `INSERT/UPDATE/DELETE` payloads.
- Auto-reconnect with exponential backoff (1s, 2s, 4s, …, cap at 30s).

**Acceptance criteria:**
- Unit tests use a mock WebSocket (`globalThis.WebSocket = MockWS`).
- Subscribe → ack → payload → callback fires.
- Disconnect → reconnect attempt observed.

**Note:** this is the largest task in the file. It may need to be split
once the engine route shape is known.

---

### T-026 — Wire `functions.invoke()`

**Files:** `src/functions/client.ts`, `src/functions/client.test.ts`

**Engine prerequisite:** `/v1/projects/:ref/functions/:slug/invoke` route.

**Scope:**
- Flip the `enabled` flag; remove the `c8 ignore` comment block.
- POST to `${url}/v1/projects/${projectRef}/functions/${slug}/invoke` with
  the JSON body.
- Return type-generic `{data: T, error}` matching the existing surface.

**Acceptance criteria:**
- Happy path with body + headers + auth-token override.
- Missing `projectRef` (neither in `createClient` nor per-call) →
  `invalid_request`.
- 5xx response → propagates `BasinError`.

---

### T-027 — Engine DELETE 501 — fix or wrap

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

### T-040 — Configurable retry + exponential backoff

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

### T-041 — `@bas-in/ssr` cookie helpers for Next.js / SvelteKit

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

### T-042 — Stop shipping stubs that throw — decision + execution

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

### T-043 — Per-request `Prefer` header pass-through audit

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

### T-044 — Coverage hole audit: `.csv()` / `.geojson()` / `.explain()`

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

### T-050 — Iceberg client: decide scope

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
