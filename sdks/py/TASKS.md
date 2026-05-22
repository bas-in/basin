# basin-py — Tasks

Forward task list. basin-py is greenfield, so this starts at Phase 0.1
(foundation) — unlike basin-js, where 0.1 is already in git history.

Tasks are sized for one Sonnet agent each (~30–60 min of focused work).
Every task names the file(s) it touches, the acceptance criteria, and any
references it needs. No hidden context — an agent reading the task in
isolation should be able to start.

Conventions:
- **Status:** `[ ]` pending, `[~]` in-progress (agent claimed), `[x]` done
- **Files:** paths relative to repo root (`src/basin/…`)
- **Mirror basin-js.** Method names + route shapes must match
  [`../basin-js`](../basin-js) exactly. When unsure, read the equivalent
  `basin-js/src/**` file — port behaviour, not just signatures.
- **Async-first.** Implement the coroutine in the async client; the sync
  facade delegates. Never two parallel implementations.
- **Tests:** `pytest` + `pytest-asyncio` + `respx` (httpx mock transport),
  added next to the module under `tests/`. `mypy --strict` + `ruff` clean.
- **Style:** `from __future__ import annotations`; no comments unless the WHY
  is non-obvious; that belongs in the commit message, not the source.
- One runtime dep (`httpx`); `pydantic` / `websockets` / `pyiceberg` only
  behind extras.

---

## Phase 0.1 — Foundation

### T-001 — Packaging + repo scaffold [ ]

**Files:** `pyproject.toml`, `src/basin/__init__.py`, `src/basin/py.typed`,
`README.md`, `LICENSE`, `.gitignore`, `ruff.toml` (or `[tool.ruff]` in
pyproject), `tests/__init__.py`, `.github/workflows/ci.yml`

**Scope:**
- `pyproject.toml`: hatchling backend, project metadata, `requires-python =
  ">=3.9"`, runtime dep `httpx>=0.27`, optional extras `pydantic` (pydantic
  v2), `realtime` (websockets), `iceberg` (pyiceberg), `dev` (pytest,
  pytest-asyncio, respx, mypy, ruff). Console script `basin-gen-types =
  "basin.codegen.__main__:main"` (stub the module so it imports).
- Resolve the **package name**: try `basin` on PyPI; if taken, distribution
  `basin-sdk` with import package `basin`. Record the decision in
  `decisions.md`.
- `src/basin/` src-layout; ship `py.typed`.
- MIT `LICENSE` (copy `../basin-js/LICENSE`).
- CI: GitHub Actions matrix Python 3.9–3.13 running `ruff check`, `ruff
  format --check`, `mypy --strict src`, `pytest`.

**Acceptance:**
- `pip install -e '.[dev]'` then `python -c "import basin"` works.
- `ruff check`, `mypy --strict src`, `pytest` all green (pytest may collect 0
  tests at this point — that's fine).

---

### T-002 — Error model (`BasinError`) [ ]

**Files:** `src/basin/errors.py`, `tests/test_errors.py`

**Scope:**
- `class BasinError(Exception)` with `code: str`, `message: str`, optional
  `status: int | None`, `details: Any`. `__str__` → `f"{code}: {message}"`.
- A `from_response(resp: httpx.Response) -> BasinError` classmethod mapping
  HTTP status → code: 401→`unauthorized`, 403→`unauthorized`,
  404→`not_found`, 409→`conflict`, 429→`rate_limited`, 5xx→`server_error`,
  501→`not_implemented`; body's `{message|error|msg}` used when present.
- Code taxonomy must match basin-js `src/errors.ts` (`network`,
  `invalid_response`, `unauthorized`, `not_found`, `conflict`,
  `rate_limited`, `server_error`, `not_implemented`).

**Acceptance:**
- Unit tests: each status maps to the expected code; JSON body message is
  extracted; non-JSON body falls back to a generic message.

**Reference:** `../basin-js/src/errors.ts`.

---

### T-003 — `Client` + `create_client` + transport [ ]

**Files:** `src/basin/client.py`, `src/basin/_http.py`, `tests/test_client.py`

**Scope:**
- `create_client(url: str, key: str, *, options: ClientOptions | None = None)
  -> Client`. `Client` holds base URL, key, and a shared
  `httpx.AsyncClient`; sets `apikey` + `Authorization: Bearer <key>` headers.
- Internal `_request(method, path, *, json=None, params=None, headers=None)`
  that issues the call, raises `BasinError.from_response` on non-2xx, and
  parses JSON (or returns `None` on 204). Wrap `httpx` transport errors as
  `BasinError("network", …)`.
- Swappable transport so tests inject `respx` / `httpx.MockTransport`.
- `await client.aclose()` + `async with create_client(...) as client:`.

**Acceptance:**
- respx tests: 200 JSON parsed; 204 → `None`; 4xx → typed `BasinError`;
  transport error → `BasinError("network")`.
- Headers (`apikey`, `Authorization`) present on every request.

**Reference:** `../basin-js/src/client.ts`.

---

### T-004 — Query builder: filters + `select` + `await` execute [ ]

**Files:** `src/basin/postgrest/builder.py`, `src/basin/postgrest/__init__.py`,
`tests/test_builder.py`; wire `Client.from_(table)`.

**Scope:**
- `client.from_(table)` → `QueryBuilder`. Filter methods returning `self`:
  `eq, neq, gt, gte, lt, lte, like, ilike, is_, in_, contains, order, limit,
  range`. Terminal-ish: `select(columns="*")`, `single()`, `maybe_single()`.
- The builder is **awaitable**: `await client.from_(t).select().eq("id", 1)`
  issues `GET /rest/v1/{table}?...` and returns
  `APIResponse(data, count=None, status, status_text)`. Use PostgREST query
  syntax basin honours (`column=eq.value`, `order=col.asc`, `limit`, etc.).
- `single()`/`maybe_single()` set the `Accept:
  application/vnd.pgrst.object+json` header and unwrap to one row (or `None`
  for `maybe_single`).

**Acceptance:**
- respx tests assert the rendered URL/params for each filter; `select()`
  parses an array; `single()` unwraps; chained filters preserve order.

**Reference:** `../basin-js/src/postgrest/builder.ts` — port the param
rendering exactly.

---

### T-005 — Query builder: `insert` / `update` / `upsert` / `delete` [ ]

**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`

**Depends on:** T-004.

**Scope:**
- `insert(rows)`, `update(values)`, `upsert(rows, *, on_conflict=None)`,
  `delete()` terminals issuing POST/PATCH/POST(`Prefer: resolution=…`)/DELETE
  with the filter set applied. `Prefer: return=representation` by default;
  `returning="minimal"` opt-out.
- `delete()` requires at least one filter unless `.delete(force=True)` —
  guardrail against full-table deletes (match basin-js behaviour if present).

**Acceptance:**
- respx tests for each verb: correct method, body, `Prefer` header, and that
  filters are applied to update/delete.

**Reference:** `../basin-js/src/postgrest/builder.ts`.

---

### T-006 — Auth: password + magic-link [ ]

**Files:** `src/basin/auth/client.py`, `src/basin/auth/types.py`,
`src/basin/auth/__init__.py`, `tests/test_auth.py`; wire `Client.auth`.

**Scope:**
- `AuthClient` with `sign_up`, `sign_in_with_password`, `sign_in_with_otp`
  (magic link), `verify_otp`, `sign_out`, `get_session`, `refresh_session`.
- Routes: `POST /auth/v1/signup`, `/auth/v1/token?grant_type=password`,
  `/auth/v1/otp`, `/auth/v1/verify`, `/auth/v1/logout`,
  `/auth/v1/token?grant_type=refresh_token` — **confirm each against
  `../basin-js/src/auth/client.ts` and use those exact shapes.**
- On successful sign-in, store the session and swap the client's bearer to
  the user access token; `sign_out` reverts to the anon key.

**Acceptance:**
- respx tests: sign-in stores session + flips Authorization header; refresh
  updates tokens; sign-out reverts; 401 → typed error.

**Reference:** `../basin-js/src/auth/client.ts`.

---

### T-007 — Sync facade (`SyncClient`) [ ]

**Files:** `src/basin/sync_client.py`, `tests/test_sync.py`; export from
`src/basin/__init__.py`.

**Depends on:** T-003, T-004, T-006.

**Scope:**
- `create_sync_client(url, key, …) -> SyncClient` wrapping the async `Client`
  with a privately-owned event loop (run coroutines via
  `loop.run_until_complete`, or `asyncio.run` per call if simpler + correct).
- `SyncClient.from_(t).select().execute()` (sync terminal) +
  `client.auth.sign_in_with_password(...)` returning resolved values, no
  `await`. Mirror the async surface method-for-method.
- Document that `SyncClient` is for scripts/notebooks/sync frameworks; async
  is the primary path.

**Acceptance:**
- Tests run without `pytest-asyncio`: a sync select + a sync sign-in against
  respx return resolved data.

---

## Phase 0.2 — basin-distinctive surface

### T-010 — NDJSON auto-detection on execute (bug, highest priority) [ ]

**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`

**Why a bug, not a feature:** the engine auto-promotes any response over
~1 MiB or 10,000 rows to NDJSON even when the caller didn't ask. A
JSON-only parser breaks on large queries.

**Scope:**
- In the execute path, branch on response `Content-Type`. If it contains
  `application/x-ndjson` or `application/jsonl`, read the body as text, split
  on newlines, parse each line. The final line is
  `{"_basin_next_cursor":"…"}` — peel it off and expose `next_cursor` on
  `APIResponse`. All other lines → `data`.
- Leave the JSON path untouched for non-NDJSON.

**Acceptance:**
- Tests: NDJSON with 3 rows + sentinel → 3 rows + `next_cursor`; 0 rows +
  sentinel → `[]` + cursor; rows with no sentinel → `next_cursor=None`;
  existing JSON tests still pass.

**Reference:** `../basin-js` T-001; server behaviour in
`../basin/crates/basin-rest/src/lib.rs` (`?stream=true` + auto-promotion).

---

### T-011 — `.cursor(token)` modifier [ ]

**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`

**Scope:** `.cursor(token)` sets `?cursor=<token>`; pairs with `.limit()` for
manual paging.

**Acceptance:** URL carries `cursor=…`; chained with other filters preserves
all params.

**Reference:** basin-js T-003.

---

### T-012 — `.paginate()` async iterator [ ]

**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`

**Depends on:** T-010, T-011.

**Scope:** `.paginate()` returns an `AsyncIterator[Row]` walking `next_cursor`
through the wrapped `{rows, next_cursor}` shape until `next_cursor is None`.
Respects `.limit()` as page size (default 1000). `async for row in
client.from_(t).select().paginate():`.

**Acceptance:** 3-page mock → yields all rows in order; single page with
`next_cursor=None` → completes; mid-pagination error → raises the same error
shape as `await`.

**Reference:** basin-js T-004.

---

### T-013 — `.stream()` async iterator (NDJSON line-by-line) [ ]

**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`

**Depends on:** T-010.

**Scope:** `.stream()` sets `?stream=true` and yields rows as they arrive via
`httpx`'s streaming response (`aiter_lines()`), skipping the trailing
`_basin_next_cursor` sentinel. Returns `AsyncIterator[Row]` (not awaitable).

**Acceptance:** mock streamed body of 3 NDJSON lines + sentinel → `async for`
yields 3 rows then completes; mid-stream error → iterator raises
`BasinError("network")`; unparseable line → `BasinError("invalid_response")`.

**Reference:** basin-js T-002.

---

### T-014 — OpenAPI fetch helper [ ]

**Files:** `src/basin/openapi/__init__.py`, `src/basin/openapi/fetch.py`,
`tests/test_openapi.py`

**Scope:** `async def fetch_openapi(url, anon_key, *, client=None) -> dict`
fetching `GET {url}/rest/v1/_openapi.json` with the anon-key header; typed
return for the bits we use (`paths`, `components.schemas`). Typed errors on
404 / malformed JSON.

**Acceptance:** valid doc parsed; 404 → typed error; malformed JSON → typed
error.

**Reference:** basin-js T-005; server route
`../basin/crates/basin-rest/src/routes/openapi.rs`.

---

### T-015 — `database.py` codegen from OpenAPI [ ]

**Files:** `src/basin/codegen/__init__.py`, `src/basin/codegen/__main__.py`,
`src/basin/codegen/emit.py`, `tests/test_codegen.py`

**Depends on:** T-014.

**Scope:**
- `python -m basin.codegen --url … --key … --out database.py [--pydantic]`
  (also the `basin-gen-types` console script). Walks
  `components.schemas` → emits per-table `TypedDict`s `{Table}Row` /
  `{Table}Insert` / `{Table}Update` (Insert = Row with default/nullable cols
  optional via `total=False` split; Update = all-optional). `--pydantic`
  emits pydantic v2 `BaseModel`s instead.
- Type map: `integer`→`int`, `number`→`float`, `string`→`str`,
  `boolean`→`bool`, `string/date-time`→`str` (or `datetime` under
  `--pydantic`), arrays→`list[T]`, nullable→`T | None`.
- Pure `openapi_to_types(doc, *, pydantic=False) -> str` function, separately
  testable from the CLI. Output is `ruff format`-clean (run ruff on the
  string before writing).

**Acceptance:** fixture doc → expected module string (TypedDict + pydantic
variants); generated file imports + type-checks under mypy.

**Reference:** basin-js T-006.

---

### T-016 — `client.admin` namespace + projects client [ ]

**Files:** `src/basin/admin/client.py`, `src/basin/admin/types.py`,
`src/basin/admin/__init__.py`, `tests/test_admin.py`; wire `Client.admin`.

**Scope:**
- `client.admin.projects.provision(project_id) -> {connection_string}`
  (`POST /admin/v1/projects`), `rotate_credentials(pgwire_user) ->
  {connection_string}`, `list_credentials(project_id) -> list[Credential]`
  (metadata only).
- 401 when claims lack `is_admin` → `BasinError("unauthorized", …)`.

**Acceptance:** respx tests for each method's route/body; 401 path typed.

**Reference:** basin-js T-007/T-008/T-009/T-010 + `../basin-js/src/admin/`.

---

## Phase 0.3 — Server-route follow-on (engine routes shipped)

### T-020 — `from_(t).delete()` live (engine DELETE shipped) [ ]
**Files:** `src/basin/postgrest/builder.py`, `tests/test_builder.py`. Confirm
the engine `DELETE` path returns representation; remove any 501 guard. (Pairs
basin-js T-027.)

### T-021 — `client.functions.invoke()` → `POST /rest/v1/rpc/:fn` [ ]
**Files:** `src/basin/functions/client.py`, `__init__.py`, tests; wire
`Client.functions`. Body = JSON object of named args; response = function
result. (basin-js T-026.)

### T-022 — `sign_in_with_oauth` → `GET /auth/v1/authorize` [ ]
**Files:** `src/basin/auth/client.py`, tests. Returns the authorize URL (+
opens/returns for the caller); `GET /auth/v1/callback` completion helper.
(basin-js T-020.)

### T-023 — `client.auth.mfa.*` (factors enroll/verify/challenge/unenroll) [ ]
**Files:** `src/basin/auth/mfa.py`, tests. Routes `POST /auth/v1/factors`,
`/factors/:id/verify`, `/factors/:id/challenge`, `/factors/:id/challenge/
verify`, `DELETE /factors/:id`. TOTP + WebAuthn. (basin-js T-021.)

### T-024 — Storage: `upload` / `download` / `delete` [ ]
**Files:** `src/basin/storage/client.py`, `__init__.py`, tests; wire
`Client.storage`. `client.storage.from_(bucket).upload(path, data)` →
`/storage/v1/object/:bucket/:path`. (basin-js T-022.)

### T-025 — Storage: `list` / `remove` / `create_signed_url` [ ]
**Files:** `src/basin/storage/client.py`, tests. `list` →
`/storage/v1/object/list/:bucket`; `remove(paths)` → bulk `DELETE
/storage/v1/object/:bucket` `{prefixes}`; `create_signed_url` →
`/storage/v1/object/sign/:bucket/:path`. (basin-js T-023/T-024.)

---

## Phase 0.6 — Realtime

### T-030 — SSE transport (single-table, read-only) [ ]
**Files:** `src/basin/realtime/sse.py`, tests. Stream
`GET /realtime/v1/sse/:project/:table` over `httpx` streaming; parse SSE
frames into events; 15s heartbeat tolerance; `Last-Event-Id` replay on
reconnect. No extra dep. (basin-js T-025.)

### T-031 — WebSocket multiplex transport [ ]
**Files:** `src/basin/realtime/ws.py`, tests. `websockets` under `[realtime]`
extra. JSON control plane: subscribe/unsubscribe (+`filter`), event/error
frames, `seq` gap detection. (basin-js T-028.)

### T-032 — Presence over WebSocket [ ]
**Files:** `src/basin/realtime/presence.py`, tests. `presence_track`/`untrack`/
`heartbeat`; `presence_state`/`presence_diff`. (basin-js T-029.)

### T-033 — `channel()` API + SSE/WS routing + replay [ ]
**Files:** `src/basin/realtime/channel.py`, `__init__.py`, tests; wire
`Client.channel(name)`. Routing rule identical to basin-js (single-table
read-only → SSE; presence/multi-table/dynamic-filter → WS). (basin-js T-030.)

---

## Phase 0.4 — DX polish

### T-040 — Retry + exponential backoff [ ]
**Files:** `src/basin/_retry.py`, `tests/test_retry.py`; integrate in
`_http.py`. Retry network/5xx/429 (honour `Retry-After`); sensible defaults;
per-call opt-out. (basin-js T-040.)

### T-041 — Sync facade completeness (`paginate`/`stream`/`channel`) [ ]
**Files:** `src/basin/sync_client.py`, tests. Sync generators for
`paginate`/`stream`; `channel()` over a background thread. The Python
headline feature. (Extends T-007.)

### T-042 — `Prefer` header pass-through audit [ ]
**Files:** `src/basin/postgrest/builder.py`, tests. Verify callers can set
arbitrary `Prefer:` values; document the supported set. (basin-js T-043.)

### T-043 — Typing guard suite (`mypy --strict` + `reveal_type`) [ ]
**Files:** `tests/typing/` `reveal_type` assertions; CI step. Ensures
`from_('users')` infers the generated row type when a `Database` typeddict is
supplied. (basin-js T-044 analogue.)

---

## Phase 0.5 — Iceberg

### T-050 — `pyiceberg` interop helper [ ]
**Files:** `src/basin/iceberg/__init__.py`, tests. `client.iceberg.catalog_url
(warehouse)` + `load_catalog(client, warehouse)` returning a configured
`pyiceberg` `Catalog` under the `[iceberg]` extra; clear error pointing to
`pip install basin[iceberg]` otherwise. Document the
`/iceberg/v1/:warehouse/*` REST catalog recipe. (basin-js T-050 analogue —
but Python has the real advantage here.)

---

## Phase 1.0 — Parity + release

### T-060 — Cross-SDK request-parity fixtures [ ]
**Files:** `tests/test_parity.py`. A matrix of representative calls asserting
basin-py emits the same method + path + params + body as the documented
basin-js shapes (encode the basin-js expectations as fixtures).

### T-061 — PyPI release + README quickstart parity [ ]
**Files:** `README.md`, `pyproject.toml`, release workflow. Trusted-publisher
PyPI release; README quickstart mirrors basin-js (typed flow as default).

---

*Created 2026-05-22. Mirrors `../basin-js` (ROADMAP + TASKS). Phase 0.1 is
greenfield foundation; 0.2–0.5 port basin-js's shipped surface to async
`httpx` Python. Method names + route shapes must not drift from basin-js.*
