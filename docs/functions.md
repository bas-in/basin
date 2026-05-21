---
title: "Wasm functions: authoring, ABI, deploy, limits"
nav_section: operations
sidebar_position: 20
summary: "TypeScript HTTP handlers compiled to WebAssembly and run inside Basin under per-invocation CPU, memory, wall-clock, and per-project concurrency caps. Authoring, host ABI, deploy, limits."
tags: [functions, wasm, security, baas]
---

# Wasm functions

Basin runs **WebAssembly** functions inside the engine. Author the source
in TypeScript or any other language that targets the WASI Preview 2
component model, compile to a `.wasm` component via `basin functions
deploy`, and Basin invokes the component on every matching
`ANY /fn/v1/:name` request. Functions get a fixed host-import ABI for SQL,
HTTP, logging, and secret access; every invocation runs under per-project
CPU / memory / wall-clock / concurrency caps.

> **This is Wasm, not V8.** Basin will not run a V8/Deno isolate pool —
> see [ADR 0019 — Declarative BaaS surface](./decisions/0019-declarative-baas-surface.md)
> for the decision and the trade-offs. WebAssembly gives Basin one
> sandbox the wasmtime team already maintains, with `epoch_interruption`
> + linear-memory reservations that compose naturally with the
> per-tenant cap structure the engine already enforces. JavaScript /
> TypeScript is still a first-class authoring language via Javy /
> ComponentizeJS; the engine just never sees the JS — it sees the
> compiled Wasm component.

## Authoring guide

### 1. Write the function

Functions are TypeScript modules that export a single `handle(req)`
entrypoint. The `@bas-in/functions` SDK (forward-spec — see
[`basin-js-design.md`](./basin-js-design.md)) ships the host-import
bindings and the request/response types.

```ts
// fn/hello.ts
import { handle, query, log } from "@bas-in/functions";

export default handle(async (req) => {
  log.info(`hello called with ${req.path}`);
  const rows = await query.exec("SELECT now()::text AS now");
  return {
    status: 200,
    headers: [["content-type", "application/json"]],
    body: new TextEncoder().encode(JSON.stringify({ now: rows[0].columns[0][1] })),
  };
});
```

The `query.exec` call runs SQL under **the calling user's identity** —
`auth.uid()` inside the function returns the JWT `sub` of whoever hit
the endpoint. Row-level security policies on every touched table fire
under the caller's principal; the function has no service-role escape
hatch.

### 2. Compile and deploy

```sh
basin functions deploy ./fn/hello.ts --name hello
```

`basin functions deploy` (see
[`basin-cli-design.md`](./basin-cli-design.md) for the full CLI surface)
runs the TypeScript through ComponentizeJS / Javy locally, producing a
`.wasm` component that exports the
`basin:functions/handler#handle(req) -> response` world. The CLI then
issues a `CREATE OR REPLACE FUNCTION hello() RETURNS bytea LANGUAGE
javascript AS '<base64>'` against the project's pgwire endpoint — the
engine validates the component (Wasm magic bytes, component-model
shape) and stores the compiled bytes in the catalog. Redeploying the
same name bumps the function's version counter so the runtime registry
invalidates the cached harness atomically.

### 3. Invoke it

```sh
curl -H "Authorization: Bearer $JWT" https://YOUR_PROJECT.basin.dev/fn/v1/hello
```

The `ANY /fn/v1/:name` mount accepts every HTTP method; the JWT auth
gate fires before the function is even resolved (a 401 short-circuits
the lookup so unknown names don't leak existence). On a successful
auth, Basin opens a `ProjectSession` under the caller's principal,
threads it into the function's per-invocation `InvocationContext`, and
hands the request to the compiled Wasm component.

## Host ABI reference

The `basin:functions` package exports four interfaces. WIT lives at
`crates/basin-fn/wit/basin-fn.wit`. All four are wired in the host
([`crates/basin-fn/src/host.rs`](https://github.com/bas-in/basin/blob/main/crates/basin-fn/src/host.rs)).

### `basin:functions/query`

```wit
interface query {
  record row { columns: list<tuple<string, string>>, }
  exec: func(sql: string) -> result<list<row>, string>;
}
```

Runs SQL through the engine. The session was opened under the
caller's JWT — every `auth.uid()` / `auth.role()` / `auth.aal()`
evaluation inside the SQL resolves to the caller's identity, and RLS
policies apply normally. Values come back as JSON-encoded strings (one
type covers every SQL type without a variant; the caller decodes by
column name). Failure surfaces as an `Err(String)` containing the
engine diagnostic — no silent swallowing.

Cross-project isolation: the executor is per-call, keyed by the
`(project, name)` of the function being run. A function in project A
cannot read project B's tables even via raw SQL — the session can't
see them.

### `basin:functions/http`

```wit
interface http {
  record request  { url: string, method: string, headers: list<tuple<string, string>>, body: option<list<u8>>, }
  record response { status: u16, headers: list<tuple<string, string>>, body: list<u8>, }
  fetch: func(req: request) -> result<response, string>;
}
```

Outbound HTTP via the engine's `basin-net` adapter. The host enforces
the project's allowlist (no SSRF to internal addresses), per-second
rate limit, body-size cap, and request timeout — the guest cannot
bypass them. Tests inject `MockHttpClient::ok(...)` / `::err(...)` to
exercise success and denial paths.

### `basin:functions/log`

```wit
interface log {
  enum level { trace, debug, info, warn, error, }
  emit: func(lvl: level, msg: string);
}
```

Emits via `tracing` at the requested level. Logs flow through the same
OTLP exporter the rest of the engine uses (`BASIN_OTLP_ENDPOINT`).

### `basin:functions/secret`

```wit
interface secret {
  get: func(name: string) -> result<string, string>;
}
```

Returns the plaintext value of a project secret. The host decrypts via
the `EncryptionProvider` (AES-256-GCM with a per-project key in
production; `PlaintextEncryption` in dev). The guest sees only its own
project's secrets — the resolver is scoped by the calling project at
call time.

### Handler shape: `/fn/v1/:name`

Functions that export `basin:functions/handler#handle` are mounted at
`ANY /fn/v1/:name` (Phase 5.11.W2). Request shape:

```wit
record request {
  method:  string,                           // "GET" | "POST" | …
  path:    string,                           // request path + query
  headers: list<tuple<string, string>>,
  body:    list<u8>,
}

record response {
  status:  u16,
  headers: list<tuple<string, string>>,
  body:    list<u8>,
}
```

The mount is JWT-gated. Unknown function names return 404
([`fn_v1_unknown_function_returns_404`](https://github.com/bas-in/basin/blob/main/tests/integration/tests/fn_handler.rs)).
A function that traps surfaces as a 500 with the trap message; a
function that returns `Err(String)` surfaces the same way.

## Limits

All caps come from the W5 + Phase 6.P0.C governance module
(`crates/basin-fn/src/governance.rs`). Defaults are env-overridable
via `BASIN_FN_*`; the per-invocation enforcement is on the
production path (`HandlerHarness::handle_with`).

| Cap | Default | Override env | Enforcement |
|---|---|---|---|
| CPU (epoch ticks per invocation; ≈ 100 ms per tick) | 50 (≈ 5 s) | `BASIN_FN_CPU_TICKS` | `wasmtime::Engine::increment_epoch` ticker; `set_epoch_deadline` traps the guest |
| Linear memory | 64 MiB | `BASIN_FN_MEM_MB` | Per-Store `MemoryLimiter` (`ResourceLimiter`) + engine-wide `memory_reservation` |
| Wall-clock | 10 s | `BASIN_FN_WALL_MS` | `tokio::time::timeout` around the dedicated-runtime blocking task; epoch bumped past deadline on expiry to interrupt the wasm thread |
| Per-project concurrency | 16 | `BASIN_FN_PROJECT_CONCURRENCY` | Per-project bounded `tokio::sync::Semaphore` |
| Per-project semaphore LRU cap (memory bound for distinct projects) | 10 000 | `BASIN_FN_PROJECT_SEM_CAP` | Bounded `LruCache` — eviction keeps per-tenant cost O(bytes), not O(projects ever seen). See [ADR 0008 — Noisy-neighbor fairness](./decisions/0008-noisy-neighbor-fairness.md) |
| Dedicated runtime worker threads | 4 | `BASIN_FN_WORKER_THREADS` | Wasm runs on a side-thread-owned `tokio::runtime::Runtime`, isolated from axum + the shard-mode executor + basin-net's blocking pool. Phase 6.P0.C — see audit `docs/audits/2026-05-21-noisy-neighbor-fairness.md` item #16 |

A function that spins past the CPU cap, allocates past the memory cap,
or sleeps past the wall-clock cap is killed and the invocation returns
a 500 with the trap message. A project that exceeds its concurrency
cap admits at most `BASIN_FN_PROJECT_CONCURRENCY` invocations
simultaneously; the rest wait at the per-project semaphore — they do
not starve other projects' invocations.

### Failure modes the caps don't cover

- **Host-call host-side latency** — a function blocked inside a long
  `query.exec` runs against the engine's statement-level wall-clock
  timeout (`BASIN_STATEMENT_TIMEOUT_MS`, default 30 s — see Phase
  6.P0.A), not the function's wall-clock cap.
- **Spawn-blocking thread leak on uninterruptible host code** —
  documented audit limitation. The W5 wall-timeout returns control
  to the caller; a thread that ignores the trap (only possible if it
  loops inside host code, not guest code) is documented in
  `docs/audits/2026-05-21-wasm-functions-perf-security.md` item A11
  and bounded by the dedicated runtime's blocking-pool size
  (`workers * 16`).

## Lifecycle

Functions are catalog-backed (Phase 5.11.W6). The standard SQL
surface works:

```sql
CREATE FUNCTION hello() RETURNS bytea LANGUAGE javascript AS '<base64>';
CREATE OR REPLACE FUNCTION hello() RETURNS bytea LANGUAGE javascript AS '<base64>';  -- bumps version
ALTER FUNCTION hello() RENAME TO greet;
DROP FUNCTION hello();
```

The runtime (`FunctionRuntime` in
[`crates/basin-fn/src/runtime.rs`](https://github.com/bas-in/basin/blob/main/crates/basin-fn/src/runtime.rs))
caches a compiled `HandlerHarness` per `(project, name, version)` tuple
so a redeploy atomically invalidates the cached entry. Concurrent
invocations of the same name share one compiled harness; a redeploy
inserts a fresh entry without invalidating any in-flight call.

## Why Wasm, not V8

From [ADR 0019](./decisions/0019-declarative-baas-surface.md):

> Basin does not run a V8/Deno isolate pool, and never will. In-DB
> compute is WebAssembly — any language that compiles to Wasm,
> including JavaScript/TypeScript via Javy / ComponentizeJS.
> Imperative HTTP handler logic outside that envelope runs on the
> customer's own app server.

The choice is load-bearing for the project's scope:

- **Reuses the runtime already shipped.** The wasmtime engine for
  Phase 5.11.J's `LANGUAGE wasm` UDFs and the `/rpc/<fn>` mount are
  already in production; W1–W7 layer the component-model bindings,
  the HTTP-handler world, and the governance plumbing on top. Adding
  V8 would mean a second sandbox, a second resource model, and a
  second security review.
- **Cleaner multi-tenant isolation.** `epoch_interruption` + a
  per-Store `ResourceLimiter` + a per-project semaphore compose into
  the same per-tenant cost model the rest of the engine enforces
  (`O(bytes)`, not `O(projects ever seen)`). V8 isolates would need a
  parallel resource-quota system that doesn't share primitives with
  the SQL path.
- **Polyglot by default.** Any language with a Wasm component-model
  toolchain works the same way — TypeScript today via ComponentizeJS,
  Rust / Python / Go as their component-model story matures. The
  host ABI doesn't change.

## See also

- [`crates/basin-fn/wit/basin-fn.wit`](https://github.com/bas-in/basin/blob/main/crates/basin-fn/wit/basin-fn.wit) — the canonical WIT
- [`tests/integration/tests/fn_handler.rs`](https://github.com/bas-in/basin/blob/main/tests/integration/tests/fn_handler.rs) — `/fn/v1/:name` integration test
- [`tests/integration/tests/fn_javascript.rs`](https://github.com/bas-in/basin/blob/main/tests/integration/tests/fn_javascript.rs) — `LANGUAGE javascript` catalog + RLS-in-function tests
- [`tests/integration/tests/wasm_functions_differential.rs`](https://github.com/bas-in/basin/blob/main/tests/integration/tests/wasm_functions_differential.rs) — function ≡ SQL differential
- [`tests/integration/tests/wasm_functions_soak.rs`](https://github.com/bas-in/basin/blob/main/tests/integration/tests/wasm_functions_soak.rs) — 100-tenant concurrent soak (short variant; 1-hour `#[ignore]` variant for releases)
- [ADR 0019 — Declarative BaaS surface](./decisions/0019-declarative-baas-surface.md) — "Wasm, not V8" decision
- [ADR 0018 — Subsystem feature flags](./decisions/0018-subsystem-feature-flags.md) — the `component-model` Cargo feature; minimal builds drop the wasmtime runtime
- [`basin-cli-design.md`](./basin-cli-design.md) — `basin functions deploy` CLI shape
- [`basin-js-design.md`](./basin-js-design.md) — `@bas-in/functions` SDK shape
