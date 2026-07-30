---
title: "Wasm functions — performance, governance and adversarial audit (2026-05-21)"
nav_section: meta
sidebar_position: 61
summary: "Read-only audit of crates/basin-fn: sandbox escape surface, resource governance, scheduling fairness and performance findings."
---

# Wasm-functions performance + governance + adversarial audit

**Date:** 2026-05-21
**Scope:** `crates/basin-fn/` as of W1 (`d5ba0f6`), W1-followup (`1672202`), W5 (`c8157a3`).
**Mode:** Read-only audit; no source edits.
**Working-tree state:** `crates/basin-fn/src/handler.rs`, `crates/basin-fn/src/runtime.rs`,
`crates/basin-fn/tests/handler_test.rs` are **untracked** in HEAD; modifications to
`harness.rs`/`lib.rs`/`wit/basin-fn.wit` are **unstaged**. This audit evaluates the
committed code; WIP items appear only in §7 with the "WIP" label.

Citations point at `crates/basin-fn/...:line` in the working tree; line numbers match
the committed blob unless flagged WIP.

---

## 1. Executive summary

| Rank | Severity | Finding | Sizing |
|---|---|---|---|
| **#1** | **P0 (correctness)** | **W1's `ComponentHarness` engine has no epoch ticker.** `harness.rs:39-49` creates a process-wide `Engine` with `epoch_interruption(true)`, sets `store.set_epoch_deadline(5)` (`harness.rs:114`), but **never calls `Engine::increment_epoch` on it**. A spinner running through `ComponentHarness::run` runs forever, despite the deadline. W5's `FunctionGovernance` builds a *second* engine with its own ticker (`governance.rs:268-280`) — it is not wired into `ComponentHarness`. | All calls through the W1 entrypoint. |
| **#2** | **P0 (correctness)** | **HEAD is broken / does not build.** Committed `lib.rs` (lines 41-42, 56) does `pub mod handler;` and `pub use handler::{FunctionStore, HandlerHarness, …}`, but `handler.rs` is **not committed in HEAD** (only present as an untracked file). `git show HEAD:crates/basin-fn/src/handler.rs` returns `fatal: path … does not exist in 'HEAD'`. The W5 commit (`c8157a3`) carried this broken state forward. |  `cargo check -p basin-fn` on a fresh clone of `main` will fail. |
| **#3** | **P1 (correctness)** | **Memory cap not attached to `ComponentHarness`.** W5's `apply_memory_cap_to_config` is applied only to the W5-owned `Engine` (`governance.rs:258-261`); the W1 `Engine` in `harness.rs:42-48` has no `memory_reservation`. The task brief flags this — confirmed. Any guest run via `ComponentHarness::run` can `memory.grow` up to wasmtime's default reservation (≈ 4 GiB on 64-bit). | Per-invocation 64-bit address-space pressure; OOM kill is the only ceiling. |
| **#4** | **P1 (correctness)** | **W1-followup http/secret "wired" claim is aspirational.** `lib.rs:12` says `basin:fn/http` is **Wired** with a `basin_net` adapter; no basin-net file references `HttpSend` or `basin-fn`. The only `HttpSend` impl is `MockHttpClient` (`engine.rs:128-153`). Production `InvocationContext` construction would have to invent the adapter at call-site. Same for `SecretStore`: only `MockSecretStore`/`StubSecretStore` exist. |  No production caller exists (search of `crates/` shows zero callers of `ComponentHarness` outside `basin-fn` tests), so user-visible blast radius is currently zero — but it is a documentation-vs-reality gap that will mislead the next reviewer. |
| **#5** | **P1 (perf)** | **Per-invocation cost dominated by `Component::instantiate` + linker re-walk.** `ComponentHarness::run` (`harness.rs:107-118`) and `HandlerHarness::handle` (WIP `handler.rs:140-159`) do **fresh `Store::new` + `BasinFunctions::instantiate`** per call. Component instantiation walks every import + initialises linear memory; for a non-trivial component this is hundreds of µs to low-ms — likely the largest per-call cost outside the SQL roundtrip. The compiled `Component` *is* cached (good — `harness.rs:78`) but the `Linker` is rebuilt only at harness construction so that part is fine. |  Cold-call latency floor ≈ 200 µs – 2 ms (estimated; not measured). |

**Biggest single perf concern:** the harness recreates `Store` + re-runs component instantiation per request — there is no instance pool. For 1k req/s on one function this is 200 ms – 2 s of CPU per server just in instantiation overhead.

**Biggest single correctness concern:** the W1 engine epoch ticker is missing, so the CPU cap is **not enforced** on any caller that uses `ComponentHarness` (the only committed entrypoint).

---

## 2. Hot-path walk-through

### 2.1 Cold start: `ComponentHarness::new(wasm_bytes)`

`harness.rs:88-97`:
1. `component_engine()` → `OnceLock` lookup; first call lazily builds a wasmtime `Engine` (Config: `wasm_component_model(true)`, `epoch_interruption(true)`; harness.rs:42-44). Subsequent calls are an `Arc::clone`.
2. `Component::new(&engine, wasm_bytes)` — **expensive**. This parses, validates, and JIT-compiles the component. For a 100 KiB Wasm component this is typically 5-50 ms wall on warm CPU.
3. `Linker::new(&engine)` + `add_host_to_linker(&mut linker)` — wires four trait impls (`host.rs:154-162`). Cheap: four `add_to_linker` calls (≪ 1 ms).

**Caching:** the compiled `Component` is stored in the `ComponentHarness` struct (`harness.rs:78`); a single shared `Engine` (`COMPONENT_ENGINE` static `OnceLock`, `harness.rs:35`) holds the JIT cache. **Caller responsibility** is to hold the `ComponentHarness` for the lifetime of the function (re-compile on every request is catastrophic — there is no de-duplication in `basin-fn` itself; the WIP `runtime.rs:160-167` adds a `(project,name,version)` cache).

### 2.2 Per-invocation cost: `ComponentHarness::run(ctx)`

`harness.rs:107-118` per call:

| Step | Cost estimate | Notes |
|---|---|---|
| `FunctionCallContext::new(ctx)` | < 1 µs | One struct wrap. |
| `FunctionHost::new(call_ctx)` | < 1 µs | One struct wrap. |
| `Store::new(&engine, host)` | ~10-50 µs | Wasmtime allocates Store internals + Arc clones engine. |
| `store.set_epoch_deadline(5)` | < 1 µs | One atomic store. |
| `BasinFunctions::instantiate(&mut store, …)` | **100 µs – 2 ms** | Real instantiation cost: linear memory init, table init, every import resolution. Dominant call. |
| `bindings.call_run(&mut store)` | guest-dependent | Plus the marshalling of `option<string>`. |

**Per-invocation Arc clones / allocations** that scale with the call (not with row count):
- `Arc<Engine>` clone inside `Store::new` (1).
- The four `Arc<dyn …>` inside `InvocationContext` (`engine.rs:158-167`) are cloned once into `FunctionHost` (4 clones).

### 2.3 Host-import roundtrip: `basin:fn/query`

`host.rs:61-74`:
1. Generated bindgen trampoline lowers WIT `string` → Rust `String` (one heap alloc).
2. `self.ctx.invocation.query.exec_sql(&sql)` — virtual call through `Arc<dyn QueryExecutor>` (`engine.rs:23-26`). Real implementation runs the full engine session.
3. Result rows are mapped: `rows.into_iter().map(|r| query::Row { columns: r.columns })` (`host.rs:68-72`). **`r.columns` is moved without allocation per cell**, but each `Row` allocates a fresh `Vec<query::Row>` of length `rows.len()`.

**Per-row cost:** ~O(columns) memory because the WIT-canonical-ABI lowering will copy each `(String,String)` into the guest's linear memory via `cabi_realloc` — *this happens inside `call_run`, not in our code*, but it is bounded by `2 × bytes(row)`. There is **no streaming `query` shape** — all rows are materialised in host memory first. A guest issuing `SELECT * FROM big_table` materialises the full result in Rust, copies it again into the guest. A 100 GB query is a 200 GB allocation. The WIT definition (`wit/basin-fn.wit:25-36`) does not expose any cursor type, so a guest cannot opt into streaming.

### 2.4 Host-import roundtrip: `basin:fn/http`

`host.rs:84-109` is the same pattern: copy request fields into `FnHttpRequest` (`engine.rs:104-109`), delegate, copy back. Per call: ≈ 6 small heap allocs (4 string clones for url/method + headers + body) — negligible vs the network RTT. No streaming bodies — the entire response body is materialised before returning to the guest.

### 2.5 Cost summary (one warm invocation, no host imports)

| Cost | Value |
|---|---|
| Cold harness construct | 5-50 ms (Component compile dominates) |
| Warm invocation overhead | 0.2-2 ms (Store + instantiate dominate) |
| Per host-import call | < 5 µs Rust-side; rest is downstream (SQL / HTTP) |
| Per row in `query` result | O(bytes); copied twice (host + guest) |

---

## 3. Governance gap matrix

For each cap in `FunctionCaps` (`governance.rs:67-78`), tracked against actual coverage on the committed path.

| Cap | Spec'd mechanism | Where wired | Actual coverage on `ComponentHarness::run` | Evasion vector |
|---|---|---|---|---|
| **CPU (epoch)** | `Engine::increment_epoch` ticked by tokio task every 100 ms; `set_epoch_deadline` + `epoch_deadline_trap()` (`governance.rs:259-287`, `327-330`) | Only on `FunctionGovernance`'s **own engine** (`governance.rs:258-263`) | **None.** `harness.rs:39-49` creates a SEPARATE process-wide `Engine` with `epoch_interruption(true)` but no ticker; `harness.rs:114` sets a deadline of 5 epochs that will never elapse. `epoch_deadline_trap()` is also not called on this store, so even if the epoch advanced the behaviour would be `Trap` only by default — but the deadline never advances anyway. | **Trivial:** `(loop $spin (br $spin))` in any guest that runs through `ComponentHarness::run`. Spins forever; only OS preemption + thread cancellation by tokio (which doesn't kill the blocking thread) can stop it. |
| **CPU (W5 path)** | Same | Only on `FunctionGovernance::invoke_with_caps` (`governance.rs:343-379`); caller must build the Store against `gov.engine()` and call `gov.prepare_store(&mut store)`. | Works in tests (`governance_caps_test.rs:140-167`), but: (a) yielding back to host doesn't help the guest because the epoch is wall-time-ticked, not instruction-counted — **this design is correct**; (b) however, **a guest that loops inside the host import handler** (e.g. spams `basin:fn/log` in a tight loop) consumes CPU but every iteration returns to the host briefly. The epoch still fires (it's wall-clock), so this is bounded. | A spin loop *inside the host import code* (Rust side) wouldn't trigger the wasm trap because the epoch check happens on entry to wasm code. None of our host imports loop, so this is fine today, but a future host import with a Rust-side loop (e.g. a hypothetical streaming query) needs explicit cooperation. |
| **Memory (W1 path)** | n/a | n/a | **None.** No `memory_reservation` is set on the W1 engine (`harness.rs:42-48`); the default reservation in wasmtime 44 is ≈ 4 GiB (64-bit) per linear memory. | A guest can `memory.grow` up to wasmtime's default reservation — a single guest can OOM the host. |
| **Memory (W5 path)** | `Config::memory_reservation(max)` + `memory_reservation_for_growth(0)` + `memory_may_move(false)` (`governance.rs:207-211`) — engine-scoped | Only on the W5 engine | Works in `governance_caps_test.rs:196-241`. **But:** the task brief flagged "`ResourceLimiter` is NOT attached to `FunctionHost`" — confirmed: `host.rs:42-50` has no `ResourceLimiter` field; `MemoryLimiter` (`governance.rs:140-180`) is **defined but never installed** on any store. The acceptance test passes only because the engine-level reservation also traps. **The gap:** the engine-level cap is shared by all stores derived from that engine — once one store has grown to N bytes, the wasmtime reservation accounting is per-Store anyway, so per-invocation isolation is *probably* fine but `MemoryLimiter`-based per-invocation accounting (e.g. for fairness or richer metrics) is unreachable. | A guest sized exactly under the cap and slow-growing across many invocations reveals no current evasion of the *hard* cap, but the soft-fairness path is unavailable. |
| **Wall clock** | `tokio::time::timeout(wall, join)` over `spawn_blocking` (`governance.rs:359-362`); on timeout, bump epoch by `cpu_ticks+1` to also trap the wasm thread (`governance.rs:370-372`). | W5 only. | Works in `governance_caps_test.rs:251-281`. **Spawn_blocking thread does NOT get interrupted:** `tokio::task::spawn_blocking` returns a JoinHandle that's polled by the runtime; if the closure ignores the trap (e.g. spins in Rust code), the thread leaks until the closure returns naturally. The mitigation (bumping the epoch) only helps if execution is currently inside wasm code. **If the closure is doing host-side work (e.g. the real `QueryExecutor` waiting on a slow SQL query), the wall timeout returns but the SQL query continues until it completes.** | A guest issuing a `SELECT pg_sleep(60)` style query (via `basin:fn/query`) — the wall guard returns after 10 s, the spawn_blocking thread is still inside `exec_sql` and will return whenever the SQL finishes. The semaphore permit is held the entire time (because `_permit` lives in the async task's frame, `governance.rs:349`). |
| **Per-project concurrency** | `DashMap<ProjectId, Arc<Semaphore>>` with `project_concurrency` permits each (`governance.rs:232, 302-309`); acquired in `invoke_with_caps` (`governance.rs:348-353`). | W5 only. | Works (`governance_caps_test.rs:285-327`). **Per-project semaphore map leaks unboundedly** — `project_sem` inserts via `DashMap::entry().or_insert_with(...)` (`governance.rs:305-308`) and **never evicts**. A project who creates 100 K projects and calls each once leaves 100 K `Arc<Semaphore>` entries forever. Per the user-memory guidance ("Per-project cost must be O(bytes), not O(pool)"), this is the wrong shape: it's O(distinct projects ever seen) × ~64 bytes per entry. | Adversary creates many short-lived projects → unbounded heap growth. ~6.4 MB per million projects — not immediate, but un-GC'd. |
| **Wall timeout vs semaphore interaction** | n/a | n/a | When the wall timeout fires (`governance.rs:364`), `invoke_with_caps` returns an error. `_permit` is dropped at that point (the `match` is the last use). **But** the spawn_blocking task is still running (per above); the next caller to acquire the semaphore can race with the still-running prior task on host resources (e.g. file handles, DB connections from the executor). The permit count is back to N, but actual in-flight work is N+1. | Guest that holds wall-clock budget tight + does CPU-heavy work in host imports — exceed permit ceiling sub-rosa. |

---

## 4. Adversarial scenarios

Each row: scenario → blast radius → mitigation status as of today's committed code.

| # | Scenario | Path | Blast radius | Mitigation |
|---|---|---|---|---|
| A1 | **CPU fork-bomb** — `(loop $spin (br $spin))` in `run` | `ComponentHarness::run` (W1) | This invocation **and** the spawn_blocking thread forever; project's project semaphore (if wired) permanently held. With 16 default permits, 16 such calls block all further function calls for that project. | **None** for W1 path: no ticker, no trap, no wall guard. W5 path mitigates only if caller routes through `invoke_with_caps`. There is no caller of `invoke_with_caps` outside tests yet. |
| A2 | **CPU fork-bomb (W5)** | `FunctionGovernance::invoke_with_caps` (governance.rs:343) | Bounded: epoch trap kills wasm in ≤ `cpu_ticks × EPOCH_TICK_MS` (≈ 5 s default). Wall guard kicks in at 10 s if the trap is somehow missed. | Effective on the W5 path. |
| A3 | **Memory hog** — `memory.grow` to 4 GiB | `ComponentHarness::run` | Single-invocation: ~4 GiB host memory until guest exits or the wasmtime default reservation is hit (which is platform-specific; on Linux it's address-space, but on memory-pressure systems an OOM-killer event kills the whole process — **all projects on this server**). | **None** on W1 path (no memory_reservation set). |
| A4 | **Memory hog (W5)** | governance.rs path | Engine reservation caps at `caps.memory_max_bytes` (64 MiB default) per Store. Guest gets a trap. | Effective. |
| A5 | **Hold project semaphore via wall clock** | governance.rs path | Sleep just under `wall_timeout` × `project_concurrency` simultaneous = fully consume permits; concurrent legitimate callers wait at `acquire_owned()` (`governance.rs:351`). | Mitigated by `project_concurrency` cap + wall timeout: worst-case wait is `wall_timeout` (10 s default). After that the malicious call returns Err but the permit returns. |
| A6 | **`query` 100 GB read** | `host.rs:62-73` → `QueryExecutor` | Host materialises all rows into a `Vec<QueryRow>`, then re-allocates the WIT lowered representation. **Potentially 200 GB of host heap.** Spawn_blocking thread OOMs → process abort → all projects. | **None at the host level.** The WIT shape (`wit/basin-fn.wit:25-36`) has no streaming primitive. The downstream `QueryExecutor` is expected to enforce row-count / byte limits but the trait has no signal. |
| A7 | **HTTP exfil + amplification** | `host.rs:84-109` → `HttpSend` | Per W1-followup commit comments: allowlist / rate-limit / body-cap / timeout "stay in the impl". The only impl is `MockHttpClient`. No production wiring → if a real impl is plugged in via `InvocationContext` without enforcing those guards, the guest reaches the open internet. | The trait contract is in comments only; nothing in the host enforces guards. |
| A8 | **Secret enumeration** | `host.rs:138-143` → `SecretStore` | The trait (`engine.rs:53-56`) takes a `name: &str` and the host code passes it straight through without project-scoping. Comments (`host.rs:128-137`) say "the `SecretStore` implementation is responsible for the project boundary" — i.e. the trait carries no project ID. A `SecretStore` implementation that maps `name → value` globally (the temptation when wiring catalog) would expose cross-project secrets. | Contract-only; not enforced at the host. |
| A9 | **Project semaphore map flood** | `governance.rs:302-309` | DashMap grows unbounded; ~64 bytes/project ever seen. | None. No TTL or LRU eviction. |
| A10 | **Concurrent same-project re-entry** | host imports during single invocation | Each invocation has its own `FunctionHost` (`harness.rs:108-110`); host imports don't share state — re-entry into `query` is serialised within a Store (wasmtime stores are not Send-across-call). | Fine by construction. |
| A11 | **Thread leak via spawn_blocking ignoring trap** | `governance.rs:359-378` | If the wasm thread is inside a host import (e.g. a long-running SQL query), the wall timeout returns but the thread is still alive. **Spawn_blocking threads come from a bounded pool (tokio default 512); enough leaks DOS the tokio runtime for everything (axum, basin-rest, all projects).** | Wall timeout returns control to caller; the thread itself is not killed. No `JoinHandle::abort()` could help — spawn_blocking is uncancellable. |

---

## 5. Concurrency notes

### 5.1 1000 simultaneous invocations on one server

- Per-call `Store::new` + `instantiate` cost adds up: 1000 × 1 ms = 1 CPU-s pure overhead.
- `spawn_blocking` pool default 512 — call #513 onward waits in a queue (W5 path); the W1 path doesn't use spawn_blocking at all and blocks whichever caller's thread runs `ComponentHarness::run` directly.
- Process-wide `COMPONENT_ENGINE` (`harness.rs:35`) is shared across all calls; wasmtime's JIT cache is thread-safe so no contention there.
- DashMap shards (`governance.rs:232`) give ~16-way sharding by default; semaphore acquire/release is fine under 1k QPS.

### 5.2 1000 on the same project (W5 path)

- Semaphore is `project_concurrency` = 16 by default. 984 callers wait at `acquire_owned()` (`governance.rs:351`).
- Wait is fair (FIFO via tokio Semaphore).
- Each waiting `_permit` future holds the closure capture; 984 closures × `work: FnOnce` size sits on the heap.
- If 16 calls each hit wall timeout, leaked spawn_blocking threads accumulate (see A11).

### 5.3 Host-import re-entry under contention

- `FunctionHost::ctx.invocation.query` is an `Arc<dyn QueryExecutor>`; `exec_sql(&self, …)` takes `&self`, so the executor must be `Send + Sync` and internally synchronise.
- The current `MockQueryExecutor` is stateless — fine.
- The (not-yet-existing) real executor backed by `ProjectSession::execute` would need to handle concurrent calls from different `FunctionHost` instances; this is **outside basin-fn's scope** but is a load-bearing assumption for correctness. Nothing in `basin-fn` documents or tests it.

---

## 6. Concrete TODO list (ranked by leverage)

| # | Fix | File:line | Effort | Why |
|---|---|---|---|---|
| **1** | **Either commit `handler.rs` or revert `lib.rs` to not reference it.** | `crates/basin-fn/src/lib.rs:42, 56` | 5 min (revert) or land WIP (1-2 h) | HEAD does not build. |
| **2** | **Wire `ComponentHarness::run` through `FunctionGovernance::invoke_with_caps`.** Either: (a) make `ComponentHarness::new` take a `&FunctionGovernance`, build against `gov.engine()`, and call `gov.prepare_store` + `invoke_with_caps`; or (b) start an epoch ticker on the W1 engine to at least give the existing `set_epoch_deadline(5)` teeth. | `harness.rs:39-49, 88-118` | 1-2 h | P0 correctness: CPU cap currently unenforced on the only committed entrypoint. |
| **3** | **Apply `apply_memory_cap_to_config` to the W1 engine** (or delete the W1 engine entirely and route all calls through W5). | `harness.rs:42-48` | 30 min | P1: 4 GiB grow is undefended. |
| **4** | **Bound the per-project semaphore map.** Either: LRU eviction on insert past a threshold (e.g. 10 K entries), or scheduled eviction of zero-held semaphores, or shift to a shared sharded semaphore keyed on (project_hash mod N). | `governance.rs:232, 302-309` | 1 h | Per the codebase rule "Per-project cost must be O(bytes), not O(pool)". |
| **5** | **Document / fix the spawn_blocking thread-leak on wall timeout.** Either (a) require the `work` closure to be cooperatively cancellable and document it; (b) give the executor an interrupter signal; (c) move host-import work that can block (HTTP, query) onto async paths so the wasm-thread side stays CPU-only and the epoch trap is sufficient. | `governance.rs:343-379` | 4-8 h (design) | P1: leaked threads consume the global pool. |
| **6** | **Add `project_id: ProjectId` to `FunctionCallContext`** and pass to `SecretStore::get_secret(project, name)` and `QueryExecutor::exec_sql` (where the executor would otherwise need to be project-scoped at construction time). Removes the "responsibility lives in the implementation" comment-only contract. | `host.rs:25-33, 138-143`; `engine.rs:23-26, 53-56` | 2-3 h | A8 mitigation. |
| **7** | **Document the WIT `query` streaming gap or add a `cursor` interface.** Today's `exec` returns the entire `list<row>` — there is no defence against a 100 GB read at the ABI level. | `wit/basin-fn.wit:25-36` | 1 day (design) | A6 mitigation. |
| **8** | **Either implement a `basin_net::HttpSend` adapter or correct the README claim** that `basin:fn/http` is "Wired". | `lib.rs:12`; `engine.rs:118-125` | 1-2 h | Documentation drift. |
| **9** | **Add `epoch_deadline_trap()` to W1's `ComponentHarness::run`.** Without it, the deadline default is `EpochDeadline::Trap` only because wasmtime 44 made it the default — verify; if not, the deadline fires no trap. | `harness.rs:114` | 5 min | Defence in depth even if fix #2 lands. |
| **10** | **Add a streaming bench / instantiation bench** to the test suite so the per-invocation cost is observable and regressions caught. | `tests/` | 2-4 h | Until cost is measured, the §2 estimates are guesses. |

---

## 7. WIP observations (not committed)

The working tree contains `handler.rs`, `runtime.rs`, `handler_test.rs` and modifications to `harness.rs`/`lib.rs`/`wit/basin-fn.wit`. Reviewing for context only:

- **`handler.rs:118` reuses `crate::harness::component_engine_pub()`** so the W2 world shares the W1 engine — meaning the W1 engine's missing-ticker issue (finding #1) is inherited by W2. Same `set_epoch_deadline(5)` line at `handler.rs:146`, same no-trap, same no-ticker. **The fix #2 above must apply to both worlds.**
- **`runtime.rs:251`** wraps `harness.handle` in `spawn_blocking` but does **not** acquire the project semaphore or call `invoke_with_caps` — it has its own ad-hoc governance hook trait (`runtime.rs:127-132`) that is unrelated to W5's `FunctionGovernance`. The two governance designs are not aligned; the runtime accepts an `Option<Arc<dyn FunctionGovernance>>` that has a `check` method (boolean gate) but no equivalent to `invoke_with_caps`'s wall-timeout / epoch bumping. A redeploy could land W6 with the W5 caps unenforced for HTTP-handler shape.
- **`runtime.rs:297` cache eviction is O(n)** (`cache.retain`) but n is bounded ("≪ 10⁴ entries per process") — fine for now.
- **`runtime.rs:287` `cache.write().await`** under the cache miss path can stall many concurrent callers on a cold-cache miss because compilation (`HandlerHarness::new`, `runtime.rs:283`) is done outside the lock but the *insert* re-acquires write; if compile takes 50 ms and 100 callers miss together, they serialize on the second write lock. Read-and-double-check inside is correct, but write contention is real.
- **`handler.rs` and `runtime.rs` both define a `FunctionStore` trait** with incompatible signatures (`handler.rs:187-196` vs `runtime.rs:106-110`). Caller has to pick one. Naming collision.

---

*End of audit.*
