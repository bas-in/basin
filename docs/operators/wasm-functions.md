---
title: "Wasm functions — operator runbook"
nav_section: operations
sidebar_position: 53
summary: "Day-2 ops guide for Basin's Wasm component-model function execution: per-invocation CPU, memory, wall-clock, and concurrency caps; runtime isolation; starvation detection; and incident playbooks."
tags: [operations, wasm, functions, governance, caps, concurrency]
---

# Wasm functions — operator runbook

Day-2 operator's guide to Basin's Wasm function execution subsystem
(`basin-fn`, Phase 5.11.W series, hardened in Phase 6.P0.C).

Basin Edge Functions run as Wasm components (WASI Preview 2 / component model).
Every invocation is subject to four independent resource caps enforced by
`FunctionGovernance` — the key type in `crates/basin-fn/src/governance.rs`.

---

## Architecture in one page

```
HTTP request → basin-rest route handler
  │
  ▼
FunctionGovernance::invoke_with_caps(project, work)
  │
  ├─ [slice gate, Phase 6.X.D] SliceGate::try_consume(project, WasmConcurrency, 1)
  │       Err → 429 "WasmConcurrency slice exhausted"
  │
  ├─ project_sems[project].acquire_owned()   (per-project semaphore)
  │       blocks when project_concurrency permits are exhausted
  │
  └─ WasmRuntime::spawn_blocking(work)       (dedicated tokio runtime)
         + tokio::time::timeout(wall_timeout)
              │
              └─ ComponentHarness::run() / HandlerHarness::handle()
                    │
                    ├─ Store::set_epoch_deadline(cpu_ticks) + epoch_deadline_trap()
                    │       fires when cpu ticks elapsed; guest is interrupted
                    └─ Store::limiter(MemoryLimiter::new(memory_max_bytes))
                            fires when guest calls memory.grow past the cap
```

On wall-clock timeout: the engine bumps the epoch by `cpu_ticks + 1` to
interrupt any running Wasm thread, then returns an error to the caller.

On CPU tick exhaustion: the epoch ticker fires the trap inside the Wasm guest.

On memory cap exceeded: `MemoryLimiter::memory_growing` returns `Err`,
causing wasmtime to trap the guest immediately.

On concurrency cap exceeded: `acquire_owned()` blocks the caller until a
permit is released. At the coordinator-slice level (Phase 6.X.D), the slice
gate rejects immediately rather than queuing.

Key source files:
- `crates/basin-fn/src/governance.rs` — caps, semaphores, runtime, all four guards
- `crates/basin-fn/src/harness.rs` — component instantiation and execution
- `crates/basin-fn/src/handler.rs` — handler-world harness
- `crates/basin-fn/src/engine.rs` — host import impls

---

## Metrics

Wasm function metrics feed into the shared `basin_budget_over_cap_seconds_total`
counter and the internal governance observability surface.

| metric | type | dims | what it tells you |
|---|---|---|---|
| `basin_budget_over_cap_seconds_total{cap=wasm_concurrency}` | counter | `project` | Seconds the project's per-partition Wasm concurrency slice was exhausted (Phase 6.X.D slice gate). |
| `basin_fn_invocations_total` | counter | `project`, `result={ok,cpu_trap,mem_trap,wall_timeout,concurrency_blocked,error}` | Per-project invocation outcomes. `cpu_trap` and `wall_timeout` indicate runaway guests. |
| `basin_fn_invocation_duration_ms` | histogram | `project` | Wall-clock latency from `invoke_with_caps` entry to return. |
| `basin_fn_concurrency_in_flight` | gauge | `project` | In-flight invocations for this project (`project_concurrency - available_permits`). |
| `basin_fn_worker_threads_in_use` | gauge | — | Blocking threads occupied in the dedicated Wasm runtime (`BASIN_FN_WORKER_THREADS × max_blocking_threads_factor`). |

---

## Configuration knobs

All knobs are read at startup from environment variables. The process must be
restarted to pick up changes.

| env var | default | description |
|---|---|---|
| `BASIN_FN_CPU_TICKS` | 50 | Max epoch ticks per invocation. Each tick ≈ 100 ms, so default ≈ 5 s of CPU time before the epoch trap fires. |
| `BASIN_FN_MEM_MB` | 64 | Max linear-memory the guest may allocate, in MiB. The `MemoryLimiter` returns an error to wasmtime which traps the guest immediately. |
| `BASIN_FN_WALL_MS` | 10 000 | Max wall-clock milliseconds per invocation. On expiry, the engine bumps the epoch past the deadline to interrupt the Wasm thread and returns an error. |
| `BASIN_FN_PROJECT_CONCURRENCY` | 16 | Max concurrent in-flight invocations per project. Additional invocations queue on the per-project semaphore. |
| `BASIN_FN_PROJECT_SEM_CAP` | 10 000 | Max distinct projects whose per-project semaphore is held in the LRU. Past this, the least-recently-used project's semaphore is evicted. |
| `BASIN_FN_WORKER_THREADS` | 4 | Number of worker threads in the dedicated Wasm tokio runtime. Wasm invocations run on this runtime's blocking pool, isolated from axum, the shard-mode executor, and basin-net. |

**Epoch tick duration**: `EPOCH_TICK_MS = 100` ms. This is a compile-time
constant in `crates/basin-fn/src/governance.rs`. The CPU cap in wall-equivalent
time is `BASIN_FN_CPU_TICKS × EPOCH_TICK_MS` ms. At defaults: 50 × 100 ms = 5 s.

**Worker thread sizing rule of thumb**: `BASIN_FN_WORKER_THREADS` controls
both the async workers AND (via the `max_blocking_threads` multiplier of 16)
the blocking pool. At the default of 4 workers, the blocking pool is
`max(4 × 16, 16) = 64` threads. The per-project semaphore (16 permits ×
number of active projects) is the actual in-flight ceiling — the blocking pool
is sized to accommodate all potential in-flight Wasm calls without queueing on
the thread pool itself.

---

## Common alerts

### ALERT: cpu_trap rate non-zero for a project

**Trigger**: `rate(basin_fn_invocations_total{result=cpu_trap}[5m]) > 0` for a project.

**What happened**: A Wasm guest function consumed `BASIN_FN_CPU_TICKS` epoch
ticks (default ≈ 5 s of CPU) and was interrupted by the epoch deadline trap.
The guest is killed; the invocation returns an error to the HTTP caller (502).

**Common cause**:

- An infinite loop or very expensive computation in the function body.
- A function that performs unbounded recursion or large allocations
  (allocation itself counts against CPU time because `memory.grow` is
  instrumented by the epoch scheduler on some wasmtime builds).
- An accidentally-deployed production function with a compute bug.

**Remediation**:

1. Check which function is trapping — the log entry includes the
   `project_id` and the function slug.
2. Inspect the function source for unbounded loops or large computations.
3. If the function *legitimately* needs more CPU time, increase the cap
   only for that project (not globally):
   ```bash
   # For now this is a global env override; per-project overrides are
   # a Phase 6.X.D governance feature (not yet landed).
   BASIN_FN_CPU_TICKS=200  # ≈ 20 s
   ```
4. If the function is from an untrusted source, review and reject the
   deployment. The `cpu_trap` is the safety net, not the design intent.

---

### ALERT: wall_timeout rate non-zero for a project

**Trigger**: `rate(basin_fn_invocations_total{result=wall_timeout}[5m]) > 0`.

**What happened**: The wall-clock timer (`BASIN_FN_WALL_MS`, default 10 s)
fired before the Wasm guest returned. The engine bumped the epoch to interrupt
the blocking Wasm thread and returned a 502 to the caller.

**Difference from cpu_trap**: `cpu_trap` means the *CPU* budget was
exhausted (blocking on Wasm instructions). `wall_timeout` means the wall-clock
limit was hit — this can happen even if the guest was mostly sleeping (e.g.
waiting on an external HTTP call via `basin:fn/http`). A function that makes
a slow outbound HTTP call and then does minimal compute can wall-timeout without
cpu-trapping.

**Remediation**:

1. Check whether the function makes outbound HTTP calls — the
   `basin:fn/http` import has its own timeout separate from the
   Wasm wall-clock. If the external service is slow, the wall-clock runs out.
2. If the wall clock is genuinely too tight, increase `BASIN_FN_WALL_MS`.
3. If the function is indefinitely blocking on an external dependency, this
   is an application-level bug — the function should implement its own
   timeout on the outbound call.

---

### ALERT: concurrency cap exhausted for a project

**Trigger**: `basin_budget_over_cap_seconds_total{cap=wasm_concurrency}` rising
(Phase 6.X.D slice gate), OR `basin_fn_concurrency_in_flight` pegged at
`BASIN_FN_PROJECT_CONCURRENCY` for an extended window.

**What happened**: The project has `BASIN_FN_PROJECT_CONCURRENCY` (default 16)
invocations simultaneously in-flight and new invocations are queuing on the
per-project semaphore. If the slice gate is active (Phase 6.X.D), additional
invocations are rejected immediately with a 429.

**Common cause**:

- A burst of concurrent requests to the same function (flash traffic).
- Long-running functions holding permits for their full wall-clock budget
  while new traffic arrives.
- A retry storm: a function that returns an error causes the caller to retry
  immediately, creating a feedback loop of concurrent invocations.

**Remediation**:

1. Check `basin_fn_invocation_duration_ms` p99 for the project — are
   invocations running long? Long invocations hold permits longer, reducing
   effective throughput.
2. Increase concurrency only if the permits are being held for legitimate
   long-running work:
   ```bash
   BASIN_FN_PROJECT_CONCURRENCY=32
   ```
3. Reduce the permit hold time by optimising the function — reduce outbound
   I/O latency, reduce computation size.
4. If the burst is traffic-driven, check whether the project's upstream
   (API, cron trigger, webhook fan-out) should have rate limiting applied
   before the function invocation.

---

### ALERT: worker thread pool saturated

**Trigger**: `basin_fn_worker_threads_in_use` approaching
`BASIN_FN_WORKER_THREADS × 16`.

**What happened**: All available blocking threads in the dedicated Wasm
runtime are occupied. New invocations will queue inside `tokio::time::timeout`
waiting for a thread to become available. In the worst case, they will
wall-timeout while waiting for a thread, before the function body even starts.

**This is distinct from the per-project semaphore** — the semaphore caps
in-flight per project; the thread pool is the global ceiling across all
projects. Saturating the thread pool is a sign that the per-project semaphore
cap sum (`num_active_projects × BASIN_FN_PROJECT_CONCURRENCY`) exceeds the
blocking pool capacity.

**Remediation**:

1. Increase `BASIN_FN_WORKER_THREADS`. Restart required.
   ```bash
   BASIN_FN_WORKER_THREADS=8   # doubles the pool
   ```
2. Reduce `BASIN_FN_PROJECT_CONCURRENCY` to shed load — fewer concurrent
   invocations per project means fewer threads occupied simultaneously.
3. Check for function hangs — a function blocked on an external I/O call
   that never returns holds a thread indefinitely. This is the scenario
   where the wall-clock timer (`BASIN_FN_WALL_MS`) is the backstop. If
   wall-clock timeout is > 10 s and functions are hanging, reduce the timeout
   or fix the external dependency.

---

## Common operations

### Check in-flight invocations per project

```bash
curl -s http://localhost:9090/metrics | grep basin_fn_concurrency_in_flight
```

Or via the admin endpoint (NOT implemented yet — intended shape):

```bash
curl -s http://localhost:8080/v1/admin/fn/in-flight | jq .
```

### Check the dedicated Wasm runtime health

```bash
# Thread count in the basin-fn-wasm runtime
ps -T -p $(pgrep basin-server) | grep basin-fn-wasm | wc -l
```

The thread count should be at most
`BASIN_FN_WORKER_THREADS × max_blocking_threads_factor` (default: 4 × 16 = 64),
bounded by actual in-flight invocations.

### Read the effective caps in effect

```bash
grep -E 'BASIN_FN_' /proc/$(pgrep basin-server)/environ | tr '\0' '\n'
# Or check the startup log line:
grep 'FunctionCaps' /var/log/basin-server.log | head -3
```

### Identify which functions are trapping (cpu or wall)

```bash
grep -E 'cpu_trap|wall-clock timeout|epoch deadline|linear-memory cap' \
  /var/log/basin-server.log | tail -50
```

The log entries include `project=<uuid>` and `function=<slug>` structured
fields when emitted via tracing.

### Check the per-project semaphore LRU

The semaphore LRU holds at most `BASIN_FN_PROJECT_SEM_CAP` (default 10 000)
distinct project semaphores. At a real SaaS deployment this is effectively
unbounded for most workloads. If the LRU is evicting entries under load
(rare — requires > 10 000 distinct projects active in the same scrape window),
the evicted project's semaphore is reset, potentially allowing a brief window
of `project_concurrency × 2` in-flight invocations for that project.

If this is a concern, increase `BASIN_FN_PROJECT_SEM_CAP`:

```bash
BASIN_FN_PROJECT_SEM_CAP=50000  # 50k projects
```

---

## Troubleshooting

### Wasm invocation returns 502 with "epoch deadline trap"

This is a CPU timeout (`cpu_trap`). The function ran for more than
`BASIN_FN_CPU_TICKS × 100 ms` of CPU-equivalent time. Check:

1. Does the function have a compute-bound loop? Profile the function locally
   with `wasm-profiler` or the wasmtime CLI.
2. Is the CPU budget correct for this function's workload? Typical simple
   edge functions (transform, filter, validate) complete in < 100 ms of CPU.
   50 ticks (5 s) is generous. If this is a compute-heavy ML function, increase
   the CPU cap.

### Wasm invocation returns 502 with "wall-clock timeout exceeded"

1. Check whether the function makes an outbound HTTP call — `basin:fn/http`.
   If the upstream is slow or unavailable, the function blocks until the wall
   clock expires.
2. Check `BASIN_FN_WALL_MS`. Default is 10 s. If the function is a long-
   running batch job, this is the wrong invocation pattern — use a cron
   function with a higher wall budget.
3. Ensure the outbound HTTP call inside the function has a reasonable timeout
   set. The `HttpSend` host import does not apply a timeout by default (it
   inherits the host's `basin-net` connection timeout, which may be longer
   than the desired per-invocation budget).

### Wasm invocation returns 502 with "linear-memory cap exceeded"

The guest called `memory.grow` beyond `BASIN_FN_MEM_MB × 1 MiB`
(default 64 MiB). The `MemoryLimiter` returned an error from
`memory_growing`, causing a wasmtime trap.

1. Check the function for large in-memory data structures. A function that
   deserialises a large JSON payload and processes it in RAM can easily
   exceed 64 MiB for even modest dataset sizes.
2. Stream the data instead of loading it all into memory.
3. Increase `BASIN_FN_MEM_MB` if the function legitimately needs more:
   ```bash
   BASIN_FN_MEM_MB=128   # 128 MiB per invocation
   ```
   Note that at `project_concurrency = 16` simultaneous invocations, the
   maximum per-project RAM from Wasm linear memory is
   `16 × 128 MiB = 2 GiB`. This is a load-bearing sizing constraint.

### Functions succeed locally but fail in production with concurrency errors

The local dev environment typically runs with `BASIN_FN_PROJECT_CONCURRENCY`
at its default (16). If production traffic bursts above 16 simultaneous calls
to the same function from the same project, new calls queue on the semaphore.
If they timeout (wall-clock) while queued, they fail.

Check whether the production traffic pattern is bursty and whether the burst
exceeds 16 concurrent calls. If yes, either:
- Increase `BASIN_FN_PROJECT_CONCURRENCY`.
- Add upstream rate limiting before the function (e.g. REST QPS cap via
  `basin_budget_over_cap_seconds_total{cap=rest_qps}`).

---

## Failure modes summary

| Failure | Visible signal | Behaviour | Recovery |
|---|---|---|---|
| **CPU trap** | 502; log: `epoch deadline trap`; `basin_fn_invocations_total{result=cpu_trap}` | Wasm guest killed at CPU budget. Other invocations unaffected. | Fix function; optionally increase `BASIN_FN_CPU_TICKS`. |
| **Memory cap** | 502; log: `linear-memory cap exceeded`; trap in guest | Wasm guest killed. Host process unaffected. | Fix function memory usage; optionally increase `BASIN_FN_MEM_MB`. |
| **Wall timeout** | 502; log: `wall-clock timeout exceeded`; `basin_fn_invocations_total{result=wall_timeout}` | Guest interrupted via epoch bump. Blocking thread exits within one tick interval. | Fix external I/O latency or increase `BASIN_FN_WALL_MS`. |
| **Concurrency exhausted** | 429 (slice gate) or queuing on semaphore; `basin_budget_over_cap_seconds_total{cap=wasm_concurrency}` | New calls queue or reject. No data corruption. | Increase `BASIN_FN_PROJECT_CONCURRENCY` or `BASIN_FN_WORKER_THREADS`; reduce hold time. |
| **Runtime thread pool exhausted** | Wall timeouts while queued for a thread; `basin_fn_worker_threads_in_use` at ceiling | All invocations queue for a thread. Long invocations hold threads, starving others. | Increase `BASIN_FN_WORKER_THREADS`; check for function hangs (wall timeout too long). |
| **Dedicated runtime crash** | Process-level panic on `WasmRuntime::new` failure | Basin server exits (the runtime construction panics at startup). | Check OS thread limits (`ulimit -u`); reduce `BASIN_FN_WORKER_THREADS`. |

---

## Cross-references

- [ADR 0018 — Subsystem feature flags](../decisions/0018-subsystem-feature-flags.md) — the `component-model` Cargo feature gate.
- [Lease ownership runbook](./lease-ownership.md) — `basin_budget_over_cap_seconds_total{cap=wasm_concurrency}` is reported through the lease budget push (Phase 6.X.D).
- `crates/basin-fn/src/governance.rs` — all constants (`DEFAULT_CPU_TICKS`, `DEFAULT_MEMORY_MAX_BYTES`, `DEFAULT_WALL_TIMEOUT_MS`, `DEFAULT_PROJECT_CONCURRENCY`, `DEFAULT_PROJECT_SEMAPHORE_CAP`, `DEFAULT_WORKER_THREADS`) and `FunctionCaps::from_env`.
- `docs/audits/2026-05-21-noisy-neighbor-fairness.md` — item #16 that drove the W5 / 6.P0.C hardening.
