---
title: "Operator runbooks — index"
nav_section: operations
sidebar_position: 49
summary: "Index of all Basin operator runbooks for day-2 production operations."
tags: [operations, runbooks, index]
---

# Operator runbooks

Production operator guides for Basin. Each runbook covers the subsystem's
architecture summary, metrics, configuration knobs, common alerts, operations,
troubleshooting procedures, and failure modes.

---

## Runbooks

| Runbook | Subsystem | Primary incidents covered |
|---|---|---|
| [lease-ownership.md](./lease-ownership.md) | Lease ownership + heartbeat budget coordination | Stuck lease, hot replica, whale partition, coordinator Postgres failure |
| [storage.md](./storage.md) | Object store, disk cache, page cache, I/O scheduler, KMS encryption, hot-tier memtable | Cache miss storms, write stalls (memtable hard cap), KMS errors, ObjectStore unavailability |
| [realtime.md](./realtime.md) | SSE / WebSocket change-event fan-out, per-project budget | BUFFER_FULL, subscriber lag, event drought, noisy project starvation |
| [wasm-functions.md](./wasm-functions.md) | Wasm component-model edge functions, per-invocation governance | CPU trap, memory cap, wall-clock timeout, concurrency exhaustion, worker thread pool saturation |
| [session-pool.md](./session-pool.md) | Native session pool (ADR 0007) | Pool exhaustion, per-project cap stall, guard leak, eviction loop stopped |

---

## Common first steps for any incident

1. **Check the coordinator Postgres** — it is the shared dependency across the
   lease, heartbeat, and session-open paths. If `basin_heartbeat_lag_ms` p99
   is rising, every subsystem that touches Postgres is degraded.
   See [lease-ownership.md](./lease-ownership.md).

2. **Check the over-cap metrics** — `basin_budget_over_cap_seconds_total` is
   the noisy-project signal across all cap types:
   - `cap=memtable_bytes` → storage write stall
   - `cap=realtime_bytes` → realtime BUFFER_FULL
   - `cap=wasm_concurrency` → edge function concurrency gate
   - `cap=rest_qps` → REST API rate limit
   - `cap=pg_qps` → SQL execution rate limit

3. **Check the ObjectStore** — the storage, WAL flush, and memtable flush paths
   all depend on the object store. A slow or unavailable object store shows up
   as elevated disk-cache miss rates and memtable growth.

---

## Metric quick-reference

The table below maps the most-paged alerts to their primary metric and runbook.

| Alert condition | Metric | Runbook |
|---|---|---|
| Coordinator Postgres slow | `basin_heartbeat_lag_ms` p99 > 1 s | lease-ownership |
| Lease stuck | `basin_lease_renew_total{result=ok}` flatlined for one replica | lease-ownership |
| Hot replica | `basin_lease_holdings_total` > 2× cluster median | lease-ownership |
| Page cache degraded | `basin_page_cache_hits / (hits + misses)` < 50 % | storage |
| Disk cache thrashing | `rate(basin_disk_cache_evictions_total)` > 100 | storage |
| Memtable write stall | `basin_budget_over_cap_seconds_total{cap=memtable_bytes}` rising | storage |
| KMS failure | ERROR log: `aes-gcm decrypt` / `envelope decrypt` | storage |
| Realtime BUFFER_FULL | `basin_budget_over_cap_seconds_total{cap=realtime_bytes}` rising | realtime |
| Subscriber lag spike | `basin_realtime_lag_events` near 1 024 | realtime |
| Wasm CPU trap storm | `basin_fn_invocations_total{result=cpu_trap}` rate > 0 | wasm-functions |
| Wasm wall timeout | `basin_fn_invocations_total{result=wall_timeout}` rate > 0 | wasm-functions |
| Wasm concurrency gate | `basin_budget_over_cap_seconds_total{cap=wasm_concurrency}` rising | wasm-functions |
| Session pool full | `basin_pool_resident_sessions` near `max_sessions` | session-pool |
| Session pool miss rate high | `basin_pool_hits / (hits + misses)` < 70 % | session-pool |
