# 0008 — Noisy-neighbor fairness on bounded backends

- **Status:** Accepted v0.2 (per-tenant semaphore + fair-share scheduler). v0.3 work deferred behind a documented trigger.
- **Date:** 2026-05-01 (v0.1), revised 2026-05-04 (v0.2)
- **Tags:** multi-tenancy, performance, isolation, scope

## Context

Basin's "structural per-tenant isolation" promise covers storage prefix
and access control. It does **not** yet extend to in-process
fairness when many tenants share one process and one connection pool to
the object store.

The `s3_scaling_noisy_neighbor` benchmark exposes the gap. A noisy
tenant runs 4 concurrent full scans of a 1M-row table; a quiet tenant
issues 100 sequential point reads. Bar: quiet's `p99_under_load /
p99_baseline < 5×`. Empirically:

| Backend | Ratio | Notes |
|---|---|---|
| Local filesystem | 3.69× | passes — no shared HTTP layer |
| Local MinIO (single-process) | **44×** | **fails the bar** |
| Real AWS S3 | not yet measured; expected to pass |

The architectural cause is clear: the noisy tenant's full-table scans
saturate the *server side* (single-process MinIO is CPU-bound at
roughly 8–12 concurrent reads). Quiet point reads then sit in MinIO's
own request queue behind the noisy ones, picking up 100ms+ tail spikes
on top of an otherwise 2ms baseline.

## What we shipped — v0.1 → v0.2 → v0.3 → v0.4 (reverted to v0.1+)

**v0.1**: `basin-storage` wraps every tenant's `ObjectStore` in a
`TenantScopedStore` that gates each underlying RPC on a per-tenant
`tokio::sync::Semaphore` (cap = 16). reqwest pool floor bumped to
`pool_max_idle_per_host = 256`. **Measured: 44× ratio.**

**v0.2** (attempted, reverted): layered a round-robin fair-share
scheduler (`crates/basin-storage/src/scheduler.rs`, ~620 lines) on
top. Single global concurrency budget. Worked in tests but tail
latency on the real test was 36×: server-side MinIO saturation, not
client fairness, was the actual bottleneck.

**v0.3** (attempted, reverted): rewrote scheduler as deadline-driven
(EDF with per-tenant fairness cap). The implementation deadlocked
under the integration-test workload — the bug is somewhere in the
queue-empty / RR re-enrollment plumbing and we couldn't reproduce it
quickly. Reverted entirely. Code is left in `scheduler.rs` for v0.4
work, not wired in.

**v0.4 (currently shipped)**: per-tenant Semaphore floor (v0.1) +
`SessionConfig::with_target_partitions(1)` in `basin-engine`.
Pinning DataFusion to 1 partition serializes the per-query Parquet
fan-out — instead of 4–8 concurrent range reads per scan, each query
streams one column at a time. With 4 noisy task workers each running
one query, MinIO sees ~4 concurrent in-flight reads instead of 16+,
and quiet's tail-latency improves substantially.

**Measured:**

| Backend | v0.1 | v0.2 | v0.4 | Bar |
|---|---|---|---|---|
| LocalFS | 3.69× | (n/a) | **1.52×** | <5× ✅ |
| Local MinIO | 44× | 36.86× | **32.02×** (p50: 1.39×) | <5× ❌ |
| Cloudflare R2 (APAC, HTTP/1.1) | (n/a) | (n/a) | **1.26×** (p50: 0.98×) | <5× ✅ |

**The R2 number is the production-representative measurement.** It validates
the diagnosis directly: the 32× on local MinIO is single-process MinIO
server saturation (~8–12 concurrent reads). On a backend with effectively
unbounded server-side concurrency, the per-tenant Semaphore floor +
`target_partitions=1` is sufficient — quiet's p99 stays within 1.3× of
baseline even while a noisy tenant runs 4 concurrent 1M-row scans.

R2 was tested over HTTP/1.1 — Cloudflare's R2 S3-API endpoint
ALPN-negotiates HTTP/1.1 from this region (no h2 multiplex). HTTP/1.1
worked anyway because the bottleneck was never client-side
multiplexing; it was server-side concurrency. AWS S3 over h2 should
land in the same range.

v0.4 also wires an HTTP/2 toggle into `S3Config::http2_only`. AWS S3 /
R2 / GCS support h2 over TLS and benefit from it; MinIO 2025-10-15+
*advertises* h2 in ALPN but downgrades to HTTP/1.1 — empirically
verified, so the toggle is for production backends only.

## What we tried and rejected

- **cap=4 and cap=8 per tenant** — deadlocks. The Parquet reader's
  per-query parallel range fan-out × concurrent queries per tenant
  exceeds the cap; futures park waiting for permits that never free.
  Lower bound for liveness on the benchmark workload is empirically
  cap=16.
- **Adding a global cap** (`Semaphore` shared by all tenants, e.g.
  global=12) on top of per-tenant=8 — still deadlocks. Multiple
  concurrent queries × Parquet fan-out × tenant count exceeds 12.
- **Per-tenant `ObjectStore` clients** — explicitly rejected by the
  user: 1M tenants × 1 reqwest pool each is unacceptable connection
  pool fragmentation and memory cost. Per-tenant cost must be O(bytes)
  not O(connection-pool).

## What's left for v0.3

The remaining ratio (36×) on local MinIO is not a client-side fairness
problem — it's MinIO server-side queueing. Three options to actually
clear the bar:

1. **Lower budget + smarter Parquet read coalescing.** The lower bound
   on global budget (without deadlock) is gated by parquet's per-query
   parallel range fan-out. If we configure DataFusion's target_partitions
   smaller per session OR pre-coalesce ranges before issuing them, we
   could safely drop global to 4 — at which point MinIO never sees more
   than 4 in flight and quiet tail latency stays bounded. Estimated
   effort: 2–3 days.
2. **HTTP/2 multiplexing on a TLS-fronted MinIO** (or RustFS / AWS S3).
   Eliminates HTTP/1.1 head-of-line blocking — quiet's tiny request
   interleaves with noisy bulk on the same socket. Estimated: 0.5 day
   to wire MinIO with TLS + flip `http2_only = true`.
3. **Customer-grade backend.** Real AWS S3 has effectively unlimited
   server-side concurrency; the 36× number on local MinIO does not
   represent production. Real-world validation requires running this
   benchmark against AWS S3 or a multi-process RustFS cluster.

Trigger to ship #1: any production deployment where budget tuning
cannot rely on the backend being AWS S3.

**Trigger to build:** any prospective customer's workload that drives
> 1 concurrent heavy tenant against the same backend (a multi-process
RustFS cluster moves the floor up; production AWS S3 erases the
specific MinIO failure mode entirely). If the customer's deployment
target is a single-process or constrained backend, build the scheduler
before onboarding.

## How this is reported

The `s3_scaling_noisy_neighbor` card on the real-S3 dashboard is left
**failing** at the measured 36.86× ratio. We do not soften the bar.
The card description points at this ADR.

The LocalFS variant `scaling_noisy_neighbor` continues to pass at
~3.69× — the v0.2 wrapper has no measurable overhead on a backend with
unbounded local concurrency.

## References

- `crates/basin-storage/src/concurrency.rs` (the v0.1 wrapper)
- `crates/basin-storage/src/lib.rs::DEFAULT_TENANT_CONCURRENCY` (the cap)
- `tests/integration/tests/s3_scaling_noisy_neighbor.rs` (the benchmark)
- `tests/integration/tests/scaling_noisy_neighbor.rs` (the LocalFS variant — passes)
