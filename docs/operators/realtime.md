---
title: "Realtime (SSE / WebSocket) — operator runbook"
nav_section: operations
sidebar_position: 52
summary: "Day-2 ops guide for Basin's realtime change-event fan-out: SSE and WebSocket subscriptions, per-project budget enforcement, BUFFER_FULL handling, subscriber lag, and replay-on-reconnect."
tags: [operations, realtime, sse, websocket, budget, subscriptions]
---

# Realtime (SSE / WebSocket) — operator runbook

Day-2 operator's guide to Basin's change-event fan-out subsystem
(`basin-realtime`, Phase 5.11.R series).

On every committed INSERT / UPDATE / DELETE the engine posts a
`ChangeEvent` to the realtime sink, which broadcasts it to every
subscriber that has an open SSE or WebSocket connection on the
`(project, table)` pair. Per-project memory budget enforcement prevents
a single noisy project from starving the shared broadcast ring.

---

## Architecture in one page

```
Engine commit
  │
  ▼
RealtimeSink::publish(event)
  │
  ├─ BudgetTracker::try_reserve(project, estimate_event_size(event))
  │     OK  → continue
  │     Err(BufferFull) → drop to webhook retry log (durable); skip broadcast
  │
  └─ ChannelRegistry::send(ChannelKey{project, table}, Arc<event>)
        │
        └─ broadcast::Sender → all receivers for (project, table)
              │
              ├─ SSE handler (GET /realtime/v1/sse/:project/:table)
              └─ WebSocket handler (WS /realtime/v1/ws/:project/:table)
                     subscriber lag → RecvError::Lagged → reconnect + replay
```

The broadcast channel capacity per `(project, table)` is
`DEFAULT_CHANNEL_CAPACITY = 1 024` events. A receiver that falls behind
the ring by more than 1 024 events receives `RecvError::Lagged` and
must reconnect with `Last-Event-ID` to replay missed events from the
webhook retry log.

Key source files:
- `crates/basin-realtime/src/budget.rs` — per-project budget enforcement
- `crates/basin-realtime/src/lib.rs` — channel registry and sink
- `crates/basin-realtime/src/sse.rs` — SSE transport
- `crates/basin-realtime/src/ws.rs` — WebSocket transport
- `crates/basin-realtime/src/presence.rs` — presence tracking
- `crates/basin-realtime/src/filter.rs` — per-subscription event filters

---

## Metrics

The realtime subsystem reports into the shared `basin_budget_over_cap_seconds_total`
metric (from `basin-common::project_counters::LeaseMetrics`) and emits
internal counters on the same telemetry path.

> **Status — read before wiring a dashboard.** Basin does not serve a
> Prometheus scrape endpoint today. The names below are the planned
> OTLP / Prometheus-convention names; the engine emits structured `tracing`
> records that become OTLP metrics when an OpenTelemetry layer is attached
> (`BASIN_OTLP_ENDPOINT`, default `http://localhost:4318`). The only HTTP
> metrics route that exists is `GET /metrics/inflight`, which returns a small
> JSON in-flight/latency snapshot. Treat every `curl .../metrics` recipe on
> this page as the intended shape, not a working command.

| metric | type | dims | what it tells you |
|---|---|---|---|
| `basin_budget_over_cap_seconds_total{cap=realtime_bytes}` | counter | `project` | Seconds the project's in-flight realtime byte budget was at or above 100 % utilisation. Rising = BUFFER_FULL events are occurring. |
| `basin_realtime_events_published_total` | counter | `project`, `table` | Total events successfully broadcast to the ring. |
| `basin_realtime_events_dropped_total` | counter | `project`, `table` | Events redirected to the webhook retry log due to BUFFER_FULL. |
| `basin_realtime_subscribers_active` | gauge | `project`, `table` | Current number of live SSE / WebSocket receivers. |
| `basin_realtime_lag_events` | gauge | `project`, `table` | How far behind the slowest receiver is on this channel's ring. |

> **Leading indicator**: `basin_budget_over_cap_seconds_total{cap=realtime_bytes}`
> is the first signal of a noisy project. A rising value means that project's
> events are being dropped to the retry log and its real-time latency for
> subscribers is degraded — they receive events from replay, not the live ring.

---

## Configuration knobs

| env var | default | description |
|---|---|---|
| `BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES` | 16 777 216 (16 MiB) | Per-project in-flight byte cap. When in-flight bytes for a project reach this, new events are redirected to the webhook retry log rather than the broadcast ring. |

The `ChannelRegistry` channel size (`DEFAULT_CHANNEL_CAPACITY = 1 024` events)
is a compile-time constant in `crates/basin-realtime/src/lib.rs`. Raising it
requires a code change and recompile. Do not raise it arbitrarily — each
broadcast ring slot holds an `Arc<ChangeEvent>` (~128 bytes + payload), so
at 1 024 × 1 KiB per event that is ~1 MiB per active `(project, table)` pair.
A SaaS deployment with 1 000 active subscriptions would hold ~1 GiB of ring
buffer in RAM.

---

## Common alerts

### ALERT: BUFFER_FULL rate rising for a project

**Trigger**: `rate(basin_budget_over_cap_seconds_total{cap=realtime_bytes}[5m]) > 0`
for a project.

**What happened**: The project's `BudgetTracker` has rejected one or more
publish calls because its in-flight byte counter hit
`BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES`. The dropped events are in the
webhook retry log — no data is lost, but subscribers on that project will
experience delayed delivery (via replay on reconnect, not the live ring).

**Diagnosis**:

```bash
# Confirm from logs — look for BUFFER_FULL messages
grep -E 'BUFFER_FULL|per-project realtime budget' /var/log/basin-server.log \
  | grep '<project_uuid>' | tail -20
```

Check current in-flight bytes:

```bash
# Hits the /v1/admin/realtime/budget endpoint (if wired in the server).
curl -s http://localhost:8080/v1/admin/realtime/budget | jq '.projects[] | select(.bytes_in_flight > 0)'
```

**Remediation — short-term** (if the project has legitimate spike traffic):

```bash
# Increase the per-project budget globally (requires restart).
BASIN_REALTIME_PER_PROJECT_BUDGET_BYTES=33554432  # 32 MiB
```

**Remediation — long-term** (if the project is noisy):

1. Reduce the project's write rate via the REST QPS cap:
   ```sql
   ALTER PROJECT acme_prod SET basin.rest_qps_cap = 500;
   ```
2. Apply event filters on the subscription to reduce fan-out volume — the
   client can subscribe to a subset of operations (`INSERT` only) or a
   column-level filter (Phase 5.11.R5) to reduce bytes per event.
3. Check whether the project is doing bulk imports — a large `COPY` or
   multi-row `INSERT` generates one `ChangeEvent` per row. Batch imports
   should use the cold load path (direct Parquet upload), not the SQL
   transactional path.

---

### ALERT: subscriber lag at or above ring capacity (1 024 events)

**Trigger**: `basin_realtime_lag_events{project=X,table=Y}` approaching 1 024.

**What happened**: One or more subscribers on this `(project, table)` have
fallen behind the broadcast ring. When lag reaches the ring capacity they will
receive `RecvError::Lagged` and be evicted from the ring. They must reconnect
and replay from the webhook retry log.

**Common cause**: A subscriber connection with a slow consumer (mobile
client, flaky network, overloaded Lambda) is holding the ring's oldest slot
while new events accumulate.

**Note**: subscriber lag does NOT starve other subscribers. Each receiver has
its own read cursor; the ring only blocks producers when the *slowest* receiver
is 1 024 events behind. Producers (the realtime sink) are not blocked by slow
receivers — a lagged receiver is simply sent `Lagged` and evicted.

**Remediation**:

The `RecvError::Lagged` path is self-healing — the slow client will reconnect
and replay. If the lag is caused by a genuinely defunct subscriber (connection
zombie), it will time out at the TCP keepalive or SSE heartbeat interval
(heartbeats are emitted every 15 s on the SSE path). At that point the
connection is closed and the lagging receiver is dropped.

If you want to proactively drop lagging connections:

```bash
# Not yet a DDL surface — must be done by restarting the replica holding
# the channel, or by closing the specific connection at the load balancer /
# ingress level (identify by project+table and the SSE response stream ID).
```

---

### ALERT: no events reaching subscribers (event drought)

**Trigger**: `rate(basin_realtime_events_published_total[5m])` = 0 for a
project that has active subscribers and known write traffic.

**Diagnosis** (work through in order):

1. **Is the realtime sink attached to the engine?** The sink is attached at
   startup via `Engine::attach_post_commit_sink`. Check the startup log:
   ```
   grep 'realtime sink' /var/log/basin-server.log | tail -5
   ```
2. **Are writes landing?** Check that the engine is receiving commits:
   ```sql
   SELECT count(*), max(committed_at) FROM acme_prod.orders WHERE committed_at > now() - interval '5 minutes';
   ```
3. **Is the BudgetTracker permanently full?** If `bytes_in_flight` is stuck at
   the hard cap and never drains, a bug may have caused a `BudgetGuard` to leak
   (dropped without releasing). This would require a replica restart.
   Check `basin_budget_over_cap_seconds_total{cap=realtime_bytes}` — if it is
   monotonically rising even after write traffic stops, restart the replica.
4. **Is the ChannelRegistry keyed correctly?** Each subscription is keyed on
   `(project_id_uuid, table_name_exact_string)`. A case mismatch in the table
   name means the subscriber is on a different channel. Verify via the
   subscription handshake log entry.

---

## Common operations

### List active subscriptions for a project

```bash
# Hits the /v1/admin/realtime/subscribers endpoint (if wired).
curl -s http://localhost:8080/v1/admin/realtime/subscribers \
  | jq '.[] | select(.project == "<uuid>")'
```

### Check in-flight bytes for all projects

```bash
curl -s http://localhost:9090/metrics \
  | grep basin_budget_over_cap_seconds_total.*realtime_bytes
```

A non-zero value for any project means that project has had BUFFER_FULL events.
A rising rate (i.e. it increases between scrapes) means it is currently active.

### Simulate a reconnect + replay for a subscriber

SSE clients that support `Last-Event-ID` reconnect automatically. To manually
simulate:

```bash
# Replace <last_seq> with the last seq the client processed.
curl -H "Authorization: Bearer <jwt>" \
     -H "Last-Event-ID: <last_seq>" \
     "http://localhost:8080/realtime/v1/sse/<project>/<table>"
```

The handler will drain missed events (those with `seq > last_seq`) from the
webhook retry log before joining the live ring. This is the correct at-least-
once delivery path after a BUFFER_FULL or lag eviction.

### Force-close all realtime connections for a project (emergency)

There is no DDL surface for this yet. Options:

1. **Replica restart** — closes all connections on that replica. Other
   replicas unaffected. Use if a zombie connection is causing a permanent
   budget stall.
2. **Load balancer drain** — set the project's traffic to 0 at the ingress
   level; existing connections will drain via TCP RST.

---

## Troubleshooting

### Events are delayed by several seconds

If committed mutations are not appearing on subscriber connections within
1–2 seconds:

1. Check `RealtimeSink::publish` call latency — it should be sub-millisecond
   (atomic CAS + broadcast send). A slow publish means the event loop is
   blocked.
2. Check if the engine's post-commit hook is synchronous — if a slow sink
   is blocking the commit ack path, write latency rises. The realtime sink
   is designed to be non-blocking (publish is a send to a bounded ring; if
   the ring is full, it drops, not blocks). If you see write latency
   correlating with subscriber count, this is the integration point to investigate.
3. Check SSE proxy keep-alive settings — many HTTP proxies and load balancers
   buffer SSE streams. Nginx default proxy buffer will hold all SSE frames
   until the buffer flushes. Set `proxy_buffering off;` for the realtime route.

### WebSocket connections drop and reconnect frequently

1. Check the server's WebSocket idle timeout. Basin's WS handler emits a
   ping every 30 s (Phase 5.11.R3) — if the load balancer has a shorter idle
   timeout, it will close the connection before the ping arrives. Set the LB
   idle timeout to ≥ 60 s.
2. Check memory pressure on the replica — if the process is OOM-killed and
   restarted, all WS connections drop. Check `basin_page_cache_current_bytes`
   and `ProjectMemState::bytes_allocated` sum.
3. Check for epoch changes on the JWT — WebSocket auth is verified at
   connection upgrade time. If the JWT expires mid-session, the connection
   is closed. The client must re-authenticate and reconnect.

---

## Failure modes summary

| Failure | Visible signal | Behaviour | Recovery |
|---|---|---|---|
| **BUFFER_FULL (noisy project)** | `basin_budget_over_cap_seconds_total{cap=realtime_bytes}` rising | Events for that project dropped to webhook retry log. Other projects unaffected. | Increase budget, or reduce project write rate. Self-healing once in-flight bytes drain. |
| **Subscriber lagged out** | `basin_realtime_lag_events` at ring cap (1 024); subscriber receives `Lagged` error | Subscriber is evicted from ring. Client reconnects; replays missed events from retry log. | Self-healing; client must handle `Lagged` and reconnect. |
| **ChannelRegistry miss** | Subscriber gets no events; `basin_realtime_subscribers_active` = 0 for the subscription | No channel exists yet — first subscriber creates it; no events emitted before that point are in the ring. | First subscriber subscribe-before-first-write; or use replay cursor to catch up. |
| **Replica crash mid-broadcast** | All connections on that replica drop | Clients reconnect (another replica or the restarted replica). Replay cursor makes them whole. | Lease TTL-style recovery; no data loss (webhook retry log is durable). |
| **Realtime sink not attached** | Events committed to DB never appear on subscriptions | Sink attachment is at-startup. If omitted, realtime is a no-op — no errors, no events. | Restart with `Engine::attach_post_commit_sink` in the startup sequence. |

---

## Cross-references

- [ADR 0023 — Lease ownership + budget coordination](../decisions/0023-leases-and-partition-routing.md) — `basin_budget_over_cap_seconds_total{cap=realtime_bytes}` flows through the lease heartbeat budget push.
- [Lease ownership runbook](./lease-ownership.md) — the over-cap metric and the `CapLabel::RealtimeBytes` dimension.
- `crates/basin-realtime/src/budget.rs` — `DEFAULT_PER_PROJECT_BUDGET_BYTES`, `ENV_PER_PROJECT_BUDGET`, `BudgetError::BufferFull`.
- `crates/basin-realtime/src/lib.rs` — `DEFAULT_CHANNEL_CAPACITY`, `ChannelRegistry`, `RealtimeSink`.
