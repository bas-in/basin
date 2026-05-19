---
title: "WebSocket subscriptions — design spec"
nav_section: meta
sidebar_position: 32
summary: "Forward-spec for basin-rest /realtime/v1/subscribe. Tails the change-event primitive (ADR 0012); emits JSON over a multiplexed single-socket protocol. Realtime parity with Supabase."
---

# WebSocket subscriptions — design spec

## 1. Purpose & scope

WebSocket subscriptions are Basin's realtime parity with Supabase Realtime: a
server-pushed, table-level change-data-capture (CDC) stream delivered over an
authenticated WebSocket connection to browser and server clients.

**What realtime IS:**

A multiplexed WebSocket endpoint that tails the `ChangeEventSink` post-commit
stream (ADR 0012) and pushes `INSERT`, `UPDATE`, and `DELETE` events to
subscribed clients in near-real-time. A single connection supports subscriptions
to multiple tables. The event shape mirrors what basin-js exposes as
`channel().on('postgres_changes', …)`.

**What realtime IS NOT:**

- Not a generic pub/sub bus. Clients cannot publish messages to each other.
  The only publisher is the Basin engine's commit path. Client-to-client
  broadcast (presence, chat) is explicitly deferred to v0.2.
- Not a durable queue. Events are buffered in a bounded ring buffer per
  subscription; older events fall off. For durable delivery use the webhook
  sink (ADR 0012, Phase 5.11.I), which has its own disk-backed retry queue.
- Not a stream-processing platform. No aggregations, no windowed queries, no
  fan-in across projects. Each event is a single committed row mutation,
  project-scoped, table-scoped.
- Not a replacement for `LISTEN` / `NOTIFY`. Basin does not implement the
  Postgres `LISTEN` / `NOTIFY` wire protocol (ADR 0012 explicitly defers full
  PG LISTEN/NOTIFY compat). Realtime is WebSocket-shaped from the start.

The scope is intentionally narrow: table CDC over WebSocket to authenticated
application clients. Everything else is a different primitive.

---

## 2. Wire protocol

The client opens **one WebSocket connection per session** to
`WS /realtime/v1/subscribe`. All table subscriptions for a given
`createClient` instance share that single socket (multiplexed by
`subscription_id`). The protocol is newline-delimited JSON frames; each
message is a single JSON object.

### Client → server frames

**Subscribe** — register interest in a table's change stream:

```json
{
  "type": "subscribe",
  "id": "sub_abc123",
  "schema": "public",
  "table": "users",
  "event": "INSERT",
  "filter": "active=true"
}
```

`id` is a client-generated opaque string used to correlate events back to
this subscription. `event` is one of `INSERT`, `UPDATE`, `DELETE`, or `*`
(all). `filter` is an optional simple equality predicate (single column,
equality only; complex predicates are deferred to v0.2).

**Unsubscribe** — deregister a subscription:

```json
{ "type": "unsubscribe", "id": "sub_abc123" }
```

**Ping** — keepalive initiated by client:

```json
{ "type": "ping" }
```

### Server → client frames

**Event** — a committed row mutation matching a subscription:

```json
{
  "type": "event",
  "subscription_id": "sub_abc123",
  "table": "users",
  "event": "INSERT",
  "old": null,
  "new": { "id": 7, "name": "alice", "active": true }
}
```

`old` is `null` for `INSERT`; `new` is `null` for `DELETE`. Both are
present for `UPDATE`.

**Missed events** — emitted when the ring buffer overflows (see §3):

```json
{
  "type": "missed_events",
  "subscription_id": "sub_abc123",
  "since_lsn": "0/1A3F400"
}
```

**Error** — subscription-level or connection-level error:

```json
{
  "type": "error",
  "subscription_id": "sub_abc123",
  "code": "PERMISSION_DENIED",
  "message": "JWT does not have SELECT on table orders"
}
```

`subscription_id` is omitted for connection-level errors (e.g., JWT expired).

**Pong** — server reply to client ping:

```json
{ "type": "pong" }
```

**Subscribed** — acknowledgement that a Subscribe frame was accepted:

```json
{ "type": "subscribed", "id": "sub_abc123" }
```

Clients must ignore unknown `type` values per Postel's law (see §10).

---

## 3. Backpressure and reconnection

### Server-side ring buffer

Each active subscription is backed by an in-memory ring buffer of `ChangeEvent`
values. The default capacity is **1 000 events per subscription**; it is
configurable via the same limits framework as ADR 0014. The buffer is
per-subscription, not per-connection, so N subscriptions on one connection hold
N independent buffers.

The buffer is held in the tokio task that manages the connection (§5). Events
are pushed from the broadcast channel (§6) into the buffer and then drained
onto the WebSocket. If the WebSocket write path is blocked (slow client) and
the buffer fills, the oldest undelivered events are dropped. When a drop
occurs, the server emits a `missed_events` frame with the `since_lsn` of the
oldest lost event before resuming normal delivery.

### Slow-client policy

If the ring buffer has been at capacity for longer than 30 seconds (i.e., the
client is consuming events slower than they arrive), the server closes the
connection with WebSocket close code `1008` (Policy Violation) and a
`"slow_consumer"` reason. This prevents a single slow client from holding a
large allocation of memory indefinitely.

### Client reconnection and replay

When a client reconnects (after a drop, network partition, or slow-consumer
disconnect), it may supply a `?resume_from=<lsn>` query parameter on the
WebSocket upgrade URL. The server will replay any events still in the ring
buffer with `seq >= lsn` before switching to live delivery. Events that have
already fallen off the ring buffer are gone; the client is responsible for
detecting the gap and re-fetching baseline state from the REST API if needed.

The reconnection strategy in basin-js is exponential backoff starting at 250 ms,
capped at 30 s, with jitter. The `resume_from` LSN is stored in memory by
basin-js's socket manager and attached automatically on reconnect.

---

## 4. Auth model

### Upgrade authentication

Browsers cannot set the `Authorization` header on a WebSocket upgrade request.
The JWT is therefore delivered via one of two mechanisms (server accepts
either):

1. **`Sec-WebSocket-Protocol` header** — client advertises the JWT as a
   sub-protocol value: `Authorization, Bearer.<jwt>`. The server accepts
   the connection and echoes back `Sec-WebSocket-Protocol: Authorization`.
   This is the mechanism basin-js uses in browser environments.
2. **Query parameter** — `?access_token=<jwt>` on the upgrade URL. Accepted
   for environments where the Sec-WebSocket-Protocol trick is unavailable or
   inconvenient (e.g., CLI tooling, test harnesses). This URL-encodes the JWT,
   so HTTPS (WSS) is required; plaintext WS must reject the upgrade when an
   `access_token` param is present.

The JWT is validated **once at upgrade time** using the same basin-auth
verification path (ADR 0005) that basin-rest uses for HTTP requests. A
connection with an invalid or expired JWT is rejected immediately with HTTP
`401` before the WebSocket handshake completes.

### Per-subscription authorization

When the server processes a `Subscribe` frame, it checks whether the JWT's
`project` and `role` claims grant `SELECT` on the requested table. The check
uses the same ACL table as basin-auth (ADR 0013). If RLS policies are active
for the table, the CDC event pipeline respects them: the `new` and `old`
row values delivered to the subscriber are filtered to only the columns the
JWT's role can `SELECT`. Rows that fail RLS predicate evaluation are silently
dropped from the stream (not delivered, no error).

A JWT that expires while the connection is live causes the connection to be
closed on the next subscription attempt or at the next keepalive cycle
(whichever comes first). Clients should proactively reconnect before token
expiry using the same refresh flow as basin-js's REST auth client.

---

## 5. Transport details

### Upgrade path

`WS /realtime/v1/subscribe` is registered on the existing `axum` router in
`basin-rest`. The upgrade is handled by `axum::extract::ws::WebSocket`. The
route handler:

1. Extracts and validates the JWT (from `Sec-WebSocket-Protocol` or query
   param).
2. Enforces the per-project concurrent connection cap (§7).
3. Calls `socket.on_upgrade(handle_connection)`.

### Per-connection task

Each accepted connection spawns **one tokio task** (`tokio::spawn`) that owns:

- The `WebSocket` split halves (read + write).
- A `HashMap<SubscriptionId, SubscriptionState>` for all active subscriptions
  on this connection.
- Per-subscription ring buffers.
- A `tokio::time::interval` for ping/pong keepalive (30 s default).

The task runs a `tokio::select!` loop across the WebSocket read half, a merged
stream of broadcast channel receivers (one per active subscription), and the
keepalive interval.

### Broadcast channel

Each `(ProjectId, TableName)` pair has a `tokio::sync::broadcast::Sender<Arc<ChangeEvent>>`
held in a `DashMap` in the `RealtimeHub` struct. The hub is instantiated once
at server startup and shared (via `Arc`) with both the `ChangeEventSink`
implementation and the connection handlers.

When a Subscribe frame arrives, the connection task calls `hub.subscribe(project, table)`
which returns a `broadcast::Receiver`. When an Unsubscribe frame arrives,
the receiver is dropped.

---

## 6. Change event source

### Sink registration

`RealtimeHub` implements the `ChangeEventSink` trait (ADR 0012). It is
registered with the engine as a **post-commit sink** at startup:

```rust
engine.attach_post_commit_sink(Arc::clone(&realtime_hub));
```

On each `publish(&event)` call, the hub looks up the `broadcast::Sender` for
`(event.project, event.table)` in the `DashMap`. If no subscribers exist for
that pair, the lookup returns `None` and the event is dropped immediately
(zero allocation). If subscribers exist, `sender.send(Arc::clone(&event))` is
called once; all `broadcast::Receiver` instances across all connections and
all subscriptions on that `(project, table)` pair receive the pointer.

### Lazy channel creation

`broadcast::Sender` instances are created lazily on first Subscribe for a
given `(project, table)` and removed when the last subscriber drops. The
`DashMap` is the authoritative registry. The broadcast channel capacity is
set to `max(ring_buffer_capacity, 1024)` — large enough that a briefly
slow connection task does not cause the sender to fail.

### Scalability at 1k subscribers per table

`tokio::sync::broadcast` is a lock-free MPMC channel; `send` is O(1)
regardless of receiver count. At 1 000 concurrent subscribers on one table,
`publish` does one `Arc::clone` and one atomic store. The per-connection task
then copies the `Arc<ChangeEvent>` into its ring buffer independently. Memory
cost is one `Arc` per active subscriber (a pointer + ref count — 16 bytes)
plus the ring buffer allocations, which are bounded per §3.

---

## 7. Connection limits

These are enforced at the per-project level, consistent with ADR 0014's
limits framework. All values are configurable via the same project-limits
table that governs query concurrency and storage quotas.

| Limit | Default | Config key |
|---|---|---|
| Concurrent WebSocket connections per project | 1 000 | `realtime.max_connections` |
| Subscriptions per connection | 100 | `realtime.max_subscriptions_per_conn` |
| Events delivered per second per connection | 10 000 | `realtime.max_events_per_sec` |

When a connection attempt exceeds `realtime.max_connections`, the upgrade is
rejected with HTTP `429 Too Many Requests`.

When a Subscribe frame would exceed `realtime.max_subscriptions_per_conn`,
the server sends an `error` frame with code `SUBSCRIPTION_LIMIT_EXCEEDED` and
leaves the connection open.

The per-connection event rate limit is enforced with a token bucket. When the
bucket empties, events are held in the ring buffer (not dropped) until the
bucket refills; if the ring buffer fills while rate-limited, the overflow
policy in §3 applies (oldest events dropped, `missed_events` frame emitted).

---

## 8. Failure modes

### Slow client

Covered in §3. After 30 seconds at ring-buffer capacity, the server closes the
connection with code `1008` and reason `slow_consumer`. The client should
re-fetch baseline state via REST and reopen the WebSocket.

### Network partition

TCP keepalive is enabled on the underlying socket (`SO_KEEPALIVE`, 15 s idle,
5 s interval, 3 probes). At the application layer, the server sends a `ping`
frame every 30 s and expects a `pong` within 10 s; failure to receive a `pong`
closes the connection. basin-js responds to pings automatically. Clients that
do not implement pong will be disconnected after one missed cycle.

### Client crash / clean disconnect

The connection task's `tokio::spawn` future resolves when the WebSocket is
closed (either by the client sending a Close frame, or by the TCP RST being
observed). On resolution, the task drops all `broadcast::Receiver` handles,
which decrements the subscriber count on each channel. The ring buffer memory
is freed. No explicit cleanup RPC is needed.

### Server restart

All in-memory state (connections, ring buffers, broadcast channels) is lost on
restart. Clients receive a TCP RST and should reconnect with exponential
backoff. Reconnecting with `?resume_from=<lsn>` will not replay any events
because the ring buffers are empty; clients must re-fetch baseline state from
the REST API. The LSN is still useful as a staleness indicator: any event
emitted after the LSN and before reconnect is lost.

---

## 9. Observability

The following Prometheus metrics are exposed by the `RealtimeHub`:

| Metric | Labels | Description |
|---|---|---|
| `basin_realtime_active_subscriptions` | `project`, `table` | Current count of live subscriptions |
| `basin_realtime_active_connections` | `project` | Current count of open WebSocket connections |
| `basin_realtime_events_emitted_total` | `project`, `table` | Cumulative events pushed to clients |
| `basin_realtime_dropped_events_total` | `project`, `table`, `reason` | Events dropped (ring buffer overflow or slow client close); `reason` is `buffer_overflow` or `slow_consumer` |
| `basin_realtime_reconnects_total` | `project` | Client reconnections carrying `resume_from` |

`reason` on `dropped_events` distinguishes overflow (ring buffer full,
event dropped silently before `missed_events` frame is sent) from
slow-consumer (connection forcibly closed).

All metrics are registered on the shared `PrometheusRegistry` that basin-rest
already uses for its HTTP metrics; no new registry is needed.

---

## 10. API stability promise

The frame schema described in §2 is **wire-stable** from v0.1. The following
guarantees hold:

- Existing frame `type` values and their fields will not be removed or renamed
  in any v0.x release.
- New frame types may be added in any release. Clients must silently ignore
  unknown `type` values (Postel's law). basin-js's `protocol.ts` already
  implements a `switch` with a default no-op case for this reason.
- New **optional** fields may be added to existing frame types. Clients must
  not fail on unknown fields.
- The `subscription_id` / `id` correlation contract is stable: a `subscribed`
  frame always echoes the `id` sent in the `subscribe` frame.

Breaking changes to the wire protocol require a new path (`/realtime/v2/...`)
and a deprecation window of at least two minor releases.

---

## 11. Repo location

The implementation lives entirely within `crates/basin-rest/`:

```
crates/basin-rest/src/realtime/
  mod.rs        — RealtimeHub struct, ChangeEventSink impl, hub lifecycle
  protocol.rs   — Serde-typed frame enums (ClientFrame, ServerFrame)
  channel.rs    — broadcast channel registry, DashMap<(ProjectId, TableName), Sender>
  auth.rs       — JWT extraction from Sec-WebSocket-Protocol / query param, ACL check
crates/basin-rest/tests/
  realtime.rs   — integration tests: subscribe/event/unsubscribe, auth rejection,
                  ring buffer overflow, reconnect with resume_from
```

The `basin-realtime` workspace crate anticipated in ADR 0012's "Future
basin-realtime crate" note is **not** created for v0.1. The implementation is
small enough to live in `basin-rest` without polluting it. If the realtime
surface grows to require independent versioning or its own release cadence,
the code can be extracted with a `git mv` and a new `Cargo.toml` — the
`ChangeEventSink` trait boundary makes this mechanical. Both paths remain open
as ADR 0012 intended.

---

## 12. Estimate

ADR 0012 status: **Accepted, 2026-05-09** — the `ChangeEventSink` trait,
`EventSinkRegistry`, and the capture point in the executor's commit path are
shipped. The WebSocket realtime sink was explicitly deferred in that ADR and
is the subject of this design. No engine changes are required; this is a
`basin-rest` addition only.

| Component | Effort |
|---|---|
| `protocol.rs` — frame types, serde | 0.5 days |
| `auth.rs` — JWT extraction from WS upgrade, ACL check | 1 day |
| `channel.rs` — broadcast channel registry, `ChangeEventSink` impl | 2 days |
| `mod.rs` — connection handler task, ring buffer, select! loop | 3 days |
| axum route registration, limits enforcement | 1 day |
| Integration tests (`realtime.rs`) | 3 days |
| Observability (Prometheus metrics wiring) | 1 day |
| basin-js `socket.ts` / `channel.ts` wiring + reconnect | 4 days |
| End-to-end testing against a live engine | 2 days |
| **Total** | **~17–18 days (~3.5 person-weeks)** |

Range: **3–4 person-weeks** for v0.1. Lower bound assumes ADR 0012's sink
trait is stable and the axum WebSocket handler is the only new transport
surface. Upper bound absorbs one week for integration instability or
basin-auth edge cases in the WS upgrade flow.

---

## 13. What's deferred to v0.2

**Presence (who's online)**

Client join/leave events and an online-presence map per channel. Requires
server-side presence state (Redis-compatible or in-memory) and a new frame
type (`presence_join`, `presence_leave`, `presence_sync`). Deferred because
it requires client-to-server publishing, which is architecturally distinct
from the CDC push model.

**Broadcast (client-to-client)**

Arbitrary JSON messages from one client to all other subscribers on a channel.
This is generic pub/sub and is out of scope for the CDC-only v0.1 design.
Adding it later is additive (new frame types on the same socket).

**Server-side filter pushdown with complex predicates**

v0.1 supports a single equality filter (`column=value`) evaluated server-side.
Compound predicates (`status='paid' AND amount > 100`), range filters,
and joins are deferred. In v0.1, clients that need finer-grained filtering
receive all matching-table events and filter client-side. Server-side
pushdown reduces unnecessary event delivery at scale and is the natural v0.2
performance improvement once the basic flow is proven.

**Per-row RLS streaming**

Full per-row RLS evaluation on the CDC stream (not just column masking but
predicate-filtered row delivery) requires the engine to re-evaluate RLS
policies in the sink pipeline, which is a meaningful engine change. Deferred
pending a customer requirement that client-side filtering cannot satisfy.
