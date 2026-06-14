---
title: "ADR 0028 — CDC bridge: commit-path capture, WAL-resumable streams, phased sinks"
nav_section: decisions
sidebar_position: 28
summary: "Basin's CDC gap: customers need change streams out to Kafka, webhooks, and Postgres logical-replication consumers. This ADR picks the capture seam (post-commit ChangeEventSink, not WAL tail), solves tx-visibility and the unlogged fast-path gap, locks the LSN-cursor resumability model, defines retention interaction with WAL truncation, and sequences five independently shippable phases from webhook-stream to pgoutput wire compat."
tags: [cdc, replication, streaming, wal, realtime, kafka]
---

# 0028 — CDC bridge: change-data-capture architecture

- **Status:** Accepted (2026-06-12)
- **Tags:** cdc, replication, streaming, wal, realtime
- **Cross-references:**
  - [ADR 0012 — Change-event primitive](./0012-change-event-primitive.md) — `ChangeEventSink` trait + `ChangeEvent` struct; CDC reuses both.
  - [ADR 0020 — WAL transaction markers](./0020-wal-transaction-markers.md) — `TxBegin`/`TxCommit`/`TxRollback` framing; CDC's WAL-tail fast path depends on this.
  - [ADR 0016 — HTAP hot-tier architecture](./0016-htap-hot-tier-architecture.md) — hot-tier UPDATE/DELETE fast paths that bypass the WAL (see §3 GAP).
  - [ADR 0009 — Multi-region architecture](./0009-multi-region-architecture.md) — per-region WAL; CDC streams are per-region by construction.
  - [ADR 0023 — Lease-based partition routing](./0023-leases-and-partition-routing.md) — epoch fencing applies to WAL appends; CDC consumers must handle lease-handoff markers.
  - [ADR 0011 — Cross-shard 2PC (deferred)](./0011-cross-shard-2pc.md) — multi-project CDC across shards is out of scope until 2PC ships.

---

## Context

CDC is a named product gap. Customers building event-driven systems, feeding
downstream analytics, or using Debezium-style pipelines need change streams out
of Basin. The competitive set (Neon, Supabase) both expose logical replication
over the Postgres wire protocol. Not having CDC means Basin loses the "source of
record" position for any architecture that fans changes out to Kafka, event buses,
search indexes, caches, or audit pipelines.

Three questions must be answered before writing any code:

1. **Where is the capture seam?** WAL tail, commit-path hook, or realtime?
2. **How do transactions appear?** Committed-only, in order, with tx boundaries?
3. **What is the retention / WAL-truncation interaction?** A slow CDC consumer
   must not cause disk runaway; a fast consumer must not miss events on reconnect.

---

## Decision

### §1 — Capture seam: post-commit `ChangeEventSink` is the primary seam

CDC events are captured at the **post-commit `ChangeEventSink` hook** already
defined in `crates/basin-common/src/events.rs` (`ChangeEvent { project, table,
op, before, after, committed_at, seq, causation_user }`). The realtime WebSocket
subsystem (`basin-realtime` / `docs/websocket-subscription-design.md`) already
uses this hook as a **live broadcast** path. CDC reuses the same hook as a
**durable, resumable** path: a new `CdcSink` implements `ChangeEventSink` and
writes committed events to a per-project durable ring (Tigris object key or a
WAL sidecar).

**Why not WAL tail?** Basin's WAL payload is an opaque Arrow IPC `RecordBatch`
(`WalEntry.payload: Bytes`). The WAL never inspects payloads; they carry
columnar batches, not row-level before/after images. A CDC consumer tailing the
WAL would need to decode Arrow IPC, apply schema, diff UPDATE before/after, and
handle tx replay suppression (`replay_wal`) — all work the engine's commit path
already does. The `ChangeEvent` at the commit hook is the already-decoded,
already-tx-filtered, committed-only fact.

**The WAL tail does have one specific future use**: Postgres `pgoutput`/`wal2json`
consumers expect an LSN cursor. Basin's per-project LSN is the WAL LSN stamped
on each `WalEntry`. The `ChangeEvent.seq` field (a monotonic per-project counter)
serves as the public cursor; internally, the CDC ring stores the WAL LSN alongside
each event for the Phase 5 pgoutput bridge. This avoids exposing internal WAL
paths but preserves the LSN bookmark.

**Why not realtime's ring buffer?** The realtime ring (`DEFAULT_CHANNEL_CAPACITY
= 1024 events`, in-memory) is intentionally lossy: overflows are dropped, server
restart wipes state, no durable replay. CDC requires at-least-once, gap-free
delivery. The realtime path becomes a **fast-path overlay** in Phase 1: if the
CDC subscriber is live and caught up, events are dispatched from the hot ring; on
reconnect, gap-fill comes from the durable CDC store.

---

### §2 — Transaction visibility and ordering

The `ChangeEventSink` hook fires **post-commit** — after `catalog.append_data_files`
succeeds and after the txn overlay has been drained to the shared `MemTableRegistry`.
This guarantees:

- Only committed events are emitted. Rolled-back transactions never reach any sink
  (the executor discards `TxState::tx_overlay` on `ROLLBACK` before the hook fires).
- Multi-statement transactions emit one `ChangeEvent` per affected row, all with
  the same `committed_at` timestamp. The `seq` counter increments per-event within
  the tx, preserving row-level ordering.
- Auto-commit statements produce exactly one event per row, in execution order.

**Transaction boundaries in the CDC wire protocol** (Phases 4–5): the upstream
`ChangeEvent` struct does not carry a transaction id. Phase 4 (Kafka sink) and
Phase 5 (pgoutput) require tx grouping. The fix is a `tx_id: Option<u64>` field
on `ChangeEvent`, populated from `TxState`'s active transaction id at commit time.
Events sharing a `tx_id` belong to the same logical transaction. The ADR 0020
`tx_id` is an engine-instance-scoped `u64` seeded from startup micros — not
globally unique, not stable across restarts. For the pgoutput bridge a stable
LSN-range (`begin_lsn..commit_lsn`) replaces `tx_id` on the wire.

---

### §3 — The unlogged fast-path gap (KNOWN GAP; must be addressed)

**What the gap is.** Basin's HTAP hot-tier adds two fast paths for UPDATE and
DELETE that operate directly on the in-memory `MemTableRegistry`, bypassing the
WAL. Quoting `crates/basin-engine/src/dml_mutate.rs`:

```
/// path (which is also registry-only — no WAL).
```

Specifically:

- `hot_tier_update_by_pk` and `hot_tier_update_by_pk_tx` write `MemRowValue::Update`
  overrides directly into the registry (or `TxState::tx_overlay` for in-tx).
- `hot_tier_delete_by_pk` and `hot_tier_delete_by_pk_tx` write `MemRowValue::Tombstone`
  directly into the registry (or overlay).

These paths do call `htap_emit_wal_begin_lazy` (they emit a `TxBegin` marker),
but the row-level mutations themselves are NOT written to the WAL as `WalEntry`
records. The WAL only records the `Begin` marker; the actual `Update`/`Tombstone`
values live only in the memtable registry until the flush loop drains them to
Vortex/Parquet. After `flush_table` runs, the mutations are in cold storage and
the WAL is truncated up to `max_lsn` (step 9 in `FlushBackend::truncate_wal`).

**What this means for CDC.** A CDC implementation that reads the WAL only would
miss every UPDATE and DELETE that took the fast path. Those operations would
appear only after the memtable flushes to cold storage — with an arbitrary delay
(up to `memtable_max_age_secs`, default not yet fixed, but minutes-scale).

**How CDC handles the gap.** The `ChangeEventSink` post-commit hook **does**
fire for fast-path UPDATE and DELETE. In `exec_delete` and `exec_update`, the
events are built with `build_delete_events` / `build_update_events` before the
hot-tier write and dispatched via `dispatch_post_commit`. This means the commit
hook captures all mutations, fast-path or not.

**Residual risk.** If a fast path is added in the future without wiring the event
dispatch, CDC would silently miss those operations. Mitigation: the CDC subsystem
tests must include a regression harness that issues UPDATE and DELETE via the hot
tier and asserts events are emitted. This is a Phase 1 acceptance gate.

**WAL-level CDC completeness (Phase 5 pgoutput)**. The pgoutput wire protocol
requires the full `before` image for UPDATE and `before` image for DELETE. These
are already in `ChangeEvent.before`. The WAL itself does NOT carry before images;
if a pgoutput bridge tried to reconstruct them from WAL tailing it would fail on
fast-path operations. All logical-replication-level CDC must flow through the
`ChangeEventSink` path, not WAL tailing.

---

### §4 — Stream model

**Per-project streams.** CDC streams are scoped to a single project. A subscriber
identifies its project via the JWT-authenticated connection. No cross-project
streams (follows the ADR 0012 contract: "every event is project-scoped").

**Per-table filters.** A stream subscriber declares a set of `(table, ops)` pairs
it wants. The CDC runtime evaluates filters server-side and suppresses delivery of
non-matching events. Filter evaluation is `O(1)` per event (bitfield check against
registered `(table, op)` masks).

**LSN-based resumable cursors.** Each `ChangeEvent` carries two cursor tokens:
- `seq: u64` — monotonic per-project counter assigned at commit, stable within a
  process lifetime. The public cursor exposed to subscribers.
- `wal_lsn: Lsn` — the partition WAL LSN at commit time. Stored in the durable
  CDC ring alongside the event; not exposed externally in Phases 1–4, but used
  internally for the pgoutput LSN bridge in Phase 5.

On reconnect, a subscriber supplies `resume_after_seq: u64`. The CDC runtime
replays events from the durable ring where `event.seq > resume_after_seq`. If the
requested `seq` has been evicted from the retention window, the subscriber receives
a `GAP` notification and must re-snapshot.

**At-least-once delivery.** The CDC runtime delivers events at-least-once. The
durable ring is written before the post-commit hook returns to the engine. On
subscriber reconnect with `resume_after_seq`, events are replayed from the ring.
Idempotency (deduplication on `seq`) is the subscriber's responsibility.

**Ordering guarantees.** Within a project: total order on `seq`. Events from
different projects are independent streams with independent `seq` counters.
Within a transaction: all events share one `committed_at` and their `seq` values
are contiguous. The per-project CDC ring is a single-writer log (the post-commit
hook is serialized by the engine's commit path per project), so append order
equals delivery order.

---

### §5 — Retention, WAL truncation, and cursor hold

**The structural tension.** After the hot-tier flush loop calls
`FlushBackend::truncate_wal(project, max_lsn)`, WAL segments up to `max_lsn` are
deleted from the object store. A CDC consumer whose cursor lags behind `max_lsn`
can no longer reconstruct the event stream from WAL. This is the Postgres
replication-slot analogy: a slot holds back WAL truncation until the slot consumer
advances past the retained range. Basin's WAL has no slot primitive today.

**The solution: CDC retention ring (independent of the WAL).** The durable CDC
store is NOT the WAL. It is a separate per-project append-only ring written by
the CDC sink at commit time. Events are stored as serialized `ChangeEvent` values
in Tigris under:

```
{root_prefix}/cdc/{project_id}/{seq_shard}/{ulid}.cdc
```

The ring is retained for a configurable window (`cdc.retention_hours`, default
24h; max configurable to 7 days). Truncation of the CDC ring is independent of
WAL truncation: the WAL truncates when Parquet flushes; the CDC ring truncates
on age, not on flush. This decouples the consumer's lag from the hot-tier's flush
cadence.

**Consequence.** A CDC consumer that falls > `cdc.retention_hours` behind will
have its cursor invalidated. It receives a `CURSOR_EXPIRED` error and must
re-snapshot. This is the standard Kafka consumer behavior.

**WAL truncation interaction.** Because CDC events are captured at the commit
hook (before WAL truncation happens at the later flush step), the CDC ring is
always a superset of what the WAL would have contained. The WAL can truncate
freely; the CDC ring is the durability contract, not the WAL.

**Disk cost.** At 10 000 events/second, each event averaging 2 KB, 24h retention:
10 000 × 2 000 × 86 400 = ~1.7 TB. At per-project cap enforcement this is
impractical as an absolute default. In practice, events are batchable: a
`RecordBatch` INSERT of 100 rows produces 100 `ChangeEvent` objects but they can
be stored as one compressed CDC segment. Real-world event sizes for OLTP workloads
(single-row INSERT/UPDATE) are 200–500 bytes per event. At 100 events/second/project,
24h = ~1.7 GB/project/day. Acceptable. Per-project `cdc.retention_hours` and
`cdc.events_per_second_cap` are enforced as quota dimensions under ADR 0008.

**No WAL-hold / slot equivalent in Phase 1.** CDC retention is time-based, not
LSN-watermark-based. If a WAL-based pgoutput consumer in Phase 5 needs to hold
back WAL truncation, a minimal slot registry (one row per active slot in the
catalog, recording the slot's LSN watermark) will be added as part of that phase.
Until then, the CDC ring is the durability contract.

---

### §6 — Security and noisy-neighbor isolation

**Auth model.** CDC stream access is authenticated via the same JWT path as
REST/WebSocket. The JWT must carry the `project` claim. Per-project streams are
fully isolated: no subscriber can read another project's events regardless of
project configuration.

**Per-project quota dimensions** (enforced under the ADR 0008 EDF scheduler):
- `cdc.events_per_second_cap` (default: 1 000 events/sec outbound) — limits
  delivery throughput from the CDC runtime to a single project's subscribers.
  Write throughput (capture at the commit hook) is unbounded; only delivery is
  capped to prevent a high-write project from monopolizing the CDC fan-out thread.
- `cdc.max_concurrent_consumers` (default: 10 per project) — limits concurrent
  long-lived CDC connections per project.
- `cdc.retention_hours` (default: 24, max: 168) — controls ring disk cost.

**Fan-out isolation.** Events written to the CDC ring are stored per-project. A
slow consumer on project A never blocks event capture for project B. The CDC ring
write (object store PUT) runs in the background post-commit; the commit path is not
blocked by slow CDC sinks (same design as the realtime `BUFFER_FULL` path: if the
background PUT queue is full, events are dropped from the in-memory queue but the
ring write is retried with exponential backoff, guaranteeing durability for
committed events).

---

## Phases and implementation plan

Each phase is independently shippable and tested. No phase blocks the next at a
code level; they share the `ChangeEventSink` trait surface.

### Phase 1 — Durable CDC ring + webhook stream (2–3 weeks)

**Goal.** Customers can subscribe to a per-project CDC stream over HTTP
long-poll or SSE and receive committed events reliably with resumable cursors.

**Commits (ordered):**

1. **`feat(cdc): CdcRingWriter — ChangeEventSink that appends to Tigris`**
   - New `crates/basin-cdc/` crate (follows `crates/basin-cron/` pattern from ADR 0012).
   - `CdcRingWriter` implements `ChangeEventSink`. On `publish(&event)`:
     1. Serialize `event` + internal `wal_lsn` to CBOR/JSON.
     2. PUT to `{root}/cdc/{project}/{seq_shard}/{ulid}.cdc` (Tigris).
     3. Update per-project in-memory `seq` watermark.
   - Attached as a **post-commit** sink at startup.
   - Unit tests: event serialization, Tigris PUT, `seq` monotonicity.

2. **`feat(cdc): CdcCursorStore — per-project seq index in Tigris`**
   - A small index object per project: `{root}/cdc/{project}/_index.json`
     tracking `{min_seq, max_seq, oldest_object_key}`. Updated on every segment
     PUT. Allows fast cursor seek without listing all objects.
   - Test: cold-start recovery reads index and resumes correctly.

3. **`feat(cdc): GET /v1/cdc/:project/stream — SSE delivery with resume`**
   - Axum route in `basin-rest`.
   - Auth: JWT `project` claim, same path as ADR 0013.
   - Query params: `?resume_after=<seq>`, `?tables=orders,users`, `?ops=insert,update`.
   - Replays from ring if `resume_after < max_seq - ring_size`; else live
     delivery from in-memory ring (the realtime channel).
   - Emits `data: {type:"event", ...}` SSE frames; `id:` header carries `seq`.
   - Test strategy: subscribe, insert rows, assert events received; disconnect,
     reconnect with `Last-Event-ID`, assert gap-fill; issue hot-tier UPDATE and
     DELETE, assert events received (regression for §3 gap).

4. **`feat(cdc): ring retention GC — background truncation task`**
   - Compares object ages against `cdc.retention_hours` on a 1h tick.
   - Deletes expired segments; updates `_index.json`.
   - Test: insert events, advance clock past retention, verify objects deleted.

5. **`test(cdc): hot-tier fast-path regression harness`**
   - Issue `UPDATE ... WHERE pk = x` via hot-tier path (single row).
   - Issue `DELETE ... WHERE pk = x` via hot-tier path.
   - Assert CDC SSE subscriber receives `op=update` / `op=delete` events.
   - This is the §3 gap regression gate. Must be a CI-blocking test.

**Non-goals for Phase 1:** no Kafka, no webhook retry (see Phase 2), no tx
grouping markers in the wire protocol, no pgoutput.

---

### Phase 2 — Webhook push sink with retry queue (1–2 weeks)

**Goal.** Customers can register an HTTPS endpoint and receive CDC events via
POST with at-least-once delivery and exponential backoff.

This reuses and extends the existing `WebhookSink` from ADR 0012 Phase 5.11.I,
adding a CDC-specific retry queue that consumes from the CDC ring rather than
inline from the commit path.

**Key commits:**

1. `ALTER TABLE ... SUBSCRIBE CDC WEBHOOK TO 'https://...' ON INSERT OR UPDATE`
   — DDL surface. Stores endpoint + filter in catalog.
2. `CdcWebhookWorker` — background task per registered endpoint. Polls the CDC
   ring, POSTs batches of events as JSON. Idempotency key: `X-Basin-CDC-Seq: <seq>`.
3. Dead-letter: after 5 retries, events are routed to `{root}/cdc/{project}/_dlq/`.

**Test strategy:** mock HTTP server; assert event delivery, retry on 5xx, DLQ on
exhausted retries.

---

### Phase 3 — Kafka/Redpanda sink (2–4 weeks)

**Goal.** Customers can route committed events to a Kafka cluster (Confluent Cloud,
MSK, Redpanda). Wire format: Confluent-compatible JSON or Avro (Schema Registry).

**Feasibility against actual WAL/event content.** The `ChangeEvent` struct carries
`before: Option<Value>` and `after: Option<Value>` as `serde_json::Value`. This
is sufficient for JSON-over-Kafka. Avro requires a schema registry URL and
schema derivation from Basin's `TableMetadata.schema`. The Basin schema
(`Arc<ArrowSchema>`) can be translated to an Avro schema mechanically. This is a
meaningful but bounded implementation task.

**Key commits:**

1. `CREATE KAFKA SINK <name> BROKER 'bootstrap:9092' TOPIC 'basin_events' PROJECT <id>`
   — DDL surface (catalog-backed, not just env-config).
2. `CdcKafkaSink` — wraps `rdkafka` (the `rdkafka` crate is already a common Rust
   Kafka client). Reads from the CDC ring, publishes to the configured topic.
3. `tx_id` field added to `ChangeEvent` — required for consumers that need to
   group events by transaction. See §2.
4. Schema Registry integration (optional, gated on `BASIN_CDC_AVRO_SCHEMA_REGISTRY`).

**Ordering.** Kafka does not guarantee cross-partition ordering. Each Basin project
maps to one Kafka topic; events within a project are published to the same partition
(keyed by `project_id`) for total-order delivery within a project.

**Test strategy:** Redpanda in Docker (CI), consume events, assert ordering and
content.

---

### Phase 4 — Postgres logical-replication wire compat (`wal2json` subset) (4–6 weeks)

**Goal.** Tools that can consume Postgres logical replication (Debezium, Airbyte,
pglogical, custom consumers) can point at Basin and receive a compatible event
stream. Wire protocol: `wal2json` output plugin format over the Postgres streaming
replication protocol.

**Feasibility assessment.** Postgres logical replication involves:

1. **`CREATE_REPLICATION_SLOT ... LOGICAL wal2json`** — Basin needs to handle this
   pgwire command and mint a slot record in its catalog.
2. **`START_REPLICATION SLOT ... LOGICAL ...`** — Basin streams WAL segments in
   the Postgres physical replication wire protocol, with `XLogData` messages
   containing `wal2json`-formatted JSON.
3. **LSN semantics** — Consumers send `StandbyStatusUpdate` messages carrying
   their flushed/applied LSN. Basin must map its `seq` cursor to a pseudo-LSN
   (`0/XXXXXXXX` format). The `wal_lsn` stored in the CDC ring alongside each
   event serves as this LSN.
4. **Transaction markers** — `wal2json` emits `{"action":"B"}` (BEGIN),
   `{"action":"C"}` (COMMIT), and `{"action":"I"/"U"/"D"}` per row. This requires
   `tx_id` grouping (Phase 3 commit) and a `committed_at`-based LSN.

**What the WAL does and does not contain.** Basin's WAL `EntryRecord.payload` is
an Arrow IPC batch — not a Postgres WAL record. A pgoutput bridge cannot read
Basin's WAL directly. It must consume events from the CDC ring (which has
decoded before/after images) and synthesize `wal2json`-formatted messages. The
WAL LSN stored in the ring (`wal_lsn`) is used as the LSN field in the synthetic
`XLogData` messages. Consumers that validate LSN monotonicity (Debezium does) will
see a monotonically increasing LSN because Basin's per-partition WAL LSN is
monotonically increasing.

**Key limitation.** Basin does not have WAL-level DDL records (no `ALTER TABLE`
in the WAL). Schema changes will appear as a reconnect-and-re-snapshot event for
affected consumers. This is documented as a known deviation from full Postgres
logical replication semantics.

**Test strategy:** Debezium + Basin in Docker (CI integration test); verify
Debezium can connect, consume events, and handle reconnect.

---

### Phase 5 — `pgoutput` binary wire compat (6–10 weeks; gated on customer demand)

This phase adds `pgoutput` (Postgres binary replication format, as opposed to
`wal2json` text JSON). It is required for native Postgres replication tooling
(e.g. `pg_logical`, AWS DMS with binary mode). The complexity is substantially
higher than `wal2json` because `pgoutput` carries OIDs, type information, and
binary-encoded tuple values. The trigger for this phase is a customer at ≥ $50k
ARR contingent on pgoutput compatibility.

---

## Non-goals

- **Cross-project CDC streams.** Each stream is project-scoped; ADR 0012 contract.
- **`LISTEN` / `NOTIFY` wire protocol.** ADR 0012 explicitly defers this; CDC
  covers the event delivery use case via a different surface.
- **PL/pgSQL `CREATE TRIGGER`-style CDC.** Out of scope; reactors (ADR 0012 Phase
  5.11.C) are Basin's answer to in-database triggers.
- **CDC as a cross-region replication mechanism.** Multi-region replication is
  S3 CRR + Raft WAL (ADR 0009); CDC is an egress path for external consumers, not
  a replication primitive.
- **Exactly-once delivery.** At-least-once with idempotency keys is the contract.
  Exactly-once requires two-phase coordination with the consumer; out of scope.
- **Full WAL-tailing by external consumers.** Basin's WAL payload is opaque Arrow
  IPC. No external consumer should parse WAL segments directly; the CDC ring is
  the supported egress.
- **Savepoint-level CDC granularity.** CDC emits committed-transaction granularity.
  `ROLLBACK TO SAVEPOINT` within a transaction does not produce CDC events.
- **CDC from cold-tier Parquet/Vortex files.** CDC is a stream of committed
  mutations, not a Parquet scan API. Historical bulk reads use the SQL query path.

---

## Consequences

### Positive

- The `ChangeEventSink` hook captures all mutations including HTAP fast-path
  UPDATE/DELETE — no architectural gap in coverage.
- Decoupling the CDC ring from the WAL means WAL truncation (triggered by hot-tier
  flushes) does not affect CDC retention; the two operate on independent schedules.
- The phased plan lets Phase 1 (SSE stream) ship quickly; each subsequent phase
  adds sink types without engine changes.
- The existing realtime WebSocket subsystem is usable as a live-delivery overlay
  on top of the durable CDC ring, unifying the realtime and CDC products.

### Negative

- Per-project CDC ring storage cost (see §5 disk cost analysis). Must be metered
  and billed.
- `ChangeEvent` schema (`before`/`after` as `serde_json::Value`) is lossy for
  binary types (e.g., UUID stored as `Decimal256` per ADR 0024). The CDC layer
  must convert to canonical UUID string representation, not raw Decimal256 bytes.
- WAL LSN and `seq` are not the same counter. The pgoutput bridge (Phase 5) must
  map between them; the internal `wal_lsn` stored in the CDC ring bridges this,
  but it adds operational complexity.
- No slot-based WAL hold in Phases 1–4. Customers who need the WAL to be held for
  CDC must wait for Phase 4+ or accept time-based retention expiry.

### Mitigations

- Disk cost: per-project quotas on `cdc.retention_hours` and event throughput.
- UUID encoding: add `cdc_canonical_value()` helper in the CDC serialization path
  that applies ADR 0024's reverse translation before serializing to JSON.
- Slot-based WAL hold: the Phase 4 `CREATE_REPLICATION_SLOT` DDL surface doubles
  as the hook to add a lightweight slot catalog entry that the flush loop checks
  before truncating.

---

## Trigger to revisit

This ADR is reopened (new successor ADR) when:

1. A customer at ≥ $30k ARR requires `pgoutput` wire compat (Phase 5 gate).
2. The CDC ring storage cost exceeds 5% of total Tigris spend per-project at the
   median, requiring a more aggressive batching or compression strategy.
3. A customer requires sub-100ms CDC latency end-to-end (currently bounded by
   the Tigris PUT in the ring write path, ~20–50ms p99). At that point, the
   durable ring write should be async with an in-memory pre-buffer; the tradeoff
   is at-least-once delivery with a small crash-loss window.

---

## Alternatives considered

### WAL tail as primary seam

Rejected. Basin's WAL payload is opaque Arrow IPC — not row-level before/after
images. The WAL does not carry HTAP fast-path UPDATE/DELETE mutations as `WalEntry`
records (only `TxBegin` markers). A WAL-tail CDC would miss all HTAP-path mutations
until the hot-tier flushes to Parquet. The `ChangeEventSink` hook is the correct
seam because it fires at the point of committed visibility, regardless of the
storage path.

### Postgres `pg_logical` / `pglogical` extension

Rejected. ADR 0002 prohibits Postgres extensions. Basin does not load `.so` files.
A native Rust implementation of the pgoutput protocol (Phase 5) is the correct
path.

### Using realtime's ring as the durable CDC store

Rejected. Realtime's ring (`DEFAULT_CHANNEL_CAPACITY = 1024`, in-memory, non-durable)
is intentionally lossy. Treating it as a CDC store would require making it durable,
adding retention semantics, and managing its interaction with the per-project
memory budget. Keeping the two separate (lossy hot ring + durable CDC ring) is
cleaner and more operational.

### Single global CDC queue across all projects

Rejected. Per-project isolation is a Basin invariant (ADR 0008, ADR 0012). A global
queue would require per-project filtering on the read path and would couple all
projects' CDC latency to each other's write rate.
