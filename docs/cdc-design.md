---
title: "CDC bridge — design specification"
nav_section: architecture
sidebar_position: 99
summary: "Design for Basin's CDC bridge: capture seam, the WAL fast-path gap, durable ring wire format, cursor and retention model, and per-phase implementation detail."
tags: [cdc, design, streaming, wal]
---

# CDC bridge — design specification

See [ADR 0028](./decisions/0028-cdc-bridge.md) for the locked architectural
decisions. This document is the implementation-level companion: it records
the wire formats, internal data structures, and per-phase acceptance criteria
that the ADR summary omits.

---

## 1. System map

```
Engine commit path (executor.rs)
  │
  ├── dispatch_pre_commit()  →  [ReactorSink, ConstraintSink]  (may abort)
  │
  │   catalog.append_data_files() succeeds here
  │
  └── dispatch_post_commit() →  [RealtimeSink, CdcRingWriter, WebhookSink]
                                       │              │
                                  live broadcast   durable ring
                                  (in-memory,      (Tigris object store,
                                   lossy)           at-least-once)
                                       │              │
                                  WebSocket/SSE     GET /v1/cdc/:project/stream
                                  (realtime)        (CDC SSE)
                                                    Kafka/Redpanda sink
                                                    Postgres pgoutput bridge
```

The capture seam is the **post-commit** `dispatch_post_commit()` call in
`crates/basin-engine/src/executor.rs`. This fires after the catalog commit
succeeds, for every code path — including the HTAP hot-tier UPDATE/DELETE fast
paths (`hot_tier_update_by_pk`, `hot_tier_delete_by_pk`) which bypass the WAL
but still call `dispatch_post_commit` via `build_update_events` /
`build_delete_events`.

---

## 2. ChangeEvent wire shape (CDC ring)

Each event is stored as a CBOR-encoded record (compact, no schema negotiation):

```
CdcRecord {
    seq:          u64,           // monotonic per-project; the public cursor
    wal_lsn:      u64,           // WAL LSN at commit time (internal; Phase 5 use)
    project:      uuid,
    table:        string,
    op:           u8,            // 1=insert, 2=update, 3=delete
    tx_id:        u64 | null,    // Phase 3+: engine-instance tx id
    committed_at: i64,           // UTC unix micros
    causation_user: string | null,
    before:       json | null,   // null for insert
    after:        json | null,   // null for delete
}
```

Records are grouped into **CDC segments** — batches of up to 1 000 events or 1
MiB, whichever comes first — and PUT as a single object:

```
{root_prefix}/cdc/{project_id}/{seq_lo_hex}/{ulid}.cdc
```

Where `{seq_lo_hex}` is the lower 32 bits of the segment's first `seq`,
left-padded to 8 hex digits. This creates a natural prefix-list ordering that
allows efficient seeks.

### Index object

A lightweight index is maintained at:

```
{root_prefix}/cdc/{project_id}/_index.json
```

Content:
```json
{
  "min_seq": 0,
  "max_seq": 1234567,
  "segment_count": 892,
  "oldest_segment_key": "cdc/.../00000000/01JQMXYZ.cdc",
  "newest_segment_key": "cdc/.../000BFFFF/01JQNABC.cdc",
  "updated_at": "2026-06-12T14:23:00Z"
}
```

The index is updated with an ETag-gated conditional PUT on every segment write
(optimistic concurrency; last writer wins; the values are monotonically
increasing so any race is harmless). On cold-start, the CDC consumer reads the
index to find the seek point for `resume_after_seq`.

---

## 3. Cursor protocol (SSE delivery)

### Subscribe

```
GET /v1/cdc/:project/stream
Authorization: Bearer <jwt>
Accept: text/event-stream
```

Optional query parameters:
| param | type | description |
|---|---|---|
| `resume_after` | u64 | Resume delivery from events with `seq > resume_after`. Default: deliver only new events (live). |
| `tables` | comma-list | Only deliver events for these tables. Default: all tables. |
| `ops` | comma-list of `insert,update,delete` | Only deliver these operation types. Default: all. |
| `snapshot` | bool | If `true`, the server sends a full-table snapshot before streaming new events (Phase 3+). |

### Event frame

```
id: 1234567
data: {"type":"event","seq":1234567,"table":"orders","op":"insert","before":null,"after":{"id":42,"status":"pending"}}

```

Each SSE frame carries `id: <seq>` so the browser's `EventSource` API tracks
the cursor automatically and sends it as `Last-Event-ID` on reconnect.

### Gap notification

When `resume_after` references a `seq` that has been evicted from the retention
window:

```
data: {"type":"cursor_expired","resume_after":999,"min_available_seq":50000}

```

The subscriber must re-snapshot before resuming.

### Heartbeat

Every 15 seconds with no events, the server sends:

```
: heartbeat

```

(SSE comment — ignored by EventSource, prevents proxy timeout.)

---

## 4. WAL fast-path gap — regression test specification

The Phase 1 acceptance gate (commit 5 in the implementation plan) must cover:

### Test 1: hot-tier UPDATE emits CDC event

```
1. CREATE TABLE t (id int PRIMARY KEY, v int);
2. INSERT INTO t VALUES (1, 0);
3. Subscribe to CDC stream for project P, table "t", op "update".
4. UPDATE t SET v = 1 WHERE id = 1;
   -- This MUST take the hot_tier_update_by_pk fast path.
5. Assert: CDC subscriber receives exactly one event with:
   - op = "update"
   - before.v = 0
   - after.v = 1
6. Assert: event is received within 2 seconds (not waiting for memtable flush).
```

Step 4 takes the hot path because: there is no live overlay, the table has a
single-column PK (`id`), and the WHERE clause is a single-PK equality predicate.

### Test 2: hot-tier DELETE emits CDC event

```
1. CREATE TABLE t (id int PRIMARY KEY, name text);
2. INSERT INTO t VALUES (2, 'bob');
3. Subscribe to CDC stream for project P, table "t", op "delete".
4. DELETE FROM t WHERE id = 2;
   -- hot_tier_delete_by_pk fast path.
5. Assert: CDC subscriber receives exactly one event with:
   - op = "delete"
   - before.name = "bob"
   - after = null
```

### Test 3: hot-tier UPDATE inside explicit transaction

```
1. CREATE TABLE t (id int PRIMARY KEY, v int);
2. INSERT INTO t VALUES (3, 10);
3. Subscribe.
4. BEGIN;
5. UPDATE t SET v = 20 WHERE id = 3;   -- hot_tier_update_by_pk_tx (tx overlay)
6. COMMIT;
7. Assert: exactly one CDC event with op=update, before.v=10, after.v=20.
8. Assert: NO event was emitted before COMMIT (post-commit-only guarantee).
```

These three tests must run in CI and block merge if they fail.

---

## 5. Multi-region CDC interaction

Per ADR 0009, each region has its own WAL and project-pinned writes route to
the home region. CDC streams are per-region by construction:

- The `CdcRingWriter` runs in the shard owner process for a project's home region.
- The CDC ring object-store prefix is under the same Tigris bucket and project
  prefix as all other Basin data for that project.
- A subscriber connecting to a non-home region endpoint will either:
  - Be forwarded to the home region (when region-routing is implemented in Phase 6).
  - Receive an error `{"type":"error","code":"WRONG_REGION"}` with the home region
    endpoint URL (current behavior until Phase 6 routing lands).

CDC does not introduce new cross-region complexity beyond what ADR 0009 already
commits to. The `Handoff { to_holder, at_epoch }` WAL marker (Phase 6.X.C) marks
lease transfers in the WAL; the CDC ring does not carry these markers (they are
not row mutations). After a lease handoff, the new leaseholder's `CdcRingWriter`
resumes publishing to the same Tigris prefix, and the index is updated with the
new segment keys.

---

## 6. Snapshot + follow

For consumers that need a consistent baseline before streaming new events:

### Phase 1: manual snapshot

Subscribers call the REST query endpoint for an initial table scan, record the
`max_seq` at query time, then connect to the CDC stream with `resume_after=<max_seq>`.
This provides a consistent snapshot-then-stream without server-side coordination.
The gap window (rows committed between snapshot and stream open) is filled by the
CDC ring — as long as the subscription opens before `max_seq` is evicted.

### Phase 3+: server-side snapshot + follow

`GET /v1/cdc/:project/stream?snapshot=true` triggers:

1. Server pins `snap_seq = current max_seq`.
2. Streams a full table scan as `{"type":"snapshot","table":"...","row":{...}}`
   events, one per row.
3. Emits `{"type":"snapshot_complete","snap_seq":N}`.
4. Switches to live delivery from `seq > snap_seq`.

This requires a consistent read of the table at `snap_seq`. Basin's existing
snapshot-isolation semantics (catalog snapshot at `current_snapshot`) provides
this naturally: the snapshot scan uses the catalog state at `snap_seq` and the
stream picks up events committed after that point.

---

## 7. Competitive comparison

| Feature | Supabase | Neon | Basin (Phase 1) | Basin (Phase 4) |
|---|---|---|---|---|
| WebSocket real-time | Yes (Supabase Realtime) | No | Yes (realtime crate) | Yes |
| SSE CDC stream | No | No | Yes | Yes |
| Webhook push | Yes | No | Yes (Phase 2) | Yes |
| Kafka sink | No (via Zapier) | No | No (Phase 3) | Yes |
| Postgres logical replication | Yes (wal2json, pgoutput) | Yes (native WAL) | No | Yes (wal2json subset) |
| LSN-cursor resumability | Yes | Yes | Yes (seq cursor) | Yes (WAL LSN) |
| Retention | 7 days | configurable | 24h default | configurable |
| Before-image on UPDATE | Yes | Yes | Yes | Yes |

Neon exposes its WAL natively (it is a Postgres fork; the WAL is Postgres WAL).
Supabase uses `wal2json` via the Postgres replication protocol. Basin's approach
is architecturally different — the event primitive is the canonical seam, not
WAL tailing — which is correct given Basin's columnar/WAL architecture but means
Phase 4 requires more synthesis work than a Postgres fork would.

---

## 8. Open questions (not blocking Phase 1)

1. **`wal_lsn` mapping accuracy.** The WAL LSN stored in the CDC ring is the
   LSN of the last WAL entry for the project/partition at commit time. For
   auto-commit INSERT statements this is the `WalEntry.lsn` of the batch. For
   HTAP fast-path UPDATE/DELETE, the only WAL entry is the `TxBegin` marker; the
   `wal_lsn` stored is the Begin marker's LSN. For Phase 5 pgoutput, these LSNs
   must be monotonically increasing — which they are, since `TxBegin` LSNs are
   assigned monotonically by the WAL regardless of whether data entries follow.
   The pgoutput bridge uses the Commit marker's LSN as the `commit_lsn` field;
   for HTAP-path transactions, the Commit marker is the WAL entry directly
   following the data entries (or the Begin marker for trivial fast-path ops).
   This needs a concrete integration test before Phase 5 begins.

2. **Schema evolution in the CDC ring.** If a column is dropped or renamed,
   events in the retention window that predate the schema change carry the old
   column names. Subscribers must handle unknown column names gracefully. The CDC
   serialization layer should embed a `schema_version` field (the catalog
   `current_snapshot` value) alongside each event so subscribers can detect and
   adapt to schema changes.

3. **CDC billing metering.** The billing model (ADR for billing is docs/
   basin-billing-connections context) should add a `cdc_events_delivered` meter
   per project. Events written to the CDC ring are metered at write time; events
   delivered over SSE are metered at delivery time. Both are inputs to the
   per-project quota enforcement.
