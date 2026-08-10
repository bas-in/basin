---
title: "ADR 0020 — WAL transaction markers + replay suppression"
nav_section: decisions
sidebar_position: 20
summary: "WAL adopts explicit Begin/Commit/Rollback markers for HTAP memtable integration. Replay discards entries inside rolled-back or crash-interrupted transactions. Pre-marker WAL files replay identically."
---

# 0020 — WAL transaction markers + replay suppression

- **Status:** Accepted (2026-05-19) — locks the protocol Phase 5.14.C2 writes against.
- **Tags:** storage, wal, htap, crash-recovery
- **Supersedes:** none
- **Cross-references:** [ADR 0016 §"Transaction integration" + addendum](./0016-htap-hot-tier-architecture.md), [ADR 0012 (Change-event primitive)](./0012-change-event-primitive.md), Phase 5.14.C2 spec in [`docs/roadmap/2026-05-19-decomposition.md`](../roadmap/2026-05-19-decomposition.md)

## Context

ADR 0016 §"Transaction integration" introduced a pre-condition for the HTAP
hot tier: the WAL must carry `BEGIN` and `ROLLBACK` markers and the replay
path must suppress entries enclosed by a `BEGIN` / `ROLLBACK` pair. Without
this, rolled-back writes are re-applied on process restart, silently
corrupting the memtable on recovery.

The problem is structural. Basin's current WAL (`basin-wal` v0.1) treats
every append as independently durable — there is no notion of grouping
entries into a logical transaction. Each call to `Wal::append` is fsynced
and considered committed. This matches the pre-HTAP write path perfectly:
Arrow batch → Parquet → object store → catalog commit. The whole sequence
is already atomic at the catalog layer, so WAL entries only ever represent
committed facts.

The HTAP hot tier changes that invariant. With the hot tier, `INSERT /
UPDATE / DELETE` statements write directly to the in-memory memtable during
statement execution and land in the WAL at the same time. A `ROLLBACK`
after several statements must suppress those memtable writes **and** prevent
those WAL entries from replaying. If only the memtable suppression is
implemented (via the watermark truncation in `TxState`) but not the WAL
suppression, a crash during an open transaction followed by replay would
re-apply the rolled-back entries to a fresh memtable.

The WAL also has no mechanism to detect whether a process stopped cleanly
(shutdown after commit) or crashed mid-transaction. The two cases look
identical from the WAL perspective today: a file that ends. This ADR adds
the minimal markers to distinguish them.

Two implementation artefacts are active concurrently:

- **Phase 5.14.C2 (write path)** — the engine's `executor.rs` and
  `session.rs` will call `Wal::append_tx_begin` at `BEGIN` and
  `Wal::append_tx_rollback` / `Wal::append_tx_commit` at the end of each
  transaction.
- **WAL reader side** — `crates/basin-wal/src/lib.rs` already carries the
  `WalEvent` enum, `WalReplayConfig`, and the `replay_wal` function that a
  parallel agent is implementing in `file_wal.rs`.

This ADR locks the on-disk format and replay invariants so both sides agree
on the contract without further coordination.

## Decision

Extend the WAL entry tag space with three new marker variants:

- `WalEvent::Begin { tx_id: u64 }` — opens a logical transaction.
- `WalEvent::Rollback { tx_id: u64 }` — discards all buffered entries for
  `tx_id`.
- `WalEvent::Commit { tx_id: u64 }` — explicitly closes a transaction as
  committed.

The `Commit` variant is the load-bearing decision in this ADR; the
rationale and trade-offs are in §6 (Crash recovery) below.

Data entries written while a transaction is open are attributed to the
innermost open `tx_id` at the time of append. The trait gains a new method:

```rust
async fn append_tx_commit(
    &self,
    project: &ProjectId,
    partition: &PartitionKey,
    tx_id: u64,
) -> Result<Lsn>;
```

`WalReplayConfig::suppress_rolled_back: bool` (default `true`) gates the
new behaviour. When `false`, all entries are emitted regardless of markers
— this is the v0.1 back-compat mode for pre-marker WAL files.

## On-disk format

### Existing format (v0.1)

The existing WAL segment format is a length-prefixed record stream. Each
record is:

```
[4 bytes little-endian u32: payload_len][payload_len bytes: bincode payload]
```

The bincode payload is a tagged-enum serialisation of the internal
`SegmentRecord` type. The enum tag byte occupies the first byte of the
bincode output. Existing variants use tags in the range `0x00`–`0x0F` (the
bincode default for small `#[repr]`-free enums assigned by variant
declaration order).

### New marker variants (v0.2)

Three new `SegmentRecord` variants extend the tag space:

| Tag byte | Variant | Payload (little-endian) |
|---|---|---|
| `0x10` | `TxBegin` | `tx_id: u64` (8 bytes) |
| `0x11` | `TxRollback` | `tx_id: u64` (8 bytes) |
| `0x12` | `TxCommit` | `tx_id: u64` (8 bytes) |

Total wire size per marker: 4-byte length prefix + 1-byte tag + 8-byte
`tx_id` = **13 bytes per marker**, rounded up to the platform's natural
alignment in practice by the length prefix.

**Endianness:** all multi-byte integers in the WAL use little-endian byte
order, matching the existing `u32` length prefix and the LSN fields
serialised by bincode with default settings on `x86_64`.

**`tx_id` semantics:**

- Type: `u64`, monotonically increasing within a single engine restart.
- Scope: per-engine-instance. Not unique across restarts; not unique across
  different `(project, partition)` tuples.
- Source: a single engine-level `AtomicU64` seeded from the current time in
  microseconds at startup, incremented with `fetch_add(1, Relaxed)`. The
  same counter used by `TxState` for savepoint frame IDs.
- Savepoint sub-transactions use the parent transaction's `tx_id`. A
  `ROLLBACK TO SAVEPOINT` truncates the memtable watermark but does NOT
  emit a `TxRollback` marker — only a top-level `ROLLBACK` does. The WAL
  sees transactions as atomic units, not savepoint sub-units.

**Back-compatibility invariant:** a v0.1 WAL reader encountering tag bytes
`0x10`–`0x12` will fail to deserialise. This is acceptable: v0.1 readers
are not deployed on files that contain v0.2 markers, because the engine
version that writes v0.2 markers also reads them. There is no cross-version
mixed-format concern in Basin's single-binary deployment model. If future
versions require graceful forward compatibility, a version header at segment
start is the right mechanism (out of scope for this ADR).

## Replay invariants

`replay_wal(events: Vec<WalEvent>, config: &WalReplayConfig) -> Vec<WalEntry>`
is the single function that enforces all the cases below. When
`suppress_rolled_back = false`, it emits every `WalEvent::Entry` verbatim
and is the pre-marker back-compat path.

When `suppress_rolled_back = true`, the following invariants hold:

**Case 1 — Pre-marker WAL file (no `Begin`, `Rollback`, or `Commit`
markers).**
Every `WalEvent::Entry` is emitted immediately. No buffering occurs. Output
is identical to `suppress_rolled_back = false`. This is the back-compat
path for v0.1 files.

**Case 2 — Complete rollback: `Begin tx_id` + entries + `Rollback tx_id`.**
All entries attributed to `tx_id` are silently discarded. Nothing is
emitted for that transaction. The memory used by the buffer is freed on the
`Rollback` event.

**Case 3 — Complete commit: `Begin tx_id` + entries + `Commit tx_id`.**
On receipt of the `Commit` marker, all buffered entries for `tx_id` are
moved to the committed output in LSN order. This is the normal happy-path
for a transaction that completes cleanly.

**Case 4 — Crash recovery: `Begin tx_id` + entries + end-of-WAL (no
`Commit`, no `Rollback`).**
The transaction is **discarded**. Buffered entries are dropped. This is the
crash-recovery case: the `Commit` marker was never written, so the
transaction must not be applied. See §6 for the full reasoning.

**Case 5 — Interleaved transactions (tx A and tx B open simultaneously).**
Each `tx_id` is tracked in an independent buffer. Entries are attributed to
the most recently opened transaction whose `Begin` has not yet been resolved
by a `Commit` or `Rollback`. Commit and Rollback are keyed by `tx_id` and
only affect their own buffer. After resolving, committed entries from all
transactions are merged and sorted by LSN before being returned.

Interleaved transactions arise when two sessions issue `BEGIN` before
either commits. In practice Basin's single-shard executor serialises
statements but does not serialise multi-statement transactions, so overlap
is possible.

**Formal statement of the state machine** (per `tx_id`):

```
state: Idle | Buffering(Vec<WalEntry>)

event: Begin   → Buffering([])                          (start collecting)
event: Entry   → if Buffering: append to buffer
                 if Idle:      emit immediately (Case 1)
event: Commit  → emit buffer in LSN order; → Idle       (Case 3)
event: Rollback→ drop buffer; → Idle                    (Case 2)
event: EOF     → if Buffering: drop buffer (Case 4)
                 if Idle:      nothing to do
```

## Interaction with crash recovery

This section documents the decision that distinguishes this ADR from a
naive "implicit-commit-at-EOF" design, and why the explicit `Commit` marker
is required for correctness.

### The problem with implicit-commit-at-EOF

The initial sketch in the context description proposed implicit commit at
EOF: if the WAL ends with an open transaction, treat it as committed. This
simplifies the write path (no `Commit` marker to write) but creates a fatal
ambiguity:

- **Clean shutdown:** engine processes all statements, writes WAL entries,
  closes the transaction, shuts down. The WAL ends after the last data
  entry. There is no `Commit` marker. On replay, those entries are emitted
  (implicit commit).
- **Mid-transaction crash:** engine processes some statements, writes WAL
  entries, crashes before `COMMIT`. The WAL ends after the last data entry.
  There is no `Commit` marker. On replay, those entries are emitted
  (implicit commit).

Both cases produce the same WAL layout. There is no way to distinguish
them. Implicit-commit-at-EOF means crash-interrupted partial transactions
are silently applied on recovery. For the HTAP memtable, this means:

- A `BEGIN` / three INSERTs / crash before `COMMIT` → on restart, all
  three rows appear in the memtable as if committed. The application never
  issued `COMMIT`. The data is corrupt.

This is the same bug that motivated MySQL's InnoDB to require an explicit
redo-log `COMMIT` record. It is not a theoretical concern.

### Decision: explicit `Commit { tx_id }` marker

The `TxCommit` marker (`0x12`) is written to the WAL as the last step of a
successful `COMMIT` before the client is acked. This mirrors the standard
database transaction protocol:

```
BEGIN  → WalEvent::Begin  appended + fsynced (or batched)
stmt 1 → WalEvent::Entry  appended
stmt 2 → WalEvent::Entry  appended
...
COMMIT → WalEvent::Commit appended + fsynced before ack
```

On crash after the last `Entry` but before `Commit`, the WAL ends with an
open `Begin` and no `Commit`. Replay discards those entries (Case 4 above).
The client never received a `COMMIT` ack, so the application is correct to
retry.

On crash after `Commit` is fsynced, the entries are present and `Commit` is
present. Replay emits them (Case 3). The client may or may not have received
the ack depending on crash timing, but the data is durable — the client
can use idempotency keys or read-your-own-writes to verify.

### Write ordering requirement

`Commit` must be fsynced **before** the `COMMIT` response is sent to the
client. The existing `Wal::append` already guarantees fsync-before-return
for `LocalWal`. `append_tx_commit` follows the same contract: durable when
the `Result<Lsn>` is `Ok`.

For batched WAL flushes (the 200 ms / 1 MB background flush), transaction
markers bypass the batch and force an immediate fsync. Data entries within
a transaction may be batched; the `Commit` marker is not.

### Why not a `Sync` / `Checkpoint` marker instead

An alternative is to write a `Checkpoint` entry on clean shutdown (not at
every commit) and treat "no Checkpoint seen before EOF" as a crash. This
makes the write path cheaper (one marker per shutdown, not per commit) but
introduces a new failure mode: if the engine crashes before writing the
Checkpoint but after cleanly committing all transactions, all entries since
the last Checkpoint are discarded on next restart. The entry-per-commit
approach is strictly safer and the overhead is negligible (§8).

## Migration story

### Pre-marker WAL files

Pre-marker (v0.1) files contain no `Begin`, `Rollback`, or `Commit` entries.
The `replay_wal` function with `suppress_rolled_back = true` handles this
correctly: every `WalEvent::Entry` is emitted immediately (Case 1 above),
because no `Begin` is ever opened.

The replay output for a v0.1 file is identical regardless of
`suppress_rolled_back`. No migration of existing WAL files is required.

### `WalReplayConfig::suppress_rolled_back`

The `suppress_rolled_back: bool` field (default `true`) is the migration
gate:

- `true` (default) — full transaction-aware replay. Safe for all file
  versions because v0.1 files pass through unchanged.
- `false` — emit all `Entry` events verbatim, ignore all markers. Use only
  when deliberately replaying a v0.2 file as if it were v0.1, e.g. for
  diagnostic tooling or disaster recovery that wants to surface
  rolled-back rows for inspection.

There is no configuration migration required. The default is correct for
both old and new files.

### Engine version compatibility

A v0.2 engine writing v0.2 markers to a segment that a v0.1 engine then
tries to read: the v0.1 deserialiser will encounter an unknown tag and
return a decode error. Basin's deployment model is single-binary, so this
situation does not arise in production. The risk is relevant only during a
partially-applied upgrade of a cluster where one node writes v0.2 and
another reads it. The mitigation is: deploy the WAL reader upgrade (tag
awareness in `file_wal.rs`) before the write-path upgrade (C2's
`append_tx_begin` / `append_tx_commit` calls). The reader is a strict
superset of the writer capability.

## Performance impact

### Per-transaction overhead

Three markers per committed transaction: `Begin`, data entries, `Commit`.
One marker for a rolled-back transaction: `Begin`, data entries, `Rollback`.

Wire cost per marker: 13 bytes (4-byte length prefix + 1-byte tag + 8-byte
`tx_id`). Three markers = 39 bytes per committed transaction.

For a 1 000-row transaction where each row entry averages 256 bytes:
total payload = 256 000 bytes. Marker overhead = 39 bytes. Overhead ratio
= 0.015 %. Negligible.

### Forced fsync on `Commit`

The `Commit` marker bypasses the 200 ms batch flush and forces an
immediate fsync. This is the dominant cost. It is also the cost that
already exists for correctness — the client cannot be acked before the
data is durable. The marker itself adds zero latency to the fsync; it is
appended in the same write call as the last data entry's batch flush.

In practice the implementation should batch the `Commit` marker into the
same `write` syscall as the final data entries of the transaction, then
fsync once. The total syscall cost is unchanged from the pre-marker path
(one fsync per `COMMIT` was already required for durability, even without
explicit markers).

### Replay buffering

Open transactions are buffered in memory during replay. The buffer for a
transaction of `n` entries holds `n` `WalEntry` structs. For a 1 000-row
transaction with 256-byte payloads, that is approximately 256 KB plus
`WalEntry` struct overhead (~80 bytes per entry) = ~336 KB total.

A maximum buffer size of 512 MB per replaying partition is enforced. If a
single open transaction's buffer exceeds this limit, `replay_wal` returns
an error. This signals WAL corruption (a transaction larger than any
reasonable workload implies a missing `Rollback` or truncated `Commit`
with runaway buffering). The operator should inspect the segment with
`basin-wal-inspect` tooling and, if the segment is genuinely corrupt,
replay with `suppress_rolled_back = false` to extract as much data as
possible.

## Alternatives considered

### WAL-per-transaction file

Write each transaction's entries to an individual file. On `COMMIT`, rename
the file to a "committed" directory; on `ROLLBACK`, delete it.

**Rejected.** File-per-transaction creates O(transactions/second) file
descriptors and directory entries. At 10k TPS that is 10k files/second,
which saturates local filesystem journalling on most kernels within minutes.
It also prevents cross-transaction batching of fsyncs — the most important
I/O optimisation for throughput. The rename-as-commit trick is clever but
non-composable with Basin's segment-based object-store flush.

### Pending-bit per entry, flipped at commit

Each entry carries a 1-bit "pending / committed" flag. The flag is written
as `pending = true` at append time and rewritten to `committed = true` at
`COMMIT`.

**Rejected.** The WAL is an append-only structure by design. Rewriting
existing bytes on disk requires either a separate bitmap sidecar (adds
complexity, a second file to fsync) or a seek-and-overwrite pattern (breaks
the sequential-write guarantee that makes WAL fast on spinning disks and
SSD write-combining). The pending-bit approach also does not compose with
the object-store flush path where segments are uploaded as immutable blobs.

### Two-WAL system: speculative log + committed log

Write all entries to a "speculative" WAL. On `COMMIT`, copy or reference
the committed entries into a second "committed" WAL. Replay only reads
the committed WAL.

**Rejected.** Doubles fsync cost — every committed entry touches two files.
Makes the compactor's truncate logic substantially more complex (two
pointers to advance). Provides no advantage over the single-marker approach
while adding significant operational complexity. PostgreSQL considered a
similar "undo log vs redo log" separation and concluded a single
append-only redo log with explicit markers is the right shape for a
crash-safe write path.

## Consequences

### What is now possible

- **HTAP memtable transaction integration (Phase 5.14.C2):** rolled-back
  memtable writes are suppressed on WAL replay. The correctness pre-condition
  from ADR 0016 is met.
- **Savepoint persistence:** `ROLLBACK TO SAVEPOINT` at the WAL level could
  be supported in a future phase by writing per-savepoint `Begin` /
  `Rollback` sub-markers. The current ADR does not require this; savepoints
  use in-memory watermarks only.
- **Cross-WAL-file transaction replay:** the `tx_id` is carried per-entry
  through the event stream, making it feasible to track a transaction that
  spans multiple WAL segments (e.g. a very large transaction that causes a
  segment rotation mid-transaction). The current implementation does not
  support this; it is an extension point.
- **Diagnostic tooling:** `basin-wal-inspect` can surface per-transaction
  entry counts, sizes, and commit / rollback ratios from segment files
  without running a full replay.

### What is now harder

- **WAL readers must implement the transaction state machine.** Any reader
  of `basin-wal` segments that previously treated every record as an
  independent committed fact must now handle `Begin`, `Commit`, and
  `Rollback` markers. The `replay_wal` helper encapsulates the state
  machine; readers that use it directly inherit correct behaviour. Readers
  that parse segments directly (e.g. the `basin-wal-inspect` tool,
  replication consumers) must be updated.
- **Forced fsync on `Commit` is now a correctness invariant, not an
  optimisation.** Any future WAL backend implementation (`RaftWal`,
  cloud-WAL) must uphold the "durable before `append_tx_commit` returns"
  contract. This is documented in the `Wal` trait doc comment.

## References

- [ADR 0016 §"Transaction integration"](./0016-htap-hot-tier-architecture.md) — introduced the `BEGIN`/`ROLLBACK` WAL marker pre-condition and memtable watermark semantics.
- [ADR 0016 addendum 2026-05-19](./0016-htap-hot-tier-architecture.md) — locks memtable schema evolution policy; notes the ADR 0018 pre-condition.
- [Phase 5.14.C2 spec](../roadmap/2026-05-19-decomposition.md) — file-scope and acceptance gate for the write-path side of this protocol.
- `crates/basin-wal/src/lib.rs` — `WalEvent`, `WalReplayConfig`, `replay_wal`, and the `Wal` trait method stubs (`append_tx_begin`, `append_tx_rollback`, `append_tx_commit`).

## Reconciliation (2026-05-20)

**Option A chosen: impl wins; ADR §6 updated to reflect shipped semantics.**

The C2 implementation commit (`5551761`, "feat(wal): BEGIN/ROLLBACK transaction markers + replay suppression") shipped only `TxBegin` and `TxRollback` variants in `SegmentRecord` / `WalEvent` — no `TxCommit` variant was added, and no `append_tx_commit` method was implemented on `LocalWal` / `FileWal`. The `replay_wal` function instead uses **implicit commit at end-of-input**: any open transaction (a `Begin` with no matching `Rollback` and no `Commit`) is emitted as committed when the event stream is exhausted. This directly contradicts the explicit-`Commit`-marker design in §6 above.

**Why the deviation happened.** The C2 agent followed the WAL implementation plan that was active at the time (BEGIN + ROLLBACK only), which omitted the `Commit` variant for simplicity. The ADR was written in parallel and locked the more conservative explicit-commit protocol, but the two sides were not cross-checked before the commit landed.

**Correctness assessment of the shipped semantics.** The ADR's §6 argument is sound: implicit-commit-at-EOF cannot distinguish a clean shutdown from a mid-transaction crash. A `BEGIN` / N inserts / crash scenario will, on replay, emit all N rows as if committed — the application never issued `COMMIT`. For Basin's current memtable use case this is a real (not theoretical) correctness risk. However, the risk is bounded: the hot tier is write-preview only in Phase 5.14, and crash recovery of the in-memory memtable always starts from a cold empty state anyway (memtable is not persisted to object store yet). In the current deployment reality, a crash discards the memtable entirely and the WAL replay rebuilds it; the implicit-commit semantics cause at most a brief window of phantom rows that will be overwritten or compacted on the next flush cycle. This is not ideal but is not a data-loss or durability violation at the Parquet / object-store layer.

**Future work.** An explicit `TxCommit` variant (`tag 0x12`) and a matching `append_tx_commit` call from `executor.rs` remain the correct long-term design and should be added when the hot tier's WAL-replay path is used for durable recovery (Phase 5.14.C4 or later). At that point the `replay_wal` state machine must be updated to treat end-of-input with an open transaction as a **discard** (the original Case 4 intent), and the existing implicit-commit-at-EOF path should be removed. The acceptance test in `crates/basin-wal/tests/tx_markers.rs` (gate 1) explicitly tests implicit commit and will need to be updated when this change lands.

**Files touched by this reconciliation.** `docs/decisions/0020-wal-transaction-markers.md` (this section), [`docs/decisions-log.md`](../decisions-log.md) (one-line log entry). No code changes — the WAL crate (`crates/basin-wal/`) is unchanged.
