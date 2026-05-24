# NEXT_WAVES.md — Strategic + Performance Backlog

This file queues work that isn't part of the current performance wave but
needs to ship for Basin to win against its actual competitors (Nile,
Supabase, Neon). The current performance wave is tracked in `decisions.md`
session logs and in-flight commits; this file is the **next-wave queue**
that picks up after performance parity is the default.

The current wave's focus is **performance**. Items below are queued for
follow-up — none of them blocks shipping the perf work that's in flight.

Rules:
- Each item names a clear deliverable, a measurable acceptance bar, and
  the user / market signal it unblocks.
- "Cloud-private" items belong in the private basin-cloud repo, not in
  OSS docs — they are referenced here only as queue items.
- "Strategic" items inform OSS roadmap but are written as engineering
  tasks, not marketing claims.

---

## Wave NOW — OLTP fastpaths default-on (the real load-bearing wave)

This is the gate that unlocks the README rewrite, removes the
"do-not-run-in-production / pre-alpha" caveat, and flips the OLTP
customer conversation from "still in flight" to "shipped". Until these
land, the env vars `BASIN_HOTTIER_DELETE_FASTPATH=1` and
`BASIN_HOTTIER_UPDATE_FASTPATH=1` are opt-in by default because flipping
them would produce wrong results on at least one read path. See
`crates/basin-engine/src/dml_mutate.rs:637` for the in-code note.

Current state (verified 2026-05-24 — the dml_mutate.rs:637 comment is
partially stale; #1 below is already done, #2 is the load-bearing gap):

0. **[DONE] Tombstone visibility in `fast_select::execute_simple_select`**
   — wired at `fast_select.rs:1886` via `apply_tombstone_filter_to_batches`,
   and the DataFusion-path `TombstoneFilteringTable` wraps cold with both
   `TombstoneFilterExec` + `UpdateOverlayExec`. The stale comment in
   dml_mutate.rs:637 should be updated to reflect this.

1. **Wire `UpdateOverlayExec` into `HtapUnionTable::scan`**
   (`session.rs:1890`). Today it applies `maybe_wrap_with_tombstone_filter`
   but **not** the Update overlay — meaning a SELECT routed through the
   HtapUnionTable provider after a fast-path UPDATE sees the stale cold
   row alongside the new in-memory row image (duplicates). Mirror the
   pattern from `TombstoneFilteringTable::scan` at
   `hot_tombstone.rs:712-733`. **Load-bearing.**

2. **Extend the C6 differential harness with a 4th mode: `fastpath-on`.**
   88 shapes × {empty, memtable, split, fastpath-on} = 352 sub-assertions.
   Round-trips: DELETE-then-SELECT, UPDATE-then-SELECT. Gate matrix:
   RETURNING, multi-col PK, RLS-enabled, soft-delete, audit-table, reactor
   — each must still route to the slow CoW path.

3. **Gate-matrix tests** (`dml_mutate.rs:660-700`). Gates already exist;
   just need one test per route that proves the slow path triggers when
   the gate condition holds. Most are one-line gate checks already
   working in production; tests pin them in place against regression.

4. **Flip the defaults.** `BASIN_HOTTIER_DELETE_FASTPATH` and
   `BASIN_HOTTIER_UPDATE_FASTPATH` default `1` instead of `0`. Add a
   kill-switch `BASIN_HOTTIER_FASTPATH_DISABLE=1` for operator rollback
   without a deploy.

5. **Resolve the deferred TxCommit WAL marker** (`decisions.md:1113`,
   ADR 0020 §6). ADR specified explicit `WalEvent::Commit { tx_id: u64 }`;
   shipped impl uses implicit commit-at-EOF. Bounded correctness gap on
   hot-tier in-memory rebuild after crash mid-tx. Add the variant + emit
   from executor at COMMIT; teach `replay_wal_into` to require matched
   BEGIN/COMMIT pairs.

6. **Regen the perf battery** with the new defaults.
   `compare_postgres*.json` rerun with no env vars set. **Acceptance:**
   single-row UPDATE p50 drops from ~118 ms (1M-row card) to <5 ms (the
   C2 acceptance gate). DELETE WHERE id IN (10) stays fast across all
   scales.

7. **README + CAPABILITIES.md rewrite** once #1-6 land. Remove "OLTP
   write path is architecturally slow until the hot-tier UPDATE/DELETE
   routes finish landing" and the "do not run in production" caveat
   around point UPDATE. Update Phase 5.14 status from "in flight" to
   "shipped". Note: `CAPABILITIES.md` is owned by the other-chat
   workstream per memory — coordinate or hand off.

8. **Re-key HTAP-cached `MemRowValue::Row` by encoded PK.** Today
   `htap_promote_to_registry` (`executor.rs:7424`) inserts cached rows
   under a monotonic-counter `RowKey`, not the row's PK-encoded key. Two
   consequences: (a) the memtable PK direct-get fast path (`79ee848`)
   returns `None` against INSERT-cached rows and falls back to the O(n)
   snapshot path — so the Concurrent SELECT bench (48× at 1M) doesn't
   move; (b) any future read-side merge that wants to dedupe hot vs cold
   by PK has no key to dedupe on. Fix: derive the PK bytes from the
   inserted batch (mirror `dml_mutate::pk_scalar_to_row_key`) and use
   that as the `RowKey`. Multi-crate touch: `basin-engine/executor.rs` +
   `basin-hottier/merge.rs` semantics (Row vs Update vs Tombstone must
   keep their distinct meanings under the PK-keyed regime). **Real fix
   for the Concurrent SELECT 16 sessions perf gap.**

**What this unlocks** (user-quoted, verbatim):
- "Single-row UPDATE p50: <5ms, on par with Postgres + index"
- Pre-alpha caveat gone from the front page
- 1M-row PG comparison flips from "1550× slower on UPDATE" to "within
  2-4× of PG on the OLTP shapes"
- Phase 5.14 flips from "in flight" to "shipped"
- OLTP customer conversation becomes possible — mutation-heavy SaaS,
  not just append-shaped
- Phase 0 customer interviews can finally land against a real product

Realistic effort: 3-5 weeks at the punch-list pace; faster in tight
sub-agent waves because the items partition cleanly by file.

---

## Wave N+1 — Beat Nile (the harder fight)

Nile is Basin's direct competitor on the multi-tenant Postgres pitch.
Nile virtualizes tenants on real PG, so it inherits PG's perf and
semantics. Basin's structural answer is the substrate economics, but
that's a calculator until there's a published head-to-head card.

1. **HTAP hot tier on-by-default** — flip `BASIN_HOTTIER_UPDATE_FASTPATH`
   and `BASIN_HOTTIER_DELETE_FASTPATH` from opt-in to default-on. Drop the
   "do not run in production" caveat from `CAPABILITIES.md`. **Acceptance:**
   integration test matrix passes with no env flags set; PR2 (UPDATE
   fastpath) and the DELETE fastpath are exercised by default.
2. **Publish `tenants × $/mo × p99 on a realistic SaaS shape` card vs
   Nile.** Today the `$0.10/project` claim is a calculator, not evidence.
   **Acceptance:** new bench card under `benchmark/` running the same
   `compare_postgres` shape against a real Nile cluster (or a published
   Nile reference if a head-to-head is impossible to provision). Honest
   `-1.0` on shapes Basin can't run.
3. **Close JSONB to ≤10x of PG on at least one of the four hot shapes**
   (`->>`, `->`, `@>`, `jsonb_set` UPDATE). Wave 2's row-group prune wiring
   + lazy byte-scanner extension are the first step; memtable-resident
   JSONB path and binary-JSONB Vortex encoder are the structural follow-up.
   **Acceptance:** at the 1M scale, at least one JSONB shape drops below
   10x slower than PG on the localfs bench.
4. **Tenant-context SQL ergonomics: `SET TENANT '<id>'` + scoped
   `auth.uid()`** — Basin's prefix-isolation is structurally stronger than
   Nile's, but it's invisible at the SQL surface. Add a `SET TENANT '...'`
   session-local that switches the engine's active project for the rest
   of the session; document in the OSS tutorial as the equivalent of
   Nile's `SET nile.tenant_id = '...'`.

## Wave N+2 — Beat Supabase (the product fight)

Supabase wins on real-PG semantics, mature dashboard, and SDK ecosystem.
Basin can match the SQL+Auth+REST surface but the product gap is what
makes evals stick.

5. **`supabase-to-basin` migration tool** — `pg_dump` + Supabase Auth
   users export + RLS policies → Basin import, with a compat report
   listing every shape that won't translate (Edge Functions, Storage
   buckets, Realtime channels). **Acceptance:** can ingest a Supabase
   project export, generate a `compat-report.md`, and run a smoke suite
   against Basin to confirm RLS + Auth + 90% of the SQL shapes work.
6. **`basin-js` ≡ `supabase-js` API surface** — `createClient(url,
   anonKey)`, same RLS UX, same realtime channel shape, same env vars.
   **Acceptance:** the public Supabase quickstart tutorial works against
   a Basin instance with only the URL changed.
7. **basin-cloud live and quoting real $/tenant/mo** — the cloud surface
   has to exist publicly before "Supabase that scales to 10k tenants
   without bankrupting you" lands. (Cloud-private — referenced here so
   the perf wave knows what it's downstream of, but the work belongs in
   the basin-cloud repo.)
8. **Pick what we concede explicitly** — Basin's ADR 0019 skips Edge
   Functions; that decision should be the first paragraph of the README's
   "vs Supabase" section, not buried in the matrix. Right now the
   advertised surface (Auth + REST + Vector + Vortex + WASM + Realtime +
   Cron + HTTP + Trgm + PostGIS + Time Travel + Branching) is **larger**
   than Supabase's, which makes "best-in-class at any of them" the
   weakness. **Acceptance:** README opens its competitor sections with
   what Basin **doesn't** ship.

## Wave N+3 — Cross-cutting (true vs both)

9. **Phase 0 customer interviews** — engineering is mature, customer
   signal isn't. WEDGE.md tracks this as the open gate. **Acceptance:**
   one real "we migrated N tenants to Basin and it works" public
   reference, sourced via the `docs/customer-interview-script.md` script.
10. **Lead with the right headline number** — `47× less RAM/conn` is real
    but the audience is shrinking (pgbouncer / RDS Proxy / Neon's pool
    cover the held-open-connection case). The **multi-project storage
    economics** is the durable wedge. Reorder the README badges and
    opening paragraph to lead with bytes-at-rest + per-tenant $0.10
    instead of RAM/conn.
11. **Extend the differential bench harness with Nile-oracle and
    Supabase-oracle rows.** Today the harness has PG-oracle. The honesty
    of the bench is itself a moat — extending it to two more oracles
    makes the comparison cards self-evident. **Acceptance:** new bench
    files `compare_nile_*.rs` and `compare_supabase_*.rs` that mirror
    `compare_postgres_*` shape-for-shape.

## Wave N+4 — Performance follow-ups (after Wave 2 lands)

> **Note:** any item that bottoms out in **DataFusion-internal cost** is
> tagged `DF-upstream` and is **deferred for user discussion** — do not
> dispatch agents on those without an explicit go-ahead. Basin-side
> levers come first.


12. **Memtable-resident JSONB path** — payload values written via
    `INSERT ... VALUES ('<json>'::jsonb)` should land in the memtable as
    pre-parsed Value bytes (or a small structured form) so the read-side
    UDFs don't re-parse. Cuts JSONB scalar extract on hot rows from
    O(parse_per_call) to O(1).
13. **Binary JSONB Vortex encoder** — closes the 800-3693x JSONB cluster
    structurally. Vortex encoder that splits documents by top-level path
    so `payload->>'key'` lowers to a column projection. Multi-week —
    queued, not in flight.
14. **Memtable lock contention rework** — single `parking_lot::RwLock<
    BTreeMap>` is the bottleneck on the `Concurrent SELECT 16 / RMW 8`
    bench shapes. Options: lock-free skip-list memtable, fine-grained
    per-shard partitioning, or read-snapshot-once-per-query. Wave 2
    addressed the point-lookup case with `entry.memtable.get(&key)`; the
    range-scan / aggregate concurrent case still uses `snapshot()`.
15. **Bulk INSERT 1M throughput** (6.5x slower than PG at 1M) — profile
    encoder + batch-size + WAL fsync rate. Likely a combination of
    Vortex Fast encoder cascade overhead and per-batch shard.tail push.
16. **Recursive CTE / Window LAG / GROUP BY + HAVING** — DataFusion-
    internal slowness. The Basin-side levers are exhausted on these.
    **DEFERRED — user-flagged "leave for last, for discussion".** Do not
    open upstream DF issues / PRs without an explicit go-ahead. Documented
    here so the wave triage doesn't accidentally pick them up.

---

## Wave N+5 — GPU acceleration (opt-in, background-only, far-future)

> **Explicitly NOT for hot OLTP.** Single-row UPDATE / point query / WAL
> fsync are sequencing-bound and PCIe round-trip latency (~10-100µs per
> kernel launch) makes GPU strictly worse for those shapes. The wedge
> here is **background and bulk work** where GPU's parallelism amortizes
> the round-trip. Queue this only after Wave NOW + binary-JSONB +
> memtable contention land — GPU is a 5× faster *version* of fixes
> Basin can already ship on CPU, not a different fix.

17. **GPU-accelerated background JSONB transcoder.** The hot cost of a
    binary-JSONB rewrite (item #13) is JSON parsing during cold-tier
    compaction. cuDF / Spark RAPIDS already does GPU JSON parse at
    10-100× CPU throughput. Background compaction is latency-insensitive
    so the PCIe round-trip is irrelevant. Net effect: Basin can transcode
    aggressively on every compaction without a compute budget concern,
    closing the JSONB cluster structurally with no wall-clock penalty.
    **Acceptance:** opt-in via env / shard config — base CPU path stays
    the default; operators with NVIDIA hardware point a flag at it.

18. **GPU vector-search worker pool.** basin-vector already ships HNSW.
    CAGRA / Faiss-GPU are 10× faster on large indices. Extends an existing
    wedge rather than fixing a loss; ROI depends on customer signal for
    billion-vector multi-tenant SaaS (most workloads are smaller). Same
    opt-in shape as #17 — base CPU path remains the default.

19. **GPU OLAP scan + aggregation** for heavy `JOIN + GROUP BY` shapes at
    10M+ rows. Basin already wins these at 100k-1M (122× faster on
    correlated subquery at 1M); GPU compounds the win further. Not the
    highest-ROI item because Basin's wedge isn't "fastest analytical
    engine" — it's "good-enough analytical on cheap storage with PG
    semantics." Queue last among the GPU items.

**Hard constraints on any GPU item:**
- Never required. Basin's target deployment is cheap object storage +
  commodity compute; mandating GPU breaks the `$0.10/tenant/month`
  substrate-economics pitch.
- Opt-in only — env var + shard config flag. CPU path is the default
  and must keep passing the same test surface.
- Background workers only (compaction, transcode, index build). The
  hot read/write path stays CPU. Per-request GPU usage is out of scope.
- Honest-negative outcomes valued: if a GPU path doesn't beat the CPU
  path by ≥3× at the operator's batch size, ship the negative and
  recommend the CPU path. The benchmark integrity rule applies.

---

## How this list is maintained

- Items are added when the user / strategic thread mentions them and
  they don't belong in the current perf wave.
- Items are removed when they land or get re-scoped to a real ADR / task.
- `decisions.md` is the session-log; this file is the next-up queue.
- This file is OSS-safe: cloud-private items (basin-cloud architecture,
  customer-specific deals) are referenced abstractly here and live in
  the private repo.
