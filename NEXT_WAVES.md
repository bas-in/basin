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

## How this list is maintained

- Items are added when the user / strategic thread mentions them and
  they don't belong in the current perf wave.
- Items are removed when they land or get re-scoped to a real ADR / task.
- `decisions.md` is the session-log; this file is the next-up queue.
- This file is OSS-safe: cloud-private items (basin-cloud architecture,
  customer-specific deals) are referenced abstractly here and live in
  the private repo.
