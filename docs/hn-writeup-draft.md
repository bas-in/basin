---
title: "HN write-up — draft"
nav_section: meta
sidebar_position: 90
summary: "Internal launch-post draft. Numbers must be re-verified against a fresh benchmark run before posting; do not cite figures the linked card no longer shows."
---

# HN write-up — DRAFT (review before posting)

> Internal draft. Numbers are from the LocalFS differential bench
> (`benchmark/RESULTS_localfs.md`, regenerated `68717f5`). Re-verify the
> headline figures against a fresh run the day before posting — bench
> numbers drift run-to-run (±15% on the analytical shapes). Do NOT post
> a number that the linked card doesn't currently show.

---

## Title options

1. **Show HN: Basin — Postgres-compatible multi-project database on object storage (Rust)**
2. **Show HN: We benchmarked our database against Postgres on every query and published the losses**
3. **Show HN: Basin — columnar Postgres for 10k-project SaaS, $0.10/project/mo**

(Option 2 leads with the integrity angle, which is the differentiator. Option 1 is the safe descriptive one. Pick based on whether you want to lead with the moat or the category.)

---

## Body

Basin is a Postgres-wire-compatible database that stores data as columnar
files (Vortex / Parquet) on object storage, with an Apache Iceberg catalog,
a file-backed WAL, native vector search, and per-project prefix isolation.
It's built for **multi-project SaaS with many projects** — the case where
running one Postgres per project gets expensive and running all projects in
one Postgres gets operationally hairy.

Apache-2.0, written in Rust, self-hostable. Repo: <link>

### The thing we did differently: a differential benchmark harness against real Postgres

Every performance claim runs the **identical** schema, identical data, and
identical query against both Basin and a real Postgres 18, at 10k / 100k /
1M rows, and records the ratio. PG runs at stock defaults (not hobbled, not
specially tuned). Unsupported query shapes show as an honest `-1.0` "(basin
gap)", never as a 0ms win. We publish the **losses** next to the wins.

We think this is rare in the database-marketing space and we'd rather lead
with it than with a cherry-picked geomean.

### Where Basin wins (1M-row SaaS workload, LocalFS)

The columnar substrate compounds on analytical shapes as data grows:

- Correlated subquery in SELECT: **~87× faster** than PG (80ms vs 6.9s)
- LATERAL JOIN: **~53× faster** (82ms vs 4.4s)
- 2-table JOIN + GROUP BY: **~37× faster** (38ms vs 1.4s)
- COUNT(*) full table: **~22× faster** (131ms vs 2.9s)
- COUNT(DISTINCT): **~5× faster**

And the wins *grow* with scale: of the shapes Basin wins at both 100k and
1M, 20 of 22 widen their lead at 1M. Six shapes flip from loss→win as data
grows (COUNT(*), window LAG, INTERSECT, …).

### Where Basin loses (and we're not hiding it)

- **Single-row UPDATE**: 7× slower at 1M (9ms vs 1.3ms). PG's index+heap is
  structurally faster on point mutations. We closed this from a brutal
  **1550× slower** (118ms) by landing an on-by-default hot-tier write path
  this month — but PG still wins the absolute number.
- **Point query `WHERE pk = ?`**: ~32× slower at 1M — Basin ~0.23ms vs PG
  ~7µs. PG answers a heap index probe from shared_buffers in microseconds;
  Basin's bloom + zone-map prune narrows to one file and reads it in a
  fraction of a millisecond. 0.23ms is fine in absolute terms; the 32× is
  just PG being sub-microsecond. Bounded and flat across scales (0.1-0.23ms
  from 10k to 1M).
- **JSONB scalar extract** (`->>`, `->`, `#>`): 100-2000× slower at scale.
  This is the honest big one — Basin parses JSON per-row where PG has a
  binary jsonb format. We shipped the GIN row-group prune infrastructure for
  `@>` containment and a binary-JSONB columnar encoder is in flight; the
  scalar-extract gap is the next structural fix, not closed yet.
- **Bulk INSERT**: ~6× slower at 1M. Writing N rows is fundamentally O(N)
  and PG's heap append is hard to beat.

### What this is good for

- Many-isolated-projects (per-customer / per-environment / per-region) where
  per-project cost is O(bytes-on-S3), not O(provisioned pool). 10k idle
  projects cost their bytes, ~1.2 KiB RAM each, not 10k Postgres instances.
- Append-heavy / audit-log / event-stream workloads — Vortex columnar
  compression is ~29× smaller on disk than PG heap on our 1M SaaS shape.
- Mixed tabular + vector (RAG) data in one database.

### What it's NOT

- Not a drop-in OLTP replacement for a write-heavy, point-mutation-heavy app
  that needs sub-millisecond UPDATE latency at scale. Use Postgres.
- Not an edge / local-first DB (that's Turso/libSQL).
- We don't ship Edge Functions / Storage buckets / a hosted dashboard — it's
  the database part, not the full BaaS-at-the-edge stack.

### Status

Pre-alpha, public eval. The hot-tier UPDATE/DELETE fast paths just went
on-by-default (with a `BASIN_HOTTIER_FASTPATH_DISABLE=1` kill-switch). We're
looking for multi-project SaaS teams who've felt the per-project Postgres
pricing wall to try it and tell us where it breaks.

---

## Anticipated HN comments + honest answers (prep)

**"Why not just use Postgres + Citus / partitioning?"** — Citus shards one
big PG; it doesn't give you O(bytes) per-project cost for 10k mostly-idle
projects. The wedge is cold-project economics on object storage, not sharding
a hot cluster.

**"JSONB 2000× slower is a dealbreaker for SaaS."** — Agreed for
JSONB-heavy workloads today; that's why it's the named next structural fix
(binary JSONB encoder). For workloads where JSONB is config/metadata (read
rarely, small docs) it's not on the hot path. We're not claiming the gap is
closed.

**"Object storage latency will kill point reads."** — True for the cold
uncached path on real S3; the bench shows this. The hot-tier + page cache
keep warm paths competitive. We publish both the LocalFS and the real-S3
cards so you can see the difference.

**"Show me the $/project math."** — `docs/multi-project.md`. ~$0.10-0.20 per
project per month at 100MB/project, dominated by storage bytes. The
head-to-head card against Nile (the direct competitor) is an open item —
we'll publish it rather than hand-wave.

**"Is the benchmark rigged?"** — The harness, the schema, the data
generators, and the PG config are all in the repo. PG gets a PRIMARY KEY and
stock defaults. Run it yourself: `cargo test -p basin-integration-tests
--test compare_postgres_100k --release`.

---

## Post-week follow-through (not part of the post)

- Start 20 cold-outreach conversations with multi-project SaaS founders the
  same week (script: `docs/customer-interview-script.md`).
- Have the Nile head-to-head card ready as the first follow-up reply if the
  pricing question dominates.
