# 0009 — Multi-region architecture (regional WALs, replicated storage, no cross-region writes)

- **Status:** Accepted (decision locked; full replication implementation
  remains future work — see "What this does NOT commit us to" below).
- **Date:** 2026-05-07
- **Tags:** scope, architecture, multi-region, replication, deferred

## Context

Today Basin is single-region. Every Basin process reads `BASIN_REGION`
out of its environment as `"local"` (the new default introduced by this
ADR's scaffolding) and treats every tenant as living in that one
region. The catalog, the WAL, the shard owners, and the storage prefix
all share one regional fate.

[ADR 0001](./0001-single-region-only.md) deferred all multi-region work
until a customer paid for it. [ADR 0004](./0004-multi-region-read-replicas.md)
then re-opened the topic for **read replicas only** — region-pinned
writes, S3-CRR-replicated bytes, region-local reads — and laid out the
two-week wedge that gets locality-of-reads to non-home-region readers.

What 0004 did *not* do is pick the architecture for the next step:
**when a customer eventually wants their writes to survive a region
incident, or a regulator demands synchronous quorum across two
regions, what shape does Basin take?** TASK.md Phase 6 names the line
item ("Multi-region: regional WAL + S3 cross-region replication") but
doesn't pick between the two architectures it could mean.

The two candidate architectures are:

- **A. Raft-replicated regional WALs.** Each region has its own Raft
  quorum on top of `basin-wal`. Writes for a region-pinned tenant are
  acknowledged once the region's quorum has them. Cross-region reads
  go through async S3 cross-region replication of the storage prefix.
  Cross-region writes fail loudly until a future 2PC ships.
- **B. S3 cross-region replication only.** One region is a tenant's
  *primary*; writes always route to it (forwarded transparently from
  replica regions per ADR 0004). Other regions get the bytes via
  bucket-level S3 CRR; they serve reads only. No WAL replication;
  cold-region reads see eventual-consistency latency.

This ADR locks the choice.

## The decision

**A hybrid that defaults to B and grows into A.** Concretely:

1. **Storage replication is S3 CRR.** Cross-region durability of the
   actual data bytes is delegated to the object store's built-in
   cross-region replication. We do not write our own bytes-mover. This
   is unchanged from ADR 0004 and is the "cheap" half of the answer.
2. **Within a region, the WAL grows into Raft.** TASK.md Phase 2
   already names regional Raft as the long-term shape of `basin-wal`.
   This ADR confirms it: the WAL becomes a per-region quorum (3 or 5
   nodes, configurable) so a single-node WAL incident inside a region
   doesn't take that region's writes down. **The Raft groups do not
   span regions.** Cross-region quorum is the gateway to the
   Spanner-class engineering 0001 explicitly defers.
3. **Tenants are region-pinned.** Every tenant has a `home_region`.
   Writes route to that region's WAL + shard owners; reads can come
   from any region (with the freshness bound 0004 already documents).
   A write submitted to a non-home pgwire endpoint either forwards to
   the home region (the 0004 forwarding path) or, in deployments where
   forwarding is disabled, fails loudly with a typed error rather than
   silently writing to the wrong region.
4. **There is no cross-region 2PC.** A multi-region transaction that
   touches tenants pinned to different regions is rejected at plan
   time. This is the same "Spanner-is-out-of-scope" line ADR 0001 drew
   and we do not move it here.

Why this and not pure A or pure B:

- **Pure B** under-delivers on regional resilience. Inside a single
  region the WAL is still single-node; one disk failure or process
  crash on the WAL host stalls every tenant pinned to that region
  until manual recovery. Customers asking for multi-region almost
  always also want HA *within* a region; shipping cross-region
  replication without intra-region quorum is a step that doesn't
  match the customer ask.
- **Pure A** over-engineers the cross-region layer. Per-tenant Raft
  groups *across* regions is the hardest possible variant: it ties
  write latency to cross-region quorum (single-digit ms blows up to
  50–250ms p99), forces the catalog to become globally consistent,
  and is the path to the multi-year "we're rebuilding Spanner"
  drift the build prompt explicitly warns against. We don't need it
  for any wedge customer and we will not bend the architecture
  around its eventual existence.
- The hybrid lets us ship the customer-facing wins (locality of
  reads, regional HA) without committing to the engineering bills
  (cross-region quorum, global catalog) that pure A would force.

## What this commits us to

These are the concrete shape changes future Phase 6 work has to
respect. Most are forward-compatible scaffolding shipped *with* this
ADR.

- **`home_region: Option<String>` on `TableMetadata`.** Catalogued
  per-table, defaults to `None` (no pinning — back-compat). Carried
  through both the in-memory and the Postgres-backed catalog;
  Postgres migration is an additive `ALTER TABLE … ADD COLUMN IF NOT
  EXISTS home_region TEXT`. The fork-table copy includes the column;
  `set_home_region` is the only mutator. Per-tenant pinning remains
  out of scope for this ADR — see ADR 0004 for the tenant-row
  variant; per-table is sufficient to lock the metadata shape, and
  per-tenant can supersede it additively later.
- **`BASIN_REGION` env var.** `basin-server` reads it at startup,
  defaults to `"local"` if unset. The router and any layer that
  needs to make a regional decision later receive it through a
  stable accessor (`local_region()` in this ADR's scaffolding).
- **No region routing in v0.1.** The scaffolding records and
  surfaces the values. It deliberately does *not* yet:
  - Reject a write submitted to the wrong region.
  - Forward a write to the home region.
  - Pick a region for a read.
  Those behaviours land in Phase 6 alongside the actual replication
  work. Until then, `BASIN_REGION` is observable in logs and via
  the router accessor; `home_region` is observable on `load_table`.
  No production code path branches on either.
- **Raft as the WAL's long-term shape.** The Phase 2 TASK.md item
  (Raft) is now considered locked-in for Phase 6 — the WAL will
  grow into a per-region quorum, not a global one. v0.1's
  single-node WAL is the genesis of that quorum, not a parallel
  architecture.
- **S3 CRR is the cross-region durability path.** No bespoke bytes-
  replicator. Operators configure CRR at the bucket layer; Basin
  observes the replicated bytes via the same `Storage` it already
  uses. (BYO-bucket tenants configure their own CRR; documented in
  CAPABILITIES.md.)

## What this does NOT commit us to

The following are explicitly out of scope for this ADR and remain
multi-month follow-ups under Phase 6:

- **The Raft cluster itself.** Migrating `basin-wal` from
  single-node to a Raft quorum is the largest piece of Phase 2/6
  work. This ADR locks the architectural choice; the
  implementation is open.
- **S3 CRR config wiring.** Documented bucket setup, IAM roles,
  replication-time-control, replication metrics. No code change in
  this ADR; runbook + ops doc in Phase 6.
- **Cross-region failover automation.** Promoting a replica region
  to primary is an explicit operator action with documented
  data-loss-window guarantees. v0.1's "no automatic failover"
  posture from ADR 0004 stays.
- **Multi-region 2PC.** Transactions that span tenants pinned to
  different regions are rejected at plan time. Lifting that
  rejection is the gateway to Spanner-class engineering and is
  governed by ADR 0001.
- **Region-aware routing.** Forwarding a non-home-region write to
  its home region (the 0004 path) is Phase 6; the v0.1 scaffolding
  records the values but doesn't act on them.
- **Per-tenant region pinning at the auth layer.** The scaffolding
  pins per-table; promoting it to per-tenant (a column on the
  tenant identity row) is additive and lands when the auth /
  control plane gets a tenant-metadata row to hang it on.
- **Catalog replication strategy details.** ADR 0004 already lays
  out logical replication of the Postgres catalog into replica
  regions. We don't redesign that here.

## Architectural compatibility

The single-region-today implementation already preserves most of
what multi-region needs:

- The storage layout under `tenants/{tenant_id}/...` is region-
  agnostic. S3 CRR replicates it as-is.
- The catalog's tenant-scoped trait surface
  (`Catalog::load_table(&tenant, &table)`) does not assume one
  global instance — a future regional catalog replica slots in.
- The WAL is keyed by `(tenant_id, partition_key)`; partition keys
  carry no regional state today and don't need to.
- Tenant IDs are ULIDs, not region-namespaced. The scaffolding
  here adds region as a *property* of a tenant's tables, not as a
  new ID space.

The single concrete thing we *should not* do meanwhile: bake
single-region assumptions into bucket policies that key off bucket
identity rather than the prefix. Already enforced by the
per-tenant prefix layout.

## Trigger to revisit

We re-open this ADR (writing 0010 or later) when **one** of:

1. A wedge customer signs ≥ $50k ARR contingent on **active-active
   cross-region writes**. (The forward path inside a single
   primary region is shipped via ADR 0004 and does not trigger a
   revisit.)
2. A regulator (one Basin will not survive without — e.g. a top-3
   EU bank or a US healthcare prime) mandates *synchronous* quorum
   across two regions for a workload that genuinely cannot be
   served by region-local writes plus async replication.
3. Basin starts to lose deals at a rate of ≥ 3/quarter explicitly
   citing the absence of cross-region writes (logged in the
   lost-deal tracker per ADR 0001's discipline).

A single prospect at smaller value, or "we'd love it someday," is
*not* the trigger. Spanner-class engineering is the gate this ADR
is keeping closed; we don't open it on speculation.

## Alternatives considered

- **Pure A — per-tenant cross-region Raft groups.** Rejected:
  cross-region quorum makes write latency hostage to inter-region
  RTT (50–250ms p99 on AWS), forces a globally consistent catalog,
  and is the multi-year drift toward rebuilding Spanner the build
  prompt warns against. No wedge customer needs this; we add it
  only if the trigger above fires.
- **Pure B — no WAL replication, S3 CRR only.** Rejected: leaves
  the WAL single-node within a region, so a single host crash
  stalls every tenant pinned to that region until manual
  recovery. Customers asking for multi-region almost always also
  want regional HA; shipping cross-region without intra-region
  quorum mismatches the ask.
- **Cell-based architecture: one cell per region, no
  replication.** Rejected (also in ADR 0004): each customer has
  to choose one region with no read-locality benefit anywhere
  else. Equivalent to today's single-region deployment, just
  deployed N times.
- **Defer the architectural decision until the trigger fires.**
  Rejected: small forward-compat scaffolding (one
  catalog column, one env var) is cheap *now* and expensive
  *later*; deferring forces the eventual implementer to do an
  invasive metadata migration on a live multi-tenant catalog.
  The decision lives in this ADR; the implementation defers.
