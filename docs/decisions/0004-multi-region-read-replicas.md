# 0004 — Multi-region read replicas (supersedes part of 0001)

- **Status:** Accepted
- **Date:** 2026-04-30
- **Tags:** scope, architecture, multi-region, supersedes-0001

## Context

ADR [0001](./0001-single-region-only.md) deferred *all* multi-region work
until a customer paid for it. Subsequent direction from the founder:
ship multi-region read replicas now as part of the wedge expansion, on
the grounds that "data residency reads close to user" is a wedge-aligned
property for the audit-log SaaS customer (their analysts in three time
zones each want fast reads of the same shared corpus).

This ADR scopes what we *will* build: **eventual-consistent cross-region
read replicas with region-local writes**. We do **not** build cross-region
2PC, strong global consistency, multi-master writes, or anything Spanner-
class — those remain deferred per `0001`.

## Decision

Basin gains a region-aware deployment topology:

1. **One primary region** per tenant (configured at provisioning time;
   stored on the tenant's catalog row). All writes for that tenant route
   to that region's WAL + shard owners.
2. **N read-replica regions**. Each replica region runs its own router +
   shard-owner pool. The Parquet substrate is replicated via S3
   cross-region replication (CRR) — async, vendor-managed, eventual.
3. **Per-region catalog read replica.** The Postgres-backed catalog
   ships a logical replication subscriber in each replica region. Reads
   in a replica region use the local catalog replica; writes (DDL and
   `append_data_files`) round-trip to the primary region.
4. **A small region-routing layer** at the router that, given a
   `(tenant_id, op_type)`, decides whether the operation can be served
   locally (read) or must forward to the primary (write).

Consistency model:
- Reads in the primary region: as today (read-your-writes).
- Reads in a replica region: monotonic-but-bounded-stale. The bound is
  whatever S3 CRR + Postgres logical replication give us, typically
  seconds to a few minutes during steady state.
- Writes: always region-local to the tenant's primary. A write submitted
  to a replica region transparently forwards to the primary; the latency
  bill includes the cross-region RPC.

What we are **not** doing:
- No cross-region 2PC.
- No active-active writes.
- No automatic failover of a tenant's primary region. Primary failover is
  a manual operator action with explicit data-loss-window guarantees.
- No global secondary indexes or cross-region joins. Queries that touch
  both a primary's catalog and a replica's catalog should be rare and
  the planner can route them through the primary.

## Consequences

**Positive**

- Audit-log readers in another region get sub-100ms reads on cached data.
- Tenant data residency is unambiguous: the primary region is the legal
  home of the tenant's data; replicas are best-effort copies.
- Implementation cost is bounded: no consensus state machine, no global
  catalog, no clock-bound consistency machinery.

**Negative**

- Operators must explicitly choose a primary region per tenant. Bad
  defaults (always us-east-1) will bias read latency for non-US customers.
- A write submitted to a replica region pays cross-region RPC. We do not
  optimise this in v1; if it becomes painful, customers move their
  writers to the primary region.
- S3 CRR has cost (per-GB replication fees). Customers see this on their
  storage bill. Explicit in `CAPABILITIES.md`.
- Replication lag during steady state is typically OK; during an S3
  regional incident it can spike. Operators must monitor it.

**Mitigations**

- Replication lag is exposed as a per-tenant metric; alerts fire when
  > 5 minutes.
- Dashboard adds a "replica freshness" card per region.
- Tenants whose workload cannot tolerate lag are configured with
  primary-only reads (one region).

## Architectural compatibility

The existing layered architecture (router / shard owner / WAL / storage
+ catalog) supports this without rewrites. The pieces that change:

- `basin-router`: a new `Region` type carried in the connection metadata;
  a routing decision per op based on `(tenant.primary_region, this_region)`.
- `basin-catalog::PostgresCatalog`: a `region` setting on the connection
  string; in replica regions it points at the local Postgres replica.
- `basin-server`: `BASIN_REGION=us-east-1`-style env var. The
  `tenant_resolver` carries the tenant's primary region.
- New crate `basin-region` (small): types + the cross-region forward path.

Storage layout under `tenants/{tenant_id}/...` is unchanged; the prefix
is region-agnostic so S3 CRR replicates it as-is.

## Trigger to expand to write-anywhere consistency

We re-open this ADR (writing 0005 or later) when:

1. A wedge customer signs ≥ $50k ARR contingent on active-active writes
   AND
2. Their use case truly cannot be served by a "writers go to one region,
   readers everywhere" model. (Most SaaS workloads can be.)

The single-customer threshold is intentional. Active-active is the gate
to Spanner-class engineering and we should not cross it on speculation.

## Alternatives considered

- **Skip multi-region entirely.** Was the previous ADR's stance.
  Founder direction overrides; document the new direction.
- **Active-active writes via per-tenant Raft groups across regions.**
  Rejected for v1: ties write latency to cross-region quorum. The wedge
  customer doesn't need it; we can add it later if they pay for it.
- **Cell-based architecture (one cell per region, no replication).**
  Rejected: each customer would have to choose one region, no read
  locality benefit. Equivalent to today's deployment, just deployed
  N times.
- **Read-only follower replicas via Postgres logical replication AND
  separate object-store buckets per region.** Rejected: doubles storage
  cost and complicates BYO-bucket. S3 CRR within one bucket is the right
  primitive.

## Implementation order

1. Add `region` to tenant catalog rows + `BASIN_REGION` env var.
2. Region-aware router decision: read locally or forward to primary.
3. Forwarding path: a thin pgwire-to-pgwire proxy from replica regions
   to the primary region, holding the tenant's connection identity.
4. Catalog replication: turn on Postgres logical replication; update
   `PostgresCatalog::connect` to accept a `read_only` flag for replicas.
5. S3 CRR config: documented bucket setup, no code change.
6. Dashboard: replication-lag and read-locality cards.

(1)–(3) is roughly two weeks of focused work and unlocks
locality-of-reads, which is the customer-facing benefit. (4)–(6) is
another week or two for the durability/ops story. We do not need to
ship all six before claiming "Basin is multi-region"; the first three
are the wedge-aligned half.
