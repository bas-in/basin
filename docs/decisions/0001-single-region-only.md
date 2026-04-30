# 0001 — Single-region only

- **Status:** Superseded in part by [0004](./0004-multi-region-read-replicas.md)
  for cross-region read replicas. The cross-region 2PC / strong-consistency
  portion remains **Accepted** and deferred.
- **Date:** 2026-04-30
- **Tags:** scope, architecture, multi-region, deferred

## Context

The build prompt's Part 6 lists "rebuilding Cockroach/Aurora" as the
single most predictable way Basin dies. Multi-region active-active is
the gateway drug to that drift: every customer eventually asks for it,
every architectural decision starts to bend around future cross-region
2PC, and the team ends up shipping nothing usable for two years while
trying to make eventual consistency feel like strong consistency.

The wedge customer (multi-tenant SaaS with audit-log workloads) is
typically *single-region for compliance reasons* — GDPR, US data
residency, BYO-bucket tenants. A multi-region story is not on their
critical path until they cross the threshold of "enterprise customers
in Asia-Pacific are demanding sub-100ms reads from Sydney." That is a
real threshold, but it is far past the threshold of "any wedge customer
exists at all."

## Decision

Basin is single-region. We do not implement:

- Cross-region replication beyond what S3 cross-region replication
  provides for free (eventual, async, read-only).
- Cross-region transactions or 2PC.
- A globally-consistent catalog. The catalog is per-region.
- Active-active multi-master writes.

Customers who need multi-region today should not pick Basin.

## Consequences

**Positive**

- The catalog stays simple (one Postgres-backed REST catalog per
  region, no replication state machine to debug).
- The WAL stays simple (Raft groups are regional).
- The team can focus on closing the wedge's actual gaps (extended
  pgwire protocol, durable catalog, ORM compat) rather than burning
  six months on consistency models.
- We can quote multi-region as a paid expansion later without having
  shipped anything broken.

**Negative**

- Some prospects will pass on Basin specifically because of this.
  Every loss should be logged in the lost-deal tracker so we can
  watch the trigger conditions.
- "Eventual consistency for cross-region reads" is a refactor, not a
  feature toggle, when we eventually do build it.

**Mitigations**

- Architectural compatibility (below) keeps the future build cheap.
- The lost-deal tracker tells us when the trigger has fired in
  aggregate vs. one squeaky prospect.

## Architectural compatibility

Single-region is a current scope choice, not a design assumption baked
into the types. The following pieces preserve future option value:

- The storage layout `/tenants/{tenant_id}/...` is region-agnostic.
  S3 cross-region replication "just works" against it.
- The WAL is keyed by `(tenant_id, partition_key)`; partition keys
  can carry a region tag in the future without rewriting the WAL.
- The catalog client trait (`basin-catalog::Catalog`) does not assume
  a single instance — a future regional catalog implementation slots
  into the same trait.
- Tenant IDs are ULIDs, not region-namespaced. We can prepend a region
  in the prefix later without an ID migration.

The single concrete thing we should *not* do meanwhile: bake
single-region assumptions into bucket policies that key off the bucket
itself rather than the prefix. Already enforced by the existing
per-tenant prefix layout.

## Trigger to reconsider

We write a successor ADR when **one** of:

1. A prospect signs a 12-month contract worth ≥ $50k ARR contingent
   on multi-region active-active reads (eventual writes acceptable),
   AND
2. The contract terms include a delivery window that allows ~3 months
   of focused engineering after signing.

A single prospect at a smaller value, or a vague "we'd love it
someday," is *not* the trigger. Log them in the lost-deal tracker but
do not start the work.

## Alternatives considered

- **Build it now, defensively.** Rejected: every architectural decision
  bends around it, the project ships nothing usable for years, and the
  brief warns explicitly against this drift.
- **Hot-standby read replicas in another region.** Rejected: this is
  ~70% of the engineering cost of full multi-region but only ~20% of
  the customer-facing value, and S3 CRR already gets us most of the
  same data-durability benefits for free.
- **Federated catalogs.** Rejected as a near-term project: too much
  consistency surface area for a feature no current customer pays for.
