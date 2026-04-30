# 0003 — Native vector search (supersedes part of 0002)

- **Status:** Accepted
- **Date:** 2026-04-30
- **Tags:** scope, sql, vector, ai, supersedes-0002

## Context

The build prompt names "AI agent platforms" as one of the three target
wedge customers. Every modern agent platform stores embeddings and does
nearest-neighbor retrieval; the de-facto Postgres-native answer is
`pg_vector`. ADR [0002](./0002-no-postgres-extensions.md) defers
Postgres extension support broadly, on the grounds that "supporting an
extension" means embedding Postgres or re-implementing C internals.

Vector search is the one place that argument is actually weaker: the
underlying primitives (HNSW indexes, dot/cosine/L2 distance) are
algorithm-shaped, not Postgres-shaped. Mature Rust crates exist
(`instant-distance`, `hnsw_rs`, `usearch`). DataFusion already supports
fixed-size-list arrays as a native Arrow type. Implementing this as a
*native Basin feature* — not as `pg_vector` extension compatibility —
is on the order of 3-4 weeks, not multiple years.

It is also directly wedge-aligned. The AI-agent-platform customer is
named in the brief; embeddings + nearest-neighbor are what they will
ask for in their first month.

## Decision

Basin will ship **native vector search** as a first-class data type
and operator set, not as `pg_vector` compatibility:

1. A `vector(N)` column type, backed by Arrow's `FixedSizeList<Float32>`
   under the hood. Users declare it as `vector(N)` (mirroring `pg_vector`
   syntax for ergonomics) but it is a Basin native type, not an
   extension-installed type.
2. Distance operators / functions matching `pg_vector` semantics:
   - `<->`  Euclidean (L2) distance
   - `<#>`  negative dot product
   - `<=>`  cosine distance
3. An `ORDER BY embedding <-> $1 LIMIT k` pattern that the engine
   recognises and routes through an HNSW index when one exists.
4. HNSW index support in `basin-storage` (write side) and a
   per-`(tenant, table)` HNSW segment file format under
   `/tenants/{id}/tables/{name}/index/{column}.hnsw`. Read side: the
   engine consults the index when the planner picks the
   nearest-neighbor path.
5. Brute-force fallback when no HNSW index exists, so correctness is
   not gated on the index build.

We are **not** committing to pg_vector wire-protocol compatibility for
its non-`vector` features (e.g. `ivfflat`, `sparsevec`). HNSW is the
chosen ANN algorithm; we revisit when a wedge customer specifically
needs IVF.

## Consequences

**Positive**

- An agent-platform customer can build a working RAG pipeline on
  Basin alone, with the per-tenant economics intact.
- The dashboard gets a new comparison axis: "vector queries per dollar"
  vs Pinecone, Weaviate, pg_vector-on-Neon, pg_vector-on-RDS.
- Extends the wedge naturally: per-tenant vector indexes, cheap
  retention of historical embeddings on object storage.

**Negative**

- The engine grows a planner extension (recognising the
  nearest-neighbor pattern). Modest complexity.
- Index maintenance under writes is not free; HNSW inserts mutate the
  index. The compactor will eventually rebuild HNSW from scratch
  per Parquet segment.

**Mitigations**

- Index implementation lives in a small new crate `basin-vector` so
  the rest of the engine stays clean.
- The native type uses Arrow primitives, so DataFusion's planner and
  query pipeline don't need a new type system.

## Architectural compatibility

This decision does not change `0002`'s overall stance against
implementing Postgres extensions. We are *implementing a feature that
happens to overlap with what `pg_vector` does*, not implementing
`pg_vector`. The same will apply if and when we judge another
extension's behavior to be wedge-aligned and algorithm-shaped (rare).

## Trigger to expand to other vector features

We add IVF-flat indexes when a wedge customer specifically needs them
(typically for billion-scale corpora) AND signs a contract. HNSW is
fine for the first 1B vectors per tenant.

## Alternatives considered

- **Embed `pg_vector` itself.** Rejected per `0002`: requires running
  Postgres.
- **Sidecar Pinecone / Weaviate.** Workable, but breaks the wedge's
  "one bill, one substrate" promise. Customers who do this are paying
  twice for storage.
- **Defer until customer asks.** Rejected: this is the rare deferred
  feature whose absence will lose the AI-agent-platform conversation
  before it starts. Vector search is table stakes for that segment.
