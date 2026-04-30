# 0002 — No Postgres extension support (PostGIS, pg_vector, etc.)

- **Status:** Superseded by [0003](./0003-native-vector-search.md) for vector
  search specifically. Still **Accepted** for PostGIS, TimescaleDB, Citus,
  and the rest of the extension landscape.
- **Date:** 2026-04-30
- **Tags:** scope, sql, compatibility, deferred

## Context

A flat-out customer request, recurring in every Postgres-adjacent
project's first year, is "does it support PostGIS / pg_vector /
pgcrypto / TimescaleDB / pg_partman / [any other Postgres extension]?"

The honest answer is that **Postgres extensions are not a feature** —
they are dynamically-loaded C code linked into the Postgres process,
that hook the planner, executor, and storage layers via Postgres's
internal C ABIs. There is no way to "support an extension" without
running Postgres. Basin's executor is DataFusion (Rust, Apache Arrow
native). Adding extension support means either:

1. Re-implementing each extension's behavior in Rust against Arrow
   types (multi-year per extension, and you'd be chasing upstream
   changes forever), or
2. Embedding a real Postgres process behind the pgwire frontend and
   shovelling rows back and forth (a ~year-long project that gives up
   most of Basin's architectural advantages).

Both are "rebuild Aurora" trajectories. The brief warns explicitly
against this.

## Decision

Basin does not support Postgres extensions and will not, in the
foreseeable future, implement them. This includes:

- PostGIS (geospatial)
- pg_vector (embeddings / vector search)
- pgcrypto, uuid-ossp, citext (cryptographic / text functions)
- TimescaleDB, Citus (time-series, sharding)
- pg_partman, pg_repack, pg_stat_statements (operational extensions)
- All other contrib and third-party extensions.

When a customer's primary tables need an extension, the recommended
pattern is **Postgres alongside Basin**:

- Basin holds the per-tenant transactional data, the audit logs, the
  cheap-retention tables.
- Postgres (RDS, Neon, self-hosted) holds the extension-dependent
  tables.
- The application writes to both and joins in code, or via a thin
  routing layer.

This is a real recommendation, not a brush-off. Several wedge
customers will end up with this hybrid setup and they will be fine.

## Consequences

**Positive**

- Engineering scope stays bounded. We do not build a feature with
  unbounded surface area.
- Sales conversations are honest. "We don't do extensions" up front
  beats "kind of, sort of" later.
- Customers who *only* need extension-heavy tables select a different
  database, which is correct for them and for us.

**Negative**

- Apps that have already standardized on pg_vector or PostGIS for
  *all* their data cannot fully migrate to Basin. That is fine. They
  are not the wedge customer.
- "We use UUIDs everywhere" via `uuid-ossp` works through standard
  client libraries (the client generates the UUID); the extension is
  not needed. We should document this so customers don't think they
  need the extension when they don't.

**Mitigations**

- `CAPABILITIES.md` is explicit about this from day one.
- Common extension functions whose behavior is trivial in pure SQL
  (e.g., `gen_random_uuid()`, `digest()` for SHA-256) are candidates
  for built-in implementation in `basin-engine` if they are blockers
  for ORM compatibility — but this is implemented case-by-case via
  the engine's function registry, not as "extension support."

## Architectural compatibility

We make no preparation for extension support. There is no hook, no
trait, no pluggable C-ABI loader. If a future ADR overturns this
decision, that ADR will own the architectural cost.

## Trigger to reconsider

We write a successor ADR when **all** of:

1. Three or more wedge customers (multi-tenant SaaS / audit-heavy
   fintech / agent platforms) name the same Postgres extension as a
   blocker, AND
2. Their use case cannot be solved by the sidecar-Postgres pattern
   above — i.e., they need cross-table joins between Basin tables and
   extension-using tables in the same query, AND
3. A clear reason exists why a separate purpose-built database (e.g.
   pgvector for embeddings, PostGIS for geo) is unacceptable.

The same extension being requested by three independent wedge
customers is the actual signal. One loud prospect is not.

## Alternatives considered

- **Embed Postgres behind pgwire.** Rejected: gives up the bucket-
  native architecture, doubles the operational surface, and ends up
  shipping a worse Postgres. A team that wants to ship Postgres should
  ship Postgres.
- **Re-implement each extension in Rust on Arrow.** Rejected: each is
  multi-year, the upstream changes constantly, and the result lags
  the canonical extension by months at all times.
- **Provide a UDF system that customers use to bring their own
  function logic in WASM/Lua.** Deferred entirely. Interesting, but
  not a wedge feature; revisit only if a wedge customer specifically
  pays for it.
