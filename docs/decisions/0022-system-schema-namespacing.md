---
title: "ADR 0022 — System-schema namespacing (reserved schemas first-class; user schemas stay flat)"
nav_section: decisions
sidebar_position: 22
summary: "Make the system namespaces (auth, storage, cron, net, realtime, public, pg_catalog, information_schema) real reserved schemas with honest (schema, table) keying + introspection + search_path. User-defined schemas stay flat-aliased to public — projects already own the tenancy/isolation axis, so arbitrary user schemas are a redundant second isolation boundary with ~zero wedge benefit."
tags: [catalog, schema, baas, oss, pg-compat]
---

# 0022 — System-schema namespacing

- **Status:** Accepted, 2026-05-20.
- **Tags:** catalog, schema, pg-compat, baas
- **Supersedes:** none
- **Cross-references:**
  [ADR 0012 (new-SaaS wedge / gave up "drop in any PG schema")](./0012-change-event-primitive.md),
  [ADR 0013 (auth per-project schema)](./0013-auth-per-project-schema.md),
  [ADR 0021 (object storage — storage.objects)](./0021-object-storage.md),
  Phase 5.6 (RLS), Phase 5.11.M (information_schema / pg_catalog).

## Context

### What a SQL schema was originally for
The SQL-92 standard defines `catalog.schema.table` and a schema as *"a named
collection of database objects owned by a single authorization identifier."*
The load-bearing phrase is **owned by a single authorization identifier** —
`CREATE SCHEMA <name> AUTHORIZATION <owner>` made the owner core, not optional.
The original problem schemas solved: many users sharing **one** database, each
needing a private object namespace so `alice.orders` and `bob.orders` don't
collide, plus a shared `public`. Postgres's still-default `search_path` of
`"$user", public` is the fossil that encodes this original per-user-namespace
intent. Postgres added schemas in 7.3 (2002) as a *lightweight* intra-database
namespace to fill the gap between "one flat database" and "separate databases
you can't join across."

### How schema usage drifted from intent
- **Schema-per-tenant multi-tenancy** — emergent, never the design intent.
- **Logical grouping** (`sales.*`, `billing.*`) — a preference; a naming
  convention (`sales_orders`) is equivalent.
- **System / extension namespacing** (`pg_catalog`, `information_schema`,
  extension schemas; Supabase's `auth` / `storage` / `extensions` / `graphql`)
  — the healthiest surviving use.
- **Per-user namespaces** (the *original* purpose) — effectively extinct.

### Where Basin stands today
Basin's isolation boundary is the **project prefix**
(`/projects/{ulid}/tables/{table}/…`). Within a project the catalog is a single
flat namespace. `CREATE SCHEMA` / `DROP SCHEMA` / `ALTER SCHEMA RENAME` /
`SET search_path` are accepted but are a **session-tracked shim**: a qualified
`myschema.t` resolves to the bare `t`, and `search_path` is stored but never
consulted at resolution. So a "schema" is a label, not a container.

Meanwhile Basin already exposes **schema-shaped system APIs** — `auth.uid()`,
`auth.users`, `storage.objects`, `storage.buckets`, `cron.job`,
`net._http_response`, realtime — and the SDK + RLS policies + PG tooling all
*expect* those to be real schema-qualified objects. Under the hood they are
faked with name-prefix hacks and reserved names:

| Surface | Faked as |
|---|---|
| auth | `basin_auth_<table>` name-prefix (ADR 0013) |
| cron | `cron.job` → reserved per-project table |
| net | `_net_http_response` name-prefix |
| storage (blobs) | `storage.buckets` / `storage.objects` (basin-blob, ADR 0021) |

This is the impedance mismatch: the system *pretends* to have schemas, but the
catalog has no notion of one, so each subsystem reinvents namespacing via
strings.

## Decision

### User-defined schemas: WON'T-DO (flat model is correct for the wedge)
We will **not** build full Postgres schema isolation for user data. Rationale:

1. **Projects already own the namespace.** The thing schemas were *originally*
   for — an isolated, owner-scoped namespace inside a shared system — is exactly
   what a Basin **project** is, promoted to a stronger primitive (hard
   storage-prefix isolation, not a soft naming convention). Projects are
   "schemas done right for the multi-tenant era."
2. **A second isolation axis is a liability, not a feature.** Basin's #1
   invariant is "one leaked row across projects and the project dies." A second
   namespacing axis is a second boundary to get RLS-scoping right against and a
   second place to leak — for a need projects already serve.
3. **It is more complex, but NOT meaningfully less efficient.** A schema is a
   metadata/naming concern: reads/writes/scans don't slow down, storage cost is
   nil, resolution is a few cached string compares. The real cost of user
   schemas is engineering complexity + correctness surface, paid for ~zero
   wedge benefit. (In Postgres schemas are essentially free — Basin can skip
   user schemas not because they're slow but because projects make them
   redundant.)
4. ADR 0012 already gave up "drop in any legacy PG schema unchanged" — Basin's
   wedge is new SaaS, where project = tenant and RLS = access control.

Concretely: a user `CREATE SCHEMA myapp` continues to be accepted and
**aliased to `public`** (flat). `myapp.t` and `public.t` are the same table.
This is a documented limitation, not a silent collision — see "Consequences".

### System schemas: MAKE THEM REAL (the bounded, high-value slice)
Introduce a fixed, **reserved** set of first-class schemas with honest
`(schema, table)` catalog keying *for these namespaces only*:

`public` · `auth` · `storage` · `cron` · `net` · `realtime` ·
`pg_catalog` · `information_schema`

- **Catalog**: key system-namespace objects by `(project, schema, table)`.
  `public` is the default schema; unqualified names resolve to `public`.
  The reserved schema list is closed (not user-extensible in v1).
- **Replace the prefix hacks** with the real mechanism: `auth.users` (not
  `basin_auth_users`), `net._http_response` (not `_net_http_response`),
  `cron.job`, `storage.objects` / `storage.buckets` become genuinely
  schema-qualified catalog entries. Migration shims keep old physical names
  readable during transition (see Consequences).
- **Resolution**: `search_path` becomes real for the reserved set — qualified
  `auth.users` always hits the `auth` schema; unqualified names walk
  `search_path` (default `public`), first match wins, with `pg_catalog`
  implicitly first as Postgres does.
- **Introspection is honest**: `information_schema.schemata` /
  `.tables.table_schema` and `pg_catalog.pg_namespace` / `pg_class.relnamespace`
  report real per-schema membership for the reserved schemas, so PG tooling and
  the SDK see `auth.users`, `storage.objects` etc. where they expect them.
- **RLS scoping**: policies `ON storage.objects` / `ON auth.*` are scoped by
  `(project, schema, table)` — no change to the per-project isolation boundary,
  which remains the hard one.

### Explicitly out of scope
- User-defined schema isolation (stays flat-aliased to `public`).
- `CREATE SCHEMA <arbitrary>` producing a real container (still accepted, still
  aliased to `public`).
- Cross-schema foreign keys / cross-schema ownership transfer.
- Schema-level GRANT (Basin is JWT/RLS, not role/grant based — see ADR 0005).

## Consequences

**Positive**
- Removes the scattered prefix-hack namespacing (auth/net/cron) in favour of one
  uniform mechanism — net complexity for the *system* namespaces may *drop*.
- `auth.users`, `storage.objects`, `cron.job`, `net._http_response` become real
  schema-qualified objects → Supabase-shaped SDK + PG introspection tooling
  (pgAdmin, Prisma, PostgREST) work against the schemas they already assume.
- The per-project isolation boundary is untouched and remains the only
  hard one — the reserved schemas live *inside* a project.

**Negative / risks**
- **Migration**: existing physical table names (`basin_auth_users`,
  `_net_http_response`, flat `public` tables) must remain readable. Ship a
  rename/alias migration with a back-compat read path; this is the
  correctness-sensitive part and is gated behind tests.
- `search_path` becomes load-bearing — resolution bugs now produce wrong-table,
  not just cosmetic. Mitigated by: reserved-schema list is closed + small, and
  unqualified resolution defaults to `public` exactly as today.
- The reserved-schema list is a fixed enum; adding a future system schema is a
  code change, not user DDL. Acceptable for a closed system set.

## Implementation (Phase 5.18 — system-schema namespacing)

Decomposed into file-scope-disjoint slices so they can land independently:

- **5.18.A — Catalog `(schema, table)` keying for reserved schemas.** Add a
  `schema` dimension to catalog object identity for the reserved set; `public`
  default; closed reserved-schema enum. InMemory + Postgres impls + round-trip
  tests. Back-compat: unqualified + `public.` resolve identically to today.
- **5.18.B — Real `search_path` + qualified-name resolution.** Resolver walks
  `search_path` (default `["pg_catalog", "$publicish"]`-style → effectively
  `public`); qualified reserved-schema names bind directly; user schemas alias
  to `public`. Replaces the cosmetic shim in `schema_ddl.rs`.
- **5.18.C — Migrate system namespaces off prefix hacks.** auth
  (`basin_auth_*` → `auth.*`), net (`_net_http_response` → `net.*`), cron
  (`cron.job` real), storage (already `storage.*` shaped — formalize). Each with
  a back-compat read/alias path + migration test. **Must not regress** the
  existing auth/storage/cron/net suites.
- **5.18.D — Honest introspection.** `information_schema.schemata` +
  `table_schema`, `pg_catalog.pg_namespace` + `pg_class.relnamespace` report
  real reserved-schema membership. Extend the Phase 5.11.M views.
- **5.18.E — Differential + tooling test.** PG-tooling introspection
  (pgAdmin/Prisma/PostgREST) sees `auth.users` / `storage.objects` in the right
  schema; RLS-on-`storage.objects` still scopes correctly; user
  `CREATE SCHEMA x` + `x.t` still aliases to `public.t` (documented).

Sequencing: **A → B** (B needs the keyed catalog), **A → C** (C migrates onto
the keyed catalog), **D** after A, **E** last (depends on A–D).
