# 0013 — Auth per-tenant schema (remove loopback pgwire)

- **Status:** Accepted, 2026-05-11
- **Tags:** auth, architecture, loopback-removal, storage
- **Supersedes (partially):** 0005 architecture section only; 0005 scope/features unchanged

## Context

basin-auth v0.1 (ADR 0005) stores identity state in a reserved internal
tenant (`INTERNAL_AUTH_TENANT_ID`) accessed via a loopback pgwire TCP
connection back to basin-server's own listener. This creates a circular
startup dependency: auth needs pgwire running to connect, but pgwire needs
auth to resolve tenant credentials. The workaround — `DeferredAuthResolver`,
`OnceLock`, and a 5-second `wait_for_pgwire_accept` polling loop — is
fragile and adds ~400 lines of bootstrap ceremony.

Supabase solves an identical problem by giving every project its own `auth`
schema inside that project's Postgres database. GoTrue connects directly to
that database with a service role — never through PostgREST or any
application-level API layer. The tenant is known from the connection target
(project URL), not discovered via a cross-project lookup.

## Decision

Auth tables (`basin_auth_users`, `basin_auth_refresh_tokens`,
`basin_auth_email_tokens`, etc.) live in each tenant's own storage
namespace under the `basin_auth` schema prefix, provisioned when the
tenant is created. The auth service accesses them via
`Engine::open_session_as(tenant_id, "basin_auth_service")` — in-process,
no TCP connection.

**Self-routing credentials.** `pgwire_user` changes format from
`tenant_<random_hex>` to `{tenant_id}_{random_hex}` (26-char ULID prefix).
Parsing the first 26 characters gives the tenant_id directly; the auth
service opens that tenant's schema without any global lookup table. API
keys embed the same tenant prefix. JWT validation is unchanged (tenant_id
already in claims).

**`AuthStore` trait.** `basin-auth` defines a high-level `AuthStore` trait
and a `PostgresAuthStore` implementation (for operators who want auth state
on external Postgres). `EngineAuthStore` is defined in `basin-server`,
wrapping `TenantSession`, and passed into `AuthService::with_store`. This
keeps basin-auth free of a basin-engine dependency.

**Startup order becomes:** engine → auth (with EngineAuthStore) →
StackedTenantResolver → pgwire. No deferred slots, no polling.

## Consequences

**Positive**
- Eliminates the loopback circular dependency and all its workarounds.
- Auth state replicates with tenant storage automatically — no separate
  replication path needed.
- Open source deployments need zero external dependencies for auth.
- `BASIN_AUTH_CATALOG_DSN` becomes an optional operator escape hatch
  (separate-blast-radius Postgres), not the default path.

**Negative**
- Existing `pgwire_user` handles change format — deployed credentials
  need a one-time migration. A compatibility shim reads old-format names
  during the transition window.
- Auth SQL now runs through the engine's DataFusion path rather than
  direct tokio-postgres. Point lookups are marginally slower (~1 ms vs
  ~0.2 ms) at low load; negligible at auth traffic volumes.

## Alternatives considered

- **Keep loopback, fix startup ordering.** Doesn't remove the TCP hop or
  the circular dependency — just hides it better.
- **Embedded SQLite.** Adds a file-format dependency and doesn't leverage
  Basin's own storage or replication model.
- **External Postgres always required.** Blocks zero-config open source
  deployments; adds ops burden for self-hosters.
