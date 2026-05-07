# Basin — cloud-platform roadmap

The customer-facing **platform** that wraps Basin core: identity, REST,
edge compute, billing, BYO-bucket / BYO-key, the customer dashboard.

Core database features live in [`TASK.md`](./TASK.md) (the build plan)
and [`CAPABILITIES.md`](./CAPABILITIES.md) (the public-facing what-it-does
page). This file owns everything *above* the SQL/pgwire interface — the
plumbing that turns Basin from "great DB engine" into "Postgres-as-a-Service
that actually multi-tenants cheaply."

Legend: `[ ]` open · `[~]` in progress · `[x]` done · `[-]` deferred / out of scope

---

## Why a separate roadmap

Basin's architectural moat is the **core DB**: pgwire-on-Parquet-on-object-store,
multi-tenant by structure, scope-disciplined. The cloud platform is a
*product layer* on top — the same engine pairs with very different
cloud-platform shapes (a hosted SaaS, a self-hosted enterprise build, a
BYO-everything appliance). Mixing them in the same roadmap blurs the
boundary the team has been deliberate about. This file keeps the
platform work visible without polluting the core DB build plan.

---

## Identity (basin-auth) — **v0.1 shipped**

Status: shipped 2026-05-01 by founder direction. ADR 0005.

- [x] `basin-auth` crate — Postgres-backed user store, bcrypt password
      hashing, JWT issuance + verification (HS256), role/membership
      tables, `current_user`-aware tenant resolution.
- [x] `JwtTenantResolver` auto-mounts on pgwire when `BASIN_AUTH_ENABLED=1`
      (`services/basin-server/src/main.rs:298-307`); JWT primary, static
      `BASIN_TENANTS` map as fallback so existing demos keep working.
- [x] REST endpoints for issue / verify / refresh.
- [ ] OIDC / Google / GitHub social login providers
- [ ] SCIM provisioning for enterprise customers
- [ ] Per-org SAML SSO
- [ ] Per-tenant secret rotation policy
- [ ] Audit log of admin actions (who-did-what for the dashboard)

## REST API (basin-rest) — **v0.1 shipped**

Status: shipped 2026-05-01 alongside basin-auth. ADR 0006. Requires
`BASIN_AUTH_ENABLED=1` per ADR.

- [x] CRUD endpoints over the JWT-resolved tenant
- [ ] `PATCH` (currently 501; awaits a richer engine UPDATE shape — note
      that the underlying engine `UPDATE`/`DELETE` shipped with Iceberg
      copy-on-write, but the REST `PATCH` codec is not yet wired)
- [ ] Pagination cursors instead of `LIMIT`/`OFFSET`
- [ ] Streaming responses for large result sets
- [ ] OpenAPI / Swagger schema generation

## V8 edge functions — **not started**

Customer-supplied JavaScript that runs in a sandboxed V8 isolate per
request, with capability-bounded access to:
- the calling tenant's tables (read/write through the engine's
  `TenantSession`, never directly against storage)
- a small allowlisted HTTP egress (reuse `basin-net`'s URL allowlist +
  rate limiter)
- a key-value scratch store keyed by tenant + function name

The shape mirrors Supabase Edge Functions / Cloudflare Workers but
isolates per-tenant via the same prefix isolation Basin already enforces
at the storage layer.

- [ ] Pick the V8 binding crate (`rusty_v8` vs `deno_core`)
- [ ] Function manifest schema (entry point, allowed origins, kv keys)
- [ ] Tenant-scoped runtime: `SessionContext` injected as a JS global
- [ ] HTTP egress proxy that forwards to `basin-net` (allowlist + rate
      limit + timeout already enforced; just need the bridge)
- [ ] CPU + memory caps per invocation (V8 heap limits + isolate
      timeouts)
- [ ] Cold-start budget < 50 ms (must beat AWS Lambda's 100-500 ms cold)
- [ ] Per-function deployment via REST: upload bundle, hot-reload on
      next invocation
- [ ] Logs + traces piped into the existing OTEL pipeline
- [ ] Pricing model integration (CPU-ms metering for Stripe billing)

Decision points:
- This is a **major scope expansion**. Only commit when the wedge
  customer interviews (Phase 0) confirm 2+ design partners specifically
  ask for it. Until then it's optional polish.
- V8 isolate sandboxing is well-trodden ground; the per-tenant
  capability binding is where the project-specific work lives.

## BYO-bucket — **planned**

Customer's own S3/R2 bucket with an IAM role that grants Basin write +
read access. Platform never holds the data; the customer can revoke
access and Basin loses the tenant cleanly.

- [ ] `TenantMetadata.byo_bucket: Option<S3Config>` on the tenant record
- [ ] `Storage` accepts a per-tenant override `ObjectStore` (the
      pluggable `dyn ObjectStore` makes this almost free at the storage
      layer; the work is the per-tenant resolution + secret handling)
- [ ] Onboarding flow: customer pastes IAM role ARN, Basin tries a
      probe write/read/delete, surfaces errors clearly
- [ ] Cleanup on tenant deletion: leave the customer's bucket intact;
      only delete Basin's prefix tree
- [ ] Cost telemetry routes to the *customer's* AWS bill, not Basin's
      egress dashboard

## BYO-key (KMS) — **planned**

Customer-managed encryption keys via AWS KMS / GCP KMS / Azure Key
Vault. Platform never sees plaintext beyond the per-request envelope.

- [ ] Envelope encryption at the storage write boundary: data key per
      file, wrapped by the customer's KMS CMK
- [ ] Key cache with explicit TTL (so revoking the CMK at the customer
      side actually stops decryption within minutes)
- [ ] Per-tenant choice of cloud KMS (AWS / GCP / Azure)
- [ ] Hardware-token attestation for the decryption agent (deferred —
      only needed for FedRAMP-class customers)

## Stripe billing — **planned**

Usage-based billing with the four meters that map to Basin's actual
costs: storage GB-month, S3 API ops, active-hours of a tenant's
shard-owner footprint, and (when V8 lands) CPU-ms.

- [ ] Per-tenant usage counters in the catalog (storage bytes, ops,
      active seconds)
- [ ] Daily roll-up job that posts to Stripe metered billing
- [ ] Customer-portal page reading the same counters for transparency
- [ ] Free-tier accounting (monthly reset)
- [ ] Overage alerts via the existing `basin-net` HTTP path (so the
      same rate limiter / allowlist applies)

## Customer dashboard — **planned**

Self-serve dashboard for customers to manage their own tenants, view
usage, run ad-hoc SQL, manage API keys. An empty placeholder directory
already exists at `services/dashboard/` (created in the initial
workspace scaffolding, no code yet).

- [ ] Tech choice: `astro` + `solid` for static pages with islands of
      interactivity (mirrors the existing benchmark dashboard, which is
      the *internal* dashboard at `benchmark/` and a different artefact)
- [ ] Read-only SQL console (talks pgwire over a websocket bridge)
- [ ] Tenant + user management UI (basin-auth admin endpoints)
- [ ] Usage charts (read straight from the same counters that feed
      Stripe)

## Control plane — **planned**

Backplane for the operator that fans tenant lifecycle commands
(create / suspend / migrate / delete) across the regional clusters,
talks to Stripe for billing webhooks, and serves the customer-portal
admin endpoints. An empty placeholder directory exists at
`services/control-plane/` (created in the initial workspace
scaffolding, no code yet).

- [ ] Tenant lifecycle API (idempotent create / suspend / resume /
      migrate / delete; emits events the dashboard consumes)
- [ ] Region directory: tenant ULID → home region; required before
      multi-region core-DB work (`TASK.md` Phase 6) can ship without
      manual DNS gymnastics
- [ ] Stripe webhook receiver (subscription created / cancelled /
      payment_failed → tenant state transitions)
- [ ] Audit log of every control-plane action (who, when, what,
      reverse-action) — fed to basin-auth's admin audit log
- [ ] Quotas: per-customer table-count, tenant-count, storage-GB caps
      that the core engine consults at write time
- [ ] gRPC + REST surfaces (REST for the dashboard, gRPC for
      service-to-service inside the cluster)

## Cross-cutting platform concerns

- [ ] Status page (uptime per region)
- [ ] Public-facing docs site (separate from the engineering docs in
      `docs/`)
- [ ] Marketing site / landing page
- [ ] Support inbox / on-call rotation
- [ ] Security review checklist before each platform feature ships

---

## What's *not* on this roadmap (and where it lives instead)

- **Pgwire / SQL surface, types, query planner, vector search, RLS,
  multi-tenancy primitives, storage tiering, caches, indexes,
  WAL/Raft, compactor, catalog, analytical engine** → all in
  [`TASK.md`](./TASK.md) (core DB).
- **Phase 0 customer interviews** → also `TASK.md`; the wedge validation
  applies to both core DB and platform shape.

---

*Last updated: 2026-05-07.*
