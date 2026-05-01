# 0005 — Auth system (basin-auth)

- **Status:** Accepted, deferred. Scope is locked here; build is gated on
  the trigger below.
- **Date:** 2026-05-01
- **Tags:** scope, auth, smtp, defers-to-trigger, related-to-0006

## Context

The previous direction ([CAPABILITIES.md](../../CAPABILITIES.md), the
discussion that produced [ADR 0004](./0004-multi-region-read-replicas.md))
was: stay focused on the database-layer wedge; integrate alongside
Supabase / Neon for auth + functions. That stays the recommended GTM.

But we want the design locked so when a customer **does** ask for "Basin
without needing a separate auth provider," we already know what we'd
ship and at what cost. This ADR scopes the auth surface; ADR 0006 scopes
the REST layer that depends on it.

The trigger to actually *build* basin-auth is at the bottom. Until then
this ADR exists so engineers don't accidentally bake auth-shaped
assumptions into other crates.

## Decision

Basin will ship a small, opinionated auth crate, **`basin-auth`**, with:

### Core capabilities (v1)

1. **Per-tenant user identity.** Each tenant has its own `auth.users`
   table (rows scoped by the standard tenant prefix). A user belongs
   to exactly one tenant; no cross-tenant identity. The `tenant_id`
   is a JWT claim.
2. **Email + password sign-up and sign-in** with bcrypt password
   hashing (cost factor 12 by default).
3. **Email verification** (one-time token, 24-hour TTL).
4. **Password reset** (one-time token, 1-hour TTL, sent via SMTP).
5. **Magic-link sign-in** (one-time token, 15-minute TTL, sent via SMTP).
6. **JWT issuance** signed HS256 with a tenant-scoped secret. Claims:
   `tenant_id`, `user_id`, `email`, `roles[]`, `iat`, `exp`.
7. **Refresh tokens** (opaque, stored in `auth.refresh_tokens`,
   30-day TTL by default). Rotation on use.
8. **Sign-out** (revokes the refresh token).

### SMTP config (required at startup)

`basin-auth` **does not start without SMTP credentials.** The auth
crate's `AuthConfig::from_env` enforces this. There is no "no-email"
mode, because every flow above except sign-in requires email delivery.

Required env vars:

```text
BASIN_AUTH_ENABLED=1                          # off by default
BASIN_AUTH_JWT_SECRET=<32+ bytes, hex>        # platform-level (or per-tenant override)
BASIN_AUTH_TOKEN_TTL=3600                     # access token, default 1h
BASIN_AUTH_REFRESH_TTL=2592000                # refresh token, default 30d

BASIN_AUTH_SMTP_HOST=smtp.example.com
BASIN_AUTH_SMTP_PORT=587
BASIN_AUTH_SMTP_USERNAME=<smtp user>
BASIN_AUTH_SMTP_PASSWORD=<smtp password>
BASIN_AUTH_SMTP_FROM=noreply@example.com
BASIN_AUTH_SMTP_TLS=starttls                  # starttls | implicit | none (dev only)
```

Optional:

```text
BASIN_AUTH_RATE_LIMIT_PER_IP_PER_MIN=20       # signup + signin + reset combined
BASIN_AUTH_PASSWORD_MIN_LEN=10
BASIN_AUTH_BCRYPT_COST=12
BASIN_AUTH_EMAIL_FROM_NAME="Basin"
```

A missing required SMTP variable is a fatal startup error, not a
warning. This is the single most load-bearing rule in this ADR — half-
configured email is the source of every "you can't sign in" support
ticket in production auth systems.

### What's *out* of v1

- Multi-factor auth (TOTP, WebAuthn) — defer to v2.
- OAuth / social providers (Google, GitHub) — defer to v2; HMAC + email
  is sufficient for the wedge customer's first deploys.
- SAML / SSO — enterprise-only; defer until paying enterprise customer.
- Per-tenant SMTP override — defer; one platform-level SMTP first.
- Org / team / role hierarchies beyond a flat `roles[]` claim.
- Anonymous / guest users.
- Phone-number / SMS auth.
- Federated identity across tenants.

### Architecture

A new crate `basin-auth` with these modules:

```
crates/basin-auth/
  src/
    lib.rs        — public API: AuthConfig, AuthService, Tokens, errors
    smtp.rs       — lettre-based SMTP sender with template renderer
    jwt.rs        — HS256 issue + verify + claims
    password.rs   — bcrypt hash + verify
    flows.rs      — signup, signin, verify, reset, magic-link state machines
    rate_limit.rs — token-bucket per IP + per email
    schema.sql    — auth.users, auth.refresh_tokens, auth.email_tokens DDL
```

`AuthService::new(AuthConfig, BasinPool)` returns a handle. The service
runs the schema migration (idempotent `CREATE TABLE IF NOT EXISTS`
against the auth namespace) on startup.

Integration with the rest of Basin:

- `basin-router::TenantResolver` gets a JWT-aware implementation
  (`JwtTenantResolver`) that decodes the bearer token and sets
  `tenant_id` from its claim. Replaces (or stacks with) the existing
  `StaticTenantResolver`.
- `basin-engine::TenantSession` does not change — it still scopes
  every operation by `tenant_id`. The auth crate's only job is to map
  external credentials to that tenant id.
- The HTTP REST layer (ADR 0006) depends on this crate.

`basin-auth` does NOT speak pgwire and is NOT consulted on every SQL
query — JWT decode happens once at connection accept; subsequent SQL
runs at native engine speed.

## Consequences

**Positive**

- Customers can ship a real product without picking a separate auth
  provider. The Supabase appeal of "everything's here" applies for
  the (small) subset of customers who genuinely want everything in one
  process.
- `basin-rest` (ADR 0006) becomes feasible — REST APIs need JWT auth,
  and we control both ends.
- Scope is bounded: auth is an old, well-understood problem. We are not
  inventing anything; we are wiring lettre + bcrypt + jsonwebtoken +
  the catalog together.

**Negative**

- Adding SMTP as a hard dependency complicates deploys (you need an
  SMTP provider — SendGrid, Mailgun, Postmark, AWS SES). Documented
  upfront so customers don't get bitten.
- Auth is a security-sensitive surface. Every bug here is a CVE
  candidate. Allocate budget for an external security review before
  any v1 launch.
- Email deliverability is operationally annoying (DKIM, SPF, DMARC,
  reputation). We don't own this; we tell customers their SMTP
  provider does.

**Mitigations**

- We use battle-tested crates only: `lettre` for SMTP, `jsonwebtoken`
  for JWTs, `bcrypt` for passwords, `governor` for rate limiting. No
  hand-rolled crypto.
- Every flow that sends email is rate-limited per IP and per email
  address. Default limits are conservative; tuneable via env.
- Token TTLs are short (15 min for magic links, 1h for verification,
  1h for password reset). Stale tokens cannot be replayed.
- The JWT signing secret rotates on a schedule (operator-driven, not
  automatic in v1). Old secrets stay valid until their TTL expires.

## Architectural compatibility

`basin-auth` plugs into `basin-router` via the existing
`TenantResolver` trait. There is no change to `basin-engine`,
`basin-storage`, `basin-catalog`, `basin-wal`, or `basin-shard`.

The auth crate's own state (users, refresh tokens, email tokens) lives
in tenant-scoped tables in the same Iceberg-style catalog. It is
durable through `PostgresCatalog` (the same backend the production
deploy already uses for table metadata).

## Trigger to start building

We start building basin-auth when **one** of:

1. A customer signs a **≥ $50k ARR** contract contingent on Basin
   shipping native auth (so the customer doesn't need Supabase Auth
   alongside), AND the contract terms allow ~6–8 weeks of focused
   engineering after signing.
2. We pivot the GTM from "alongside Supabase" to "Supabase
   alternative" deliberately (a separate decision, would supersede the
   recommendation in earlier ADRs and the README's positioning).

**Single squeaky prospect is not the trigger.** Auth is too big a
surface to start on speculation.

## Estimated effort once triggered

| Component | Effort |
|---|---|
| schema + signup/signin/verify/reset state machines | 1 week |
| SMTP integration with templated emails (~6 templates) | 0.5 week |
| JWT + refresh token issue/verify/rotate | 0.5 week |
| Rate limiting + abuse protection | 0.5 week |
| `JwtTenantResolver` integration into router | 0.5 week |
| Tests (signup, signin, expired tokens, replay attempts, rate-limit) | 1 week |
| External security review (CVE review, threat model, penetration) | 1–2 weeks (vendor) |
| **Total** | **~6–8 weeks calendar** |

## Alternatives considered

- **No auth at all.** Status quo. Fine for the database-layer wedge; a
  hard "no" if the BaaS strategy is ever picked.
- **Embed an existing auth library** (e.g. ory/kratos, keycloak,
  zitadel). Rejected — these are heavyweight services with their own
  data stores; running another stateful service alongside Basin
  doubles the operational surface. Lettre + jsonwebtoken + bcrypt is
  ~400 lines of glue against Basin's catalog.
- **Per-tenant SMTP from day one.** Rejected for v1 — adds a
  configuration surface no one has yet asked for. Easy to add later.
- **Magic-link only (no passwords).** Considered. Rejected because
  every customer support call about "I can't sign in to my own
  account" is now an email-delivery investigation. Passwords are
  unfashionable but they let people own their own access.
- **Cookies vs. bearer tokens.** Bearer tokens. Cookies are fine for
  browsers but cookies don't work for the SDK / CLI / mobile clients
  the wedge customer's apps use. Bearer tokens are universal and
  stateless from Basin's side.
