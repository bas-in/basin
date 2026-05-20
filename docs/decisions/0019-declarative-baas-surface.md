---
title: "ADR 0019 — Declarative BaaS surface: inbound webhooks + RPC mount"
nav_section: decisions
sidebar_position: 19
summary: "Two declarative primitives — CREATE INBOUND WEBHOOK and POST /rpc/<fn> — cover ~95% of edge-function use cases as SQL, with no V8/Deno runtime or language sandbox."
tags: [rest, baas, security, change-events]
---

# 0019 — Declarative BaaS surface: inbound webhooks + RPC mount

- **Status:** Proposed, 2026-05-19.
- **Tags:** rest, baas, security, change-events
- **Supersedes:** none
- **Cross-references:**
  [ADR 0006 (REST API layer)](./0006-rest-api-layer.md),
  [ADR 0012 (change-event primitive)](./0012-change-event-primitive.md),
  [ADR 0013 (auth per-project schema)](./0013-auth-per-project-schema.md),
  [ADR 0018 (subsystem feature flags)](./0018-subsystem-feature-flags.md).

## Context

The "BaaS gap" question — *can Basin cover the use cases customers
reach for edge functions for?* — resolves to a 7-row taxonomy. Six of
the seven rows are coverable with declarative SQL primitives. Basin
already ships four of the primitives needed. **Two are missing**:
inbound HTTP receivers and HTTP-mounted RPC over catalog functions.

The taxonomy and Basin's current coverage:

| # | BaaS use case | Coverage |
|---|---|---|
| 1 | Inbound webhook (Stripe / Resend / GitHub → DB) | **Missing — this ADR** |
| 2 | DB-event → outbound HTTP | ✅ Phase 5.11.I (`SUBSCRIBE WEBHOOK`, `basin-webhooks` crate shipped) |
| 3 | Scheduled jobs | `crates/basin-cron` scheduler ✅ shipped (5.8 v0.1); **SQL surface (`SELECT cron.schedule(…)`) tracked as TASK.md 5.8.A v0.2** — until then schedule via the Rust API. Same applies to `net.*` UDFs. |
| 4 | Compute-over-HTTP (dashboard data, parametric reads) | **Missing — this ADR** |
| 5 | Auth-triggered side effects (signup → profile + welcome email) | ✅ Phase 5.11.C reactors + 5.11.I webhooks |
| 6 | Image / file processing | Composes — outbound webhook to external service + inbound webhook for result |
| 7 | Multi-step orchestration (saga: charge → succeed / fail / refund) | Composes — status column + reactors + outbound + inbound webhooks |

V8/Deno edge functions remain explicitly out of scope (consistent with
ADR 0012's "new SaaS only" wedge call). Customers needing imperative
HTTP handler logic run that on their own app server. The Wasm UDF
escape hatch in Phase 5.11.J stays customer-gated as a last-resort
in-DB compute path for the ~5% of cases SQL can't express.

## Decision

**Ship two new declarative primitives. Both are config-row + SQL-body
shaped. Both reuse existing engine infrastructure. Both compose with
existing primitives so the BaaS surface becomes symmetric — Basin can
already emit HTTP via outbound webhooks; this ADR adds the receiving
side, plus an HTTP front door for catalog functions.**

### Primitive 1 — RPC mount (`POST /rpc/<fn>`)

Mount a single dynamic route in `basin-rest` that dispatches into
existing `LANGUAGE sql` functions (Phase 5.11.D, shipped) and
`LANGUAGE sql RETURNS TABLE` functions (Phase 5.11.E, shipped).

```http
POST /rest/v1/rpc/dashboard_for_user
Authorization: Bearer <jwt>
Content-Type: application/json

{ "user_id": 42, "since": "2026-01-01" }
```

```sql
-- Catalog-defined function (already supported by 5.11.D/E)
CREATE FUNCTION dashboard_for_user(user_id int, since date)
  RETURNS jsonb
  LANGUAGE sql
  AS $$
    SELECT jsonb_build_object(
      'orders', (SELECT count(*) FROM orders WHERE user_id = $1 AND created_at >= $2),
      'revenue_cents', (SELECT coalesce(sum(amount_cents), 0) FROM payments WHERE user_id = $1 AND created_at >= $2)
    )
  $$;
```

- Body: JSON object → bound to function parameters by name.
- Response: function return value serialised as JSON (scalar / object
  for `RETURNS`; array of objects for `RETURNS TABLE`).
- Auth: identical to existing `basin-rest` routes (ADR 0006 § flow) —
  JWT extracted, project scope applied, function resolved per project.
- RLS: function body executes under the caller's identity; existing
  per-project schema isolation (ADR 0013) and any row-level policies
  apply to statements inside the function.
- When Phase 5.11.J ships, the same route dispatches `LANGUAGE wasm`
  functions transparently — no second mount needed.

**Cost:** ~2-3 days assuming `basin-rest`'s router supports parametric
routes today (verified prior to this ADR's commit). Zero new
dependencies. Binary cost: negligible.

### Primitive 2 — Inbound webhook receivers (`CREATE INBOUND WEBHOOK`)

Inverse of `SUBSCRIBE WEBHOOK` (5.11.I). Mount an HTTP endpoint that
verifies a signature, parses a JSON body, and executes a SQL body
with the body bound to `payload`.

```sql
CREATE INBOUND WEBHOOK stripe_payment_events
  ON POST '/in/stripe/payments'
  WITH SECRET vault('STRIPE_WEBHOOK_SECRET')
  VERIFY HMAC SHA256
    HEADER 'Stripe-Signature'
    SCHEME 'stripe-v1'           -- t=<ts>,v1=<hex>; replay window 5 min
  EXECUTE
    INSERT INTO payments (
      stripe_id, amount_cents, status, raw_payload
    )
    VALUES (
      payload->>'id',
      (payload->'amount_total')::bigint,
      payload->>'status',
      payload
    )
    ON CONFLICT (stripe_id) DO NOTHING;  -- idempotency
```

- `payload` is a `jsonb` bind parameter (no string interpolation, no
  SQL-injection surface).
- Body is a single SQL statement (same constraint as reactors,
  Phase 5.11.C).
- Endpoint URL is project-scoped at routing time (the path lives
  inside the project's prefix; see Security § routing).
- Returns HTTP 200 on commit, 4xx on signature failure, 4xx on
  payload schema mismatch, 5xx on engine error.

**Cost:** ~1-2 weeks. Lives in `basin-rest` alongside the table CRUD
routes; reuses the existing engine + executor + jsonb + auth +
storage stack. Crypto deps (`hmac`, `sha2`, `subtle`) come from the
RustCrypto family already in `basin-net`'s outbound-webhook signing
path.

## Security model for inbound webhooks

CSRF does **not** apply — webhook endpoints have no browser session
and no cookie. Authentication is per-request via HMAC. The real
threats and how each is addressed:

| Threat | Mitigation |
|---|---|
| **Spoofed events** (attacker POSTs fake event) | HMAC-SHA256 over the raw body with a shared secret. Constant-time compare via the `subtle` crate (no early-exit byte-by-byte). |
| **Replay attacks** (capture + replay later) | Signature schemes that include a timestamp (Stripe-v1, GitHub-v1) are verified against a configurable window (default 5 min). Schemes without timestamps fall back to body-level idempotency (`ON CONFLICT … DO NOTHING` on a payload id). |
| **Body tampering** | HMAC covers the byte-exact body; even single-bit changes invalidate. Body is buffered and signed before parsing (no parse-then-verify race). |
| **SQL injection** | `payload` is a `jsonb` bind parameter, not string-interpolated. Same protection as every other parameterised query in the engine. |
| **Cross-tenant routing** | Endpoint URL is project-scoped: `/in/<project_slug>/<webhook_name>`. The HMAC secret is stored per-webhook and only resolved when the URL matches the project. No global namespace. |
| **DoS / rate flooding** | Reuses `basin-net`'s per-project rate limit (`BASIN_NET_RATE_LIMIT_QPS`) and body cap (`BASIN_NET_BODY_LIMIT_BYTES`). 4xx on cap; events queued via existing in-memory channel + per-project byte semaphore (see ADR 0012's webhook-fanout note about per-tenant bounding). |
| **Secret exposure** | Secret stored encrypted at rest using the `EncryptionProvider` trait (already shipped). Masked in `pg_proc` / `information_schema.routines` output; never logged in plaintext; redacted in error messages. |
| **Timing attacks on HMAC compare** | `subtle::ConstantTimeEq` or `hmac::Mac::verify` — constant-time by construction. Never compare raw bytes with `==`. |
| **TLS downgrade** | TLS required by default; HTTP-only inbound webhooks rejected unless `BASIN_NET_ALLOW_PLAINTEXT_WEBHOOKS=true` (debug-only env, never set in prod). |
| **IP allowlist (optional, defence in depth)** | `WITH ALLOWLIST '198.51.100.0/24'` clause supported; pins to a provider's published IP ranges (Stripe / GitHub publish these). Off by default — HMAC is the primary check. |
| **Header injection / smuggling** | axum + hyper parse headers strictly; no opt-in for legacy CRLF handling. |
| **Negative bodies / chunked-encoding tricks** | Body cap enforced before signature verification; chunked decoding capped by hyper defaults. |

**Crypto provenance.** No hand-rolled crypto. All primitives come from
audited Rust crates already used elsewhere in the workspace:
- `hmac` (RustCrypto) — HMAC-SHA1/256/512
- `sha2` (RustCrypto) — digest functions
- `subtle` (Dalek) — constant-time equality
- `hyper` / `rustls` — TLS termination (already in basin-net)

**Idempotency contract.** Inbound webhooks are at-least-once by design
(the caller may retry on 5xx; the network may double-deliver). The
SQL body is responsible for idempotency, typically via `ON CONFLICT
… DO NOTHING` on a payload-supplied id. The DDL syntax warns when no
unique constraint or `ON CONFLICT` clause is detected, but does not
reject — sometimes the caller really does want at-least-once writes.

## The surface map (after this ADR ships)

Three conceptual buckets; every primitive's body is SQL; every
primitive is declarative:

```
   ┌──────────────────────────── BASIN ENGINE ────────────────────────────┐
   │                                                                       │
   │   INCOMING                                                            │
   │     ├── POST /in/<project>/<name>   (NEW — inbound webhook)           │
   │     ├── POST /rpc/<fn>              (NEW — RPC mount)                 │
   │     └── /rest/v1/<table>            (✅ ADR 0006 — table CRUD)        │
   │                                                                       │
   │   DB EVENTS (ChangeEventSink trait, ADR 0012)                         │
   │     ├── ReactorSink                 (✅ 5.11.C — SQL body)            │
   │     └── WebhookSink                 (✅ 5.11.I — outbound HTTP)       │
   │                                                                       │
   │   CLOCK                                                               │
   │     └── basin-cron                  (✅ scheduler shipped 5.8 v0.1;   │
   │                                      SQL surface tracked as 5.8.A    │
   │                                      v0.2 — `SELECT cron.schedule…`) │
   │                                                                       │
   │   DEFERRED                                                            │
   │     └── basin-realtime              (SSE / WebSocket fanout —         │
   │                                      gated per ADR 0012 trigger)      │
   │                                                                       │
   └───────────────────────────────────────────────────────────────────────┘
```

`basin-realtime` is the "DB → server-pushed to client" corner. It
stays deferred per ADR 0012's trigger conditions (≥2 design partners
asking AND unable to bridge an existing realtime provider). When it
ships, it slots into the same `ChangeEventSink` trait as a third
sink implementation — no engine change, no surface change.

## What this does NOT commit us to

- **V8 / Deno edge functions.** Out, permanently — re-stated for the
  record. Compute that needs a JS runtime belongs on the customer's
  app server. The same reasoning applies as ADR 0012's PL/pgSQL
  rejection: permanent-maintenance-load not justified by wedge fit.
- **Hand-written HTTP middleware in user code.** Inbound webhooks
  and RPC are *terminating endpoints*, not middleware. Filtering /
  rewriting / chaining live in the customer's app server.
- **Imperative orchestration in the engine.** Deep conditional trees
  ("if X then A elif Y then B elif Z…") get verbose as declarative
  sagas. That's deliberate — the natural boundary where compute
  moves to the app tier.
- **Streaming responses from RPC.** v0.1 is request/response with the
  body fitting in memory. Streaming is a follow-up if a real use case
  appears.
- **`LANGUAGE plpgsql` parity.** Single-statement SQL bodies are a
  known reduced-expressiveness gap vs Supabase's Database Functions.
  Per ADR 0012, that's the conscious wedge call. Customers needing
  plpgsql stay on Postgres or Supabase.
- **Long-running async work.** Inbound webhook handlers complete
  synchronously (≤1s typical). Long work = enqueue a row, let a
  reactor or cron job pick it up.

## Trigger to revisit (V8 / Deno edge functions)

**Reopen and consider a V8/Deno edge-function runtime** when ALL of:

1. ≥3 Phase 0 design partners explicitly ask for V8-shaped HTTP
   handlers and rule out running them on their own app server, AND
2. The use cases they describe genuinely cannot be expressed as the
   declarative composition above (inbound + reactor + outbound + RPC),
   AND
3. The engineering org has budget for the ~4-6 months of build plus
   permanent ops load (sandbox, isolate pool, cold starts, deploy
   pipeline, log streaming, multi-tenant CPU/memory isolation).

Without all three, the answer stays "no." The Wasm UDF escape hatch
(Phase 5.11.J) is the in-DB compute path; the customer's app server
is the imperative HTTP path.

## References

- [ADR 0006 — REST API layer](./0006-rest-api-layer.md) — this ADR
  amends ADR 0006's stale "stored functions out of scope" line.
- [ADR 0012 — Change-event primitive](./0012-change-event-primitive.md)
  — the `ChangeEventSink` trait that makes the surface symmetric;
  inbound webhooks complete the picture.
- [ADR 0013 — Auth per-project schema](./0013-auth-per-project-schema.md)
  — how `/rpc/<fn>` and `/in/<project>/<name>` resolve to projects.
- [ADR 0018 — Subsystem feature flags](./0018-subsystem-feature-flags.md)
  — both primitives live behind the `rest` feature flag; no marginal
  binary cost when disabled.
- 2026-05-19 conversation log — the BaaS-gap walk-through and the
  "two new primitives close the gap declaratively" framing.
