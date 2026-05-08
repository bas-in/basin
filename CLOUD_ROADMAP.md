# Basin Cloud — product roadmap

The hosted multi-tenant Postgres-compatible database that lives on
object storage. Per-tenant isolation, ~$0.10–$0.20 per tenant per
month all-in, no per-project tier wall.

This is the **product** roadmap. The engine — pgwire, SQL, storage,
catalog, vector, auth, REST — lives in the open-source repo and is
the substrate this product runs on. Everything below is what wraps
the engine into a thing customers can sign up for.

> **Repository note:** this file is intended to move to its own
> public repo (e.g. `basin-cloud/cloud-roadmap`) once the landing
> page ships. Until then it lives alongside the engine repo so the
> two roadmaps stay in lock-step.

---

## Brand & visual identity

**Theme: topographic / watershed.** Literal — the product is named
*Basin*. Sediment layers (Parquet ≈ stratigraphy) and watershed
boundaries (per-tenant prefix isolation ≈ catchment) are the visual
metaphors. This avoids the generic SaaS purple-gradient look and gives
us a defensible aesthetic that extends through docs, dashboard, and
swag.

### Palette

- `#0F1115` — ink (background, text)
- `#FAF7F0` — paper (light surfaces, off-white)
- `#3B6F73` — basin teal (primary accent, water)
- `#C4732B` — sediment ochre (secondary accent, layers)
- `#5C5C5C` — graphite (dividers, contour lines)
- `#8FB59B` — moss (success / shipped status)
- `#B8462E` — clay (error / blocked status)

No neons, no purple gradients, no glassmorphism. Flat, technical,
slightly hand-drawn (think USGS topographic maps and 1970s field-
manual diagrams, not SF tech).

### Typography

- **Display:** `Söhne Breit` or `Inter Display` (heavy weights only,
  for hero / section heads)
- **Body:** `Inter` (regular / medium)
- **Mono:** `JetBrains Mono` or `IBM Plex Mono` (code samples,
  contour-line annotations, terminal blocks)

All variable fonts; self-hosted (no Google Fonts CDN — privacy +
performance both win).

### Visual motifs (use sparingly, not as decoration)

- **Contour-line backgrounds** behind hero / section heads. SVG,
  hand-drawn-feel curves at low opacity. Generated procedurally (a
  small script seeds them from a hash of the section title — same
  section always gets the same lines).
- **Sediment-layer dividers** between sections. Three-to-five
  horizontal stripes in graphite, with subtle texture. Each layer is
  labelled in mono with a Basin concept (`row group`, `data file`,
  `snapshot`, `partition`, `tenant prefix`) — the metaphor is
  literal: this is what's stacked under each tenant.
- **Watershed iconography** for the per-tenant isolation diagram.
  Shows two adjacent basins (catchments) with the ridge between
  labelled "tenant prefix boundary." The point is visual: *the data
  cannot flow across the ridge by construction.*
- **Schematic / blueprint annotations** rather than 3D illustrations.
  ASCII-art-grade, callouts in mono, dimension lines.

### Tone of voice

Technical, candid, slightly dry. *"Per-tenant cost: ~$0.10/mo. Yes,
that includes storage."* Not playful, not aggressive. Closer to
Stripe's docs voice than Vercel's marketing voice. Numbers,
benchmarks, trade-offs — not vibes.

---

## Phase 0 — Landing page + waitlist (2–3 weeks)

**The gate.** Phase 1+ doesn't start until this is live and pulling
real signups.

### Goals

1. Convert technical visitors (the buyers / champions) into a waitlist.
2. Tell the wedge story honestly: per-tenant cost, multi-tenant
   architecture, drop-in pgwire compatibility.
3. Set up the brand so the dashboard, docs, and edge-function
   product all inherit it without rework.

### Stack

- **Astro** for static generation (mirrors the existing benchmark
  dashboard tech stack — same build muscle memory).
- **React (JSX, not TSX)** for the few interactive islands (cost
  calculator, signup form, animated watershed diagram). **No
  TypeScript on the frontend.** Pure `.jsx` everywhere; the type
  surface adds nothing for a brochure site and slows iteration on
  the visual side. PropTypes are fine if a component grows large
  enough to want them, but the default is plain JSX.
- **Tailwind** with a *minimal* config: only the topographic palette,
  the three font stacks, and the spacing scale. No DaisyUI / shadcn
  defaults — they'd dilute the theme.
- **Self-hosted**: deploys to Cloudflare Pages (zero egress to match
  the deployment story Basin sells).
- **No tracking other than a single self-hosted Plausible** —
  privacy-conscious developers are the buyer, leaking their visit
  to GA4 is off-brand.

### Frontend conventions

- Files are `.jsx` (not `.tsx`). `tsconfig.json` is **not** present in
  the repo. Astro's `astro.config.mjs` enables the React integration
  *without* the TypeScript checker.
- Components default to function components + hooks. No class
  components, no MobX/Redux/Zustand state libraries — `useState` and
  React Context cover everything Phase 0 needs.
- Style: Tailwind utility classes for layout + spacing; one
  `globals.css` for the contour-line / sediment-layer backgrounds and
  any custom keyframes. No CSS-in-JS.
- Linting: `eslint` with `eslint-config-react`, no
  `@typescript-eslint`. Format via Prettier.
- Imports: bare-specifier paths (no `@/` alias gymnastics until the
  page tree justifies it).

### Pages

- **`/`** Landing. Hero, wedge proof, code sample, cost calculator,
  feature grid, quickstart preview, waitlist CTA.
- **`/why`** The wedge story long-form. Why Postgres-per-project
  doesn't work for multi-tenant SaaS. Charts (storage cost vs
  tenant count, Postgres vs Basin).
- **`/architecture`** Schematic. The watershed diagram, sediment
  layers, request flow. Each layer cross-links to the open-source
  repo so technical visitors can verify the claim.
- **`/pricing`** Cost calculator + a *single* paid tier ("$X / GB-
  month + $Y / 1M ops"). No "Contact us for Enterprise" until we
  have an actual enterprise tier worth gating.
- **`/docs`** Routes to the engine docs (open source) for now.
  Cloud-specific docs (auth, BYO-bucket, dashboard) land in Phase 2.
- **`/changelog`** Markdown-driven, RSS-feedable. Every shipped
  feature gets a dated entry. Establishes credibility.

### Hero (the load-bearing 5 seconds)

Above the fold, on a contour-line background:

```
      Multi-tenant Postgres
           on object storage
─────────────────────────────────
$0.10 per tenant per month, all-in.
Same SQL your app already speaks.

  [ Join waitlist ]   [ Read the architecture ]

  $ psql -h db.basin.cloud -U your_app
  basin=> SELECT count(*) FROM tenants;
   3,142
```

The point: *one* benefit (cheap multi-tenant), *one* compatibility
claim (psql works), *one* CTA (waitlist), *one* technical proof (the
psql block). Not three columns of "Why Basin?" with smiling avatars.

### Cost calculator (the wedge proof)

Interactive island. Inputs: tenant count, avg storage GB per tenant,
ops per tenant per month. Outputs: Basin Cloud monthly cost, "what
this would cost on Postgres-per-project" comparison (Neon / Supabase
list prices). Code samples for both paths so the comparison is
honest: *"this is the same workload, here's both."*

The calculator is the page's argument. If a visitor leaves having
typed their numbers in and seen a 10× delta, the waitlist conversion
is structural.

### Waitlist

Single field — email. No company / role / use-case form. Those go in
the follow-up onboarding. The friction at signup is what kills these.

Backed by basin-auth's email-link flow (Phase 5.10 of the engine
roadmap, just shipped). When Phase 1 (signup) lands, the waitlist
emails get an "early access" link that consumes their first magic-link.

### Phase 0 checklist

- [ ] Brand kit: palette, typography, motifs, voice — captured in a
      short `brand.md` so future hires inherit it
- [ ] Astro + React (JSX) + Tailwind scaffold; deploys to CF Pages
- [ ] Landing page with the five sections above
- [ ] Cost calculator (interactive island)
- [ ] Waitlist form → basin-auth email-link API
- [ ] `/architecture`, `/why`, `/pricing`, `/changelog` routes
- [ ] Self-hosted Plausible, no GA / Segment / Facebook Pixel
- [ ] DNS at `basin.cloud` (or chosen domain), TLS via CF
- [ ] Open-graph cards for `/`, `/architecture`, `/why` — show the
      contour-line motif so a Twitter share is on-brand
- [ ] Lighthouse score ≥ 95 on mobile (any worse and the brand looks
      like vapor)

---

## Phase 1 — Sign-up + first tenant (3–4 weeks)

**Trigger:** waitlist hits 200 emails OR 10 explicit "I'd pay for this"
replies.

- [ ] Convert waitlist email → real account (basin-auth signup flow)
- [ ] First tenant provisioning: pick a region, get a connection string
- [ ] In-browser SQL console (read-only first, then writable) backed
      by a websocket bridge to pgwire
- [ ] Onboarding flow: "create your first table" tutorial walking
      through the architecture diagram from the landing page
- [ ] Free tier: 1 tenant, 100 MB, 1M ops / month. Designed to be
      enough for a side-project demo, not a production SaaS

## Phase 2 — Customer dashboard (4–6 weeks)

The self-serve operations surface. Lives at `app.basin.cloud`. Same
brand kit as the landing page; the contour motif carries through.

- [ ] Tenant list + per-tenant detail page (storage, ops, latency
      cards — read straight from the engine's `Engine::tenant_counters`)
- [ ] User + role management (basin-auth admin endpoints + the new
      API-key surface from Phase 5.10)
- [ ] Schema browser (table list, columns, indexes, snapshots)
- [ ] Snapshot timeline UI (lever for the engine's PITR — pick a
      snapshot, click "rollback")
- [ ] Usage charts (storage GB-month, S3 ops, active hours, CPU-ms
      when V8 lands) — read from the same counters that feed billing
- [ ] Settings: regions, billing, usage alerts
- [ ] Empty placeholder dir already exists at `services/dashboard/`
      in the engine repo (initial workspace scaffolding); the cloud
      project takes that name

## Phase 3 — BYO-bucket (3–4 weeks)

Customer's own S3/R2 bucket with an IAM role granting Basin write +
read access. Platform never holds the data; revoking the role
cleanly evicts the tenant.

- [ ] `TenantMetadata.byo_bucket: Option<S3Config>` on the tenant
      record (engine catalog change — opens the door for this layer
      without the cloud product being live yet)
- [ ] `Storage` accepts a per-tenant override `ObjectStore` (the
      pluggable `dyn ObjectStore` makes this almost free at storage
      layer; the work is per-tenant resolution + secret handling)
- [ ] Onboarding flow: customer pastes IAM role ARN, Basin probes
      write/read/delete, surfaces errors clearly
- [ ] Cleanup on tenant deletion: leave the customer's bucket intact;
      only delete Basin's prefix tree
- [ ] Cost telemetry routes to the *customer's* AWS bill, not Basin's
      egress dashboard

## Phase 4 — Stripe billing (3–4 weeks)

Usage-based billing with four meters: storage GB-month, S3 API ops,
active-hours of a tenant's shard-owner footprint, and (when V8 lands)
CPU-ms.

- [ ] Per-tenant usage counters in the catalog (storage bytes, ops,
      active seconds) — engine-side counters from Phase 6 telemetry
      already exist; cloud-side adds daily roll-up
- [ ] Daily roll-up job that posts to Stripe metered billing
- [ ] Customer-portal page reading the same counters for transparency
- [ ] Free-tier accounting (monthly reset, hard cap then usage cap)
- [ ] Overage alerts via the engine's `basin-net` HTTP path (so the
      same rate limiter / allowlist applies)
- [ ] Stripe webhook receiver: subscription created / cancelled /
      payment_failed → tenant state transitions

## Phase 5 — BYO-key (KMS) (4–6 weeks)

Customer-managed encryption keys via AWS KMS / GCP KMS / Azure Key
Vault. Platform never sees plaintext beyond the per-request envelope.

- [ ] Envelope encryption at the storage write boundary: data key
      per file, wrapped by the customer's KMS CMK
- [ ] Key cache with explicit TTL (so revoking the CMK at the
      customer side actually stops decryption within minutes)
- [ ] Per-tenant choice of cloud KMS (AWS / GCP / Azure)
- [ ] Hardware-token attestation for the decryption agent (deferred
      until a FedRAMP-class customer asks)

## Phase 6 — Auth / REST cloud-only extensions (4–6 weeks)

The OSS bundle (engine repo, Phase 5.10) already ships basin-auth and
basin-rest. These are the *cloud-tier-only* extensions, gated to paid
plans:

- [ ] OIDC / Google / GitHub social login providers
- [ ] SCIM provisioning for enterprise customers
- [ ] Per-org SAML SSO
- [ ] Per-tenant secret-rotation policy with operator-driven enforcement
- [ ] Admin-action audit log feeding the customer dashboard
- [ ] REST API: rate-limit overrides per paid tier
- [ ] REST API: signed-URL download endpoints backed by BYO-bucket
- [ ] Org-level workspaces (multiple users, role hierarchy)

## Phase 7 — Control plane (multi-region, 6–8 weeks)

The backplane that fans tenant lifecycle commands across regional
clusters. An empty placeholder dir exists at `services/control-plane/`
in the engine repo.

- [ ] Tenant lifecycle API (idempotent create / suspend / resume /
      migrate / delete; emits events the dashboard consumes)
- [ ] Region directory: tenant ULID → home region; required before
      multi-region engine work can ship without manual DNS gymnastics
- [ ] Audit log of every control-plane action (who, when, what,
      reverse-action) — feeds Phase 6's admin audit log
- [ ] Quotas: per-customer table-count, tenant-count, storage-GB caps
      that the engine consults at write time
- [ ] gRPC + REST surfaces (REST for the dashboard, gRPC for
      service-to-service inside the cluster)

## Phase 8 — V8 edge functions (gated, 8–12 weeks once unblocked)

Customer JavaScript that runs in a sandboxed V8 isolate per request,
with capability-bounded access to the calling tenant's tables, an
allowlisted HTTP egress, and a key-value scratch store.

**Decision gate:** only commit when 2+ design partners specifically
ask for it. Until then this is optional polish; the wedge customer
(multi-tenant SaaS audit logs) does not run business logic in edge
functions.

- [ ] Pick the V8 binding crate (`rusty_v8` vs `deno_core`)
- [ ] Function manifest schema (entry point, allowed origins, kv keys)
- [ ] Tenant-scoped runtime: `TenantSession` injected as a JS global
- [ ] HTTP egress proxy via basin-net (allowlist + rate limit +
      timeout reused unchanged)
- [ ] CPU + memory caps per invocation (V8 heap limits + isolate
      timeouts)
- [ ] Cold-start budget < 50 ms (must beat AWS Lambda's 100–500 ms cold)
- [ ] Per-function deployment via REST: upload bundle, hot-reload on
      next invocation
- [ ] Logs + traces piped into the existing OTEL pipeline
- [ ] CPU-ms metering for Stripe billing

## Phase 9 — Compliance posture (multi-month, gated on paying customers)

- [ ] SOC 2 Type 2 (≈ 12 months observation period; start as soon as
      paying customers exist)
- [ ] GDPR DPA template + sub-processor list
- [ ] HIPAA readiness assessment (deferred until a healthcare prospect
      asks; not a default investment)
- [ ] Penetration test before public beta (independent firm, public
      executive summary)
- [ ] Bug bounty program before GA

---

## Cross-cutting

- [ ] Status page at `status.basin.cloud` (Atlassian Statuspage or
      a small self-hosted clone matching the brand)
- [ ] Public-facing docs site separate from the engine docs
- [ ] Support inbox (`support@`) + on-call rotation
- [ ] Marketing site changelog + a low-volume newsletter (one issue
      per shipped phase, no growth-hack tactics)
- [ ] Security review checklist before each phase ships

---

## What's *not* here (and where it lives instead)

- The engine itself — pgwire, SQL, storage, catalog, query planner,
  vector search, RLS, multi-tenancy primitives, storage tiering,
  caches, indexes, WAL, compactor, analytical engine, basin-auth,
  basin-rest. All in the **engine repo** (`TASK.md`).
- Per-tenant fairness, rate-limiting, cost-based query rejection.
  Engine concerns.
- Phase 0 customer interviews for the wedge — engineering doesn't
  start until that gate passes; tracked in the engine repo.

---

*Last updated: 2026-05-08.*
