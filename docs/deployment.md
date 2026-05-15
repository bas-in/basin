# Deployment & cloud architecture

How to actually run Basin in production. Companion to [`architecture.md`](./architecture.md)
(the four-layer system) and the multi-region ADRs ([0001](./decisions/0001-single-region-only.md),
[0004](./decisions/0004-multi-region-read-replicas.md)).

This doc is the practical "where do I put the bytes and which Fly/AWS region" guide.

> For ops on a managed fleet, see [`basin-cli`](https://github.com/bas-in/basin-cli)
> (`basin login`, `basin projects list`, `basin sql run`) and
> [`basin-cloud`](https://github.com/bas-in/basin-cloud) (control plane,
> per-project Fly Machine orchestration). For app-side integration against a
> deployed engine, see [`basin-js`](https://github.com/bas-in/basin-js).

---

## TL;DR

- **One Basin cluster per region. Not per customer.** That's the wedge — Postgres can't multi-project cheaply, Basin can.
- **Recommended cloud:** Fly.io machines + Tigris (or another S3-compatible store) in the same metro. ~5–30 ms RTT, zero egress fees.
- **Multi-region is by deployment, not by code:** spin up an independent cluster per region, customers pin to one region at signup.
- **Cross-region read replicas:** built when first paid customer asks ([ADR 0004](./decisions/0004-multi-region-read-replicas.md)). Cross-region strong-consistency writes: deferred ([ADR 0001](./decisions/0001-single-region-only.md)) — Spanner-class, not until a paying customer demands it.

---

## Required env vars (quickstart)

basin-server is a single process. To deploy it you set:

| Var | Purpose |
|---|---|
| `BASIN_BIND` | pgwire listener, e.g. `0.0.0.0:5433` |
| `BASIN_DATA_DIR` | Parquet + catalog directory (durable volume) |
| `BASIN_WAL_DIR` | WAL directory (durable volume, ideally NVMe) |
| `BASIN_PROJECTS` | Static project list, e.g. `acme=*,beta=*`. basin-auth's reserved entry `basin_auth=<reserved-ulid>` is auto-injected, so operators do not list it. |
| `BASIN_AUTH_ENABLED=1` | Enables the auth subsystem (signup, JWT, refresh). Auth state lives in the embedded catalog over the loopback pgwire — no separate database. SMTP vars from [ADR 0005](./decisions/0005-auth-system.md) become required when this is on. |

That is the full required surface for a single-region deploy. No external
Postgres, no external catalog service. See `BASIN_BIND` / `BASIN_DATA_DIR`
in [`../README.md`](../README.md) for the boot example.

---

## Why shared-cluster, not instance-per-customer

Supabase and Neon charge per project (~$25/mo each). They have to — Postgres is fundamentally bad at multi-project:

| Postgres pain point | Forces them to spin a new instance per project |
|---|---|
| Schemas share buffer pool, WAL, autovacuum | Noisy-neighbor inevitable |
| Connection slots are global (~200 useful) | Multi-project connection pool collapses fast |
| RLS is logical-only (one bug = cross-project leak) | Per-project DB is the only real isolation |
| ~10k schemas before catalog tables choke | Hard scaling wall |

Basin has none of these:

| Basin design | Why shared cluster works |
|---|---|
| Per-project bucket prefix | Structural data isolation; one bug ≠ leak |
| Tokio task per connection (~few KB) | 10k+ connections per process easily |
| Per-project Semaphore + scheduler ([ADR 0008](./decisions/0008-noisy-neighbor-fairness.md)) | Fairness without spawning processes |
| Catalog scales linearly with project count | No 10k limit |

**Cost per project:** essentially `bytes × $0.023/GB + ops × $0.40/1M + $0.02 per project`. At 10,000 projects × 100 MB each, total cost is ~$1k/month all-in. Same workload on Supabase costs $25k/month minimum (at $25/project).

---

## Recommended deployment shape

```
                        Customer pgwire connection
                              │
                              ▼
                   ┌──────────────────────┐
                   │  Regional DNS         │
                   │  db-us.basin.app      │
                   └─────────┬────────────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
  ┌──────────┐         ┌──────────┐         ┌──────────┐
  │ router 1 │         │ router 2 │         │ router N │   stateless;
  │ pgwire   │         │ pgwire   │         │ pgwire   │   load-balanced
  └─────┬────┘         └─────┬────┘         └─────┬────┘
        │                    │                    │
        └────── consistent-hash (project_id) ──────┘
                             │
        ┌────────────────────┼────────────────────┐
        ▼                    ▼                    ▼
  ┌──────────┐         ┌──────────┐         ┌──────────┐
  │ shard    │         │ shard    │         │ shard    │   stateful;
  │ owner 1  │         │ owner 2  │         │ owner M  │   each owns
  │ (~5k     │         │ (~5k     │         │ (~5k     │   ~5k projects'
  │ projects) │         │ projects) │         │ projects) │   in-mem state
  └─────┬────┘         └─────┬────┘         └─────┬────┘
        │                    │                    │
        └─────── shared bucket per region ────────┘
                             │
                             ▼
                   ┌──────────────────────┐
                   │  S3-compatible store  │
                   │  (us-east region)    │
                   │  projects/<id1>/...   │
                   │  projects/<id2>/...   │
                   │  projects/<id3>/...   │
                   └──────────────────────┘
```

### Key rules

1. **One object-store bucket per region.** Storage stays anchored. EU customer's bytes never leave EU.
2. **Routers are stateless.** Scale horizontally on connection count.
3. **Shard owners are stateful.** Each owns a subset of projects' in-memory state. Consistent-hash routing.
4. **All shard owners share the same bucket.** Compute scales independently of storage.
5. **basin-auth issues JWTs scoped to a project.** Router decodes, hashes project_id → shard.

### Concrete: Fly.io + Tigris (recommended)

Best fit for Basin's design today:

- **Fly.io** Machines are the compute layer. ~5–30 ms RTT to Tigris from Fly's `nrt` / `iad` / `lhr`.
- **Tigris** (Fly's native S3-compatible store) has zero egress within Fly's network and no per-region bucket management overhead.
- **AWS S3**, **Backblaze B2**, **MinIO**, and other S3-compatible stores also work; pick by cost/latency/compliance.

Per-region resource shape, rough sizing for the first 1k–10k projects:

| Resource | Tier | Notes |
|---|---|---|
| Router | Fly Performance 1× × 2 (HA) | ~$30/mo. Stateless; scales linearly with connection count. |
| Shard owners | Fly Performance 4× × 2 | ~$240/mo. Each handles ~5k projects' working set. Add more as project count grows. |
| Object store bucket | regional, public-bucket-disabled | Tigris: ~$0.02/GB + $0.01/GB egress (Fly-internal is free). AWS S3: $0.023/GB + $0.09/GB egress. |
| Catalog backend | none — embedded | Catalog state (project list, table schemas, snapshot manifests, file refs, plus `basin-auth`'s `auth.users` / `auth.refresh_tokens`) lives in the engine's own pgwire loopback, durable on the WAL volume. ~50 MB per 10k projects. No external Postgres to provision. |
| Optional: NVMe disk cache | Fly volume, 50 GB | Phase 5.7-A1 cache. ~$5/mo. Cuts cold S3 fetches from ~50 ms → ~100 µs. |

**Total cost for one region with 10k projects × 100 MB each, mostly cached:** ~$300–500/month all-in.

---

## Multi-region: status today

### What works without code changes

✅ **Single-region multi-project** — that's the default. Deploy one Basin cluster per region, each with its own object-store bucket. Customers connect to the regional endpoint. Their project_id pins them to that region's bucket prefix.

You can ship this today. It's "multi-region by deployment", not by code.

### What needs small DB changes (~1 day)

To ship multi-region cleanly:

1. **Add `region` to `ProjectMetadata`** in catalog. Used at signup, recorded forever.
2. **Reject signups without a region.**
3. **Region check in project resolver** so misrouted requests fail fast with a clear error.
4. **DNS routing layer**: `<region>.basin.example.com` → that region's router fleet.

These are operational glue. Documented as Phase 1 below.

### What needs Phase 6 work (~3-4 weeks)

For cross-region **read replicas** ([ADR 0004](./decisions/0004-multi-region-read-replicas.md)):

| Piece | Effort |
|---|---|
| Object-store cross-region replication (storage layer) | Flip a switch in your provider (e.g. Tigris global replication, S3 CRR). **No Basin code.** |
| Catalog replication | Snapshot-and-pull of the embedded catalog's WAL between regions, replayed at the destination. ~1 week. Operators who run `BASIN_AUTH_CATALOG_DSN` against external Postgres can layer logical replication on that backend instead. |
| Replica role on basin-router | Read-only session marker. ~3 days. |
| Snapshot freshness lag visibility | Stats endpoint + optional `READ AT SNAPSHOT <id>` SQL. ~3 days. |

### What's deferred (Spanner-class)

🚫 **Strong-consistency cross-region writes** ([ADR 0001](./decisions/0001-single-region-only.md)). Multi-week of consensus + clock synchronization. Not until a paying customer has a hard requirement.

---

## Rollout phases

### Phase 1 — ship single-region multi-project (this week)

- Pick 2–3 regions to launch (e.g. `us-east-1`, `eu-west-1`, `ap-southeast-1`).
- Deploy independent Basin clusters in each, one object-store bucket per region.
- Customer signup picks region; record `project.region` in catalog.
- DNS: `<region>.basin.example.com` → regional router fleet.

This covers ~95% of B2B SaaS shapes (data residency + low local latency).

### Phase 2 — cross-region read replicas (when first paid customer asks)

- Enable cross-region replication for that project's bucket (provider-level configuration).
- Replicate catalog state (snapshot+pull of embedded catalog; PG logical only if operator opted into `BASIN_AUTH_CATALOG_DSN`).
- Add `replica` role marker to basin-router; read-only sessions land on replicas.
- Surface replica lag in stats and as a `WARNING` if stale > N seconds.

ADR 0004 covers the scope.

### Phase 3 — strong-consistency cross-region writes (only if Stripe/Shopify-class customer pays)

Spanner-class engineering. ADR 0001 documents the deferral and the trigger.

---

## When you actually want a dedicated Basin instance per customer

Three legitimate reasons (none "by default"):

1. **Whale project** — one customer with 100× the load of others. **Pin them to their own shard owner with bigger compute.** Same bucket, different in-memory shard. Cheap because storage is shared.
2. **Compliance customer** — needs BYO-bucket or BYO-key (Phase 6, [TASK.md](../TASK.md)). They get their own bucket; data never touches Basin's bucket. Same Basin process, different storage backend per session.
3. **Region-restricted** — solved by per-region cluster, not per-customer instance. Customer picks region at signup.

If a sales lead asks for "dedicated Postgres instance" → educate them on Basin's structural isolation, then offer (1) or (2) at premium pricing.

---

## Suggested pricing tiers

Mapping the architecture to billing:

| Tier | Architecture | Cost driver |
|---|---|---|
| Free | Shared cluster, 100 MB cap, 25 max connections | Bytes only |
| Hobby / Pro / Team | Shared cluster, higher caps + caches enabled | Bytes + active hours |
| Scale | Shared cluster, dedicated compute pool isolation | Bytes + active hours |
| Enterprise | Dedicated shard owner pinned, BYO-bucket + BYO-KMS optional | Compute + bytes |

This mirrors what Snowflake and BigQuery do. Supabase / Neon's per-project pricing is forced on them by Postgres; Basin doesn't have that constraint.

---

## Operational checklist (per region)

- [ ] Fly.io app created in the region
- [ ] Object-store bucket created in the region (Tigris, S3, or other S3-compatible)
- [ ] `BASIN_DATA_DIR` + `BASIN_WAL_DIR` volumes attached (the embedded catalog lives here — no external Postgres to provision)
- [ ] DNS: `<region>.basin.example.com` → router fleet
- [ ] Auth signing key (one global key OR per-region keys)
- [ ] Monitoring: per-project ops/s, p50/p99, RAM, IO via OTLP
- [ ] Backup: bucket versioning enabled (Iceberg snapshots are the data; no `pg_dump` needed)
- [ ] Rate limit: per-project guard for compute and `basin-net` outbound HTTP
- [ ] Disaster recovery: bucket replication target (manual cross-region copy if Phase 2 not yet built)

---

## Optional: external auth catalog

By default `basin-auth` stores `auth.users`, `auth.refresh_tokens`, and
`auth.email_tokens` in the same engine catalog as application tables, over
the loopback pgwire. One process, one durability domain, one backup target.
That is the recommended shape for ≥ 99% of self-hosted deploys.

Some operators prefer to keep identity tables on a separate OLTP store:
durability isolation (a Basin engine crash that loses uncompacted WAL is
recoverable; an `auth.users` corruption locks every customer out), tighter
compliance scoping, or an existing managed Postgres they already operate.

Set `BASIN_AUTH_CATALOG_DSN=postgres://...` to point the auth catalog at an
external Postgres (Fly PG, RDS, Neon, self-managed — any pgwire-speaking
backend works). When set:

- Application tables continue to use the embedded catalog. Only the
  `auth.*` namespace is redirected.
- The external DB needs `CREATE` on its target schema; basin-auth runs the
  same idempotent `schema.sql` it would otherwise run against the embedded
  catalog.
- The connection string is read at startup only. Rotating the password
  requires a restart.
- Engine crashes do not corrupt the external store. Outages on the external
  store fail new logins fast (5xx from `/auth/v1/*`); existing JWTs keep
  working until they expire.

Trade-off: one more thing to back up, one more thing to monitor, one more
network hop on signin (a few ms typically). The override exists because
some prod workloads value durability separation while basin's engine
catalog matures; it is not the default.

---

## Resetting basin-auth state

Because basin-auth's tables live inside the engine catalog (the loopback
shape described above), wiping them does not require destroying the WAL or
the application data tables — it is just a series of `DROP TABLE`s scoped
to the `basin_auth_*` namespace. The `basinctl reset-auth` command bundles
that into one safe call.

**When to use it**

- Local / CI / test environments that want a clean slate between runs.
- Schema-incompat upgrades where the rebootstrap on next start needs to
  run against an empty namespace (e.g. a column type changed and
  idempotent `CREATE TABLE IF NOT EXISTS` won't re-shape an existing
  table).
- Recovering from a broken-state auth catalog (e.g. a botched manual
  migration) without rebuilding the entire engine instance.

**What it does**

```sh
# Wipe users, sessions, api keys, magic links, … but keep project-pgwire creds.
basinctl reset-auth --yes

# Same, but also clear the per-project pgwire-credentials table — this logs
# every customer out of the pgwire endpoint until creds are re-issued.
basinctl reset-auth --yes --include-project-creds

# Point at a non-default engine endpoint.
basinctl reset-auth --yes \
  --engine-url postgres://basin_auth:basin_auth@10.0.0.7:5433/basin?sslmode=disable
```

The command refuses to run without `--yes`. It connects as the reserved
`basin_auth` user (whose creds the static resolver maps via the
auto-injected reserved project) and runs `DROP TABLE IF EXISTS` for each
`basin_auth_*` table in child-before-parent order. By default the
per-project pgwire-credentials table (`basin_auth_auth_project_credentials`)
is left intact — pass `--include-project-creds` to drop it as well.

**What it does not touch**

- Application / customer data tables — only identifiers in the
  `basin_auth_*` namespace are affected.
- WAL segments, Iceberg snapshots, object-store buckets — there is no object-store
  cleanup involved.
- The reserved `basin_auth` project entry on the router — that is
  auto-injected on every start.

**After running**

Restart `basin-server`. Boot calls `basin_auth::schema::run_migrations`
which is idempotent and recreates the empty schema. There is no separate
"init" step.

**Verify the reset**

```sh
psql "postgres://basin_auth:basin_auth@127.0.0.1:5433/basin?sslmode=disable" \
  -c "SELECT count(*) FROM basin_auth_users"
# ERROR: relation "basin_auth_users" does not exist   (before restart)
# 0                                                   (after restart)
```

Or, more thoroughly, list every surviving `basin_auth_*` table:

```sh
psql "$BASIN_AUTH_URL" -c "
  SELECT table_name FROM information_schema.tables
  WHERE table_name LIKE 'basin_auth_%'
  ORDER BY table_name"
```

Pre-restart: empty result set. Post-restart: the 8 tables from
`basin_auth::schema::run_migrations`.

---

## References

- [`architecture.md`](./architecture.md) — the four-layer system
- [ADR 0001 — Single-region only](./decisions/0001-single-region-only.md)
- [ADR 0004 — Multi-region read replicas](./decisions/0004-multi-region-read-replicas.md)
- [ADR 0008 — Noisy-neighbor fairness](./decisions/0008-noisy-neighbor-fairness.md)
- [`TASK.md`](../TASK.md) — Phase 6 production hardening checklist
- [`CAPABILITIES.md`](../CAPABILITIES.md) — what's shipped vs deferred
