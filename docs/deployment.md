---
title: "Deployment and cloud architecture"
nav_section: deployment
sidebar_position: 10
summary: "How to run Basin in production: storage backends, deployment topologies, configuration."
---

# Deployment & cloud architecture

How to actually run Basin in production. Companion to [`architecture.md`](./architecture.md)
(the four-layer system) and the multi-region ADRs ([0001](./decisions/0001-single-region-only.md),
[0004](./decisions/0004-multi-region-read-replicas.md)).

This doc is the practical "where do I put the bytes and which Fly/AWS region" guide.

> For ops on a managed fleet, see [`basin-cli`](https://github.com/vul-os/basin/tree/main/cli)
> (`basin login`, `basin projects list`, `basin sql run`). Control-plane
> concerns — org/project management, per-project Fly Machine orchestration —
> sit above this repo and are out of scope here. For app-side integration
> against a deployed engine, see [`basin-js`](https://github.com/vul-os/basin/tree/main/sdks/js).

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
| `BASIN_CATALOG` | Catalog backend: `memory` (default — **volatile**, all catalog state lost on restart; dev/test only) or a Postgres connection string (`postgres://…` or libpq keyword form) for durable production deploys. `BASIN_CATALOG_SCHEMA` picks the schema (default `basin_catalog`). |
| `BASIN_DATA_DIR` | Vortex (default) / Parquet data directory (durable volume) |
| `BASIN_WAL_DIR` | WAL directory (durable volume, ideally NVMe). In raft mode, raft log/vote/snapshot persist under `${BASIN_WAL_DIR}/raft`. |
| `BASIN_PROJECTS` | Static project list, e.g. `acme=*,beta=*`. |
| `BASIN_AUTH_ENABLED=1` | Enables the auth subsystem (signup, JWT, refresh). Per [ADR 0013](./decisions/0013-auth-per-project-schema.md), auth state lives in each project's own storage under the `basin_auth` schema prefix and is reached in-process (`EngineAuthStore` over `ProjectSession`) — no loopback pgwire connection, no reserved internal project, no separate database. SMTP vars from [ADR 0005](./decisions/0005-auth-system.md) become required when this is on. |
| `BASIN_AUTH_CATALOG_DSN` | Optional escape hatch (ADR 0013): point auth state at an external pgwire-speaking Postgres for separate blast radius. Unset = the default in-process per-project-schema path. |

That is the full required surface for a single-region deploy. A production
deploy needs one external Postgres for the catalog (`BASIN_CATALOG=postgres://…`);
the `memory` default is volatile and suitable only for dev/test. Auth state
needs no separate database (ADR 0013). See `BASIN_BIND` / `BASIN_DATA_DIR`
in [`../README.md`](../README.md) for the boot example.

---

## Raft / lease knobs (multi-node)

The knobs below are additive: every other env var and behavior from the
table above is unchanged when these are set. They require
`BASIN_SHARD_ENABLED=1`.

### WAL durability mode

| Var | Values | Purpose |
|---|---|---|
| `BASIN_WAL_MODE` | `local` (default) \| `raft` | `local` is the unchanged single-node file-backed WAL. `raft` opens a `RaftWal` (openraft) whose durability boundary is a quorum ack from a majority of the cluster. Any other value is a **startup error** (strict parse — a typo must not silently downgrade durability). `raft` requires `BASIN_SHARD_ENABLED=1`, `BASIN_RAFT_BIND`, and `BASIN_RAFT_PEERS` or the server refuses to start. |

### Raft cluster identity (required when `BASIN_WAL_MODE=raft`)

| Var | Purpose |
|---|---|
| `BASIN_NODE_ID` | This node's stable numeric raft id (positive `u64`). Must appear in `BASIN_RAFT_PEERS`. Must be unique across all nodes in the cluster. |
| `BASIN_RAFT_BIND` | This node's raft gRPC listen address, e.g. `10.0.0.1:6010`. Must match the address registered for `BASIN_NODE_ID` in `BASIN_RAFT_PEERS` — the server validates this at startup and refuses to start on a mismatch. |
| `BASIN_RAFT_PEERS` | Comma-separated `id@host:port` list of **all** cluster voters including this node, e.g. `1@10.0.0.1:6010,2@10.0.0.2:6010,3@10.0.0.3:6010`. The same value should be set on every node. Duplicate ids are a startup error. |
| `BASIN_RAFT_BOOTSTRAP` | Set to `1` on **exactly one** node for the initial cluster bring-up. The designated node calls `initialize` with the full peer set when its raft log is empty. On subsequent restarts with a non-empty log this flag is silently ignored — `initialize` is skipped (source: `build_raft_wal` in `main.rs` gates on `last_log_index == 0 && commit_index == 0`). Do not set on more than one node — you will initialize two disjoint clusters. |

Validation at startup (`parse_raft_env` in `main.rs`): `BASIN_NODE_ID`
must be > 0; `BASIN_RAFT_PEERS` must be non-empty; the node's own id
must appear in the peer list with an address matching `BASIN_RAFT_BIND`;
duplicate ids are rejected. Config errors are hard startup failures, not
warnings.

### Raft transport mTLS (optional)

The tonic transport defaults to plaintext for a private cluster network
(VPC / 6PN). For deployments where the raft port is reachable from outside
a fully trusted network, enable mutual TLS:

| Var | Purpose |
|---|---|
| `BASIN_RAFT_TLS_CERT` | PEM path to this node's leaf certificate (signed by the cluster CA). |
| `BASIN_RAFT_TLS_KEY` | PEM path to this node's private key. |
| `BASIN_RAFT_TLS_CA` | PEM path to the cluster CA bundle used to verify peers. |
| `BASIN_RAFT_TLS_DOMAIN` | (optional) SNI / verification hostname; default `basin-raft`. Leaf certs must carry this as a SAN. |

All three of CERT/KEY/CA are required together — a partial config is a
**startup error** (no silent plaintext fallback). With TLS on, bare
`host:port` peers are dialed over `https://` automatically. Both ends
present a CA-signed cert and verify the peer, so a node that cannot prove
cluster membership is rejected at the handshake. Source:
`crates/basin-wal/src/raft_net/tls.rs`; see
[`runbooks/failover.md`](./runbooks/failover.md#raft-mode-mtls-setup) for
the operator walkthrough.

**v1 caveats:**

- **One raft group per region (no cross-region quorum).** A project's
  `home_region` selects which region's raft group owns its writes
  (`basin_common::raft_group_for`); writes to a non-home region are
  forwarded or rejected at the engine region gate. Cross-region data sync
  is by S3 cross-region replication at the bucket layer, not raft.
  Multi-region is by independent per-region deployment.
- **`GET /admin/v1/cluster` not yet wired.** `RaftWal::cluster_status()`
  exists and is logged at startup; the HTTP route is an open seam in
  `main.rs` (`let _ = &raft_wal` with a TODO comment). Use the startup
  log for cluster observability until the route lands.
- **Per-project counters not plumbed through raft.** `attach_project_counters`
  is a no-op in `RaftWal`; per-project ops metrics in raft mode are a
  follow-up.

### Writer leases (optional, `BASIN_WAL_MODE=local` use-case)

| Var | Values | Purpose |
|---|---|---|
| `BASIN_LEASE_MODE` | `off` (default) \| `required` | `off` is the unchanged single-replica behaviour (no enforcement). `required` wires the catalog's `LeaseRegistry` into the shard: each `(project, partition)` must hold a writer lease before writes proceed; a `LeaseNotHeld` (SQLSTATE 40001, retryable) is returned otherwise. Requires `BASIN_SHARD_ENABLED=1` — a startup error otherwise. |
| `BASIN_REPLICA_ID` | any string | Stable lease-holder id for this node. Default: `host:pid:salt` (changes on restart). Set a stable value for predictable lease-transfer on rolling restarts. |
| `BASIN_LEASE_TTL_SECS` | `15` (default) | Lease TTL. A node that stops heartbeating loses its lease after this window; peers can steal it. |
| `BASIN_LEASE_RENEW_SECS` | `5` (default) | Heartbeat cadence. The background renewal loop (shared with compaction/eviction) pings the catalog on this interval. |

**Precedence:** when `BASIN_WAL_MODE=raft` and `BASIN_LEASE_MODE=required`
are both set, raft leadership is the write fence and **supersedes the
lease**. The server logs `"lease mode: required — but BASIN_WAL_MODE=raft
is set; raft leadership supersedes the writer lease"` and wires both — but
a non-leader write is refused by the raft check before the lease is
consulted. The lease registry is wired but redundant. Source:
`services/basin-server/src/main.rs` (lease-mode wiring block).

### Per-project connection ceiling

| Var / Route | Purpose |
|---|---|
| `POST /admin/v1/projects/:id/max-connections` | Set the per-project pgwire connection ceiling. Enforced on every new connection via `CatalogConnectionLimitProvider` + `ConnectionLimiter`. Exceeding the ceiling returns SQLSTATE `53300` (`too_many_connections`). Default for projects with no stored ceiling: 25 (Free tier). Source: `services/basin-server/src/main.rs` (connection-limiter wiring), `crates/basin-router/src/connection_limit.rs`. |

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
| Catalog backend | small managed Postgres (`BASIN_CATALOG=postgres://…`) | Catalog state (project list, table schemas, snapshot manifests, file refs, compaction watermarks) lives in Postgres — it is the durable commit point. Fly Postgres / Neon smallest tier suffices (~50 MB per 10k projects, low QPS). The `memory` default is volatile: dev/test only. Auth state (`basin_auth_users` / `basin_auth_refresh_tokens` / …) needs no extra database — it lives in each project's own storage under the `basin_auth` schema, reached in-process (ADR 0013). |
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

By default (ADR 0013) `basin-auth` stores `basin_auth_users`,
`basin_auth_refresh_tokens`, and `basin_auth_email_tokens` in each project's
own storage under the `basin_auth` schema prefix, reached in-process via
`EngineAuthStore` over a `ProjectSession` — no loopback pgwire hop. One
process, one durability domain, one backup target. That is the recommended
shape for ≥ 99% of self-hosted deploys. Set `BASIN_AUTH_CATALOG_DSN` to
route auth state at an external Postgres instead (the escape hatch below).

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

Because basin-auth's tables live in the project's own `basin_auth` schema
(the in-process shape described above), wiping them does not require
destroying the WAL or the application data tables — it is just a series of
`DROP TABLE`s scoped to the `basin_auth_*` namespace. The
`basinctl reset-auth` command bundles that into one safe call.

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

# External-auth-catalog deploys (BASIN_AUTH_CATALOG_DSN set): point at it.
basinctl reset-auth --yes \
  --engine-url postgres://user:pass@10.0.0.7:5432/basin?sslmode=disable
```

The command refuses to run without `--yes`. It runs `DROP TABLE IF EXISTS`
for each `basin_auth_*` table in the project's `basin_auth` schema in
child-before-parent order. By default the
per-project pgwire-credentials table (`basin_auth_auth_project_credentials`)
is left intact — pass `--include-project-creds` to drop it as well.

**What it does not touch**

- Application / customer data tables — only identifiers in the
  `basin_auth_*` namespace are affected.
- WAL segments, Iceberg snapshots, object-store buckets — there is no object-store
  cleanup involved.
- Application project entries on the router — auth tables are rebootstrapped
  in each project's `basin_auth` schema on next start (ADR 0013).

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
- [ADR 0023 — Leases and partition routing](./decisions/0023-leases-and-partition-routing.md)
- [`runbooks/failover.md`](./runbooks/failover.md) — raft-mode 3-node walkthrough, leader-loss behavior, procedures
- [`runbooks/durability.md`](./runbooks/durability.md) — WAL mode durability ladder
- [`TASK.md`](../TASK.md) — Phase 6 production hardening checklist
- [`CAPABILITIES.md`](../CAPABILITIES.md) — what's shipped vs deferred
