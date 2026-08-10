---
title: "Basin architecture"
nav_section: architecture
sidebar_position: 10
summary: "Four-layer design: router, shard owners, WAL service, storage. Reference doc for how the code is laid out."
---

# Basin Architecture

This document describes how Basin is put together. It is the design reference
that the code is built against. For the phased build plan see
[`TASK.md`](./TASK.md).

> Integrating from app code? See [`basin-js`](https://github.com/vul-os/basin/tree/main/sdks/js)
> (engine-direct TypeScript SDK). Running and operating a fleet? See
> [`basin-cli`](https://github.com/vul-os/basin/tree/main/cli) and
> [`deployment.md`](./deployment.md).

Basin is a bucket-native, multi-project, Postgres-compatible database. The whole
system is organized around four ideas:

1. **Projects are first-class.** Every byte on disk and every row in memory
   belongs to exactly one project, addressed by a stable `ProjectId`.
2. **The bucket is the source of truth for long-term state.** Parquet files
   under an Iceberg catalog are the canonical record. Compute is disposable.
3. **Durability is decided by Raft, not by S3.** The hot path never blocks on
   object storage.
4. **Isolation is enforced at every layer**, not just at the SQL boundary.

---

## 1. Four-layer overview

Basin is four layers, each with a single job. Requests flow top to bottom on
writes; reads short-circuit at whichever layer holds the answer.

```
                       client (psql, pgx, asyncpg, ORM)
                                     |
                                     v
            +------------------------------------------------+
            |  Layer 1: Routers (stateless)                  |
            |  - pgwire v3 termination                       |
            |  - SQL parse via libpg_query (ADR 0014);       |
            |    typed-AST dispatch -> DDL/DML handler or    |
            |    PgNode->LogicalPlan; DataFusion executes    |
            |  - RLS predicate injection                     |
            |  - placement lookup, fan-out + merge           |
            |  - OLTP vs analytical decision                 |
            +------------------------------------------------+
                                     |
                          (gRPC, mTLS, per-project token)
                                     |
                                     v
            +------------------------------------------------+
            |  Layer 2: Shard owners (stateful)              |
            |  - own (project_id, partition) -> Arrow state   |
            |  - lazy load from WAL + Parquet                |
            |  - point lookups, range scans, single-shard tx |
            |  - background compactor -> Parquet + catalog   |
            +------------------------------------------------+
                                     |
                                     v
            +------------------------------------------------+
            |  Layer 3: Regional WAL (Raft, 3-5 nodes)       |
            |  - durability boundary                         |
            |  - append keyed by (project_id, partition)      |
            |  - quorum commit to local NVMe                 |
            |  - async batched flush to S3 every ~200 ms     |
            +------------------------------------------------+
                                     |
                                     v
            +------------------------------------------------+
            |  Layer 4: Object storage + Iceberg catalog     |
            |  - Parquet under /projects/{project_id}/...      |
            |  - Iceberg REST catalog (Lakekeeper-compatible)|
            |  - WAL segment archive                         |
            |  - analytical queries read here directly       |
            +------------------------------------------------+
```

### Per-project prefix layout

Every project gets a single prefix in object storage. Nothing belonging to
project A is ever stored under project B's prefix. The layout is:

```
/projects/{project_id}/
    tables/
        {table_name}/
            metadata/                 Iceberg metadata (vN.metadata.json, manifests)
            data/yyyy/mm/dd/{ulid}.parquet
    wal/
        {partition}/{epoch}-{seq}.log archived WAL segments
    snapshots/
        {snapshot_id}.json            point-in-time references
    branches/
        {branch_name}/                copy-on-write metadata only
```

The prefix is the unit of IAM, the unit of billing, and the unit of branching.
Storage code refuses to issue any read or write whose key does not begin with
the requesting project's prefix; this check is implemented at the
`basin-storage` API boundary, not at call sites.

### What runs where

| Layer        | Crate / service                | State          | Scaling axis             |
| ------------ | ------------------------------ | -------------- | ------------------------ |
| Router       | `basin-router`                 | none           | horizontal, behind LB    |
| Auth         | `basin-auth`                   | state in per-project `basin_auth.*` schema, accessed via `Engine::open_session_as` (in-process) | horizontal, peer of router |
| REST         | `basin-rest`                   | none           | horizontal, peer of router |
| Shard owner  | `basin-shard`                  | RAM, NVMe cache| (project, partition) hash |
| WAL          | `basin-wal`                    | Raft log + S3  | one Raft group per region (or shard group) |
| Storage      | `basin-storage`, `basin-catalog` | bucket + catalog | object storage         |
| Placement    | `basin-placement`              | etcd / FDB     | quorum                   |
| Analytical   | `basin-analytical`             | none           | horizontal, off OLTP     |

### SQL frontend

The canonical SQL parser is `libpg_query` — the actual PostgreSQL parser
(`gram.y` + `scan.l`), vendored as a Rust crate ([ADR 0014][adr-0014]).
Every incoming statement is parsed by libpg_query first, then dispatched
on the typed protobuf AST.

```
client SQL  ->  libpg_query (pg_ast.rs)  ->  ParseTree (typed nodes)
                                                  |
                                                  v
                                       pg_ast::stmt_kind +
                                       reject_unsupported  (SQLSTATE 0A000)
                                                  |
                              +-------------------+-------------------+
                              v                                       v
                   DDL/DML handler (typed                Phase 2: PgNode ->
                   AST matchers — Phase 1                DataFusion LogicalPlan
                   shipped)                              (pg_plan.rs)
                              \                                   /
                               \___________________ ____________ /
                                                   v
                                       DataFusion executor + optimiser
                                       (logical-plan executor, not a
                                       SQL frontend any more)
```

The migration runs in three phases:

- **Phase 1 — shipped.** libpg_query parses every statement; typed-AST
  matchers replace the fourteen textual pre-screens that used to sit
  above sqlparser. `reject_unsupported` returns SQLSTATE 0A000 early for
  statement kinds Basin doesn't ship (`LISTEN`, `NOTIFY`, `PREPARE`,
  `DECLARE CURSOR`, `LOCK`, `VACUUM`, `CLUSTER`, `ANALYZE`,
  `CREATE EXTENSION`, `CREATE TRIGGER`, `BEGIN`/`COMMIT`/`ROLLBACK`).
- **Phase 2 — in flight.** `pg_plan.rs` translates the pg_query AST for
  SELECT (subqueries, CTEs, window functions, LATERAL) directly into a
  DataFusion `LogicalPlan`. DataFusion is demoted from SQL frontend to
  logical-plan executor.
- **Phase 3 — queued.** Window frame `EXCLUDE` / `GROUPS`,
  `GROUPING SETS` / `ROLLUP` / `CUBE`, recursive CTE depth, `MERGE`,
  lateral specifics, `TABLESAMPLE`. Each lands as a translator pass.

`sqlparser-rs` is retained as a **transitional fallback** for the
Basin-extension forms libpg_query rejects: `ALTER TABLE … SET cold_after`,
`REFRESH MATERIALIZED VIEW … WITH (full=true)`,
`ALTER TABLE … SUBSCRIBE WEBHOOK`, `ALTER TABLE … REACT ON … EXECUTE`,
and `DROP REACTOR`. That surface shrinks each phase.

Precedent: every serious PG-compat database converges on owning the
parse → plan path. DuckDB vendored libpg_query and wrote their own
translator; CockroachDB forked PG's parser into Go; Spanner-PG built a
translator; YugabyteDB forked PG outright. Basin follows DuckDB's lane.

[adr-0014]: ./decisions/0014-pg-query-as-canonical-parser.md

---

`basin-auth` is a peer crate of `basin-router`, not a separate service.
Auth tables (`basin_auth_users`, `basin_auth_refresh_tokens`,
`basin_auth_email_tokens`, etc.) are provisioned in **each project's own
storage namespace** under a `basin_auth` schema prefix when the project is
created — the same pattern Supabase uses with an `auth` schema inside
every project's own Postgres database. There is no reserved internal
project. The auth service accesses project auth state via
`Engine::open_session_as(project_id, "basin_auth_service")` — in-process,
no TCP connection, no loopback. Startup order is: engine → auth (with
`EngineAuthStore`) → `StackedProjectResolver` → pgwire; there is no
circular startup dependency. Operators who want auth state on a separate
external Postgres instance (blast-radius isolation) can supply
`BASIN_AUTH_CATALOG_DSN` as an optional override — see
[`deployment.md`](./deployment.md#optional-external-auth-catalog).
Operators reset basin-auth state via `basinctl reset-auth --yes`
(see [`deployment.md`](./deployment.md#resetting-basin-auth-state)).

---

## 2. Write path

The write path is engineered so that the client's `COMMIT` returns the moment
durability is guaranteed by Raft, and not a millisecond later. S3 work happens
in the background.

1. **Client -> router (~0.1 ms intra-AZ).** pgwire frame arrives. Router
   authenticates the connection and binds it to a `ProjectId`.
2. **Parse + plan + RLS injection (~0.5-1 ms).** `libpg_query` parses the SQL
   (ADR 0014); the typed AST dispatches to a DDL/DML handler or through the
   Phase-2 PgNode -> DataFusion LogicalPlan translator. The router rewrites
   the plan to inject `project_id = $current_project` into every base-table
   predicate. There is no opt-out for this rewrite.
3. **Placement lookup (~0.1 ms, cached).** Router asks `basin-placement` for
   the owner of `(project_id, partition)`. Result is cached with a short TTL
   and invalidated on owner-not-found responses.
4. **Router -> shard owner (gRPC, ~0.3-0.5 ms).** The plan (or its mutating
   fragment) is shipped to the owner over mTLS.
5. **Shard owner -> WAL append (~2-4 ms).** The owner serializes the mutation
   into a WAL record keyed by `(project_id, partition)` and calls Raft append.
   The WAL leader replicates to followers and waits for quorum acknowledgment
   on local NVMe.
6. **Quorum ack -> in-memory apply (~0.1 ms).** Once the WAL returns
   committed, the shard owner applies the mutation to its in-memory Arrow
   state. The client sees the row immediately on a subsequent read against
   the same shard.
7. **Router -> client COMMIT.** Total p50 budget is 2-5 ms intra-AZ; p99 is
   under 10 ms.

In parallel, decoupled from the hot path:

- **WAL batch flush (~every 200 ms or 1 MB).** The WAL leader uploads a
  segment to S3 under `/projects/{project_id}/wal/...`. Failure here triggers
  retry and alerting; it does **not** retract the durability ack.
- **Compactor (minutes).** Per-project background task drains WAL segments
  into Parquet files, then issues an atomic `append_data_files` commit to
  the Iceberg catalog. Only after the catalog commit succeeds may the WAL
  segments be garbage collected.

### Latency targets

| Step                                   | Target p50 | Target p99 |
| -------------------------------------- | ---------- | ---------- |
| Router parse + plan + RLS              | < 1 ms     | < 3 ms     |
| Router -> owner RPC (intra-AZ)         | < 0.5 ms   | < 2 ms     |
| WAL quorum commit                      | 2-3 ms     | < 5 ms     |
| End-to-end `INSERT` ack                | 2-5 ms     | < 10 ms    |
| WAL -> S3 flush (async)                | ~200 ms    | < 1 s      |
| Compactor WAL -> Parquet (per partition) | 30-300 s | n/a        |

---

## 3. Read path

Reads have two flavors: point-or-range lookups against live shard state, and
analytical scans against Iceberg. This section covers the OLTP path. For the
analytical path see section 7.

1. **Client -> router.** Same as the write path through plan + RLS injection.
2. **Placement lookup.** Router resolves the owner(s) for the keys touched by
   the query. Single-key lookups hit one owner; range scans may fan out.
3. **Router -> owner(s) (gRPC).** The owner serves the query out of its
   Arrow state in RAM. Index lookups (btree/hash) are nanoseconds; bounded
   range scans are sub-millisecond.
4. **Result merge.** Router merges results from multiple shards if the query
   fanned out, then streams to the client.

### Cold-start handling for idle projects

Idle projects are evicted from RAM after a configurable timeout (default 5
minutes). When a request arrives for an evicted or never-loaded project the
shard owner reconstructs state lazily.

1. **Router routes to owner.** Placement still knows the owner; the owner
   simply has nothing in memory for `(project_id, partition)`.
2. **Owner reads catalog.** It pulls the current Iceberg snapshot for each
   table the query touches.
3. **Owner streams the latest Parquet files** that satisfy the query
   predicate, with predicate + projection pushdown. Only relevant column
   chunks come over the wire.
4. **Owner replays the WAL tail** since the last snapshot to apply
   uncompacted writes.
5. **Owner serves the query** and keeps the warm state for subsequent
   requests.

**Cold-start target: < 200 ms for a typical small project.** Beyond that
hobbyists pick a different system, and rightly so. Achieving it depends on:

- Iceberg snapshots small enough to fetch in tens of milliseconds.
- Parquet files sized so the working set is a handful of objects, not
  thousands.
- WAL tails kept short by the compactor.
- Owner pre-fetching adjacent partitions for the same project.

---

## 4. The durability rule

> **The WAL is the durability boundary, not S3.**
>
> A write is durable when it is committed to a Raft quorum's local disks.
> Returning `COMMIT` to the client requires nothing more, and waits for
> nothing else.
>
> The S3 flush is for long-term storage and analytics. It is asynchronous,
> batched, and best-effort with retries. **Never put S3 on the hot path.**
>
> If S3 is degraded, writes continue to commit at WAL-quorum latency. Only
> when the WAL's local disks fill up — minutes to hours later — do we shed
> load. Conflating S3 availability with write availability is the single
> fastest way to ruin tail latency and uptime.

This rule is what makes the bucket-native design viable. Object storage is
cheap, durable, and globally available, but it is not low-latency and never
will be. Basin uses it where its strengths apply (long-term persistence,
analytical scans, branching) and never where its weaknesses bite
(synchronous commit on the user's request thread).

### Durability vs. exactly-once under retry

No-loss (a committed write survives a crash) is not the same as exactly-once.
If a client connection drops mid-`COPY`/`INSERT`, the client cannot always tell
whether the last batch committed, so a robust loader **resyncs by re-sending**
rows it cannot prove landed. Whether that re-send produces duplicates depends
entirely on whether the table has a key:

- **`PRIMARY KEY` / `UNIQUE` tables are exactly-once under retry.** PK/UNIQUE is
  enforced on every ingest path — plain `INSERT`, multi-row `INSERT`, and the
  `COPY` bulk fast path all run `enforce_pk_on_insert` /
  `enforce_unique_on_insert` (`crates/basin-engine/src/constraints.rs`). The
  existing-row check sources its candidate files from the authoritative
  object-store `LIST`, so it catches a key written by a *prior* committed
  statement, not just one inside the same batch. Re-sending an already-committed
  key is **rejected** with a unique violation; the table never holds a second
  copy.

- **`INSERT ... ON CONFLICT (cols) DO NOTHING` is the recommended retry shape.**
  A resync re-send is **silently absorbed** — the conflict filter drops the
  already-present keys before the unique check runs, so the re-send is a no-op
  for committed rows and admits only genuinely new ones, with no error. This is
  what a retry-safe loader should issue.

- **Keyless (no-PK, no-UNIQUE) append tables are at-least-once, not
  exactly-once.** With no key, the engine has nothing to dedup a blind re-send
  against — re-sending duplicates rows. A keyless bulk load that must be
  retry-safe has to close this client-side: declare a `PRIMARY KEY` / `UNIQUE`
  on a natural or synthetic key (and, for silent retries, use
  `ON CONFLICT DO NOTHING`). The regression suite
  `crates/basin-engine/tests/exactly_once_resync.rs` pins all three behaviours.

---

## 5. Project isolation: defense in depth

> **Top security invariant: one leaked row across projects and the project
> is dead.**

Multi-project SaaS customers are buying isolation. A single confirmed
cross-project leak — a row, a query result, an object key, an error message
that contains another project's data — is not a bug to patch and move on
from. It is a credibility-ending event for a database vendor. Every layer
enforces isolation independently so that no single failure can cause one.

### Layer 1: Router predicate injection

- Every connection is bound to exactly one `ProjectId` at authentication
  time. The binding is immutable for the life of the connection.
- The DataFusion plan rewriter injects `project_id = $current_project` into
  every base-table scan. There is no flag, hint, session variable, or
  superuser bypass. Plans that cannot be rewritten are rejected.
- User-defined `CREATE POLICY` (Postgres-syntax row-level security) layers
  on top of the mandatory project predicate; it can narrow access further,
  never widen it.
- Cross-project fuzz tests run continuously. Any escape is a P0.

### Layer 2: Shard owner physical segregation

- A shard owner process holds many projects, but their Arrow state lives in
  separate per-project arenas with separate metadata.
- RPCs into the owner carry the `ProjectId` in a signed metadata field. The
  owner refuses requests whose claimed project does not match a server-side
  authorization check against the placement service.
- The owner's query executor takes `(project_id, partition)` as required
  parameters of every storage handle. There is no "ambient" project.

### Layer 3: Bucket IAM scoped per request

- Storage credentials are minted per request and scoped to
  `arn:.../projects/{project_id}/*`. They do not grant access to any other
  project prefix.
- For BYO-bucket customers (Phase 6), the platform assumes a customer-
  provided IAM role and operates entirely under that role for that project.
- For BYO-key customers, the platform never sees plaintext; KMS decrypts
  occur in an enclave.
- Storage code reasserts the prefix invariant on every operation: the
  resolved object key must begin with the per-request project prefix or
  the call panics in tests and returns a hard error in production.

### Layer 4: Operational

- Logs and traces redact bodies by default; project data never appears in
  shared dashboards.
- A bug bounty runs from public beta onward.
- A security review gates each phase boundary.

---

## 6. Project lifecycle

Projects move through well-defined states. The control plane (`services/
control-plane`) owns transitions; data-plane components observe state via
the placement service.

| State              | Meaning                                                              | Trigger                                |
| ------------------ | -------------------------------------------------------------------- | -------------------------------------- |
| `create`           | Provisioning prefix, IAM, catalog namespace                          | API call                               |
| `active`           | At least one shard owner holds state in RAM; sub-ms reads            | First write or read after `create`     |
| `idle`             | No shard owner holds state; data lives only in WAL + Parquet         | No traffic for the eviction window     |
| `cold reactivation`| Lazy load on next request; target < 200 ms                           | Request arrives for an idle project     |
| `delete`           | Soft delete, then prefix purge after the retention window            | API call                               |
| `branch`           | Copy-on-write fork via catalog metadata; no data is copied at fork   | API call                               |

Notes on each transition:

- **create.** Allocates `ProjectId`, creates the bucket prefix, registers an
  Iceberg namespace, mints initial credentials. No shard owner is assigned
  until the first request.
- **active <-> idle.** Eviction is per `(project_id, partition)`, not per
  project. A large project may have some partitions hot and others cold.
- **cold reactivation.** Described in section 3. The owner reads the
  catalog, streams the relevant Parquet, replays the WAL tail.
- **delete.** Marks the project `deleted` in placement so routers reject
  new requests immediately. WAL segments and Parquet under the prefix are
  retained for the configured window (for restore + compliance), then
  purged with the prefix itself.
- **branch.** Creates a new project whose catalog metadata points at the
  parent's existing data files. Subsequent writes to the branch land under
  the branch's own prefix; subsequent writes to the parent do not affect
  the branch. Branching is metadata-only and runs in milliseconds.

---

## 7. Two query paths: transactional vs analytical

### Transactional path

- **Engine:** DataFusion, executing inside the shard owner against the
  in-memory Arrow state.
- **Latency:** sub-ms point lookups; low-ms bounded scans.
- **Consistency:** read-your-writes within a session; single-shard
  ACID transactions.
- **Use it for:** primary-key lookups, indexed range scans, single-shard
  joins, OLTP mutations.

### Analytical path

- **Engine:** DataFusion running in `basin-analytical`, reading Iceberg
  directly from object storage. Single engine — analytical flexibility
  comes from:
  1. Vortex scan layer with projection/predicate pushdown against zone-maps
     (skip chunks before the object-store GET).
  2. Catalog-statistics file pruning (skip irrelevant files entirely).
  3. Incremental/continuous pre-aggregation (`CREATE MATERIALIZED VIEW …
     WITH (basin.continuous)`) — make heavy aggregates nearly free at
     query time.
  4. Surgical custom DataFusion physical operators where benchmarks prove
     a gap.
  5. Stateless pooled compute over shared object storage — elastic scale-out
     enabled precisely because there is no embedded second engine to
     coordinate with.
- **Latency:** seconds to minutes for residual heavy scans; sub-second for
  pre-aggregated queries.
- **Consistency:** snapshot-isolated against the catalog snapshot picked at
  query start.
- **Use it for:** large aggregates, full-table scans, joins across many
  tables, time-travel queries, anything that would otherwise blow out the
  shard owner's RAM.

### How the router chooses

The router runs a small set of heuristics on the planned query before
dispatch:

1. **Estimated rows scanned.** Above a configurable threshold (default
   ~10 M rows or ~1 GB) the query goes analytical.
2. **Aggregation shape.** Wide group-bys and unindexed full scans go
   analytical regardless of size.
3. **Time-travel hints.** *(planned, v0.2 — see `basin-analytical`'s
   v0.2 note.)* Once shipped, any `AS OF SNAPSHOT` / `AS OF TIMESTAMP`
   clause will force analytical, since shard owners only hold the
   current state. Today the parser does not accept these clauses; the
   routing rule is documented here so the design intent is on record.
4. **Explicit hint.** Customers can pin a query with `/*+ analytical */`
   or `/*+ transactional */`. If the hint is incompatible (e.g.
   `transactional` on a 1 TB scan) the router rejects it rather than
   silently degrading the cluster.
5. **Default.** When in doubt, transactional. Misclassifying an
   analytical query as transactional hurts one query; misclassifying an
   OLTP query as analytical hurts one query *and* takes seconds to
   discover.

The two paths share the same RLS injection and per-project prefix scoping.
The analytical pool's bucket credentials are scoped per request just like
the shard owner's.

---

## 8. Multi-region

Basin starts single-region. Multi-region is Phase 6 work; this section
records the target shape so we don't paint ourselves into a corner.

### Single-region (the starting point)

- One Raft WAL group per region, 3-5 nodes across AZs.
- Shard owners and routers in the same region as the WAL.
- Object storage is regional with whatever durability the provider gives
  (S3 standard is 11 nines; that's the floor).
- Catalog (Lakekeeper-compatible, basin-native) runs in-process on the
  shard owners, backed by the regional WAL + Parquet. No external
  database is provisioned. `basin-auth` stores identity tables in each
  project's own storage namespace; auth state and table metadata share
  the same durability domain by default.

### Multi-region (target)

- **Regional WALs.** Each region runs its own Raft group. A project is
  homed in one region at a time; cross-region active-active is explicitly
  not on the roadmap (see non-goals).
- **S3 cross-region replication** for the project prefix, enabling
  disaster recovery and failover. Replication is asynchronous; failover
  semantics are documented per project SLA tier.
- **Catalog replication.** Snapshot-and-pull of the embedded catalog
  between regions, replayed at the destination — same shape as WAL
  segment shipping, no external service required. Auth state replicates
  naturally with each project's storage because it lives in the same
  per-project namespace; no separate auth replication path is needed.
  Operators who supply `BASIN_AUTH_CATALOG_DSN` (external Postgres
  override) can layer that vendor's logical replication on the auth
  backend instead. Decision recorded in Phase 6.
- **Project migration** between regions runs as: drain to WAL, snapshot
  to S3, replicate, attach in the destination region, repoint placement.
  Downtime budget is seconds, not minutes.
- **Routers** can live in any region; they look up placement and route
  to the home region of the project.

Cross-region writes pay the cross-region RTT for WAL quorum; this is a
deliberate choice, because cross-region active-active without a global
clock or global consensus produces silent data corruption, and Basin will
not ship that.

---

## 9. Non-goals

To keep the project from drifting into things it is not, Basin is
explicitly *not*:

- **A Postgres replacement for high-frequency single-project OLTP.** If
  you have one giant project doing 100k writes/sec on a single primary key
  range, run Postgres or CockroachDB. Basin's wedge is many projects,
  each modest in size, sharing infrastructure cleanly.
- **A data lake.** Iceberg is the storage layer, but Basin is not a
  general-purpose lakehouse. It is a transactional database that happens
  to expose its underlying tables to analytical engines. If you want
  Spark/Trino federation across arbitrary buckets, use a lakehouse.
- **Embedded.** Basin is a network database. There is no in-process
  mode, no SQLite-like single-binary embedding, no WASM build target on
  the roadmap.
- **Globally consistent.** A project lives in exactly one region at a
  time. There is no global serializable transaction. There is no
  multi-master. Cross-region failover is asynchronous with documented
  RPO and RTO. If you need global linearizability, this is not the tool.
- **A from-scratch implementation of Raft, the SQL parser, or the table
  format.** We integrate `openraft` (or `tikv/raft-rs`), `libpg_query`
  (the real PG parser; ADR 0014), DataFusion as a logical-plan executor,
  and Iceberg + Lakekeeper. If a PR starts implementing any of those from
  scratch, it is rejected.

---

## See also

- [`TASK.md`](./TASK.md) — phased build plan and current status.
- [`../README.md`](../README.md) — repo layout and build commands.
