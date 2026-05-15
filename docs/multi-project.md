# Multi-project SaaS on Basin

Basin's primary positioning is "cheap Postgres on object storage." A direct
consequence of that architecture is that running thousands of isolated
projects on one cluster gets very cheap — which makes Basin a natural fit
for multi-project SaaS, where each customer is one project. This page is the
detailed story for that workload shape.

If you're a single-app developer building a side project, ignore this page —
the [main README](../README.md) is the right entry point.

---

## The economic problem with multi-project on Postgres

Postgres-as-a-Service vendors charge per project because Postgres can't
multi-project cheaply:

- A fresh Postgres process holds **~10 MB of RAM minimum** — even with zero
  rows.
- Each connection forks a backend process consuming **~7.9 MB of resident
  memory**.
- One Postgres cluster per customer means N customers cost roughly N times
  the per-customer fixed overhead.
- "Logical multi-project" via schemas + RLS works but: RLS is logical-only
  (one bad query plan can leak), schema-per-project breaks ORMs that assume
  `public`, and pg_dump per project becomes a custom data pipeline.

The wedge cases hit hardest at:

- **100+ customers with most idle** — paying $25/mo × 100 customers = $2,500/mo
  on Supabase Pro before storage. Most of that compute is unused.
- **Compliance / data-residency products** — "give each customer their own
  bucket prefix so I can prove isolation in an audit" is hard to do cleanly
  in a shared Postgres.
- **Audit-log heavy products** — storage cost grows linearly with retention.
  Postgres heap on a million audit events per customer per month is
  expensive; ZSTD-1 Parquet is 12.5× smaller.

---

## How Basin handles it structurally

**One project = one bucket prefix.** A project's data lives under
`projects/<project_id>/...` in your object store. The prefix is the IAM
boundary — a single bucket policy revokes all access to a project's data
even if every other layer is bypassed. Cross-project access is not a
software check that could be wrong; it's an IAM denial.

**Idle projects cost only their bytes.** No backend process, no warm
connection pool, no provisioned compute. A project that hasn't been touched
in a week sits on disk costing $0.015–$0.02/GB/mo on object storage — for a 100 MB project,
in a week sits on disk costing $0.015/GB/mo on R2 — for a 100 MB project,
that's $0.0015/mo, or about $0.018/year.

**Active projects share a compute pool.** The shard owner holds in-memory
state for many projects per process and evicts on idle (default 5 min). The
RAM per active project is **~1.2 KiB** versus Postgres's ~10 MB — a 10,000×
reduction. One Fly Machine running Basin handles workloads that would need
50–100 Fly Machines running Postgres.

**Per-project credentials work like managed Postgres.** `POST /admin/v1/projects`
returns `postgres://<user>:<password>@host:5433/<db>` for each project.
Customer apps connect with their own URL; Basin parses the project ID out of
the username (no global lookup table, no cross-project leakage path).
Rotation is `POST /admin/v1/projects/{user}/rotate` with the old password
invalidating immediately.

**Per-project fairness.** A semaphore caps each project at 16 concurrent
storage ops; an Earliest-Deadline-First scheduler prioritises latency-
sensitive ops (HEAD, list, small range) over bulk PUTs. One bursting customer
cannot starve every other customer. See
[ADR 0008](./decisions/0008-noisy-neighbor-fairness.md).

**Project deletion is a prefix delete.** `Storage::delete_project(project_id)`
issues a parallel LIST + bulk `DeleteObjects` under the prefix plus a
`drop_namespace` on the catalog. 100K rows / 100 files deletes in
~4 ms on local FS, ~1.5–2 s on a real S3-compatible store — versus Postgres's
~4 ms on local FS, ~1.5–2 s on Cloudflare R2 — versus Postgres's
`DROP SCHEMA CASCADE` which is faster on tiny tables (a few unlinks) but
linear-in-disk-extents on multi-GB ones.

---

## The numbers

| | Basin | Postgres | Result |
|---|---|---|---|
| **Idle-project RAM cost** (1,000 projects) | **1.2 KiB / project** | ~10 MB / project (one DB each) | **Basin wins ~10,000×** |
| **Cross-project isolation under 2,000 mixed ops** | **0 leaks** | n/a | Structural via bucket prefix |
| **Noisy-neighbor p99 degradation** | **2.27×** | n/a (per-DB cluster) | Passes the < 5× bar |
| **Project deletion** (100K rows / 100 files, local FS) | 4.77 ms | 3.47 ms | PG wins 0.73× at this size — see below |
| **Connection scaling under 1,000-conn flood** | 1,000 held, 0 refused | 100 hard cap, 900 refused | **Basin wins 10×, structural** |

> **Honest mixed result on project deletion at 100K rows on local FS:** PG's
> `DROP SCHEMA CASCADE` is a few catalog rows and an `unlink`; Basin's
> `O(file count)` deletion does 100 separate `delete()` calls. The wedge claim
> — bucket-prefix delete vs vacuum / extent walks — surfaces at scale (multi-GB
> projects on S3), not on a small tmpfs table.

Full dashboard: [`benchmark/index_localfs.html`](../benchmark/index_localfs.html).
Real-cloud (S3-compatible store) numbers: [`benchmark/index_real.html`](../benchmark/index_real.html).
Real-cloud (R2 / AWS S3) numbers: [`benchmark/index_real.html`](../benchmark/index_real.html).

---

## Cost math at scale

The per-project all-in cost on basin-cloud, for a typical 100 MB / project /
month workload with modest query traffic, lands around **$0.10–$0.20 per
project per month**. That's the headline number that makes 10,000-project
workloads feasible:

- Storage: 100 MB compressed to ~80 MB Parquet on object storage = $0.0012–$0.0016/mo
- Storage: 100 MB compressed to ~80 MB Parquet on R2 = $0.0012/mo
- Compute amortised: ~$0.05/project/mo on a shared Fly Machine pool
- Platform overhead (catalog rows, observability, billing): ~$0.05/project/mo

At 10,000 active projects with this profile: **~$1,500/mo total**. The same
workload on Supabase Pro is 10,000 × $25/mo = $250,000/mo (assuming you could
even get 10,000 projects provisioned). On Neon Launch: 10,000 × $19/mo = $190,000/mo.
The cost story is two orders of magnitude. That's the wedge.

For pricing on basin-cloud specifically, see [`PRICING.md`](../PRICING.md).

---

## Auth in a multi-project world

basin-auth runs identity per project — `signup`, `signin`, JWT issuance,
refresh-token rotation, email-link login, per-project API keys, password
reset, email verification. Auth state lives in each project's own storage
namespace (the same per-project `auth` schema model Supabase uses) — no
reserved internal project, no loopback pgwire connection, no separate
Postgres required.

`auth.uid()`, `auth.role()`, `auth.jwt()` are SQL session functions
populated from the verified JWT at connection open. Write Supabase-style RLS
policies and they transfer directly:

```sql
CREATE POLICY "own rows" ON items
  FOR ALL USING (owner_id = auth.uid());
```

Both `auth.uid()` (schema-qualified) and `auth_uid()` (underscore) spellings
work. Anonymous sessions return `NULL` / `'anon'` matching Supabase
behaviour. See [ADR 0005](./decisions/0005-auth-system.md) and
[ADR 0013](./decisions/0013-auth-per-project-schema.md).

---

## Row-Level Security

`ALTER TABLE … ENABLE ROW LEVEL SECURITY` + `CREATE POLICY` work as in
Postgres, predicate-injected at the logical-plan layer. The security suite
([`tests/integration/tests/security.rs`](../tests/integration/tests/security.rs))
runs 1,000-iteration cross-project fuzzes plus four explicit bypass shapes
(UNION, CTE, etc.). All zero-leak. A P0 RLS bypass via UNION + CTE was found
by that suite during 5.6 development and fixed before the release tag.

---

## Bring-your-own-bucket / bring-your-own-key

Enterprise customers can bring their own object store (S3 in their AWS
account) and their own KMS:

- **BYO-bucket**: basin-cloud runs the compute Fly Machines but never holds
  the bucket credentials; you grant Basin's IAM role the minimum needed
  permissions on your bucket. Egress, replication, lifecycle policies all
  stay in your control.
- **BYO-key**: every Parquet file is envelope-encrypted with a fresh
  AES-256-GCM data key, which is wrapped by your KMS CMK. Basin sees the
  wrapped key, never the plaintext data key. Rotation is your KMS's
  rotation. The OSS engine ships the `EncryptionProvider` trait
  ([`basin-storage/src/encryption.rs`](../crates/basin-storage/src/encryption.rs))
  plus per-project `ProjectStorageConfig` registry; external callers plug in
  their own KMS adapter.

This satisfies the SOC2 / HIPAA blast-radius story: customer auditors can
walk through "Basin compute never had decryption-permissioned access to your
data" and verify it from your IAM logs.

See [`PRICING.md`](../PRICING.md#plans) for the plans that include these.

---

## Where multi-project on Basin is *not* the answer

- **Each project needs the full Postgres extension ecosystem** (PL/pgPython,
  loadable .so extensions, every-niche extension on PGXN). Basin's wedge is
  "Postgres surface" not "Postgres extensions." See
  [ADR 0002](./decisions/0002-no-postgres-extensions.md).
- **Each project needs strong cross-region transactional consistency**.
  That's Spanner-class. Basin's multi-region story is read replicas with
  region-local writes — see [ADR 0001](./decisions/0001-single-region-only.md).
- **Single-project high-frequency OLTP** — Postgres / Aurora is the right
  answer. Basin's per-row write path is competitive but not
  Postgres-internal-page-cache fast for a one-DB hot workload.
- **PL/pgSQL embedded data-science workloads** — Basin's trigger / function
  model is reactor-based and SQL-bodied per
  [ADR 0012](./decisions/0012-change-event-primitive.md). If your customers
  need `PL/Python` for ML scoring inside the DB, use Postgres.

---

## See also

- [Main README](../README.md) — landing-page overview
- [PRICING.md](../PRICING.md) — basin-cloud plans, all start at 1 project
- [Architecture](./architecture.md) — the four-layer stack
- [Capabilities](../CAPABILITIES.md) — every feature, status-tagged
- [Decision log](./decisions/) — every "no" with the trigger that would change our mind
- [Security suite source](../tests/integration/tests/security.rs) — the cross-project fuzz test that backs the "0 leaks" claim
