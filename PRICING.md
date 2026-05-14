# Pricing

Basin is **Apache-2.0** and free to self-host forever. The pricing below is for
the **basin-cloud** managed service — same engine, run on Fly Machines + Tigris
with the dashboard, control plane, and billing wrapped around it.

A project is the unit of isolation: its own bucket prefix, its own credentials,
its own RLS policies, its own snapshots. The cost story is simple: storage is
cheap because it lives on Tigris (Fly's S3-compatible store), compute is cheap because the engine is a tokio
the **basin-cloud** managed service — same engine, run on Fly Machines + Cloudflare
R2 with the dashboard, control plane, and billing wrapped around it.

A project is the unit of isolation: its own bucket prefix, its own credentials,
its own RLS policies, its own snapshots. The cost story is simple: storage is
cheap because it lives on R2, compute is cheap because the engine is a tokio
server that holds 1,000 connections in 165 MiB of RAM, and project creation is
free because a project is metadata, not infrastructure.

Last updated: 2026-05-14.

---

## Plans

### Free — $0 / month forever

- **1 project**
- **100 MB** storage
- **Scales to zero** — project pauses after 5 min idle, resumes in under a second on first connect
- **1k SQL requests / day**
- pgwire + REST + auth all enabled
- Native vector search, JSONB, UUID, all built-in extensions
- No credit card. No expiry.

For evaluation, side projects, and learning. Upgrade in place when ready.

### Hobby — $5 / month

- **1 always-on project** (no scale-to-zero)
- **5 GB** storage included
- **50 GB / month** egress included (this is the API budget; Fly-internal Tigris traffic doesn't count against it)
- **50 GB / month** R2 egress included (R2 itself has no egress fees; this is the API budget)
- 50k SQL requests / day
- Daily backup (24-hour retention)
- Community support

Adding a second project: $1/mo (storage and compute pooled).

### Pro — $29 / month

- **10 always-on projects**
- **50 GB** storage included across all projects
- **Daily backups + point-in-time restore** to any minute in the last 7 days
- **Zero-copy branches** — fork a project via Iceberg metadata, diverge on next write, no data copy
- 500k SQL requests / day
- Email support, 24-hour response SLA
- Custom domains for REST endpoints

Adding projects beyond 10: $1/mo each.

### Scale — $99 / month

- **100 always-on projects**
- **250 GB** storage included
- **Point-in-time restore** to any minute in the last 30 days
- **Multi-region read replicas** (read-after-write within region, eventually-consistent cross-region)
- **99.95% uptime SLA**
- Priority routing — projects on Scale sit on dedicated compute pools, isolated from Free/Hobby noise
- 5M SQL requests / day
- Email + Slack support, 4-hour response SLA, named contact

Adding projects beyond 100: $0.50/mo each.

### Enterprise — talk to us

- **Unlimited projects**
- **Bring your own bucket** — your S3, your AWS account, your IAM role; basin-cloud runs the compute but never touches your data plane
- **Bring your own key** — your KMS, your CMK, your rotation policy. Every Parquet write envelope-encrypted with a per-file data key wrapped by your CMK
- **SSO** — SAML, OIDC, SCIM provisioning
- **Compliance** — SOC2 Type II report, HIPAA BAA, GDPR DPA on request
- **99.99% uptime SLA**
- **Audit log export** to your SIEM (Splunk, Datadog, Sumo Logic)
- **Dedicated compute** — single-tenant Fly Machine pools, no shared neighbours
- **Custom data residency** — pick which regions your data lives in; cross-region replication on request
- Custom contracts, MSA, indemnification
- Named TAM, 1-hour P1 response

Pricing depends on storage volume, project count, and which add-ons you select.
Typical Enterprise customers are $2k–$15k/mo.

---

## Add-ons

Available on Hobby, Pro, Scale (Enterprise terms are custom):

| Add-on | Cost |
|---|---|
| Extra storage (Hobby) | $0.02 / GB / month |
| Extra storage (Pro) | $0.015 / GB / month |
| Extra storage (Scale) | $0.01 / GB / month |
| Extra projects (Hobby / Pro) | $1 / project / month |
| Extra projects (Scale) | $0.50 / project / month |
| Always-on compute over the daily request budget | $0.05 / 100k requests |
| Cross-region replicas (Pro) | $10 / replica / month |
| Custom domain TLS (Hobby) | included on Pro+ |
| Extended PITR (Pro) — 30-day window | $20 / month |
| Audit-log export (Pro) | $50 / month |
| BYO-bucket | Enterprise only |
| BYO-key (KMS) | Enterprise only |

Tigris traffic within Fly's network is zero-egress. The "egress included" line in each plan refers to
R2 itself is zero-egress. The "egress included" line in each plan refers to
the rate at which basin-cloud will serve traffic from your compute pool before
flagging the project for review — a fair-use guard, not a metered cost.

---

## How it stays cheap

Three structural reasons Basin can charge less than per-Postgres-project
vendors:

**Storage.** ZSTD-1 Parquet vs Postgres heap on the same data is 12.5× smaller
on audit-log workloads and 3-5× smaller on broader OLTP workloads. Object storage
(Tigris on basin-cloud) is $0.02/GB/mo with zero Fly-internal egress. A project
storing 1 GB of "Postgres data" weighs about 80 MB on Basin and costs Basin about
$0.0016/mo to store.
on audit-log workloads and 3-5× smaller on broader OLTP workloads. R2 storage
itself is $0.015/GB/mo with zero egress. A project storing 1 GB of "Postgres
data" weighs about 80 MB on Basin and costs Basin about $0.0012/mo to store.

**Compute.** A from-scratch Rust + tokio server holds 1,000 connections in
~165 MiB of RAM versus ~7.9 GiB for the same Postgres footprint. One Fly Machine
running Basin handles the load that needed 50× the Fly Machines running
Postgres. The compute pool amortises across projects.

**Project creation is free.** A new project is a new bucket prefix. There's no
new VM to provision, no new Postgres process to fork, no per-DB minimum.
That's why we can give 10 projects on Pro for $29/mo — the marginal cost of
the 10th project, given the 1st, is nearly zero.

---

## Compared to other managed Postgres-like services

For 10 projects, 50 GB total storage, modest workload:

| Service | Approximate monthly cost |
|---|---|
| **Basin Pro** | **$29** — covers all 10 projects, 50 GB storage included |
| Neon (Launch) | ~$190 — $19/mo minimum × 10 projects |
| Supabase Pro | ~$250 — $25/mo per project × 10 |
| AWS RDS db.t4g.micro × 10 | ~$170 (compute) + ~$5 (storage) |
| Aurora Serverless v2 × 10 | ~$430 (idle 0.5 ACU × 730 hr × 10) + storage |

Numbers are list prices from those vendors' public pages, current as of
2026-05-14. Storage costs grow with volume; compute costs grow with always-on
hours; per-project minimums are the line item that snaps once you cross
~5 projects.

Caveat: those vendors offer features Basin does not (e.g. full PostGIS,
extensions ecosystem, native logical replication). Pick the right tool for
your workload — Basin's cost story is sharpest when project count or storage
volume drives the bill.

---

## Self-hosted

The OSS engine ([`README.md`](./README.md)) is Apache-2.0 and free to run
anywhere. Single binary, configure with env vars, point at any S3-compatible
bucket. The cloud product adds the dashboard, the control plane, the billing
glue, the per-project Fly Machine orchestration, and the operational team that
keeps it up — none of which the OSS users have to think about.

If you're operating Basin yourself:

- Buy R2 / S3 / Tigris storage directly — usually $0.01–$0.02/GB/month
- Pay your own compute (Fly Machines, Hetzner, AWS, bare metal — your call)
- Run as many projects as you want for free; the OSS bundle includes the project resolver, auth, REST, and dashboard-server APIs

The cloud product never restricts the OSS engine. Anything basin-cloud does is
something an OSS user can do too — operating it is the work you're paying us
not to do.

---

## FAQ

### What counts as an "always-on project"?

The compute pool keeps the project's session warm 24/7. Connections don't pay
a cold-start. The Free tier suspends after 5 minutes of no requests and
resumes in <1s on first connect — perceptible only on the very first request
after idle.

### Can I move from one plan to another?

Yes, in either direction, no downtime. Going up is immediate. Going down
takes effect at the next billing cycle.

### Can I move my data off Basin?

Yes. `pg_dump`-style export of every project as SQL, or direct Parquet export
from the underlying bucket. The Parquet files are standard Apache Iceberg
tables — DuckDB, Spark, Snowflake, Athena, ClickHouse, and Trino can all read
them natively.

### What happens to my data if I cancel?

90-day retention on Hobby/Pro/Scale (you can restore by resubscribing within
the window), then permanent deletion. Enterprise gets a custom retention
schedule. Free tier: data deleted at suspension after 30 days of full idle.

### Do you offer a non-profit or open-source discount?

Yes. 50% off Pro and Scale for registered non-profits and open-source projects
with public source code. Email pricing@basin.

### Is there a Basin equivalent of Supabase's Edge Functions?

Not in the OSS bundle and not on the immediate roadmap — see ADR 0006. Basin's
scope is the database + auth + REST. Edge Functions / Realtime / Storage stay
out of scope; pair Basin with a serverless functions provider of your choice.

### Can I run Basin on AWS S3 instead of Tigris?

Yes. The OSS engine takes `BASIN_STORAGE_BACKEND=s3` with standard
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars, or `BASIN_STORAGE_BACKEND=r2`
for Cloudflare R2. basin-cloud runs on Tigris because it is Fly's native store
(zero Fly-internal egress, no credential management overhead) — but Enterprise
BYO-bucket customers run on whatever object store they bring.
### Can I run Basin on AWS S3 instead of R2?

Yes. The OSS engine takes `BASIN_STORAGE_BACKEND=s3` with standard
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars. basin-cloud uses R2
because R2's zero-egress is structural to the pricing — but Enterprise BYO-bucket
customers run on whatever object store they bring.

---

## Get started

- [Sign up](https://basin.app/signup) — Free tier, no card
- [Self-host the OSS engine](./README.md#quickstart) — Apache-2.0, run anywhere
- [Read the architecture](./docs/architecture.md) — full stack, four layers
- [Compare capabilities](./CAPABILITIES.md) — every feature, status-tagged
- [Multi-project SaaS story](./docs/multi-tenancy.md) — per-project isolation, scheduler, cost math
