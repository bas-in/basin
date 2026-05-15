# Pricing

Basin is **Apache-2.0** and free to self-host forever. The pricing below is for
the **basin-cloud** managed service — same engine, run on Fly Machines + Tigris
with the dashboard, control plane, and billing wrapped around it.

A project is the unit of isolation: its own bucket prefix, its own credentials,
its own RLS policies, its own snapshots. The cost story is simple: storage is
cheap because it lives on Tigris (Fly's S3-compatible store), compute is cheap
because the engine is a tokio server that holds 1,000 connections in 165 MiB of
RAM, and project creation is free because a project is metadata, not
infrastructure.

**basin-cloud is serverless and usage-billed.** You pay for bytes stored
($0.023/GB/mo), operations ($0.40/1M ops), and a small per-project base
($0.02/project/mo). There is no reserved capacity, no charge for idle projects,
and no per-connection fee. The monthly plan price is the plan — the scarcity
levers are per-tier *ceilings* (a hard cap on max concurrent connections, set far
above Neon/Supabase because tokio tasks are cheap), not reservations.

**Self-hosted OSS has no limits.** Connection caps, project counts, and storage
limits are cloud control-plane concepts, not engine restrictions. If you run
basin-server yourself, the engine imposes none of them.

Last updated: 2026-05-15.

---

## Plans

### Free — $0 / month forever

- **1 project**
- **100 MB** storage cap
- **25 max concurrent connections**
- **Scales to zero** — project pauses after 5 min idle, resumes in under a second on first connect
- pgwire + REST + auth all enabled
- Native vector search, JSONB, UUID, all built-in extensions
- No credit card. No expiry.

For evaluation, side projects, and learning. Upgrade in place when ready.

### Hobby — $9 / month

- **1 project**
- **2 GB** storage cap
- **75 max concurrent connections**
- Always-on (no scale-to-zero pause)
- Daily snapshot retention, 7-day PITR
- Community support

### Pro — $39 / month

- **10 projects**
- **25 GB** storage cap across all projects
- **250 max concurrent connections**
- **Daily backups + point-in-time restore** to any minute in the last 7 days
- **Zero-copy branches** — fork a project via Iceberg metadata, diverge on next write, no data copy
- Email support, 24-hour response SLA
- Custom domains for REST endpoints

### Team — $199 / month

- **25 projects**
- **75 GB** storage cap across all projects
- **750 max concurrent connections**
- **30-day PITR**
- Audit-log retention (unlimited duration)
- Shared Slack support channel
- All Pro features

### Scale — $249 / month

- **100 projects**
- **150 GB** storage cap across all projects
- **3,000 max concurrent connections**
- **30-day PITR**
- **Multi-region read replicas** (read-after-write within region, eventually-consistent cross-region)
- **99.95% uptime SLA**
- Priority routing — projects on Scale sit on dedicated compute pools, isolated from Free/Hobby noise
- Email + Slack support, 4-hour response SLA, named contact

### Enterprise — talk to us

- **Unlimited projects**
- **10,000 max concurrent connections**
- **Bring your own bucket** — your S3, your AWS account, your IAM role; basin-cloud runs the compute but never touches your data plane
- **Bring your own key** — your KMS, your CMK, your rotation policy. Every Parquet write envelope-encrypted with a per-file data key wrapped by your CMK
- **SSO** — SAML, OIDC, SCIM provisioning
- **Compliance** — SOC2 Type II report, HIPAA BAA, GDPR DPA on request
- **99.99% uptime SLA**
- **Audit log export** to your SIEM (Splunk, Datadog, Sumo Logic)
- **Dedicated compute** — single-project Fly Machine pools, no shared neighbours
- **Custom data residency** — pick which regions your data lives in; cross-region replication on request
- Custom contracts, MSA, indemnification
- Named TAM, 1-hour P1 response

Pricing depends on storage volume, project count, and which add-ons you select.
Typical Enterprise customers are $2k–$15k/mo.

---

## Plan summary

| Plan | $/mo | Projects | Storage cap | Max concurrent connections |
|---|---|---|---|---|
| Free | $0 | 1 | 100 MB | 25 |
| Hobby | $9 | 1 | 2 GB | 75 |
| Pro | $39 | 10 | 25 GB | 250 |
| Team | $199 | 25 | 75 GB | 750 |
| Scale | $249 | 100 | 150 GB | 3,000 |
| Enterprise | contact sales | unlimited | unlimited | 10,000 |

Max concurrent connections are hard ceilings enforced by the control plane,
not reserved capacity. Connections over the ceiling wait (not error) while a
slot becomes free. Because the engine is tokio-based, these ceilings are
deliberately set far above typical Neon/Supabase connection limits — they exist
to prevent one account from exhausting the pool, not to sell capacity.

Usage over the included storage cap is billed at the standard storage rate.
Operations beyond the plan's included ops are billed at the standard ops rate.
There is no charge for idle projects.

---

## Add-ons

Available on Hobby, Pro, Team, Scale (Enterprise terms are custom):

| Add-on | Cost |
|---|---|
| Extra storage (Hobby) | $0.02 / GB / month |
| Extra storage (Pro) | $0.015 / GB / month |
| Extra storage (Team / Scale) | $0.01 / GB / month |
| Cross-region replicas (Pro) | $10 / replica / month |
| Extended PITR (Pro) — 30-day window | $20 / month |
| Audit-log export (Pro / Team) | $50 / month |
| BYO-bucket | Enterprise only |
| BYO-key (KMS) | Enterprise only |

Tigris traffic within Fly's network is zero-egress. There is no separate egress
line item for basin-cloud; the standard storage and ops rates cover the full
data path.

---

## How it stays cheap

Three structural reasons Basin can charge less than per-Postgres-project
vendors:

**Storage.** The storage format is compact Parquet (Vortex-encoded on recent
engine versions), not Postgres heap. On audit-log workloads the same data is
12.5× smaller; on broader OLTP workloads, 3–5× smaller. Object storage
(Tigris on basin-cloud) runs $0.02/GB/mo with zero Fly-internal egress. A
project storing 1 GB of "Postgres data" weighs about 80 MB on Basin and costs
Basin about $0.0016/mo to store.

**Compute.** A from-scratch Rust + tokio server holds 1,000 connections in
~165 MiB of RAM versus ~7.9 GiB for the same Postgres footprint. One Fly Machine
running Basin handles the load that needed 50× the Fly Machines running
Postgres. The compute pool amortises across projects.

**Project creation is free.** A new project is a new bucket prefix. There's no
new VM to provision, no new Postgres process to fork, no per-DB minimum.
That's why we can offer 10 projects on Pro for $39/mo — the marginal cost of
the 10th project, given the 1st, is nearly zero.

---

## Compared to other managed Postgres-like services

For 10 projects, 25 GB total storage, modest workload:

| Service | Approximate monthly cost |
|---|---|
| **Basin Pro** | **$39** — covers all 10 projects, 25 GB storage cap included |
| Neon (Launch) | ~$190 — $19/mo minimum × 10 projects |
| Supabase Pro | ~$250 — $25/mo per project × 10 |
| AWS RDS db.t4g.micro × 10 | ~$170 (compute) + ~$5 (storage) |
| Aurora Serverless v2 × 10 | ~$430 (idle 0.5 ACU × 730 hr × 10) + storage |

Numbers are list prices from those vendors' public pages, current as of
2026-05-15. Storage costs grow with volume; compute costs grow with always-on
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
glue, and the operational team that keeps it up — none of which OSS users have
to think about.

The connection caps, project limits, and storage ceilings listed above are
cloud control-plane concepts. The OSS engine enforces none of them — you can
run as many projects as you want, with as many connections as your hardware
supports.

If you're operating Basin yourself:

- Buy Tigris / S3 / MinIO storage directly — usually $0.01–$0.02/GB/month
- Pay your own compute (Fly Machines, Hetzner, AWS, bare metal — your call)
- Run as many projects as you want; the OSS bundle includes the project resolver, auth, REST, and dashboard-server APIs

The cloud product never restricts the OSS engine. Anything basin-cloud does is
something an OSS user can do too — operating it is the work you're paying us
not to do.

---

## FAQ

### What counts as a concurrent connection?

An open TCP connection to the pgwire endpoint counts against the ceiling while
it is open. Idle connections count. The ceiling is enforced by the control
plane, not the engine — connections over the cap wait for a slot, they do not
receive an error.

### Can I move from one plan to another?

Yes, in either direction, no downtime. Going up is immediate. Going down
takes effect at the next billing cycle.

### Can I move my data off Basin?

Yes. `pg_dump`-style export of every project as SQL, or direct Parquet export
from the underlying bucket. The Parquet files are standard Apache Iceberg
tables — DuckDB, Spark, Snowflake, Athena, ClickHouse, and Trino can all read
them natively.

### What happens to my data if I cancel?

90-day retention on Hobby/Pro/Team/Scale (you can restore by resubscribing
within the window), then permanent deletion. Enterprise gets a custom retention
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
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars, or
`BASIN_STORAGE_BACKEND=r2` for Cloudflare R2. basin-cloud runs on Tigris
because it is Fly's native store (zero Fly-internal egress, no credential
management overhead) — but Enterprise BYO-bucket customers run on whatever
object store they bring.

---

## Get started

- [Sign up](https://basin.app/signup) — Free tier, no card
- [Self-host the OSS engine](./README.md#quickstart) — Apache-2.0, run anywhere
- [Read the architecture](./docs/architecture.md) — full stack, four layers
- [Compare capabilities](./CAPABILITIES.md) — every feature, status-tagged
- [Multi-project SaaS story](./docs/multi-project.md) — per-project isolation, scheduler, cost math
