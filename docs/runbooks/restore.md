---
title: "Restore runbook — backup, disaster restore, per-table snapshot rollback"
nav_section: operations
sidebar_position: 55
summary: "Operator runbook for restoring Basin: consistent backups, full disaster restore from object store + catalog, the manual per-table snapshot rollback that works today, and verification."
tags: [operations, restore, backup, snapshots, runbooks]
---

# Restore runbook

How to take a restorable backup of a Basin deployment, how to restore it
after a disaster, and how to rewind a single table to an earlier
snapshot **with the mechanisms that exist today**.

> **Status honesty.** There is no `basinctl restore --to` yet — that CLI
> verb is planned, not built (today's `basinctl` subcommands are `ping`,
> `projects`, `tables`, `query`, `version`, `reset-auth`,
> `import-from-postgres`, `fn`; see `services/basinctl/src/main.rs`).
> Per-table point-in-time restore exists at the catalog level
> (`Catalog::rollback_to_snapshot`, marked 🛠 in `CAPABILITIES.md`) and is
> reachable only via the Rust API or by the manual catalog SQL procedure
> documented below. Everything in this runbook uses commands that exist.

Read [durability.md](./durability.md) first if you are not sure what is
durable when.

---

## What state Basin has, and where it lives

A restore is the reassembly of these surfaces in a consistent pair:

| Surface | Location | Restorable? |
|---|---|---|
| Table data (Parquet/Vortex files) | Object store, `{BASIN_STORAGE_ROOT_PREFIX}/projects/{project_id}/…` | Yes — this is the bulk of the bytes |
| Catalog (tables, schemas, snapshot manifests, data-file refs, auth/project config) | `BASIN_CATALOG=postgres://…` Postgres, schema `basin_catalog` (default). **`BASIN_CATALOG=memory` is volatile and cannot be restored.** | Yes, via `pg_dump` |
| WAL segments (un-compacted hot tail of shard INSERTs) | `{BASIN_WAL_ROOT_PREFIX}/wal/{project}/{partition}/{ulid}.seg` under `BASIN_WAL_DIR` (local backend) or the WAL bucket (`BASIN_WAL_BACKEND=s3\|tigris`) | Yes — replayed automatically at startup |
| Hot-tier memtable (recent point UPDATE/DELETE overlays) | Process RAM only | **No** — see [durability.md](./durability.md#hot-tier-memtable-durability-caveat) |
| Disk/page caches | `BASIN_DISK_CACHE_ROOT`, RAM | Disposable — never back up, never restore |

**Consistency rule:** the catalog references data files by object-store
path (`snapshots.data_files` JSONB). A catalog state is restorable only
against an object-store state that still contains every file it
references. Background jobs **delete** files (WAL truncation after
compaction, stripe-merge replacing old files, tiering moving hot → cold,
rollback orphan cleanup), so "catalog dump from Monday + live bucket
from Thursday" is **not** guaranteed consistent.

---

## Prerequisites

1. **Durable catalog.** `BASIN_CATALOG=postgres://…`. If you are running
   the default `memory` catalog there is nothing to restore from — fix
   this before you need this runbook.
2. **Object-store versioning enabled** on the data bucket (and on the
   WAL bucket if `BASIN_WAL_BACKEND=s3|tigris`). This is already on the
   per-region checklist in [deployment.md](../deployment.md). Versioning
   is what makes "bucket state at time T" recoverable without quiescing.
3. **Catalog backups.** Scheduled `pg_dump` of the catalog schema, or
   your managed Postgres provider's PITR.
4. Record the **project-id ↔ user mapping**. Project ids (ULIDs) appear
   in bucket prefixes and in every catalog row; a restored deployment
   must resolve the same ids (`BASIN_PROJECTS=user=<ulid>`, or the same
   auth state, which lives in the catalog/engine storage and is restored
   with it).

---

## Procedure: take a consistent backup

Cold (or quiesced) backups are the simple, supported shape today.

1. **Quiesce writes.** Stop application traffic, or stop accepting
   connections at your load balancer.
2. **Wait ~60 s.** This covers one shard compaction interval (30 s — WAL
   tail drained to columnar files) and the hot-tier overlay age window
   (15 s trigger on a 5 s tick), so the in-RAM surfaces drain to durable
   storage.
3. **Optionally stop `basin-server`** (`SIGINT`; the shutdown path
   flushes the WAL buffer — `services/basin-server/src/main.rs`).
4. **Dump the catalog:**

   ```sh
   pg_dump "$BASIN_CATALOG" \
     --schema=basin_catalog \
     --format=custom \
     --file=basin-catalog-$(date -u +%Y%m%dT%H%M%SZ).dump
   ```

   (Add every schema you configured via `BASIN_CATALOG_SCHEMA`.)
5. **Capture the object-store state.** With versioning on, recording the
   backup timestamp is sufficient (you can later restore the bucket to
   that instant). Without versioning, take a full copy now:

   ```sh
   # data files
   aws s3 sync "s3://$BUCKET/$BASIN_STORAGE_ROOT_PREFIX" ./backup/data/
   # WAL segments (only if BASIN_WAL_BACKEND=s3|tigris; local WAL lives in BASIN_WAL_DIR)
   aws s3 sync "s3://$WAL_BUCKET/$BASIN_WAL_ROOT_PREFIX/wal" ./backup/wal/
   ```

   For the local backends, archive `BASIN_DATA_DIR` and `BASIN_WAL_DIR`
   with the process stopped.
6. **Resume traffic.**

Ordering note: dump the catalog **before or at the same instant as** the
bucket capture. A bucket state *newer* than the catalog dump only adds
orphan files (harmless); a bucket state *older* than the catalog dump
can be missing files the catalog references (read errors).

---

## Procedure: full restore (disaster recovery)

Scenario: the deployment is gone (machine lost, region rebuilt) and you
have a catalog dump + an object-store state from the same backup.

1. **Provision an empty catalog Postgres** and restore the dump:

   ```sh
   pg_restore --dbname="$NEW_CATALOG_DSN" --no-owner basin-catalog-<ts>.dump
   ```

2. **Provision the object store.** If the original bucket survived, use
   it as-is. Otherwise restore the backup copy (or roll the versioned
   bucket back to the backup timestamp) under the same
   `BASIN_STORAGE_ROOT_PREFIX`. Restore the WAL prefix the same way if
   the WAL backend was S3.
3. **Restore the WAL directory** (`BASIN_WAL_DIR`) from the archive if
   the WAL backend was local and the volume is gone. If you have no WAL
   backup, you lose only the un-compacted tail — everything already
   compacted is in the data files + catalog.
4. **Start `basin-server`** with the same env as before, pointing at the
   restored stores:

   ```sh
   BASIN_CATALOG="$NEW_CATALOG_DSN" \
   BASIN_STORAGE_BACKEND=s3 \
   BASIN_STORAGE_ROOT_PREFIX=<same prefix> \
   BASIN_SHARD_ENABLED=1 \
   BASIN_WAL_BACKEND=<same as before> \
   BASIN_WAL_DIR=<restored dir, if local> \
   BASIN_BIND=0.0.0.0:5433 \
   basin-server
   ```

   Recovery is automatic: on open, the WAL lists every `*.seg` segment
   and rebuilds per-partition LSN state (`recover_partitions`,
   `crates/basin-wal/src/file_wal.rs`); the shard replays surviving WAL
   entries into its in-memory tail on first access and the compactor
   re-commits them to columnar files. There is no separate
   "recovery mode" or fsck step.
5. **Verify** (next section) before re-opening traffic.

### Expected anomalies after a crash-restore

- **Duplicate-batch window.** The shard truncates the WAL *after* the
  catalog commit (`compact_one`,
  `crates/basin-shard/src/in_process.rs`). A crash between those two
  steps means the same small batch exists both in a committed data file
  and in the WAL; replay re-ingests it and the next compaction commits
  it again — duplicate rows for that batch. The code comments call this
  the accepted worst case. Check recently-written tables for duplicate
  PKs (query below) after any unclean restore.
- **Lost hot-tier overlays.** Point UPDATE/DELETEs acked in the last
  ~15–60 s before a crash may be gone (they are registry-only — see
  [durability.md](./durability.md)). Rows revert to their pre-update
  images; nothing dangles.
- **Lost async-commit INSERT tail.** Up to 200 ms of
  `synchronous_commit = off` INSERT acks (the WAL flush window).

---

## Procedure: per-table snapshot rollback (manual PITR — works today)

Basin's catalog keeps an Iceberg-style snapshot history per table.
`Catalog::rollback_to_snapshot(project, table, snapshot_id)` rewinds the
head pointer and truncates history (implemented for the Postgres and
in-memory catalogs — `crates/basin-catalog/src/postgres.rs`). No SQL or
CLI surface invokes it for operators yet, so the manual procedure is
catalog SQL that performs exactly what that method does.

> **Warnings, read all of them.**
> - Run with `basin-server` **stopped** (the engine caches table
>   metadata; a live engine will not observe the rewind reliably and
>   its compactor can race you).
> - Make sure the WAL is **drained first** (quiesce ≥ 60 s before the
>   stop, then confirm the table's WAL prefix is empty). Any surviving
>   WAL entries for this table replay at startup and get committed at a
>   **new** snapshot — resurrecting post-target rows.
> - Data files newer than the target snapshot are left **orphaned** in
>   the object store (physical GC is v0.2 per `CAPABILITIES.md`). They
>   waste space but are harmless.
> - This rewinds **one table**. Cross-table consistency is on you.
> - Take a catalog dump first so the rollback itself is reversible.

1. **Stop the server** (after a quiesced drain, see backup procedure
   steps 1–3).
2. **List the table's snapshot history** against the catalog Postgres:

   ```sql
   SELECT snapshot_id, parent_id, operation, committed_at,
          summary_json
   FROM basin_catalog.snapshots
   WHERE project_id = '<project_ulid>'
     AND schema_name = 'public'
     AND table_name  = '<table>'
   ORDER BY snapshot_id;
   ```

   Pick the target `snapshot_id` — the newest one at or before your
   restore point (`committed_at`).
3. **Rewind**, mirroring `rollback_to_snapshot`'s transaction:

   ```sql
   BEGIN;

   -- lock out concurrent commits (none should exist; server is stopped)
   SELECT 1 FROM basin_catalog.tables
   WHERE project_id = '<project_ulid>'
     AND schema_name = 'public'
     AND table_name  = '<table>'
   FOR UPDATE;

   -- drop snapshots newer than the target
   DELETE FROM basin_catalog.snapshots
   WHERE project_id = '<project_ulid>'
     AND schema_name = 'public'
     AND table_name  = '<table>'
     AND snapshot_id > <target_snapshot_id>;

   -- move the head pointer back
   UPDATE basin_catalog.tables
   SET current_snapshot = <target_snapshot_id>
   WHERE project_id = '<project_ulid>'
     AND schema_name = 'public'
     AND table_name  = '<table>';

   COMMIT;
   ```

4. **Restart `basin-server`** and verify (next section).

### Planned: `basinctl restore --to`

The intended CLI shape is a project-scoped, timestamp-addressed wrapper
over the project-wide catalog primitives that already exist in Rust
(`list_snapshots_project_wide`, `diff_snapshots`,
`rollback_to_snapshot_project_wide` — shipped for Migration Manager
v0.2, see `crates/basin-catalog/src/lib.rs`), plus the orphan-file GC.
Until it ships, the manual procedure above is the supported path.

---

## Verification

After any restore, before re-opening traffic:

1. **Connectivity + project resolution:**

   ```sh
   basinctl ping "postgres://<user>@<host>:5433/basin"
   basinctl --url "$BASIN_URL" tables
   ```

2. **Row counts per table** (compare against pre-incident numbers or the
   backup manifest you keep alongside dumps):

   ```sh
   basinctl --url "$BASIN_URL" query "SELECT count(*) FROM <table>"
   basinctl --url "$BASIN_URL" query "SELECT min(id), max(id) FROM <table>"
   ```

3. **Duplicate check** on tables that were receiving shard INSERTs at
   crash time (covers the WAL-truncate crash window):

   ```sh
   basinctl --url "$BASIN_URL" query \
     "SELECT id, count(*) FROM <table> GROUP BY id HAVING count(*) > 1 LIMIT 10"
   ```

4. **Catalog ↔ bucket consistency** — every *live* file the catalog
   references must exist. The live set is the snapshot-chain replay the
   engine itself uses (`TableMetadata::live_data_files`: each snapshot's
   `removed_paths` drops paths, its `data_files` adds them, capped at
   `current_snapshot`). In catalog SQL:

   ```sql
   WITH events AS (
     SELECT s.project_id, s.table_name, s.snapshot_id,
            f->>'path' AS path, 'add' AS action
     FROM basin_catalog.snapshots s
     CROSS JOIN LATERAL jsonb_array_elements(s.data_files) AS f
     UNION ALL
     SELECT s.project_id, s.table_name, s.snapshot_id,
            r.path, 'remove'
     FROM basin_catalog.snapshots s
     CROSS JOIN LATERAL jsonb_array_elements_text(
       coalesce(s.removed_paths_json, '[]'::jsonb)) AS r(path)
   ),
   scoped AS (
     SELECT e.*
     FROM events e
     JOIN basin_catalog.tables t
       ON  t.project_id = e.project_id
       AND t.table_name = e.table_name
     WHERE e.snapshot_id <= t.current_snapshot
   ),
   last_event AS (
     SELECT DISTINCT ON (project_id, table_name, path)
            project_id, table_name, path, action
     FROM scoped
     ORDER BY project_id, table_name, path, snapshot_id DESC, action ASC
   )
   SELECT path FROM last_event WHERE action = 'add' ORDER BY path;
   ```

   Spot-check the result against `aws s3 ls` (or `ls` under
   `BASIN_DATA_DIR`). Missing files = the bucket state is older than the
   catalog state; restore an older catalog dump or a newer bucket state.
   (The authoritative check is simply a `SELECT count(*)` per table — a
   missing live file surfaces as a storage NotFound error.)
5. **Application spot checks** — a handful of known business rows.

---

## Rolling back a bad restore

- The restore procedure never destroys the source backup: keep the
  catalog dump and bucket version/copy until verification passes.
- A bad per-table rollback is recoverable by restoring the catalog dump
  taken in step 0 of that procedure (the deleted snapshot rows come
  back; the data files were never touched).
- With bucket versioning, object deletions are soft until lifecycle
  expiry — `aws s3api list-object-versions` recovers an over-eager
  cleanup.

---

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Restored server starts with zero tables | Catalog was `BASIN_CATALOG=memory` (volatile), or restore pointed at an empty/wrong catalog DSN/schema | Restore the catalog dump; check `BASIN_CATALOG_SCHEMA` |
| `SELECT` fails with object-store NotFound on a data-file path | Catalog state newer than bucket state (missing referenced file) | Restore the bucket to a timestamp ≥ the catalog dump, or restore an older catalog dump |
| Old rows reappear after per-table rollback | WAL tail wasn't drained before the rollback; replayed entries re-committed at a new snapshot | Re-run the rollback after confirming `wal/{project}/…` prefix is empty |
| Duplicate rows after crash recovery | Crash between catalog commit and WAL truncate (documented window in `compact_one`) | After the replayed tail has recompacted (WAL prefix empty again), rewind the table one snapshot — the duplicate file is the newest commit; or `DELETE` + re-`INSERT` the affected keys |
| Clients can't log in after restore | Auth state lives in catalog/engine storage; mapping or creds not restored, or `BASIN_PROJECTS` ids differ | Restore auth tables with the catalog; pin the same project ULIDs |
| Counts slightly lower than the last seconds before a crash | Async-commit WAL window (≤ 200 ms of INSERTs) and/or hot-tier overlay loss (point UPDATE/DELETEs) | Expected per the [durability contract](./durability.md); use `synchronous_commit = on` going forward if unacceptable |

---

## Not yet supported (honest list)

- `basinctl restore --to <timestamp>` — planned CLI; not built.
- SQL surface for listing or rolling back snapshots (`Catalog` Rust API
  + manual catalog SQL only).
- Physical GC of orphaned post-rollback files (v0.2).
- Cross-table / project-wide *atomic* restore — the project-wide catalog
  primitives exist in Rust but have no operator surface; per-table
  procedures only today.
- Continuous archiving / WAL-shipping-style PITR between snapshots — the
  restore granularity is a table's snapshot history plus whatever WAL
  tail survives.

---

## Cross-references

- [durability.md](./durability.md) — what each ack means.
- [failover.md](./failover.md) — node loss without data restore.
- [deployment.md](../deployment.md) — backup checklist items (bucket versioning).
- [Storage runbook](../operators/storage.md) — cache/object-store incident handling.
- `CAPABILITIES.md` — "Point-in-time restore (catalog level)" status row.
