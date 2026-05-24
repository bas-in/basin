# Operator guide: per-project usage and external quotas

Basin exposes a per-project SQL view, `basin_project_usage`, that surfaces the
live counters from the engine's `ProjectCounterRegistry`. Operators read this
view to drive external enforcement (quota cron, billing aggregation,
throttling decisions).

## What the view is

`basin_project_usage` is a live, per-session, per-project view. Each session
sees exactly one row — its own project's counters. Cross-project rows are
never visible (the provider is constructed with the session's `ProjectId`).

Columns:

| Column                | Type        | Meaning                                                  |
|-----------------------|-------------|----------------------------------------------------------|
| `project_id`          | TEXT        | the calling project's ULID                               |
| `bytes_read_total`    | BIGINT      | cumulative bytes read from object storage                |
| `bytes_written_total` | BIGINT      | cumulative bytes written to object storage               |
| `class_a_ops_total`   | BIGINT      | cumulative Class-A object-store ops (PUT/COPY/DELETE)    |
| `class_b_ops_total`   | BIGINT      | cumulative Class-B object-store ops (GET/HEAD/LIST)      |
| `cpu_seconds_total`   | DOUBLE      | cumulative CPU seconds (registry stores micros, /1e6)    |
| `snapshot_at`         | TIMESTAMPTZ | `now()` at scan time                                     |

Counters are monotonically increasing for the lifetime of the engine process
(a restart resets them; the basin-cloud aggregator reconciles across
restarts).

## How operators read it

Any session that opens against a project can `SELECT` the view:

```sql
SELECT * FROM basin_project_usage;
```

To use it from an external quota cron, open a session per project (the same
way the engine does), then query — there is no `WHERE project_id = ...` step
because the view is already scoped.

## v0.1 scope: observability only

Basin v0.1 does NOT enforce quotas in the engine. There is no per-project
hard ceiling, no automatic suspension, no `SQLSTATE 53400`-on-overage. The
view is purely an observability surface. Enforcement lives outside the
engine; the expected pattern is:

1. A cron job (typically in basin-cloud) opens a session per project on a
   fixed cadence (every 30s–5min).
2. It reads `basin_project_usage` for that project.
3. If the project is over budget, the cron takes action: drop the project's
   lease, throttle writes at the gateway, suspend writes via a feature flag,
   or page an operator for manual review.

## Sample enforcement queries

Find every project that has written more than 1 GiB this billing period:

```sql
SELECT project_id, bytes_written_total
FROM basin_project_usage
WHERE bytes_written_total > 1073741824
ORDER BY bytes_written_total DESC;
```

Identify CPU-hot projects (candidates for plan auditing or tier upgrades):

```sql
SELECT project_id, cpu_seconds_total, snapshot_at
FROM basin_project_usage
WHERE cpu_seconds_total > 3600;  -- > 1 CPU-hour
```

## Caveats

- **Per-process counters.** Counters are reset on engine restart. For
  long-window billing, the basin-cloud aggregator persists scrapes — do not
  rely on the engine's in-process state for billing-grade durability.
- **No back-fill on restart.** If the engine restarts mid-period, the
  counters start at zero again; the meter should compare deltas between
  snapshots, not absolute values.
- **CPU-seconds is wall-clock-ish.** It accumulates query exec time as seen
  from the engine's execute path, plus Wasm function wall-clock from
  `basin-fn`. It is not a hardware perf counter.
