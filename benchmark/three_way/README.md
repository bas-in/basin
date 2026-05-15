# 3-way Frankfurt benchmark — Neon vs Supabase vs Basin Cloud

This directory contains the harness for a Postgres-wire benchmark comparing
Neon, Supabase, and Basin Cloud — all in Frankfurt — using an identical
workload over the standard libpq wire protocol.

**This harness is operator-gated.** It requires live connection strings for all
three services.  It will not run without them.  See "Running" below.

---

## Files

| File | Purpose |
|---|---|
| `run_three_way.sh` | Main harness script |
| `three_way_schema.sql` | Schema applied identically to all three endpoints |
| `three_way_queries.sql` | Query battery documentation (queries are also embedded in the harness) |
| `RESULTS_three_way.template.md` | Writeup template with methodology; fill in after a live run |
| `out/` | Directory where result JSONs are written (gitignored) |

---

## Prerequisites

### `psql` (required)

The harness uses `psql` (part of libpq) to connect to all three endpoints.

```sh
# macOS
brew install libpq && brew link --force libpq

# Ubuntu / Debian
apt-get install -y postgresql-client
```

### `jq` (optional)

Used for pretty-printing the summary.  If absent, the raw JSON is printed.

```sh
brew install jq   # macOS
apt-get install -y jq   # Ubuntu
```

---

## Running

### Step 1 — provision all three endpoints in Frankfurt

- **Neon:** Create a project in region `eu-central-1` (Frankfurt).
- **Supabase:** Create a project in region `eu-central` (Frankfurt).
- **Basin Cloud:** Deploy Basin Cloud to the Frankfurt Fly.io region (`fra`).

The harness cannot verify that endpoints are actually in Frankfurt — it stamps
`REGION_LABEL` into the output JSON for provenance.  The operator is responsible
for region co-location.  Cross-region measurements will be artificially inflated
by inter-region latency and are not a fair comparison.

### Step 2 — set environment variables

```sh
export NEON_DATABASE_URL='postgres://user:pass@ep-xxx.eu-central-1.aws.neon.tech/neondb'
export SUPABASE_DATABASE_URL='postgres://postgres:pass@db.xxx.supabase.co:5432/postgres'
export BASIN_DATABASE_URL='postgres://user:pass@basin-cloud-fra.fly.dev:5432/db'
export REGION_LABEL=fra
```

Optional tuning:

```sh
export ROW_COUNT=100000   # rows to insert (default: 100000)
export ITERATIONS=10      # query repetitions for p50/p95 (default: 10)
export OUT_DIR=/path/to/output   # where to write JSON (default: ./benchmark/three_way/out/)
```

**Never** pass credentials as positional arguments or in `.env` files.  The
harness reads only the four env vars listed above.

### Step 3 — dry run (recommended first)

Validate all env vars and print the execution plan without connecting:

```sh
./benchmark/three_way/run_three_way.sh --dry-run
```

Expected output:
```
[three_way] === DRY RUN — no connections will be made ===
[three_way] Plan:
[three_way]   Region label   : fra
[three_way]   Row count      : 100000
[three_way]   Iterations     : 10
...
```

### Step 4 — run the benchmark

```sh
./benchmark/three_way/run_three_way.sh
```

The harness will:
1. Create `bench_three_way.events` on each endpoint.
2. Insert `ROW_COUNT` rows in 1,000-row batches.
3. Run the four-query battery `ITERATIONS` times per endpoint.
4. Print a summary table to stdout.
5. Write a result JSON to `benchmark/three_way/out/three_way_fra_<timestamp>.json`.
6. Drop `bench_three_way` schema on all three endpoints (idempotent teardown,
   trap-based — runs even on Ctrl-C or failure).

If any of the four required env vars is unset, the harness names the missing
variable(s) and exits non-zero without connecting to anything.

### Step 5 — review output

```
benchmark/three_way/out/three_way_fra_<timestamp>.json
```

The JSON contains one `metrics[]` array with p50/p95 for each query plus
on-disk bytes and insert time — see "JSON schema" below.

---

## JSON schema

The emitted file follows the `compare` shape consumed by the Basin benchmark
dashboard:

```jsonc
{
  "kind": "compare",
  "id": "three_way_fra",
  "name": "Neon vs Supabase vs Basin Cloud (fra, 100000 rows)",
  "claim": "...",
  "available": true,
  "region": "fra",
  "row_count": 100000,
  "iterations": 10,
  "generated_at": "@<unix-secs>",
  "metrics": [
    {
      "label": "Bulk insert 100000 rows",
      "neon":     1234,          // ms, integer; null if unavailable
      "supabase": 2345,
      "basin":    987,
      "unit": "ms",
      "better": "basin",         // "neon" | "supabase" | "basin" | "tie" | null
      "ratio_text": null,
      "note": "..."              // per-metric caveat, or null
    }
    // ... one entry per metric
  ],
  "note": "..."
}
```

### Relationship to the existing 2-column compare shape

The existing `compare_postgres.json` uses `basin` and `postgres` as the two
value columns.  This harness generalises that to three columns: `neon`,
`supabase`, and `basin`.  The `postgres` key is omitted because there is no
standalone Postgres endpoint in this comparison.

The existing `compareCard()` renderer in `assets/dashboard.js` reads `m.basin`
and `m.postgres` and renders a 2-bar chart.  To render 3-column cards you have
two options:

**Option A — extend dashboard.js (recommended for the cloud landing):**
Add a `threeWayCompareCard()` function that reads `m.neon`, `m.supabase`, and
`m.basin` and renders three bars.  The card structure (`compare-metric`,
`compare-bar`, etc.) is the same CSS — only the data binding changes.

**Option B — keep 2-column cards, show Basin vs each competitor:**
Emit two separate JSON files (`compare_neon.json`, `compare_supabase.json`),
each following the existing `{basin, postgres}` shape, where `postgres` is the
competitor.  The existing renderer handles them without change.  The harness
currently emits a single 3-column file; splitting it into two is a one-liner
`jq` post-processing step.

---

## Folding results into the cloud benchmark landing

The existing `benchmark/bundle.py` script reads `benchmark/<data_dir>/*.json`
and bundles them into `<data_dir>/results.js`.  To add the 3-way comparison:

1. Copy the result JSON to a new data directory:

   ```sh
   mkdir -p benchmark/data_three_way
   cp benchmark/three_way/out/three_way_fra_<timestamp>.json \
      benchmark/data_three_way/compare_three_way_fra.json
   ```

2. Create a minimal `manifest.json` in that directory:

   ```json
   {
     "viability": [],
     "scaling":   [],
     "compare":   ["three_way_fra"]
   }
   ```

3. Append a `[[dashboard]]` row to `benchmark/dashboards.toml` (do not modify
   `bundle.py` or `template.html`):

   ```toml
   [[dashboard]]
   slug             = "three_way"
   storage_backend  = "tigris"
   compute_backend  = "cloud"
   environment      = "cloud"
   data_dir         = "data_three_way"
   title            = "Basin Cloud — 3-way Frankfurt benchmark"
   h1               = "Basin Cloud — Frankfurt 3-way"
   subtitle         = "Neon vs Supabase vs Basin Cloud — identical workload, same region."
   footer           = "Run by the operator after deploying all three services to eu-central/fra."
   ```

4. Run the bundler:

   ```sh
   python3 benchmark/bundle.py --dir data_three_way
   ```

   This writes `benchmark/data_three_way/results.js` and
   `benchmark/RESULTS_three_way.md`.

5. Open `benchmark/index_three_way.html` to see the dashboard.

> **Note:** Step 4 will use the existing `compareCard()` renderer which shows
> only `basin` and `postgres` columns.  Implement `threeWayCompareCard()` in
> `assets/dashboard.js` (see Option A above) for the full 3-column view.

---

## Safety design

| Property | How it is enforced |
|---|---|
| Partial run impossible | All four env vars are validated before any connection is attempted; exit non-zero if any is missing, naming the missing var(s) |
| Dry-run safe | `--dry-run` prints the plan and exits 0 without connecting |
| Self-cleaning | `trap cleanup EXIT` drops `bench_three_way` schema on all three endpoints on any exit (success, failure, Ctrl-C) |
| Idempotent teardown | `DROP SCHEMA IF EXISTS … CASCADE` — safe to re-run |
| No secret leakage | URLs are redacted in dry-run output; never written to stdout in the live run; never read from files |
| No partial-endpoint run | Schema creation on each endpoint sets a flag; teardown only fires for endpoints that were actually prepared |

---

## Known limitations

- **Basin Cloud on-disk size:** `pg_total_relation_size()` reports Postgres
  heap size.  Basin stores data in Vortex on Tigris (S3-compatible object
  storage); the function may be unavailable or return a synthetic value.  The
  harness records `null` with a note in that case rather than fabricating a
  number.  A future version could query Basin's native storage API directly.
- **No cold-start measurement:** This harness does not explicitly flush OS page
  caches or force a serverless cold start.  The OSS `compare_postgres.rs` has
  a dedicated cold-start metric; that is not replicated here because it
  requires process-level control not available over a remote psql connection.
- **1-second timer fallback:** On systems where `date +%s%3N` is unavailable
  (some BSD variants), the harness falls back to `$SECONDS` (1-second
  resolution).  On Linux and macOS with GNU/BSD date this is not an issue.
- **Sequential endpoint execution:** Endpoints are benchmarked one at a time
  to avoid client-side contention.  This means the total wall-clock time is
  ~3× the per-endpoint time.
