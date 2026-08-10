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

### Step 2 — supply credentials (TOML config OR env vars)

**Option A — TOML config (recommended, persists across runs):**

```sh
cp .basin-three-way.example.toml .basin-three-way.toml
$EDITOR .basin-three-way.toml   # fill in the three DSNs
```

The file is gitignored.  The harness loads it automatically from the repo
root (override via `BASIN_THREE_WAY_CONFIG=/path/to/cfg.toml`).  Format:

```toml
[endpoints]
neon         = "postgres://user:pass@ep-xxx.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
supabase     = "postgres://postgres.[ref]:pass@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require"
basin        = "postgres://user:pass@basin-engine-main.fly.dev:5432/db?sslmode=disable"
region_label = "fra"
# row_count   = 100000   # optional
# iterations  = 10       # optional
# out_dir     = "..."    # optional
```

Requires Python 3.11+ (stdlib `tomllib`) or `tomli` on older Python — if
neither is installed, the TOML is silently skipped and env vars are used.

**Option B — environment variables (no file on disk):**

```sh
export NEON_DATABASE_URL='postgres://user:pass@ep-xxx.eu-central-1.aws.neon.tech/neondb'
export SUPABASE_DATABASE_URL='postgres://postgres:pass@db.xxx.supabase.co:5432/postgres'
export BASIN_DATABASE_URL='postgres://user:pass@basin-engine-fra.fly.dev:5432/db'
export REGION_LABEL=fra
```

Optional tuning (either TOML or env):

```sh
export ROW_COUNT=100000   # rows to insert (default: 100000)
export ITERATIONS=10      # query repetitions for p50/p95 (default: 10)
export OUT_DIR=/path/to/output   # where to write JSON (default: ./benchmark/three_way/out/)
```

**Precedence:** explicit env vars override TOML values.  This lets you point
at a one-off endpoint via `NEON_DATABASE_URL=... ./run_three_way.sh` without
editing the file.

**Never** commit `.basin-three-way.toml` — it contains live credentials.
Only `.basin-three-way.example.toml` (placeholder values) is committed.
The harness does NOT read `.env` / `.env.*` files of any kind.

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

## How outputs flow into the landing page

The `benchmark/bundle.py` script reads `benchmark/<data_dir>/*.json` and
bundles them into `<data_dir>/results.js`.  The 3-way dashboard is already
wired in — no TOML edits required.

### Step 1 — after a live run, copy the result JSON

```sh
cp benchmark/three_way/out/three_way_fra_<timestamp>.json \
   benchmark/data_three_way/compare_three_way_fra.json
```

The target directory `benchmark/data_three_way/` and its `manifest.json` are
already committed.  The manifest lists `three_way_fra` under `compare`, which
matches the `id` field in the JSON emitted by this harness.

### Step 2 — bundle the dashboard

```sh
python3 benchmark/bundle.py --dir data_three_way
```

This writes:
- `benchmark/data_three_way/results.js` — script-tag bundle for the HTML dashboard
- `benchmark/RESULTS_three_way.md` — plain-text report with a 3-column table
- `benchmark/index_three_way.html` — the static HTML dashboard (re-rendered from
  `benchmark/template.html`)

```sh
open benchmark/index_three_way.html
```

### Step 3 — mirror to a site that renders the results

A downstream site can mirror the bundled JSON into its own
`public/benchmarks/data_three_way/` directory — that mirroring step lives in
the site's repo, not here.  It copies the JSON (including
`compare_three_way_fra.json` and `manifest.json`) out of
`basin/benchmark/data_three_way/`.  The landing page's benchmark
selector will then show the "3-way Frankfurt" option; it is **hidden** when the
directory is empty (the `.gitkeep` placeholder does not trigger the selector).

### JSON shape consumed by bundle.py

The 3-way compare reports use a superset of the standard `compare` shape:
metrics carry `neon`, `supabase`, and `basin` value columns instead of
`basin` + `postgres`.  `bundle.py`'s `render_compare()` detects this
automatically (by the presence of a `neon` or `supabase` key in any metric) and
renders a 3-column Markdown table instead of the legacy 2-column table.

The `dashboard.js` renderer in the static HTML dashboard uses the existing
`compareCard()` which reads `m.basin` and `m.postgres`; it will display only
the Basin column for 3-way results.  A future `threeWayCompareCard()` extension
to `assets/dashboard.js` would render all three columns (see Option A above)
— that is out of scope for the current wiring pass.

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
