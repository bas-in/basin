# Basin benchmark dashboard

A static HTML dashboard that visualizes Basin's viability, scaling, and
head-to-head Postgres results. Each rendered dashboard shows three
sections:

- **Viability** — fixed-bar tests that each clear a hard threshold.
- **Scaling** — scale-up curves along one axis at a time.
- **Postgres head-to-head** — same-workload comparisons against Postgres 18.

The dashboard reads `<data_dir>/results.js`, which is regenerated from
the per-test JSON reports under `<data_dir>/` after each test run.
Missing tests render as "not yet run" placeholders, so you can iterate
test-by-test without breaking the page. A companion plain-text report
lives next to each dashboard at `RESULTS_<slug>.md`, regenerated from
the same data.

## Use it

```sh
# 1. Run the tests (writes <data_dir>/<kind>_<id>.json per test):
cargo test -p basin-integration-tests --tests -- --nocapture

# 2. Bundle the JSON into <data_dir>/results.js + RESULTS_<slug>.md
#    and (re-)render index_<slug>.html from the template:
python3 benchmark/bundle.py

# 3. Open a dashboard. Just double-click, or:
open benchmark/index_localfs.html
```

Re-run steps 1–2 whenever you want fresh numbers. To bundle a single
dashboard, pass `--dir <data_dir>` (e.g. `--dir data_real`).

### Fast path: parallel LocalFS + SeaweedFS run

The serial flow above is slow. For a full LocalFS+SeaweedFS run with a
≤10 min wall-clock target, use the split harness:

```sh
./benchmark/run/run_all.sh                  # both configs, parallel
ONLY=localfs   ./benchmark/run/run_all.sh   # LocalFS groups only
ONLY=seaweedfs ./benchmark/run/run_all.sh   # SeaweedFS groups only
```

It builds the test binaries once, starts one shared SeaweedFS gateway
(trap-torn-down on exit), runs each config×category group with bounded
concurrency, rebuilds each `manifest.json` from the JSONs the run
actually produced (failed/absent cards are omitted, not carried forward
with stale numbers), re-bundles the dashboards, prints the total
wall-clock, and fails loudly if it exceeds 12 min. Individual groups are
independently runnable: `./benchmark/run/_group.sh localfs_viability`.
The legacy `./benchmark/run_pg_compare.sh` still works unchanged for the
single Postgres-compare suite. Does NOT run the real-cloud (`data_real`)
config — that needs live credentials and is operator-gated.

## Dashboards are config-driven

The set of dashboards lives in [`dashboards.toml`](./dashboards.toml).
Each row is one dashboard:

```toml
[[dashboard]]
slug             = "localfs"
storage_backend  = "localfs"        # localfs | seaweedfs | r2 | s3 | b2 | …
compute_backend  = "single-process" # single-process | multi-shard | …
environment      = "local"          # "local" or "cloud"
data_dir         = "data"           # benchmark/<data_dir>/ holds the JSONs
title            = "Basin — viability and scaling"
h1               = "Basin — LocalFS"
subtitle         = "…"
footer           = "…"
```

Every row produces:

- `benchmark/<data_dir>/results.js` — the script-tag bundle
- `benchmark/RESULTS_<slug>.md` — the plain-text mirror
- `benchmark/index_<slug>.html` — the rendered HTML

The HTML is rendered from [`template.html`](./template.html) with these
placeholders substituted in: `{{TITLE}}`, `{{H1}}`, `{{SUBTITLE}}`,
`{{FOOTER}}`, `{{DATA_DIR}}`, `{{NAV_LINKS}}`, `{{SECTION_SUFFIX}}`,
`{{ENVIRONMENT_BADGE}}`, `{{STORAGE_BADGE}}`, `{{COMPUTE_BADGE}}`. The
three badges render as small pills next to the H1 so you can see at a
glance which (storage, compute, environment) combination a dashboard
covers.

### Add a new dashboard

1. Create `benchmark/<your-data-dir>/` and write at least a
   `manifest.json` into it (the test harness handles this).
2. Append a `[[dashboard]]` block to `dashboards.toml`.
3. Run `python3 benchmark/bundle.py`. The new
   `index_<slug>.html` and `RESULTS_<slug>.md` show up alongside the
   existing ones, and every dashboard's nav links pick up the new entry.

No edits to `bundle.py` or `template.html` are required.

### Sanity rules

The bundler enforces (and refuses to render) two invariants:

- `localfs` and `seaweedfs` are local-only (`environment = "local"`).
- `r2`, `s3`, `b2`, and `r2/s3/b2` are cloud-only (`environment = "cloud"`).

If you add a new storage backend, edit `LOCAL_STORAGE_BACKENDS` /
`CLOUD_STORAGE_BACKENDS` at the top of `bundle.py` accordingly.

## Why a bundle?

Browsers block `fetch()` over `file://`. Reading a `<script src=…>` is
allowed, so `bundle.py` rewrites the per-test JSONs into one
`window.__BASIN_RESULTS = {...}` script that each `index_<slug>.html`
loads as a plain script tag. No HTTP server needed.

If you prefer a server (e.g. for live-reload while editing the dashboard
code), run `./serve.sh` and open `http://localhost:8000/` —
`dashboard.js` falls back to `fetch()` automatically when the bundle is
absent.

## Parquet vs Vortex comparison

`benchmark/vortex_compare/` is a standalone Rust binary (not a workspace member — it lives outside the main workspace so it can depend on Vortex, which requires Arrow 58, without conflicting with the workspace's Arrow 54 pin). It generates the same 1 M-row synthetic audit-log dataset used in `viability_compression_ratio.rs`, writes it once as ZSTD Parquet and once as a Vortex file using Vortex's default encodings, then prints sizes and cold full-scan times to stdout and writes `benchmark/data/compare_vortex_parquet.json`. On the numbers measured so far: with **default encodings** Vortex is ~7.5× larger than ZSTD Parquet (24 MB vs 3.2 MB); with the **BtrBlocks+compact cascade** (Zstd for strings, Pco for numerics, enabled via `vortex-file`'s `zstd` feature) Vortex shrinks to ~1.6 MB — 1.95× smaller than Parquet — while retaining a ~18× scan-speed advantage over Parquet. Run it from the repo root with:

```sh
cargo run --manifest-path benchmark/vortex_compare/Cargo.toml --release
```

## Stack

No build step, no framework. Just `template.html`, `dashboards.toml`,
`assets/style.css`, `assets/dashboard.js`, and Chart.js loaded from
`cdn.jsdelivr.net` at a pinned version. Python stdlib only on the
bundler side (`tomllib` requires Python 3.11+).
