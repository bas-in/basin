# Basin dashboard

A static HTML dashboard that visualizes Basin's viability, scaling, and
head-to-head Postgres results. It shows 11 tests in three sections:

- **Viability** — six fixed-bar tests that each clear a hard threshold.
- **Scaling** — four scale-up curves along one axis at a time.
- **Postgres head-to-head** — one same-workload comparison vs Postgres.

The dashboard reads `data/results.js`, which is generated from the
per-test JSON reports under `data/` after each test run. Missing tests
render as "not yet run" placeholders, so you can iterate test-by-test
without breaking the page.

## Use it

```sh
# 1. Run the tests (writes data/<kind>_<id>.json per test):
cargo test -p basin-integration-tests --tests -- --nocapture

# 2. Bundle the JSON into data/results.js (so file:// works):
python3 dashboard/bundle.py

# 3. Open the dashboard. Just double-click, or:
open dashboard/index.html
```

Re-run steps 1–2 whenever you want fresh numbers.

## Why a bundle?

Browsers block `fetch()` over `file://`. Reading a `<script src=...>` is
allowed, so `bundle.py` rewrites the per-test JSONs into one
`window.__BASIN_RESULTS = {...}` script that `index.html` loads as a
plain script tag. No HTTP server needed.

If you prefer a server (e.g. for live-reload during dashboard work), run
`./serve.sh` and open `http://localhost:8000/` — `dashboard.js` falls
back to `fetch()` automatically when the bundle is absent.

## Stack

No build step, no framework. Just `index.html`, `assets/style.css`,
`assets/dashboard.js`, and Chart.js loaded from `cdn.jsdelivr.net` at a
pinned version.
