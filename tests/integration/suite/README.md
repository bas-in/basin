# Basin compliance suite

The single entry point for "run the full Basin compliance suite". It runs
the cross-cutting harnesses in order — fast first, slow last — and prints a
per-category pass/fail summary.

```sh
# From the repo root:
scripts/run-suite.sh            # fast tier only (CI default, < ~10 min)
scripts/run-suite.sh --slow     # also run the #[ignore]-gated slow tier
scripts/run-suite.sh --help     # usage
```

The runner is a thin wrapper over `cargo test`. It does not change the
Cargo layout; every category below is an ordinary `--test <name>` target
you can run individually.

## Categories (in run order)

| # | Category            | Test target(s)                        | What it proves | Tier |
|---|---------------------|---------------------------------------|----------------|------|
| 1 | SQL compatibility   | `sql_support_matrix`                  | Every SQL fragment is classified (supported / clean-error / out-of-scope); regenerates `docs/sql-support.md` | fast |
| 2 | Feature coverage    | `feature_coverage`                    | One assertion per `CAPABILITIES.md` ✅ row | fast |
| 3 | Differential (PG)   | `differential_pg`                     | Identical SQL vs real Postgres oracle (skips cleanly if `PG_DIFF_TEST_DSN` unset) | fast |
| 4 | Performance         | `perf_suite`                          | Per-shape timing with documented thresholds → `perf_suite.csv` | fast + slow |
| 5 | Perf scaling sweeps | `perf_suite -- --ignored`             | Bulk-insert throughput, GROUP BY / JOIN scaling over 10k–1M rows | slow |

The runner treats categories 1–4 (fast) as the default gate. Category 5
(and any other `#[ignore]`-gated shapes) run only with `--slow`.

## How the perf harness works

`tests/integration/tests/perf_suite.rs` is the consolidated home for
*timing* benchmarks. It unifies what used to be scattered across:

- `crates/basin-engine/tests/vortex_vs_parquet_smoke.rs` — the per-shape
  Vortex-vs-Parquet ratio battery.
- `tests/integration/tests/viability_perf_stack.rs` — the layered
  cold-start point-query stack.
- the various `viability_*` perf bars.

Each shape carries:

- a **category** (`point_query`, `scan`, `groupby`, `join`, `insert`,
  `vortex_ratio`, `overhead`),
- a **metric** (`p50_ms`, `p99_ms`, `rows_per_sec`, `ratio`),
- a documented **threshold** (regression bar — generous on shared CI; the
  point is to catch a 10×-class regression, not micro-optimise), and
- a **tier** (`Fast` runs by default; `Slow` is `#[ignore]`-gated).

Output is a CSV at the repo root (`perf_suite.csv`,
`perf_suite_scaling.csv`, `perf_suite_bulk_insert.csv`) that CI can diff
run-over-run. The fast tier must finish < 5 min and fails if any shape
regresses beyond its threshold.

### basin_stat_statements overhead (Phase 5.16 gate)

The fast tier reports the `stat_statements_p99` shape: the per-query p99
cost with the query-shape-stats recording path live. The Phase 5.16
acceptance gate is ≤ 1% p99 overhead; on shared CI that 1% relative figure
is dominated by scheduler jitter, so the *failing* condition is the
absolute p99 ceiling. Run on a dedicated perf box to validate the 1%
relative bar.

## What this suite is NOT

- **Not correctness for Vortex** — that's `vortex_parquet_differential.rs`
  and `hottier_differential.rs` (see `tests/integration/tests/DIFFERENTIAL_README.md`).
  The perf harness only times queries those harnesses have proven correct.
- **Not the viability dashboard** — the `viability_*` and `scaling_*`
  tests emit JSON sidecars for `benchmark/index.html`; see
  `tests/integration/tests/viability_README.md`. The perf suite is a
  CI-gating harness with hard thresholds, complementary to those.

## Complementary audit-driven test work

Three audit reports landed in parallel under `docs/audits/`. The test work
they unblock is **deliberately deferred** to follow-up tasks and is NOT in
this suite yet:

- `docs/audits/2026-05-21-noisy-neighbor-fairness.md` — fairness / row-cost
  cap / wall-clock-timeout tests land after the audit's fixes.
- `docs/audits/2026-05-21-wasm-functions-perf-security.md` — Wasm function
  CPU-cap + perf tests land after the harness wiring fix.
- `docs/audits/2026-05-21-billing-meter-gap.md` — billing-meter coverage is
  out of scope for this suite.

Security tests (task #21) and realtime/Wasm e2e surfaces are also deferred
to follow-ups.
