# Basin viability suite

Six locally-runnable tests that, together, are evidence the architecture is
plausibly better than Postgres / Neon / Supabase / Turso for the wedge
customer (multi-project SaaS with mostly-idle projects). Run with `cargo test
-p basin-integration-tests --tests viability_ --no-fail-fast -- --nocapture`.

| # | Test                                       | Bar                  | Evidence for                                       |
|---|--------------------------------------------|----------------------|----------------------------------------------------|
| 1 | `viability_compression_ratio`              | parquet >=10x vs CSV | columnar substrate beats row-store on audit logs   |
| 2 | `viability_idle_project_ram`                | <500 KiB / project    | one process holds many idle projects for cheap      |
| 3 | `viability_predicate_pushdown`             | <1% of file bytes    | point queries don't scan the table                 |
| 4 | `viability_project_deletion`                | <1000 ms             | project lifecycle is O(file_count), small constant  |
| 5 | `viability_isolation_under_load`           | 0 cross-project leaks | the load-bearing security invariant holds          |
| 6 | `viability_large_dataset_pointquery`       | <1000 ms / 10M rows  | datasets that would crush SQLite-class systems     |
