# Basin viability suite

Six locally-runnable tests that, together, are evidence the architecture is
plausibly better than Postgres / Neon / Supabase / Turso for the wedge
customer (multi-tenant SaaS with mostly-idle tenants). Run with `cargo test
-p basin-integration-tests --tests viability_ --no-fail-fast -- --nocapture`.

| # | Test                                       | Bar                  | Evidence for                                       |
|---|--------------------------------------------|----------------------|----------------------------------------------------|
| 1 | `viability_compression_ratio`              | parquet >=10x vs CSV | columnar substrate beats row-store on audit logs   |
| 2 | `viability_idle_tenant_ram`                | <500 KiB / tenant    | one process holds many idle tenants for cheap      |
| 3 | `viability_predicate_pushdown`             | <1% of file bytes    | point queries don't scan the table                 |
| 4 | `viability_tenant_deletion`                | <1000 ms             | tenant lifecycle is O(file_count), small constant  |
| 5 | `viability_isolation_under_load`           | 0 cross-tenant leaks | the load-bearing security invariant holds          |
| 6 | `viability_large_dataset_pointquery`       | <1000 ms / 10M rows  | datasets that would crush SQLite-class systems     |
