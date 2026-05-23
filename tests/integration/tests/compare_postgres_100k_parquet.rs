//! Basin vs Postgres 18 — 100 000-row SaaS+OLAP head-to-head, Parquet.
//!
//! Sibling of `compare_postgres_100k.rs` (100k Vortex). Same workload,
//! same fairness rules, same 15-metric shape — Basin is pinned to
//! `basin.file_format='parquet'` so the dashboard renders both Basin
//! storage modes side-by-side against PG 18.
//!
//! See `compare_postgres_common.rs` for the suite definition.

#![allow(clippy::print_stdout)]

#[path = "compare_postgres_common.rs"]
mod common;

use common::{run_full_compare, BasinFormat};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_100k_parquet() {
    run_full_compare(
        100_000,
        BasinFormat::Parquet,
        "postgres_100k_parquet",
        "Basin (Parquet) vs Postgres 18 (100k-row SaaS+OLAP workload, no index)",
        "Same 100k-row SaaS+OLAP workload as the Vortex card, but Basin is pinned \
         to basin.file_format='parquet'. Shows how the legacy / Iceberg-read-compat \
         Parquet path compares against both PG heap and the Vortex-default card.",
        "basin_compare100kpq",
    )
    .await;
}
