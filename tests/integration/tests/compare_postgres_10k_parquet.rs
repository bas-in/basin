//! Basin vs Postgres 18 — 10 000-row SaaS+OLAP head-to-head, Parquet.
//!
//! Sibling of `compare_postgres_10k.rs` (10k Vortex). Same workload,
//! same fairness rules, same 15-metric shape — Basin is pinned to
//! `basin.file_format='parquet'` so the dashboard renders both Basin
//! storage modes side-by-side against PG 18 at the small-project scale.
//!
//! See `compare_postgres_common.rs` for the suite definition.

#![allow(clippy::print_stdout)]

#[path = "compare_postgres_common.rs"]
mod common;

use common::{run_full_compare, BasinFormat};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_10k_parquet() {
    run_full_compare(
        10_000,
        BasinFormat::Parquet,
        "postgres_10k_parquet",
        "Basin (Parquet) vs Postgres 18 (10k-row SaaS+OLAP workload, no index)",
        "Same 10k-row small-project SaaS+OLAP workload as the Vortex card, but \
         Basin is pinned to basin.file_format='parquet'. Honest small-scale \
         baseline for the Parquet path against PG heap and the Vortex card.",
        "basin_compare10kpq",
    )
    .await;
}
