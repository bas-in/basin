//! Basin vs Postgres 18 — 1 000 000-row SaaS+OLAP head-to-head, Parquet.
//!
//! Sibling of `compare_postgres.rs` (1M Vortex). Same workload, same
//! fairness rules, same shape of card — Basin is pinned to
//! `basin.file_format='parquet'` so the dashboard renders both Basin
//! storage modes (Vortex default vs Parquet legacy / Iceberg-read compat)
//! side-by-side against PG 18.
//!
//! See `compare_postgres_common.rs` for the 15-metric suite definition.

#![allow(clippy::print_stdout)]

#[path = "compare_postgres_common.rs"]
mod common;

use common::{run_full_compare, BasinFormat};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_parquet() {
    run_full_compare(
        1_000_000,
        BasinFormat::Parquet,
        "postgres_parquet",
        "Basin (Parquet) vs Postgres 18 (1M-row SaaS+OLAP workload, no index)",
        "Same 1M-row SaaS+OLAP workload as the Vortex card, but Basin is pinned to \
         basin.file_format='parquet'. Shows how the legacy / Iceberg-read-compat \
         Parquet path compares against both PG heap and the Vortex-default card.",
        "basin_compare1mpq",
    )
    .await;
}
