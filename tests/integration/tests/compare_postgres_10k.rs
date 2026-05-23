//! Basin vs Postgres 18 — 10 000-row SaaS+OLAP head-to-head, Vortex.
//!
//! The small-scale card: 10k events split across 1k users. At this size
//! the entire working set fits inside PG's `shared_buffers`, so PG should
//! win MORE of the latency metrics than at 100k / 1M — that's the honest
//! picture for early-stage / hobby SaaS workloads. Basin's columnar
//! shapes (OLAP COUNT/DATE_TRUNC/analytics JOIN) should still pull ahead
//! because the win there is algorithmic (column stats, vectorised
//! aggregation), not cache-residency.
//!
//! See `compare_postgres_common.rs` for the 15-metric suite definition.

#![allow(clippy::print_stdout)]

#[path = "compare_postgres_common.rs"]
mod common;

use common::{run_full_compare, BasinFormat};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_10k() {
    run_full_compare(
        10_000,
        BasinFormat::Vortex,
        "postgres_10k",
        "Basin vs Postgres 18 (10k-row SaaS+OLAP workload, no index)",
        "At small-project scale (10k rows = 1k users + 10k events), the entire \
         working set fits in PG's shared_buffers, so PG wins more of the OLTP \
         latency metrics. Basin still leads on the OLAP shapes (COUNT(*), \
         DATE_TRUNC rollup, analytics JOIN) where the win is algorithmic. \
         Honest small-scale baseline for the 100k / 1M siblings.",
        "basin_compare10k",
    )
    .await;
}
