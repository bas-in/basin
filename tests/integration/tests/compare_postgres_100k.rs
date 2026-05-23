//! Basin vs Postgres 18 — 100 000-row SaaS+OLAP head-to-head, Vortex.
//!
//! This is the typical-SaaS / OLTP working-set card: 100k events split
//! across 10k users, no indexes, with a 15-metric suite (12 SaaS shapes +
//! 3 OLAP/time-series shapes) shared with the 10k and 1M siblings.
//!
//! See `compare_postgres_common.rs` for the suite definition, fairness
//! rules, schema-drop guard, and the bench-card contract.

#![allow(clippy::print_stdout)]

#[path = "compare_postgres_common.rs"]
mod common;

use common::{run_full_compare, BasinFormat};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres_100k() {
    run_full_compare(
        100_000,
        BasinFormat::Vortex,
        "postgres_100k",
        "Basin vs Postgres 18 (100k-row SaaS+OLAP workload, no index)",
        "At typical SaaS / OLTP scale (100k rows split across users + events), \
         Basin's columnar substrate matches or beats Postgres heap on reads \
         (point, range, aggregate, JOIN, ILIKE, pagination, COUNT(*), DATE_TRUNC, \
         analytics JOIN) and pays a per-INSERT tax on bulk writes. No indexes \
         on either side — measures substrate, not btree machinery.",
        "basin_compare100k",
    )
    .await;
}
