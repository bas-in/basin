//! Basin vs Postgres 18 — 1 000 000-row SaaS+OLAP head-to-head, Vortex.
//!
//! This is the largest of the six `compare_postgres_*` cards
//! ({10k, 100k, 1M} × {Vortex, Parquet}). They all share the same 15-metric
//! scaffold (12 SaaS/OLTP shapes + 3 OLAP/time-series shapes) so the
//! dashboard can render apples-to-apples comparisons across scales and
//! Basin storage formats.
//!
//! See `compare_postgres_common.rs` for the suite definition, fairness
//! rules, schema-drop guard, and the bench-card contract.
//!
//! This file ALSO carries `perf_smoke_pg_10k`, a fast (~seconds)
//! correctness oracle that HARD-asserts Basin's point/range/aggregate
//! results against PG and prints a perf table. It's NOT one of the six
//! comparison cards — it's a loop-able regression guard for Vortex-default
//! work and lives here for historical reasons (was always co-located with
//! the 1M Vortex test).

#![allow(clippy::print_stdout)]

use std::time::Instant;

use arrow_array::{Array, Int64Array};
use basin_common::ProjectId;
use basin_engine::ExecResult;

#[path = "compare_postgres_common.rs"]
mod common;

use common::{
    build_basin_engine, median, run_full_compare, try_connect, BasinFormat, SchemaGuard,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_5_compare_postgres() {
    run_full_compare(
        1_000_000,
        BasinFormat::Vortex,
        "postgres",
        "Basin vs Postgres 18 (1M-row SaaS+OLAP workload, no index)",
        "At 1M rows on the SaaS shape (users + events, joins, ILIKE, pagination, \
         bulk DML, COUNT(*), DATE_TRUNC rollup, analytics JOIN), Basin's columnar \
         substrate uses dramatically less disk than PG heap and wins the OLAP \
         shapes while matching/beating PG on selective reads. No indexes either \
         side — measures substrate, not btree machinery.",
        "basin_compare1m",
    )
    .await;
}

fn payload_for(i: i64) -> String {
    format!("payload-{:040}", i)
}

/// Fast perf + correctness smoke vs local Postgres (10 000 rows, ~seconds).
///
/// Loop-able regression signal for the Vortex-default work: it HARD-ASSERTS
/// result correctness against Postgres (point row, range COUNT/SUM, full
/// COUNT/SUM) and PRINTS a Basin(Vortex)-vs-PG timing table for the common
/// query shapes (point / range / aggregate). Perf is logged, not gated
/// (environment-sensitive); only a catastrophic-regression guard is asserted.
/// Skips cleanly when no local Postgres is reachable.
#[tokio::test]
async fn perf_smoke_pg_10k() {
    const N: i64 = 10_000;
    let (pg, conn_str) = match try_connect().await {
        Some(v) => v,
        None => {
            println!("[PERF SMOKE] postgres unavailable on 127.0.0.1:5432 — skipping");
            return;
        }
    };
    let suffix = ProjectId::new().as_ulid().to_string().to_lowercase();
    let schema = format!("basin_smoke_{suffix}");
    let _guard = SchemaGuard {
        schema: schema.clone(),
        conn_str: conn_str.clone(),
    };
    pg.simple_query(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("pg create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {schema}.events (id BIGINT, ts BIGINT, payload TEXT)"
    ))
    .await
    .expect("pg create table");

    // Seed both sides identically (single 10k batch).
    let mut pg_vals = String::with_capacity(N as usize * 48);
    let mut basin_vals = String::with_capacity(N as usize * 48);
    for id in 0..N {
        if id > 0 {
            pg_vals.push(',');
            basin_vals.push(',');
        }
        let tuple = format!("({id}, {}, '{}')", id * 1000, payload_for(id));
        pg_vals.push_str(&tuple);
        basin_vals.push_str(&tuple);
    }
    pg.simple_query(&format!(
        "INSERT INTO {schema}.events (id, ts, payload) VALUES {pg_vals}"
    ))
    .await
    .expect("pg seed");

    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    // No WITH clause → Vortex (the default under test).
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL, ts BIGINT NOT NULL, payload TEXT NOT NULL)",
    )
    .await
    .unwrap();
    sess.execute(&format!("INSERT INTO events VALUES {basin_vals}"))
        .await
        .expect("basin seed");

    let target: i64 = N / 2 + 7;
    let lo: i64 = N / 4;
    let hi: i64 = lo + N / 2; // ~5 000-row range

    // ---- Postgres reference values (correctness oracle) ----
    let pg_i64 = |c: &tokio_postgres::Row, i: usize| c.get::<usize, i64>(i);
    let pg_point: i64 = {
        let r = pg
            .query_one(
                &format!("SELECT id FROM {schema}.events WHERE id = {target}"),
                &[],
            )
            .await
            .expect("pg point");
        pg_i64(&r, 0)
    };
    let (pg_range_cnt, pg_range_sum): (i64, i64) = {
        let r = pg
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint, COALESCE(SUM(id),0)::bigint \
                     FROM {schema}.events WHERE id BETWEEN {lo} AND {hi}"
                ),
                &[],
            )
            .await
            .expect("pg range");
        (pg_i64(&r, 0), pg_i64(&r, 1))
    };
    let (pg_all_cnt, pg_all_sum): (i64, i64) = {
        let r = pg
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint, COALESCE(SUM(id),0)::bigint FROM {schema}.events"
                ),
                &[],
            )
            .await
            .expect("pg all");
        (pg_i64(&r, 0), pg_i64(&r, 1))
    };

    // ---- Basin extraction helpers ----
    async fn basin_i64s(sess: &basin_engine::ProjectSession, sql: &str) -> Vec<Vec<i64>> {
        match sess.execute(sql).await.unwrap() {
            ExecResult::Rows { batches, .. } => {
                let mut out = Vec::new();
                for b in &batches {
                    for row in 0..b.num_rows() {
                        let mut cols = Vec::with_capacity(b.num_columns());
                        for c in 0..b.num_columns() {
                            let a = b
                                .column(c)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .expect("basin smoke expects Int64 result columns");
                            cols.push(a.value(row));
                        }
                        out.push(cols);
                    }
                }
                out
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
    async fn timed(
        sess: &basin_engine::ProjectSession,
        sql: String,
    ) -> (f64, Vec<Vec<i64>>) {
        let rows = basin_i64s(sess, &sql).await; // warm + result snapshot
        let mut samples = Vec::with_capacity(3);
        for _ in 0..3 {
            let t = Instant::now();
            let _ = basin_i64s(sess, &sql).await;
            samples.push(t.elapsed().as_secs_f64() * 1000.0);
        }
        (median(&samples), rows)
    }

    // ---- Correctness (HARD asserts vs Postgres) ----
    let (basin_point_ms, point_rows) =
        timed(&sess, format!("SELECT id FROM events WHERE id = {target}")).await;
    assert_eq!(point_rows.len(), 1, "basin point query must return one row");
    assert_eq!(
        point_rows[0][0], pg_point,
        "basin point id must equal postgres"
    );

    let (basin_range_ms, range_rows) = timed(
        &sess,
        format!("SELECT COUNT(*), SUM(id) FROM events WHERE id BETWEEN {lo} AND {hi}"),
    )
    .await;
    assert_eq!(
        range_rows[0][0], pg_range_cnt,
        "basin range COUNT(*) must equal postgres"
    );
    assert_eq!(
        range_rows[0][1], pg_range_sum,
        "basin range SUM(id) must equal postgres"
    );

    let (basin_agg_ms, agg_rows) =
        timed(&sess, "SELECT COUNT(*), SUM(id) FROM events".to_string()).await;
    assert_eq!(
        agg_rows[0][0], pg_all_cnt,
        "basin full COUNT(*) must equal postgres"
    );
    assert_eq!(
        agg_rows[0][1], pg_all_sum,
        "basin full SUM(id) must equal postgres"
    );

    println!(
        "\n[PERF SMOKE vs Postgres — Vortex default, {N} rows]\n\
         {:<16}{:>14}\n\
         {:<16}{:>13.2}ms\n\
         {:<16}{:>13.2}ms\n\
         {:<16}{:>13.2}ms\n\
         (correctness vs Postgres: point/range/aggregate all verified equal)\n",
        "query",
        "basin(vortex)",
        "point",
        basin_point_ms,
        "range(~5k)",
        basin_range_ms,
        "aggregate(10k)",
        basin_agg_ms,
    );

    // Catastrophic-regression guard only (not a perf gate — env-sensitive).
    assert!(
        basin_point_ms < 1000.0,
        "basin point query p50 {basin_point_ms:.1}ms — catastrophic regression"
    );

    // Clean up the PG schema explicitly on the live async connection,
    // then DEFUSE the guard with `mem::forget`. `SchemaGuard::drop` does
    // `thread::spawn(|| handle.block_on(..)).join()`, a re-entrant
    // block_on + blocking join on a runtime worker that deadlocks under
    // PG-lock contention — that is the post-print "freeze" this test hit.
    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    std::mem::forget(_guard);

    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    instance.wal.close().await.unwrap();
}
