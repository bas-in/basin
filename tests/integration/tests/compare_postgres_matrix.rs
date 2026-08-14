//! Basin vs live Postgres 18 — shape × size matrix.
//!
//! This is a companion to the `compare_postgres_{10k,100k,1m}[_parquet].rs`
//! family (see `compare_postgres_common.rs`), not a replacement. Those cards
//! own the 29-metric SaaS/OLAP scaffold at a fixed set of scales. This file
//! answers a narrower, explicitly-requested question: for a MATRIX of query
//! shapes (point lookup, range scan, full scan, aggregates, GROUP BY at two
//! cardinalities, ORDER BY with/without LIMIT, joins at two selectivities, a
//! correlated subquery, and INSERT) crossed with a MATRIX of row counts
//! (including sizes that straddle the executor's 8192-row batch boundary),
//! which engine actually served each query (Basin's owned engine vs the
//! DataFusion fallback) and how does the wall-clock compare to Postgres.
//!
//! # Why a new file instead of extending the 29-metric scaffold
//!
//! `compare_postgres_common.rs` pins its shape list, sample counts, and
//! scale list per historical card and is already 6.8k lines; folding a
//! cross-product matrix with a different shape taxonomy (LOW vs HIGH
//! cardinality GROUP BY, LIMIT-early-exit verification, join selectivity
//! variants, engine-served labeling) into it would multiply its complexity
//! rather than reuse it. Instead this file reuses its two genuinely shared,
//! pub primitives: [`common::build_basin_engine`] (engine/storage/catalog/WAL
//! bootstrap) and [`common::try_connect`]/[`SchemaGuard`]-style teardown
//! discipline (reimplemented here against a configurable host/port so this
//! file can point at an ephemeral local Postgres instance instead of the
//! hardcoded `127.0.0.1:5432`). Query shapes, seeding, sampling, and the
//! engine-served accounting below are new — nothing here duplicates a shape
//! that file already measures.
//!
//! # What can fail
//!
//! A timing is a measurement, and a measurement cannot fail. If this file only
//! timed queries it could never report a defect — a shape that returned zero
//! rows or the wrong sum would be printed as a spectacular win. So before any
//! warm-up or timed sample, every shape's statement is run ONCE on each side
//! and the two result sets are compared (see "Result verification" below); the
//! INSERT shapes are followed by a row-count comparison. Those comparisons are
//! the only assertions in this file, and the test fails if any of them
//! disagree, or if the run somehow compared nothing at all.
//!
//! # Engine-served honesty
//!
//! Basin's owned engine (`basin-plan` + `basin-exec`, the DataFusion
//! replacement under construction on this branch) is gated behind the
//! `BASIN_OWNED_ENGINE` env flag, default OFF — see `owned_engine.rs`. This
//! harness turns it ON (`std::env::set_var` at the top of the test) because
//! the whole point is to report which engine actually answered each query.
//! With the flag off, every query would silently run on DataFusion and the
//! "which engine served" column would be a constant lie. Per-shape, we read
//! [`basin_engine::Engine::owned_engine_served_count`] and
//! [`basin_engine::Engine::owned_engine_fallback_count`] before and after the
//! timed sample loop and label the shape `own` (all samples served by the
//! owned engine), `datafusion` (all samples fell back), `mixed` (some of
//! each — only possible if a shape's plan is data-dependent, which none of
//! ours are, so `mixed` should not appear and its appearance is itself a
//! finding), or `n/a` (DML — the owned-engine bridge is SELECT-only, so
//! INSERT shapes never touch it and are always DataFusion/executor-served).
//!
//! # Cache state
//!
//! Every timed shape gets two untimed warm-up passes (identical statement,
//! identical projection) on BOTH engines before any timed sample — the same
//! protocol `compare_postgres_common.rs` uses and for the same reason: the
//! first execution against cold Vortex/Parquet files pays a footer-fetch +
//! decoder-init cost that Postgres's already-hot `shared_buffers` doesn't,
//! so an asymmetric warm-up would not be comparing steady-state engine cost.
//! All numbers in this matrix are WARM (OS page cache + Basin's in-process
//! caches primed, Postgres's shared_buffers primed). No cold-storage numbers
//! are reported here — see `benchmark/BENCHMARKS.md` for Basin's cold-path
//! numbers (S3 GET latency, disk-cache stack) measured separately.
//!
//! # Timeouts
//!
//! Every single query attempt (warm-up or timed) is wrapped in
//! `tokio::time::timeout`. A shape that hangs (e.g. an un-decorrelated
//! correlated subquery going O(N²)) is reported as `TIMEOUT` for that
//! (shape, size, engine) cell instead of hanging the whole matrix — a
//! timeout is itself a finding, not a harness bug to hide.
//!
//! # Running
//!
//! Needs a reachable Postgres 18, addressed either by `PG_DIFF_TEST_DSN` (this
//! repo's standard oracle-Postgres variable — see `DIFFERENTIAL_README.md`) or
//! by the `BASIN_BENCH_PG_HOST` / `BASIN_BENCH_PG_PORT` / `BASIN_BENCH_PG_USER`
//! triple (defaults `127.0.0.1:5455`, user `postgres`) that
//! `benchmark/pg_matrix/run.sh` uses for its scratch instance. Skips cleanly
//! (prints a note saying nothing was compared, does not fail) if Postgres is
//! unreachable.
//!
//! ```sh
//! ./benchmark/pg_matrix/run.sh
//!
//! # or against an already-running server, scoped to a bounded size set:
//! PG_DIFF_TEST_DSN=postgres://pc@127.0.0.1:5432/postgres \
//!   BASIN_MATRIX_SIZES=1000,8192,8193 \
//!   cargo test -p basin-integration-tests --test compare_postgres_matrix \
//!     --profile bench-fast -- --nocapture
//!
//! # prove the verification step is live — this run MUST fail:
//! BASIN_MATRIX_SELFTEST_CORRUPT=count_star … (same command)
//! ```

#![allow(clippy::print_stdout, dead_code)]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::Array;
use basin_engine::{Engine, ExecResult};
use tokio_postgres::{Client, NoTls};

#[path = "compare_postgres_common.rs"]
mod common;

use common::build_basin_engine;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Core row-count matrix. 1k (sub-batch, everything fits in less than one
/// executor batch), 100k (~12.2 batches — the historical "small" comparison
/// card size), 1M and 10M (the two scale-up sizes every other benchmark in
/// this repo already tracks — chosen so this matrix's numbers are directly
/// comparable to `benchmark/BENCHMARKS.md`'s scale-up table).
const CORE_SIZES: &[usize] = &[1_000, 100_000, 1_000_000, 10_000_000];

/// The executor emits batches of 8192 rows (see `basin-exec`'s scan
/// operator). A benchmark that only samples 1k/100k/1M/10M can't see a
/// per-batch fixed cost or a boundary-condition bug — and a correctness bug
/// AT exactly 8192 has already happened in this repo. These two sizes
/// straddle the boundary: one full batch exactly, one full batch plus a
/// single row that forces a second (mostly-empty) batch.
const BOUNDARY_SIZES: &[usize] = &[8_192, 8_193];

/// At 10M rows we only run the shapes that matter most for the "beats
/// Postgres at every size" claim, to keep total wall-clock bounded — see
/// the module doc. Everything else (join variants beyond one, ORDER BY
/// without LIMIT, the correlated subquery, single-row INSERT loop) is
/// documented as *not run* at this size rather than silently thinned.
const CORE_SHAPES_10M: &[&str] = &[
    "pt_lookup",
    "count_star",
    "sum_avg",
    "gb_low",
    "gb_high",
    "ord_limit",
    "scan_1col",
    "join_inner_unsel",
    "ins_batch",
];

/// At the 8192/8193 boundary probe we only run the shapes most sensitive to
/// a per-batch fixed cost or an off-by-one at the batch edge.
const BOUNDARY_SHAPES: &[&str] = &["pt_lookup", "count_star", "scan_1col", "ord_limit"];

/// The full default matrix seeds 10M rows one `INSERT … VALUES` statement at a
/// time and takes well over half an hour end to end, which makes it unusable
/// for "did this change break the harness" verification and unusable in CI.
/// `BASIN_MATRIX_SIZES=1000,8192,8193` restricts the row-count axis to the
/// listed sizes. Each listed size keeps the shape filter it would have had in
/// the full matrix, so a scoped run measures exactly the cells the full run
/// would have measured — it drops cells, it never silently changes one.
/// Unset (the default) runs the complete matrix.
fn run_plan() -> Vec<(usize, Option<&'static [&'static str]>)> {
    let filter_for = |rows: usize| -> Option<&'static [&'static str]> {
        if rows == 10_000_000 {
            Some(CORE_SHAPES_10M)
        } else if BOUNDARY_SIZES.contains(&rows) {
            Some(BOUNDARY_SHAPES)
        } else {
            None
        }
    };
    match std::env::var("BASIN_MATRIX_SIZES") {
        Ok(spec) if !spec.trim().is_empty() => spec
            .split(',')
            .filter_map(|s| s.trim().replace('_', "").parse::<usize>().ok())
            .map(|rows| (rows, filter_for(rows)))
            .collect(),
        _ => CORE_SIZES
            .iter()
            .chain(BOUNDARY_SIZES.iter())
            .map(|&rows| (rows, filter_for(rows)))
            .collect(),
    }
}

const ACCOUNTS_N: i64 = 1_000;
const REGIONS_N: i64 = 20;

fn group_high_n(rows: usize) -> i64 {
    (rows as i64).min(100_000).max(1)
}

// ---------------------------------------------------------------------------
// Postgres connection (configurable host/port so this can point at a
// throwaway local instance rather than assuming one is already on :5432)
// ---------------------------------------------------------------------------

async fn connect_pg() -> Option<(Client, String)> {
    // `PG_DIFF_TEST_DSN` is this repo's standard "here is the oracle Postgres"
    // variable (see `tests/integration/tests/DIFFERENTIAL_README.md` and
    // `scripts/check-differential.sh`). Honour it first so this harness points
    // at the same server as every other PG-comparing test; the BASIN_BENCH_PG_*
    // triple stays as the fallback because `benchmark/pg_matrix/run.sh` starts
    // a scratch instance on an ephemeral port and passes it that way.
    let conn_str = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(dsn) if !dsn.trim().is_empty() => dsn,
        _ => {
            let host =
                std::env::var("BASIN_BENCH_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
            let port = std::env::var("BASIN_BENCH_PG_PORT").unwrap_or_else(|_| "5455".to_string());
            let user =
                std::env::var("BASIN_BENCH_PG_USER").unwrap_or_else(|_| "postgres".to_string());
            format!("host={host} port={port} user={user} dbname=postgres")
        }
    };
    match tokio_postgres::connect(&conn_str, NoTls).await {
        Ok((client, conn)) => {
            tokio::spawn(async move {
                let _ = conn.await;
            });
            // Pin deterministic PG plans regardless of local postgresql.conf,
            // matching the documented reproducibility protocol in
            // compare_postgres_common.rs.
            let _ = client.simple_query("SET work_mem = '64MB'").await;
            let _ = client.simple_query("SET enable_seqscan = on").await;
            // Bound the query SERVER-side, not just client-side.
            //
            // Every PG measurement below is wrapped in `tokio::time::timeout`,
            // which is purely local: dropping the future sends no
            // `CancelRequest`, so the backend keeps executing. Because this
            // harness runs every shape over ONE connection, the abandoned
            // query then blocks the next statement, and the shape *after* the
            // slow one is billed for its predecessor's time. Observed while
            // bringing `benchmark/pg_matrix/run.sh` up: a correlated-subquery
            // shape blew its budget and the run then sat 20+ minutes on the
            // FOLLOWING shape while one orphaned `EXPLAIN ANALYZE` ground away
            // (confirmed in `pg_stat_activity`). Those cells reported
            // `TIMEOUT/ERR` and looked like a Postgres result; they were a
            // harness artifact.
            //
            // 70s deliberately sits ABOVE `budget_for`'s 60s ceiling, so this
            // can only ever kill a query the harness had already given up on
            // — never one it would have accepted and recorded.
            let _ = client.simple_query("SET statement_timeout = '70s'").await;
            Some((client, conn_str))
        }
        Err(e) => {
            eprintln!("[pg_matrix] could not connect to postgres at {conn_str}: {e}");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

fn budget_for(rows: usize) -> Duration {
    let secs = 10 + (rows as f64 / 100_000.0) as u64;
    Duration::from_secs(secs.clamp(10, 60))
}

fn median(v: &[f64]) -> f64 {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = s.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        s[n / 2]
    } else {
        (s[n / 2 - 1] + s[n / 2]) / 2.0
    }
}

fn spread(v: &[f64]) -> (f64, f64) {
    let mut s = v.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        s.first().copied().unwrap_or(f64::NAN),
        s.last().copied().unwrap_or(f64::NAN),
    )
}

/// Samples per shape, scaled down at larger row counts the same way
/// `compare_postgres_common.rs::samples_for` does — a fixed sample budget
/// would make the 10M-row cells dominate total wall-clock.
fn samples_for(rows: usize) -> usize {
    if rows <= 10_000 {
        15
    } else if rows <= 100_000 {
        9
    } else if rows <= 1_000_000 {
        5
    } else {
        3
    }
}

fn parse_pg_exec_time(rows: &[tokio_postgres::SimpleQueryMessage]) -> Option<f64> {
    for m in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
            if let Some(line) = r.get(0) {
                if let Some(idx) = line.find("Execution Time:") {
                    let after = &line[idx + "Execution Time:".len()..];
                    let trimmed = after.trim();
                    if let Some(num_end) = trimmed.find(' ') {
                        if let Ok(v) = trimmed[..num_end].parse::<f64>() {
                            return Some(v);
                        }
                    }
                }
            }
        }
    }
    None
}

/// Server-side execution time for one PG statement, via `EXPLAIN ANALYZE`.
/// Excludes client round-trip/parse overhead — the same choice
/// `compare_postgres_common.rs` makes, so Basin (timed in-process, also
/// excluding any network hop) and Postgres are compared on the same basis:
/// engine execution time, not transport.
/// Bound Postgres SERVER-side, not just client-side.
///
/// `tokio::time::timeout` around `simple_query` drops the client future; it
/// does NOT cancel the query. Postgres keeps executing it, and because this
/// harness uses ONE connection, every subsequent statement queues behind the
/// abandoned one. Observed directly: at 100k rows the correlated-subquery
/// shape blew its 11s budget nine times in a row, after which a
/// `SELECT COUNT(*)` — a query that takes milliseconds — also "timed out",
/// turning a real check into a false `inconclusive`. Setting
/// `statement_timeout` to the same budget makes Postgres abort the statement
/// itself, so the timeout is attributable to the shape that caused it and the
/// connection is usable immediately afterwards.
async fn set_pg_statement_timeout(pg: &Client, budget: Duration) {
    let ms = budget.as_millis().max(1000);
    if let Err(e) = pg
        .simple_query(&format!("SET statement_timeout = {ms}"))
        .await
    {
        eprintln!("[pg_matrix] could not set statement_timeout: {e}");
    }
}

async fn pg_exec_ms(pg: &Client, sql: &str, budget: Duration) -> Option<f64> {
    let q = format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql}");
    match tokio::time::timeout(budget, pg.simple_query(&q)).await {
        Ok(Ok(r)) => parse_pg_exec_time(&r),
        Ok(Err(e)) => {
            eprintln!("[pg_matrix] pg error: {e} sql={sql}");
            None
        }
        Err(_) => {
            eprintln!("[pg_matrix] pg TIMEOUT (> {:?}) sql={sql}", budget);
            None
        }
    }
}

async fn basin_exec_ms(
    sess: &basin_engine::ProjectSession,
    sql: &str,
    budget: Duration,
) -> Option<(f64, ExecResult)> {
    let t0 = Instant::now();
    match tokio::time::timeout(budget, sess.execute(sql)).await {
        Ok(Ok(res)) => Some((t0.elapsed().as_secs_f64() * 1000.0, res)),
        Ok(Err(e)) => {
            eprintln!("[pg_matrix] basin error: {e:?} sql={sql}");
            None
        }
        Err(_) => {
            eprintln!("[pg_matrix] basin TIMEOUT (> {:?}) sql={sql}", budget);
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Result verification
//
// The timing loop above cannot tell a fast query from a wrong one. A shape
// that returns zero rows, or the wrong sum, is reported as a spectacular win.
// So before any warm-up or timed sample, every shape's statement is run ONCE
// on each side and the two result sets are compared. This is untimed and
// symmetric (one extra pass on each engine), and it is the only thing in this
// file that can fail.
//
// Comparison is order-insensitive: both sides are canonically sorted before
// the cell walk, so the check verifies the multiset of rows, not the physical
// order. That is deliberate — none of the shapes has a total order (`ORDER BY
// val` has ties once `rows > 100_000`), so comparing physical order would
// produce false divergences.
// ---------------------------------------------------------------------------

/// Above this many rows the cell-by-cell walk costs more than the whole timed
/// matrix, so those shapes are verified on row count alone and reported as
/// `rows-only` rather than silently claimed as a full match.
const VERIFY_CELL_ROW_CAP: usize = 50_000;

type Cells = Vec<Vec<Option<String>>>;

enum VerErr {
    /// The engine did not answer inside the budget. Not attributable to
    /// either side as a defect — reported as inconclusive, never as a pass.
    Timeout(String),
    /// The engine rejected or failed the statement.
    Failed(String),
}

#[derive(Clone, Debug, PartialEq)]
enum Verify {
    /// Every cell matched, on `rows` rows.
    Match { rows: usize },
    /// Row counts matched but the result was too wide to walk (see
    /// [`VERIFY_CELL_ROW_CAP`]) or is not deterministic row-for-row.
    RowsOnly { rows: usize },
    /// A real disagreement between Basin and Postgres. Fails the test.
    Diverged(String),
    /// One or both sides could not answer. Reported, not asserted.
    Inconclusive(String),
    /// Shape carries no comparable result set (the timed DML shapes).
    Skipped(&'static str),
}

impl Verify {
    fn label(&self) -> String {
        match self {
            Verify::Match { rows } => format!("match ({rows} rows)"),
            Verify::RowsOnly { rows } => format!("rows-only ({rows} rows)"),
            Verify::Diverged(why) => format!("**DIVERGED** — {why}"),
            Verify::Inconclusive(why) => format!("inconclusive — {why}"),
            Verify::Skipped(why) => format!("skipped ({why})"),
        }
    }

    fn is_diverged(&self) -> bool {
        matches!(self, Verify::Diverged(_))
    }

    fn is_compared(&self) -> bool {
        matches!(self, Verify::Match { .. } | Verify::RowsOnly { .. })
    }
}

async fn pg_rows(pg: &Client, sql: &str, budget: Duration) -> Result<Cells, VerErr> {
    match tokio::time::timeout(budget, pg.simple_query(sql)).await {
        Ok(Ok(msgs)) => {
            let mut out = Cells::new();
            for m in &msgs {
                if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                    out.push((0..r.len()).map(|i| r.get(i).map(str::to_owned)).collect());
                }
            }
            Ok(out)
        }
        Ok(Err(e)) => Err(VerErr::Failed(format!("postgres rejected it: {e}"))),
        Err(_) => Err(VerErr::Timeout(format!("postgres > {budget:?}"))),
    }
}

async fn basin_rows(
    sess: &basin_engine::ProjectSession,
    sql: &str,
    budget: Duration,
) -> Result<Cells, VerErr> {
    let res = match tokio::time::timeout(budget, sess.execute(sql)).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return Err(VerErr::Failed(format!("basin rejected it: {e:?}"))),
        Err(_) => return Err(VerErr::Timeout(format!("basin > {budget:?}"))),
    };
    let batches = match &res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { tag } => {
            return Err(VerErr::Failed(format!(
                "basin returned no result set (tag {tag})"
            )))
        }
    };
    let opts = FormatOptions::default();
    let mut out = Cells::new();
    for b in batches {
        let fmts: Vec<ArrayFormatter<'_>> = match (0..b.num_columns())
            .map(|c| ArrayFormatter::try_new(b.column(c), &opts))
            .collect::<Result<_, _>>()
        {
            Ok(f) => f,
            Err(e) => return Err(VerErr::Failed(format!("basin arrow format: {e}"))),
        };
        for r in 0..b.num_rows() {
            out.push(
                (0..b.num_columns())
                    .map(|c| {
                        if b.column(c).is_null(r) {
                            None
                        } else {
                            Some(fmts[c].value(r).to_string())
                        }
                    })
                    .collect(),
            );
        }
    }
    Ok(out)
}

/// Cell equality. Exact string match first (covers integers, text, NULL), then
/// a numeric comparison with a relative epsilon — Postgres renders `AVG` as
/// `NUMERIC` (`49999.5000000000000000`) where Basin renders `Float64`
/// (`49999.5`); those are the same number and must not be a divergence.
fn cells_equal(a: &Option<String>, b: &Option<String>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            if x == y {
                return true;
            }
            match (x.parse::<f64>(), y.parse::<f64>()) {
                (Ok(xf), Ok(yf)) => {
                    (xf - yf).abs() <= 1e-9 * xf.abs().max(yf.abs()).max(1.0)
                }
                _ => false,
            }
        }
        _ => false,
    }
}

fn canonicalize(mut rows: Cells) -> Cells {
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    rows
}

async fn verify_shape(
    pg: &Client,
    pg_sql: &str,
    sess: &basin_engine::ProjectSession,
    basin_sql: &str,
    cell_compare: bool,
    budget: Duration,
) -> Verify {
    let pg_res = pg_rows(pg, pg_sql, budget).await;
    let basin_res = basin_rows(sess, basin_sql, budget).await;

    let (pg_cells, basin_cells) = match (pg_res, basin_res) {
        (Ok(p), Ok(b)) => (p, b),
        (Err(VerErr::Timeout(w)), _) | (_, Err(VerErr::Timeout(w))) => {
            return Verify::Inconclusive(w)
        }
        (Err(VerErr::Failed(w)), Ok(_)) => {
            return Verify::Diverged(format!("postgres could not run it but basin could — {w}"))
        }
        (Ok(_), Err(VerErr::Failed(w))) => {
            return Verify::Diverged(format!("basin could not run it but postgres could — {w}"))
        }
        (Err(VerErr::Failed(p)), Err(VerErr::Failed(b))) => {
            return Verify::Inconclusive(format!("both sides failed — pg: {p}; basin: {b}"))
        }
    };

    if pg_cells.len() != basin_cells.len() {
        return Verify::Diverged(format!(
            "row count: basin {} vs postgres {}",
            basin_cells.len(),
            pg_cells.len()
        ));
    }
    let n = pg_cells.len();
    // Every shape in this matrix returns at least one row against a seeded
    // table. Two empty result sets "agree" while proving nothing, which is
    // exactly the failure mode this whole section exists to prevent.
    if n == 0 {
        return Verify::Diverged(
            "both sides returned 0 rows — this shape compared nothing".to_string(),
        );
    }
    if !cell_compare || n > VERIFY_CELL_ROW_CAP {
        return Verify::RowsOnly { rows: n };
    }

    let pg_sorted = canonicalize(pg_cells);
    let basin_sorted = canonicalize(basin_cells);
    for (i, (pr, br)) in pg_sorted.iter().zip(basin_sorted.iter()).enumerate() {
        if pr.len() != br.len() {
            return Verify::Diverged(format!(
                "row {i}: basin has {} columns, postgres has {}",
                br.len(),
                pr.len()
            ));
        }
        for (c, (pv, bv)) in pr.iter().zip(br.iter()).enumerate() {
            if !cells_equal(pv, bv) {
                return Verify::Diverged(format!(
                    "row {i} col {c}: basin {bv:?} vs postgres {pv:?}"
                ));
            }
        }
    }
    Verify::Match { rows: n }
}

/// Self-test hook: `BASIN_MATRIX_SELFTEST_CORRUPT=<shape_id>` replaces that
/// shape's BASIN-side statement with a valid statement that returns a
/// deliberately wrong answer — the same query, one row short. Nothing else
/// changes: the statement is still parsed, planned and executed by Basin, and
/// Postgres still runs the original. A run with this set MUST fail in that
/// shape's verification cell. If it passes, the verification step is comparing
/// nothing and every green run of this harness is worthless.
fn selftest_wrong_basin_sql(id: &str) -> Option<&'static str> {
    match id {
        "count_star" => Some("SELECT COUNT(*) FROM events WHERE id >= 1"),
        "scan_1col" => Some("SELECT SUM(val) FROM events WHERE id >= 1"),
        "gb_low" => Some(
            "SELECT group_low, COUNT(*), SUM(val) FROM events WHERE id >= 1 GROUP BY group_low",
        ),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Schema + seeding
// ---------------------------------------------------------------------------

const EVENTS_COLS: &str = "id, group_low, group_high, account_dense, account_sparse, val, payload";

fn events_ddl() -> &'static str {
    "id BIGINT NOT NULL, group_low BIGINT NOT NULL, group_high BIGINT NOT NULL, \
     account_dense BIGINT NOT NULL, account_sparse BIGINT, val BIGINT NOT NULL, \
     payload TEXT NOT NULL"
}

fn accounts_ddl() -> &'static str {
    "id BIGINT NOT NULL, region TEXT NOT NULL"
}

fn events_tuple(i: i64, rows: usize) -> String {
    let group_low = i % 10;
    let group_high = i % group_high_n(rows);
    let account_dense = i % ACCOUNTS_N;
    let account_sparse = if i % 20 == 0 {
        (i % ACCOUNTS_N).to_string()
    } else {
        "NULL".to_string()
    };
    let val = (i * 37 + 11) % 100_000;
    let payload = format!("payload-{:040}", i);
    format!("({i},{group_low},{group_high},{account_dense},{account_sparse},{val},'{payload}')")
}

fn accounts_tuple(i: i64) -> String {
    format!("({i},'region_{:02}')", i % REGIONS_N)
}

fn seed_batch_size(rows: usize) -> usize {
    if rows <= 10_000 {
        rows.max(1)
    } else if rows <= 100_000 {
        5_000
    } else {
        10_000
    }
}

/// Seed `events` (rows `lo..hi`, exclusive) + optionally `accounts` on BOTH
/// engines identically, batched. Returns nothing — mutates via the two
/// connections. `start_id`/`end_id` let this be reused for the INSERT
/// shapes (seeding beyond the read-shape row count on an already-populated
/// table, rather than a fresh one).
async fn seed_events(
    pg: &Client,
    pg_schema: &str,
    sess: &basin_engine::ProjectSession,
    rows: usize,
    start_id: i64,
    end_id: i64,
) {
    let batch = seed_batch_size(rows) as i64;
    let mut i = start_id;
    while i < end_id {
        let hi = (i + batch).min(end_id);
        let tuples: Vec<String> = (i..hi).map(|k| events_tuple(k, rows)).collect();
        let values = tuples.join(",");
        pg.simple_query(&format!(
            "INSERT INTO {pg_schema}.events ({EVENTS_COLS}) VALUES {values}"
        ))
        .await
        .expect("pg seed events");
        sess.execute(&format!("INSERT INTO events ({EVENTS_COLS}) VALUES {values}"))
            .await
            .expect("basin seed events");
        i = hi;
    }
}

async fn seed_accounts(pg: &Client, pg_schema: &str, sess: &basin_engine::ProjectSession) {
    let tuples: Vec<String> = (0..ACCOUNTS_N).map(accounts_tuple).collect();
    let values = tuples.join(",");
    pg.simple_query(&format!(
        "INSERT INTO {pg_schema}.accounts (id, region) VALUES {values}"
    ))
    .await
    .expect("pg seed accounts");
    sess.execute(&format!(
        "INSERT INTO accounts (id, region) VALUES {values}"
    ))
    .await
    .expect("basin seed accounts");
}

// ---------------------------------------------------------------------------
// Shape definitions
// ---------------------------------------------------------------------------

struct Shape {
    id: &'static str,
    label: &'static str,
    pg_sql: String,
    basin_sql: String,
    /// Rows the engine must touch to answer this query, for a rows/sec
    /// figure. `None` for shapes where that number isn't meaningful
    /// (point lookups, small-LIMIT queries, correlated subqueries).
    rows_touched: Option<u64>,
}

fn core_shapes(rows: usize) -> Vec<Shape> {
    let target = (rows / 2) as i64;
    let sel_window = (rows / 100).max(1) as i64; // ~1% window
    let sel_lo = target - sel_window / 2;
    let sel_hi = sel_lo + sel_window;
    let unsel_window = (rows / 2).max(1) as i64; // ~50% window
    let unsel_lo = (rows / 4) as i64;
    let unsel_hi = unsel_lo + unsel_window;
    let n = rows as u64;

    let mut shapes = vec![
        Shape {
            id: "pt_lookup",
            label: "Point lookup (WHERE id = ?)",
            pg_sql: format!("SELECT id, val, payload FROM {{schema}}.events WHERE id = {target}"),
            basin_sql: format!("SELECT id, val, payload FROM events WHERE id = {target}"),
            rows_touched: None,
        },
        Shape {
            id: "range_sel",
            label: "Range scan, selective (~1% window)",
            pg_sql: format!(
                "SELECT COUNT(*), SUM(val) FROM {{schema}}.events WHERE id BETWEEN {sel_lo} AND {sel_hi}"
            ),
            basin_sql: format!(
                "SELECT COUNT(*), SUM(val) FROM events WHERE id BETWEEN {sel_lo} AND {sel_hi}"
            ),
            rows_touched: Some(sel_window.max(1) as u64),
        },
        Shape {
            id: "range_unsel",
            label: "Range scan, unselective (~50% window)",
            pg_sql: format!(
                "SELECT COUNT(*), SUM(val) FROM {{schema}}.events WHERE id BETWEEN {unsel_lo} AND {unsel_hi}"
            ),
            basin_sql: format!(
                "SELECT COUNT(*), SUM(val) FROM events WHERE id BETWEEN {unsel_lo} AND {unsel_hi}"
            ),
            rows_touched: Some(unsel_window.max(1) as u64),
        },
        Shape {
            id: "scan_1col",
            label: "Full scan, project 1 column (SUM)",
            pg_sql: "SELECT SUM(val) FROM {schema}.events".to_string(),
            basin_sql: "SELECT SUM(val) FROM events".to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "scan_allcol",
            label: "Full scan, project all columns",
            pg_sql: "SELECT COUNT(*) FROM {schema}.events WHERE id >= 0 AND group_low >= 0 \
                      AND group_high >= 0 AND account_dense >= 0 AND val >= 0 AND payload <> ''"
                .to_string(),
            basin_sql: "SELECT COUNT(*) FROM events WHERE id >= 0 AND group_low >= 0 \
                         AND group_high >= 0 AND account_dense >= 0 AND val >= 0 AND payload <> ''"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "count_star",
            label: "COUNT(*)",
            pg_sql: "SELECT COUNT(*) FROM {schema}.events".to_string(),
            basin_sql: "SELECT COUNT(*) FROM events".to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "sum_avg",
            label: "SUM + AVG over one column",
            pg_sql: "SELECT SUM(val), AVG(val) FROM {schema}.events".to_string(),
            basin_sql: "SELECT SUM(val), AVG(val) FROM events".to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "gb_low",
            label: "GROUP BY, LOW cardinality (10 groups)",
            pg_sql: "SELECT group_low, COUNT(*), SUM(val) FROM {schema}.events GROUP BY group_low"
                .to_string(),
            basin_sql: "SELECT group_low, COUNT(*), SUM(val) FROM events GROUP BY group_low"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "gb_high",
            label: "GROUP BY, HIGH cardinality (up to 100k groups)",
            pg_sql:
                "SELECT group_high, COUNT(*), SUM(val) FROM {schema}.events GROUP BY group_high"
                    .to_string(),
            basin_sql: "SELECT group_high, COUNT(*), SUM(val) FROM events GROUP BY group_high"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "ord_nolimit",
            label: "ORDER BY, no LIMIT (full sort)",
            pg_sql: "SELECT id FROM {schema}.events ORDER BY val".to_string(),
            basin_sql: "SELECT id FROM events ORDER BY val".to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "ord_limit",
            label: "ORDER BY ... LIMIT 10 (should early-exit)",
            pg_sql: "SELECT id, val FROM {schema}.events ORDER BY val LIMIT 10".to_string(),
            basin_sql: "SELECT id, val FROM events ORDER BY val LIMIT 10".to_string(),
            rows_touched: None,
        },
        Shape {
            id: "join_inner_sel",
            label: "INNER JOIN, selective (~5% match)",
            pg_sql: "SELECT COUNT(*) FROM {schema}.events e JOIN {schema}.accounts a \
                      ON e.account_sparse = a.id WHERE a.region = 'region_00'"
                .to_string(),
            basin_sql: "SELECT COUNT(*) FROM events e JOIN accounts a \
                         ON e.account_sparse = a.id WHERE a.region = 'region_00'"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "join_inner_unsel",
            label: "INNER JOIN, unselective (~100% match)",
            pg_sql: "SELECT COUNT(*) FROM {schema}.events e JOIN {schema}.accounts a \
                      ON e.account_dense = a.id"
                .to_string(),
            basin_sql: "SELECT COUNT(*) FROM events e JOIN accounts a ON e.account_dense = a.id"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "join_left_sel",
            label: "LEFT JOIN, selective (~5% match rate)",
            pg_sql: "SELECT COUNT(*), COUNT(a.id) FROM {schema}.events e LEFT JOIN {schema}.accounts a \
                      ON e.account_sparse = a.id"
                .to_string(),
            basin_sql: "SELECT COUNT(*), COUNT(a.id) FROM events e LEFT JOIN accounts a \
                         ON e.account_sparse = a.id"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "join_left_unsel",
            label: "LEFT JOIN, unselective (~100% match rate)",
            pg_sql: "SELECT COUNT(*), COUNT(a.id) FROM {schema}.events e LEFT JOIN {schema}.accounts a \
                      ON e.account_dense = a.id"
                .to_string(),
            basin_sql: "SELECT COUNT(*), COUNT(a.id) FROM events e LEFT JOIN accounts a \
                         ON e.account_dense = a.id"
                .to_string(),
            rows_touched: Some(n),
        },
        Shape {
            id: "corr_subq",
            label: "Correlated subquery (per-group AVG, LIMIT 100)",
            pg_sql: "SELECT id FROM {schema}.events e WHERE e.val > \
                      (SELECT AVG(val) FROM {schema}.events e2 WHERE e2.group_low = e.group_low) \
                      LIMIT 100"
                .to_string(),
            basin_sql: "SELECT id FROM events e WHERE e.val > \
                         (SELECT AVG(val) FROM events e2 WHERE e2.group_low = e.group_low) \
                         LIMIT 100"
                .to_string(),
            rows_touched: None,
        },
    ];

    if let Ok(target) = std::env::var("BASIN_MATRIX_SELFTEST_CORRUPT") {
        let target = target.trim().to_string();
        let wrong = selftest_wrong_basin_sql(&target).unwrap_or_else(|| {
            panic!(
                "BASIN_MATRIX_SELFTEST_CORRUPT={target} names no shape with a \
                 deliberately-wrong variant; see selftest_wrong_basin_sql"
            )
        });
        let hit = shapes.iter_mut().find(|s| s.id == target).unwrap_or_else(|| {
            panic!("BASIN_MATRIX_SELFTEST_CORRUPT={target} is not a shape id")
        });
        println!("[pg_matrix] SELFTEST: basin SQL for shape `{target}` replaced with `{wrong}` — this run MUST fail");
        hit.basin_sql = wrong.to_string();
    }

    shapes
}

/// Whether a shape's rows are comparable cell-by-cell, or only by count.
fn shape_cell_compare(id: &str, rows: usize) -> bool {
    match id {
        // `LIMIT 100` with no ORDER BY: which 100 rows come back is not
        // determined by the SQL, so only the row count is comparable.
        "corr_subq" => false,
        // `ORDER BY val LIMIT 10` is deterministic only while `val` is unique.
        // `val = (i * 37 + 11) % 100_000`, so values repeat once rows > 100k
        // and the tie-break is engine-specific.
        "ord_limit" => rows <= 100_000,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EngineServed {
    Own,
    DataFusion,
    Mixed,
    NotApplicable,
    Unknown,
}

impl EngineServed {
    fn label(self) -> &'static str {
        match self {
            EngineServed::Own => "own engine",
            EngineServed::DataFusion => "DataFusion",
            EngineServed::Mixed => "MIXED (!)",
            EngineServed::NotApplicable => "n/a (DML)",
            EngineServed::Unknown => "unknown",
        }
    }
}

struct CellResult {
    shape_id: &'static str,
    shape_label: &'static str,
    rows: usize,
    basin_ms: Option<f64>,
    basin_spread: Option<(f64, f64)>,
    pg_ms: Option<f64>,
    pg_spread: Option<(f64, f64)>,
    served: EngineServed,
    basin_rows_sec: Option<f64>,
    pg_rows_sec: Option<f64>,
    /// Did Basin and Postgres agree on the ANSWER, not just the clock. See the
    /// "Result verification" section above.
    verify: Verify,
}

impl CellResult {
    fn ratio_text(&self) -> String {
        match (self.basin_ms, self.pg_ms) {
            (Some(b), Some(p)) if b > 0.0 && p > 0.0 => {
                if b <= p {
                    format!("{:.2}x FASTER", p / b)
                } else {
                    format!("{:.2}x SLOWER", b / p)
                }
            }
            _ => "n/a".to_string(),
        }
    }

    fn basin_wins(&self) -> Option<bool> {
        match (self.basin_ms, self.pg_ms) {
            (Some(b), Some(p)) => Some(b <= p),
            _ => None,
        }
    }
}

fn fmt_ms(v: Option<f64>) -> String {
    match v {
        Some(v) if v.is_finite() => format!("{v:.3}"),
        _ => "TIMEOUT/ERR".to_string(),
    }
}

fn fmt_spread(v: Option<(f64, f64)>) -> String {
    match v {
        Some((lo, hi)) if lo.is_finite() && hi.is_finite() => format!("{lo:.3}\u{2013}{hi:.3}"),
        _ => "-".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Shape runner
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_shape(
    pg: &Client,
    pg_schema: &str,
    sess: &basin_engine::ProjectSession,
    engine: &Engine,
    shape: &Shape,
    rows: usize,
) -> CellResult {
    let budget = budget_for(rows);
    let n = samples_for(rows);
    let pg_sql = shape.pg_sql.replace("{schema}", pg_schema);

    // Answer check first, before any timing: a wrong answer makes every
    // millisecond below meaningless. One untimed pass on each engine, so the
    // warm-up protocol stays symmetric.
    let verify = verify_shape(
        pg,
        &pg_sql,
        sess,
        &shape.basin_sql,
        shape_cell_compare(shape.id, rows),
        budget,
    )
    .await;
    if verify.is_diverged() {
        println!(
            "[pg_matrix]   !! DIVERGENCE rows={rows} shape={} — {}",
            shape.id,
            verify.label()
        );
    }

    // Symmetric warm-up: two untimed passes on both engines before any
    // timed sample (see module doc).
    let _ = pg_exec_ms(pg, &pg_sql, budget).await;
    let _ = pg_exec_ms(pg, &pg_sql, budget).await;
    let _ = basin_exec_ms(sess, &shape.basin_sql, budget).await;
    let _ = basin_exec_ms(sess, &shape.basin_sql, budget).await;

    let mut pg_samples = Vec::with_capacity(n);
    for _ in 0..n {
        if let Some(ms) = pg_exec_ms(pg, &pg_sql, budget).await {
            pg_samples.push(ms);
        }
    }

    let served_before = (
        engine.owned_engine_served_count(),
        engine.owned_engine_fallback_count(),
    );
    let mut basin_samples = Vec::with_capacity(n);
    let mut last_result: Option<ExecResult> = None;
    for _ in 0..n {
        if let Some((ms, res)) = basin_exec_ms(sess, &shape.basin_sql, budget).await {
            basin_samples.push(ms);
            last_result = Some(res);
        }
    }
    let served_after = (
        engine.owned_engine_served_count(),
        engine.owned_engine_fallback_count(),
    );
    let _ = last_result;

    let served_delta = served_after.0 - served_before.0;
    let fallback_delta = served_after.1 - served_before.1;
    let served = if shape.id.starts_with("ins_") {
        EngineServed::NotApplicable
    } else if served_delta > 0 && fallback_delta > 0 {
        EngineServed::Mixed
    } else if served_delta > 0 {
        EngineServed::Own
    } else if fallback_delta > 0 {
        EngineServed::DataFusion
    } else {
        EngineServed::Unknown
    };

    let basin_ms = if basin_samples.is_empty() {
        None
    } else {
        Some(median(&basin_samples))
    };
    let pg_ms = if pg_samples.is_empty() {
        None
    } else {
        Some(median(&pg_samples))
    };

    let basin_rows_sec = match (shape.rows_touched, basin_ms) {
        (Some(rt), Some(ms)) if ms > 0.0 => Some(rt as f64 / (ms / 1000.0)),
        _ => None,
    };
    let pg_rows_sec = match (shape.rows_touched, pg_ms) {
        (Some(rt), Some(ms)) if ms > 0.0 => Some(rt as f64 / (ms / 1000.0)),
        _ => None,
    };

    CellResult {
        shape_id: shape.id,
        shape_label: shape.label,
        rows,
        basin_ms,
        basin_spread: if basin_samples.is_empty() {
            None
        } else {
            Some(spread(&basin_samples))
        },
        pg_ms,
        pg_spread: if pg_samples.is_empty() {
            None
        } else {
            Some(spread(&pg_samples))
        },
        served,
        basin_rows_sec,
        pg_rows_sec,
        verify,
    }
}

// ---------------------------------------------------------------------------
// INSERT shapes — reuse the already-seeded `events` table, insert beyond
// its current max id so read shapes (already run) are unaffected.
// ---------------------------------------------------------------------------

async fn run_insert_shapes(
    pg: &Client,
    pg_schema: &str,
    sess: &basin_engine::ProjectSession,
    engine: &Engine,
    rows: usize,
    run_single: bool,
) -> Vec<CellResult> {
    let budget = budget_for(rows);
    let mut out = Vec::new();
    let mut next_id = rows as i64;

    if run_single {
        let k = samples_for(rows).min(30).max(5);
        // The two engines are timed in separate loops (so neither pays the
        // other's scheduling noise) but must insert the SAME `k` tuples, not
        // two adjacent ranges. Inserting different ids leaves the two tables
        // with equal row counts and different CONTENT, which silently defeats
        // any content comparison made after this point.
        let first_id = next_id;
        let mut pg_samples = Vec::with_capacity(k);
        for j in 0..k as i64 {
            let sql = format!(
                "INSERT INTO {pg_schema}.events ({EVENTS_COLS}) VALUES {}",
                events_tuple(first_id + j, rows)
            );
            let q = format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql}");
            if let Ok(Ok(r)) = tokio::time::timeout(budget, pg.simple_query(&q)).await {
                if let Some(ms) = parse_pg_exec_time(&r) {
                    pg_samples.push(ms);
                }
            }
        }
        let mut basin_samples = Vec::with_capacity(k);
        for j in 0..k as i64 {
            let sql = format!(
                "INSERT INTO events ({EVENTS_COLS}) VALUES {}",
                events_tuple(first_id + j, rows)
            );
            if let Some((ms, _)) = basin_exec_ms(sess, &sql, budget).await {
                basin_samples.push(ms);
            }
        }
        next_id = first_id + k as i64;
        let basin_ms = if basin_samples.is_empty() {
            None
        } else {
            Some(median(&basin_samples))
        };
        let pg_ms = if pg_samples.is_empty() {
            None
        } else {
            Some(median(&pg_samples))
        };
        out.push(CellResult {
            shape_id: "ins_single",
            shape_label: "INSERT, single-row (rows/sec = 1000/median_ms)",
            rows,
            basin_ms,
            basin_spread: if basin_samples.is_empty() {
                None
            } else {
                Some(spread(&basin_samples))
            },
            pg_ms,
            pg_spread: if pg_samples.is_empty() {
                None
            } else {
                Some(spread(&pg_samples))
            },
            served: EngineServed::NotApplicable,
            basin_rows_sec: basin_ms.filter(|m| *m > 0.0).map(|m| 1000.0 / m),
            pg_rows_sec: pg_ms.filter(|m| *m > 0.0).map(|m| 1000.0 / m),
            verify: Verify::Skipped("DML — checked by post_ins_count below"),
        });
    }

    // Batched insert: batch_size new rows in one statement, repeated a
    // handful of times for a median.
    let batch_size = (rows / 10).clamp(100, 10_000) as i64;
    let reps = samples_for(rows).min(5).max(2);
    let mut pg_samples = Vec::with_capacity(reps);
    let mut basin_samples = Vec::with_capacity(reps);
    for _ in 0..reps {
        let hi = next_id + batch_size;
        let tuples: Vec<String> = (next_id..hi).map(|k| events_tuple(k, rows)).collect();
        let values = tuples.join(",");
        let sql = format!("INSERT INTO {pg_schema}.events ({EVENTS_COLS}) VALUES {values}");
        let q = format!("EXPLAIN (ANALYZE, FORMAT TEXT) {sql}");
        if let Ok(Ok(r)) = tokio::time::timeout(budget, pg.simple_query(&q)).await {
            if let Some(ms) = parse_pg_exec_time(&r) {
                pg_samples.push(ms);
            }
        }
        let sql = format!("INSERT INTO events ({EVENTS_COLS}) VALUES {values}");
        if let Some((ms, _)) = basin_exec_ms(sess, &sql, budget).await {
            basin_samples.push(ms);
        }
        next_id = hi;
    }
    let basin_ms = if basin_samples.is_empty() {
        None
    } else {
        Some(median(&basin_samples))
    };
    let pg_ms = if pg_samples.is_empty() {
        None
    } else {
        Some(median(&pg_samples))
    };
    out.push(CellResult {
        shape_id: "ins_batch",
        shape_label: "INSERT, batched (dynamic batch size)",
        rows,
        basin_ms,
        basin_spread: if basin_samples.is_empty() {
            None
        } else {
            Some(spread(&basin_samples))
        },
        pg_ms,
        pg_spread: if pg_samples.is_empty() {
            None
        } else {
            Some(spread(&pg_samples))
        },
        served: EngineServed::NotApplicable,
        basin_rows_sec: basin_ms
            .filter(|m| *m > 0.0)
            .map(|m| batch_size as f64 / (m / 1000.0)),
        pg_rows_sec: pg_ms
            .filter(|m| *m > 0.0)
            .map(|m| batch_size as f64 / (m / 1000.0)),
        verify: Verify::Skipped("DML — checked by post_ins_count below"),
    });

    // Every INSERT above went to BOTH engines with identical tuples, so the
    // two tables must now hold the same number of rows. This is the only
    // check that can catch the write path dropping rows, or a read path that
    // cannot see rows that have not been flushed to storage yet — the INSERT
    // timings above are blind to both. The served/fallback delta is captured
    // around it because "which engine answered" is the first question a
    // divergence here raises.
    let before = (
        engine.owned_engine_served_count(),
        engine.owned_engine_fallback_count(),
    );
    let post = verify_shape(
        pg,
        &format!("SELECT COUNT(*) FROM {pg_schema}.events"),
        sess,
        "SELECT COUNT(*) FROM events",
        true,
        budget,
    )
    .await;
    let after = (
        engine.owned_engine_served_count(),
        engine.owned_engine_fallback_count(),
    );
    let post_served = match (after.0 - before.0, after.1 - before.1) {
        (0, 0) => EngineServed::Unknown,
        (s, f) if s > 0 && f > 0 => EngineServed::Mixed,
        (s, _) if s > 0 => EngineServed::Own,
        _ => EngineServed::DataFusion,
    };
    if post.is_diverged() {
        println!(
            "[pg_matrix]   !! DIVERGENCE rows={rows} shape=post_ins_count (basin side served by {}) — {}",
            post_served.label(),
            post.label()
        );
    }
    out.push(CellResult {
        shape_id: "post_ins_count",
        shape_label: "Row count after the INSERT shapes (both engines)",
        rows,
        basin_ms: None,
        basin_spread: None,
        pg_ms: None,
        pg_spread: None,
        served: post_served,
        basin_rows_sec: None,
        pg_rows_sec: None,
        verify: post,
    });

    out
}

// ---------------------------------------------------------------------------
// Per-size driver: seed both engines, run every applicable shape, tear down.
// ---------------------------------------------------------------------------

async fn run_size(pg: &Client, rows: usize, shape_filter: Option<&[&str]>) -> Vec<CellResult> {
    // Every statement at this size shares one per-query budget, so bound the
    // server to it before touching anything — see `set_pg_statement_timeout`.
    set_pg_statement_timeout(pg, budget_for(rows)).await;

    let suffix = basin_common::ProjectId::new().as_ulid().to_string().to_lowercase();
    let pg_schema = format!("basin_bench_{suffix}");
    pg.simple_query(&format!("CREATE SCHEMA {pg_schema}"))
        .await
        .expect("pg create schema");
    pg.simple_query(&format!(
        "CREATE TABLE {pg_schema}.events ({})",
        events_ddl()
    ))
    .await
    .expect("pg create events");
    pg.simple_query(&format!(
        "CREATE TABLE {pg_schema}.accounts ({})",
        accounts_ddl()
    ))
    .await
    .expect("pg create accounts");

    let mut instance = build_basin_engine().await;
    let sess = instance
        .engine
        .open_session(instance.project)
        .await
        .unwrap();
    sess.execute(&format!("CREATE TABLE events ({})", events_ddl()))
        .await
        .expect("basin create events");
    sess.execute(&format!("CREATE TABLE accounts ({})", accounts_ddl()))
        .await
        .expect("basin create accounts");

    println!("[pg_matrix] seeding rows={rows} ...");
    let t_seed = Instant::now();
    seed_events(pg, &pg_schema, &sess, rows, 0, rows as i64).await;
    seed_accounts(pg, &pg_schema, &sess).await;
    println!(
        "[pg_matrix] seeded rows={rows} in {:.1}s",
        t_seed.elapsed().as_secs_f64()
    );

    // Settle Basin into its steady read state: flush the in-memory WAL tail
    // into Parquet/Vortex, then stop the background compactor so the timed
    // window is free of background I/O. See compare_postgres_common.rs's
    // reproducibility protocol note #3 for why this mirrors PG's
    // autovacuum-quiesced-by-fresh-insert state.
    instance.shard.flush_to_parquet().await.unwrap();
    if let Some(bg) = instance.bg.take() {
        bg.shutdown().await;
    }
    // ANALYZE so PG's planner has real stats too (fairness: Basin's file
    // footers always carry stats; PG needs an explicit ANALYZE for parity).
    pg.simple_query(&format!("ANALYZE {pg_schema}.events"))
        .await
        .ok();
    pg.simple_query(&format!("ANALYZE {pg_schema}.accounts"))
        .await
        .ok();

    let mut results = Vec::new();
    for shape in core_shapes(rows) {
        if let Some(filter) = shape_filter {
            if !filter.contains(&shape.id) {
                continue;
            }
        }
        // corr_subq is bounded by LIMIT 100 but is still an
        // un-decorrelated-worst-case risk at large N; the per-query
        // timeout (budget_for) is the safety net, not a size skip, so we
        // can honestly report a TIMEOUT rather than silently omitting it.
        println!("[pg_matrix]   rows={rows} shape={} ...", shape.id);
        let r = run_shape(pg, &pg_schema, &sess, &instance.engine, &shape, rows).await;
        results.push(r);
    }

    let run_ins_single = shape_filter.map(|f| f.contains(&"ins_single")).unwrap_or(true);
    let run_ins_batch = shape_filter.map(|f| f.contains(&"ins_batch")).unwrap_or(true);
    if run_ins_batch {
        println!("[pg_matrix]   rows={rows} shape=insert ...");
        let ins = run_insert_shapes(
            pg,
            &pg_schema,
            &sess,
            &instance.engine,
            rows,
            run_ins_single,
        )
        .await;
        results.extend(ins);
    }

    let _ = pg
        .simple_query(&format!("DROP SCHEMA IF EXISTS {pg_schema} CASCADE"))
        .await;
    instance.wal.close().await.unwrap();
    // `instance.dir` (TempDir) drops here, removing Basin's on-disk files.

    results
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

fn print_report(all: &[CellResult]) {
    println!("\n# Basin vs Postgres 18.2 — shape x size matrix\n");

    println!("## Result verification (identical SQL, both engines)\n");
    println!("| Shape | Rows | Verification |");
    println!("|---|---|---|");
    for r in all {
        println!("| {} | {} | {} |", r.shape_label, r.rows, r.verify.label());
    }
    let compared = all.iter().filter(|r| r.verify.is_compared()).count();
    let diverged = all.iter().filter(|r| r.verify.is_diverged()).count();
    let inconclusive = all
        .iter()
        .filter(|r| matches!(r.verify, Verify::Inconclusive(_)))
        .count();
    println!(
        "\n{compared} cells compared · {diverged} DIVERGED · {inconclusive} inconclusive\n"
    );

    println!(
        "| Shape | Rows | Basin ms (median, spread) | Postgres ms (median, spread) | Ratio | Engine served |"
    );
    println!("|---|---|---|---|---|---|");
    for r in all {
        println!(
            "| {} | {} | {} ({}) | {} ({}) | {} | {} |",
            r.shape_label,
            r.rows,
            fmt_ms(r.basin_ms),
            fmt_spread(r.basin_spread),
            fmt_ms(r.pg_ms),
            fmt_spread(r.pg_spread),
            r.ratio_text(),
            r.served.label(),
        );
    }

    println!("\n## Rows/sec (where meaningful)\n");
    println!("| Shape | Rows | Basin rows/sec | Postgres rows/sec |");
    println!("|---|---|---|---|");
    for r in all {
        if r.basin_rows_sec.is_some() || r.pg_rows_sec.is_some() {
            println!(
                "| {} | {} | {} | {} |",
                r.shape_label,
                r.rows,
                r.basin_rows_sec
                    .map(|v| format!("{v:.0}"))
                    .unwrap_or_else(|| "-".to_string()),
                r.pg_rows_sec
                    .map(|v| format!("{v:.0}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
    }

    println!("\n## Shapes where Basin LOSES (basin_ms > postgres_ms)\n");
    let mut losses: Vec<&CellResult> = all
        .iter()
        .filter(|r| r.basin_wins() == Some(false))
        .collect();
    losses.sort_by(|a, b| {
        let ra = a.basin_ms.unwrap_or(0.0) / a.pg_ms.unwrap_or(1.0);
        let rb = b.basin_ms.unwrap_or(0.0) / b.pg_ms.unwrap_or(1.0);
        rb.partial_cmp(&ra).unwrap()
    });
    if losses.is_empty() {
        println!("(none in this run — see caveats above about which cells actually ran)");
    } else {
        println!("| Shape | Rows | Basin ms | Postgres ms | Basin is slower by | Engine served |");
        println!("|---|---|---|---|---|---|");
        for r in &losses {
            println!(
                "| {} | {} | {} | {} | {} | {} |",
                r.shape_label,
                r.rows,
                fmt_ms(r.basin_ms),
                fmt_ms(r.pg_ms),
                r.ratio_text(),
                r.served.label(),
            );
        }
    }

    let mut served_counts: HashMap<&str, u32> = HashMap::new();
    let mut total_labeled = 0u32;
    for r in all {
        if r.served != EngineServed::NotApplicable {
            total_labeled += 1;
            *served_counts.entry(r.served.label()).or_insert(0) += 1;
        }
    }
    println!("\n## Engine-served summary (SELECT shapes only, BASIN_OWNED_ENGINE=1)\n");
    for (k, v) in served_counts.iter() {
        println!("- {k}: {v}/{total_labeled}");
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scaling_6_compare_postgres_matrix() {
    // Turn on the owned-engine bridge for the duration of this test so the
    // "which engine served" column reflects real routing decisions rather
    // than a hardcoded "always DataFusion" (the flag's default). See the
    // module doc's "Engine-served honesty" section.
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let (pg, _conn_str) = match connect_pg().await {
        Some(v) => v,
        None => {
            println!(
                "[pg_matrix] SKIPPED: no reachable Postgres (connect target logged above). \
                 Set PG_DIFF_TEST_DSN, or run via benchmark/pg_matrix/run.sh to start a \
                 scratch instance. NOTHING WAS COMPARED."
            );
            return;
        }
    };

    let mut all = Vec::new();

    let plan = run_plan();
    assert!(
        !plan.is_empty(),
        "run_plan() produced no (size, shapes) cells — BASIN_MATRIX_SIZES is set \
         to something unparseable. Refusing to report a matrix that measured nothing."
    );
    for (rows, filter) in plan {
        let mut r = run_size(&pg, rows, filter).await;
        all.append(&mut r);
    }

    print_report(&all);

    let _ = pg.simple_query("SELECT 1").await; // keep conn alive till here

    // ── The only failing conditions in this file ───────────────────────────
    //
    // Everything above is a measurement, and a measurement cannot fail. These
    // two can, and they are what make a green run mean something.
    let compared = all.iter().filter(|r| r.verify.is_compared()).count();
    assert!(
        compared > 0,
        "no shape produced a comparable result set — this run measured timings \
         against nothing. {} cells attempted.",
        all.len()
    );

    let diverged: Vec<String> = all
        .iter()
        .filter(|r| r.verify.is_diverged())
        .map(|r| format!("  rows={} shape={} — {}", r.rows, r.shape_id, r.verify.label()))
        .collect();
    assert!(
        diverged.is_empty(),
        "Basin and Postgres returned different answers for {} of {compared} compared cells:\n{}",
        diverged.len(),
        diverged.join("\n")
    );
}
