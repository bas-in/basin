//! Differential test for the statistical aggregate and window functions
//! `aggregate.rs` and `window.rs` implement — `stddev*`/`var*`, `bool_and`/
//! `bool_or`, `bit_and`/`bit_or`/`bit_xor`, the nine `regr_*` plus `corr`/
//! `covar_pop`/`covar_samp`, and `percent_rank`/`cume_dist`/`ntile`.
//!
//! # Why a differential test rather than more unit tests
//!
//! The unit tests in `aggregate.rs` and `window.rs` pin values that were read
//! off a live server once and then transcribed. That catches regressions but
//! not transcription-shaped mistakes, and it cannot cover the input space
//! where these functions actually go wrong: the variance family's answer
//! depends on *floating-point accumulation order*, so a datasets-of-three
//! test can agree with PostgreSQL by luck while a 50-row one disagrees in the
//! last three digits. This file asks the server itself, at test time, over
//! randomised data, and compares **bit for bit** — `f64 == f64`, no epsilon.
//!
//! An epsilon comparison would defeat the purpose. The whole reason
//! `VarAcc`/`RegrAcc` reproduce PostgreSQL's Youngs-Cramer arithmetic
//! operation by operation, rather than computing the same quantity a cleaner
//! way, is that "close enough" hides exactly the divergence this file exists
//! to measure. See the section comment above `VarAcc` in
//! `crates/basin-exec/src/aggregate.rs` for what that arithmetic is and how
//! it was recovered from the server (by calling `float8_accum` directly and
//! watching the transition array evolve one row at a time).
//!
//! # Skipping
//!
//! Requires `PG_DIFF_TEST_DSN`, the same env var as
//! `crates/basin-pgcatalog/tests/catalog_fidelity.rs`,
//! `crates/basin-engine/tests/differential_pg.rs` and this crate's own
//! `function_equivalence.rs`. When it is unset these tests skip cleanly —
//! the same posture `crates/basin-engine/tests/
//! scan_predicate_column_alignment.rs` takes — and each one says so on
//! stderr, so a run with no server is not silently indistinguishable from a
//! run that verified everything.
//!
//! ```text
//! PG_DIFF_TEST_DSN='postgres://pc@127.0.0.1:5432/postgres' \
//!   cargo test -p basin-exec --test statistical_aggregates
//! ```

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use basin_exec::aggregate::{AggFunc, AggregateSpec, HashAggregate, RegrKind, VarKind};
use basin_exec::operator::Operator;
use basin_exec::scan::{Scan, VecBatchSource};
use basin_exec::window::{OrderKey, WindowAgg, WindowFunc, WindowSpec};

// ─────────────────────────────────────────────────────────────────────────
// Connection plumbing
// ─────────────────────────────────────────────────────────────────────────

/// A live connection, or `None` when `PG_DIFF_TEST_DSN` is unset. Every test
/// that gets `None` prints why and returns rather than passing silently.
fn connect(who: &str) -> Option<(tokio::runtime::Runtime, tokio_postgres::Client)> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(d) if !d.is_empty() => d,
        _ => {
            eprintln!(
                "\n*** {who}: SKIPPED — PG_DIFF_TEST_DSN is unset, so ZERO statistics were \
                 checked against a live server this run. Set it to a libpq DSN (e.g. \
                 postgres://pc@127.0.0.1:5432/postgres) to actually run this test. ***\n"
            );
            return None;
        }
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let client = rt.block_on(async {
        let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap_or_else(|e| panic!("PG_DIFF_TEST_DSN connect failed: {e}"));
        tokio::spawn(async move {
            let _ = conn.await;
        });
        client
    });
    Some((rt, client))
}

// ─────────────────────────────────────────────────────────────────────────
// Deterministic pseudo-random data
//
// A fixed 64-bit LCG rather than the `rand` crate: this crate has no `rand`
// dependency, and a failure that cannot be reproduced from the seed printed
// in the assertion message is much less useful than one that can.
// ─────────────────────────────────────────────────────────────────────────

struct Lcg(u64);

impl Lcg {
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }

    /// A float in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() % (1 << 40)) as f64 / (1u64 << 40) as f64
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

/// One randomised `(x, y)` dataset. Deliberately hostile to a naive
/// `sum(x^2)` accumulator: `x` sits near a large offset so that the squares
/// share ~8 leading digits and cancel catastrophically, while the spread that
/// carries the actual variance is small. `y` is on a completely different
/// scale, which is what makes `regr_slope`/`corr` sensitive to the
/// accumulation order rather than merely to the formula.
///
/// Roughly one row in nine has a NULL in one column or the other, so the
/// skip-unless-both-non-NULL rule is exercised on every dataset rather than
/// only in the hand-written unit test.
fn random_pairs(seed: u64, rows: usize) -> Vec<(Option<f64>, Option<f64>)> {
    let mut rng = Lcg(seed);
    let offset = 1e8_f64;
    (0..rows)
        .map(|i| {
            let x = offset + rng.unit() * 1000.0 + i as f64 * 0.7;
            let y = rng.unit() * 3.0 - i as f64 * 0.013;
            match rng.below(9) {
                0 => (None, Some(y)),
                1 => (Some(x), None),
                _ => (Some(x), Some(y)),
            }
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────
// Running one aggregate through the real operator
// ─────────────────────────────────────────────────────────────────────────

fn source(schema: SchemaRef, batches: Vec<RecordBatch>) -> Box<dyn Operator> {
    let projection = (0..schema.fields().len()).collect();
    Box::new(
        Scan::new(
            Box::new(VecBatchSource::new(Arc::clone(&schema), batches)),
            projection,
            Vec::new(),
        )
        .expect("identity projection over the fixture's own schema"),
    )
}

/// Feed one batch through an ungrouped `HashAggregate` and hand back the
/// single output row.
fn run_aggregates(schema: SchemaRef, batch: RecordBatch, specs: Vec<AggregateSpec>) -> RecordBatch {
    let input = source(Arc::clone(&schema), vec![batch]);
    let mut agg = HashAggregate::new(input, vec![], specs, usize::MAX)
        .expect("HashAggregate::new over a well-typed input");
    let out = agg
        .next_batch()
        .expect("aggregate must not error")
        .expect("an ungrouped aggregate always emits exactly one row");
    assert_eq!(out.num_rows(), 1);
    assert!(
        agg.next_batch().expect("drain").is_none(),
        "an ungrouped aggregate emits exactly one batch"
    );
    out
}

fn cell_f64(batch: &RecordBatch, col: usize) -> Option<f64> {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float8 aggregate output");
    (!a.is_null(0)).then(|| a.value(0))
}

fn cell_i64(batch: &RecordBatch, col: usize) -> Option<i64> {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("bigint aggregate output");
    (!a.is_null(0)).then(|| a.value(0))
}

fn cell_bool(batch: &RecordBatch, col: usize) -> Option<bool> {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean aggregate output");
    (!a.is_null(0)).then(|| a.value(0))
}

/// PostgreSQL renders `float8` with shortest-round-trip digits by default
/// (`extra_float_digits = 1` since PG 12), so parsing its text recovers the
/// exact double it holds — which is what makes an exact `==` comparison
/// meaningful rather than a comparison against a rounded decimal. `NaN` and
/// `Infinity` both parse.
fn parse_pg_f64(s: Option<&str>) -> Option<f64> {
    s.map(|s| {
        s.parse::<f64>()
            .unwrap_or_else(|e| panic!("cannot parse PostgreSQL float8 text {s:?}: {e}"))
    })
}

/// Exact comparison that still treats `NaN` as equal to `NaN` — Postgres's
/// own rule for these aggregates' results (a non-finite input poisons the
/// accumulator to NaN on both sides), and one plain `==` would report as a
/// divergence.
fn same_f64(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a == b || (a.is_nan() && b.is_nan()),
        _ => false,
    }
}

// ─────────────────────────────────────────────────────────────────────────
// The one-argument variance family
// ─────────────────────────────────────────────────────────────────────────

const VAR_KINDS: &[(VarKind, &str)] = &[
    (VarKind::VarPop, "var_pop"),
    (VarKind::VarSamp, "var_samp"),
    (VarKind::StddevPop, "stddev_pop"),
    (VarKind::StddevSamp, "stddev_samp"),
];

#[test]
fn variance_family_matches_live_postgres_bit_for_bit_over_random_data() {
    let Some((rt, client)) = connect("variance_family") else {
        return;
    };

    let select: String = VAR_KINDS
        .iter()
        .map(|(_, name)| format!("{name}(x)::text"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {select} FROM unnest($1::float8[]) AS t(x)");

    let mut checked = 0usize;
    for seed in 1..=12u64 {
        let pairs = random_pairs(seed, 53);
        let xs: Vec<Option<f64>> = pairs.iter().map(|(x, _)| *x).collect();

        let row = rt
            .block_on(client.query_one(sql.as_str(), &[&xs]))
            .expect("variance query");

        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float64Array::from(xs.clone())) as ArrayRef],
        )
        .unwrap();
        let specs: Vec<AggregateSpec> = VAR_KINDS
            .iter()
            .enumerate()
            .map(|(i, (kind, _))| AggregateSpec {
                func: AggFunc::Variance(*kind),
                input_col: Some(0),
                distinct: false,
                filter_col: None,
                alias: format!("v{i}"),
            })
            .collect();
        let out = run_aggregates(schema, batch, specs);

        for (i, (_, name)) in VAR_KINDS.iter().enumerate() {
            let want = parse_pg_f64(row.get::<_, Option<&str>>(i));
            let got = cell_f64(&out, i);
            assert!(
                same_f64(got, want),
                "{name} diverged on seed {seed} (53 rows): basin {got:?}, PostgreSQL {want:?}. \
                 Reproduce the dataset with random_pairs({seed}, 53)."
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 12 * VAR_KINDS.len());
    eprintln!("variance_family: {checked} values verified bit-for-bit against live PostgreSQL");
}

// ─────────────────────────────────────────────────────────────────────────
// The two-argument family
// ─────────────────────────────────────────────────────────────────────────

/// Every member except `regr_count`, which is `bigint` and checked
/// separately. `regr_slope(Y, X)` — the dependent variable is the *first*
/// argument, on both sides.
const REGR_KINDS: &[(RegrKind, &str)] = &[
    (RegrKind::Sxx, "regr_sxx"),
    (RegrKind::Syy, "regr_syy"),
    (RegrKind::Sxy, "regr_sxy"),
    (RegrKind::AvgX, "regr_avgx"),
    (RegrKind::AvgY, "regr_avgy"),
    (RegrKind::Slope, "regr_slope"),
    (RegrKind::Intercept, "regr_intercept"),
    (RegrKind::R2, "regr_r2"),
    (RegrKind::Corr, "corr"),
    (RegrKind::CovarPop, "covar_pop"),
    (RegrKind::CovarSamp, "covar_samp"),
];

#[test]
fn regr_family_matches_live_postgres_bit_for_bit_over_random_data() {
    let Some((rt, client)) = connect("regr_family") else {
        return;
    };

    let mut select: Vec<String> = REGR_KINDS
        .iter()
        .map(|(_, name)| format!("{name}(y, x)::text"))
        .collect();
    select.push("regr_count(y, x)::text".into());
    let sql = format!(
        "SELECT {} FROM unnest($1::float8[], $2::float8[]) AS t(x, y)",
        select.join(", ")
    );

    let mut checked = 0usize;
    for seed in 101..=112u64 {
        let pairs = random_pairs(seed, 53);
        let xs: Vec<Option<f64>> = pairs.iter().map(|(x, _)| *x).collect();
        let ys: Vec<Option<f64>> = pairs.iter().map(|(_, y)| *y).collect();

        let row = rt
            .block_on(client.query_one(sql.as_str(), &[&xs, &ys]))
            .expect("regr query");

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(xs.clone())) as ArrayRef,
                Arc::new(Float64Array::from(ys.clone())) as ArrayRef,
            ],
        )
        .unwrap();
        let mut specs: Vec<AggregateSpec> = REGR_KINDS
            .iter()
            .enumerate()
            .map(|(i, (kind, _))| AggregateSpec {
                func: AggFunc::Regr {
                    kind: *kind,
                    x_col: 0,
                },
                input_col: Some(1),
                distinct: false,
                filter_col: None,
                alias: format!("r{i}"),
            })
            .collect();
        specs.push(AggregateSpec {
            func: AggFunc::Regr {
                kind: RegrKind::Count,
                x_col: 0,
            },
            input_col: Some(1),
            distinct: false,
            filter_col: None,
            alias: "n".into(),
        });
        let out = run_aggregates(schema, batch, specs);

        for (i, (_, name)) in REGR_KINDS.iter().enumerate() {
            let want = parse_pg_f64(row.get::<_, Option<&str>>(i));
            let got = cell_f64(&out, i);
            assert!(
                same_f64(got, want),
                "{name} diverged on seed {seed} (53 rows): basin {got:?}, PostgreSQL {want:?}. \
                 Reproduce the dataset with random_pairs({seed}, 53)."
            );
            checked += 1;
        }

        let n_idx = REGR_KINDS.len();
        let want_n: i64 = row
            .get::<_, Option<&str>>(n_idx)
            .expect("regr_count is never NULL")
            .parse()
            .expect("regr_count is an integer");
        assert_eq!(
            cell_i64(&out, n_idx),
            Some(want_n),
            "regr_count diverged on seed {seed}: it must count only rows where BOTH \
             arguments are non-NULL"
        );
        checked += 1;
    }
    assert_eq!(checked, 12 * (REGR_KINDS.len() + 1));
    eprintln!("regr_family: {checked} values verified bit-for-bit against live PostgreSQL");
}

// ─────────────────────────────────────────────────────────────────────────
// bool_and / bool_or / bit_and / bit_or / bit_xor
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn boolean_and_bitwise_aggregates_match_live_postgres() {
    let Some((rt, client)) = connect("boolean_and_bitwise") else {
        return;
    };

    let bool_sql = "SELECT bool_and(b)::text, bool_or(b)::text \
                    FROM unnest($1::bool[]) AS t(b)";
    let bit_sql = "SELECT bit_and(i)::text, bit_or(i)::text, bit_xor(i)::text \
                   FROM unnest($1::int8[]) AS t(i)";

    let mut checked = 0usize;
    for seed in 201..=216u64 {
        let mut rng = Lcg(seed);
        // Deliberately includes the degenerate lengths: an empty input (all
        // five aggregates are NULL, never the operator's identity element)
        // and an all-NULL one.
        let rows = (seed % 5) as usize * 3;
        let bs: Vec<Option<bool>> = (0..rows)
            .map(|_| match rng.below(4) {
                0 => None,
                n => Some(n == 1),
            })
            .collect();
        let is: Vec<Option<i64>> = (0..rows)
            .map(|_| match rng.below(5) {
                0 => None,
                // Signed, and spanning zero: two's-complement `bit_and`/
                // `bit_or` over negatives is where a width- or sign-confused
                // implementation shows up.
                _ => Some(rng.next_u64() as i64 % 1024 - 512),
            })
            .collect();

        let brow = rt
            .block_on(client.query_one(bool_sql, &[&bs]))
            .expect("bool query");
        let irow = rt
            .block_on(client.query_one(bit_sql, &[&is]))
            .expect("bit query");

        let bschema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("b", DataType::Boolean, true)]));
        let bbatch = RecordBatch::try_new(
            Arc::clone(&bschema),
            vec![Arc::new(BooleanArray::from(bs.clone())) as ArrayRef],
        )
        .unwrap();
        let bout = run_aggregates(
            bschema,
            bbatch,
            vec![
                AggregateSpec {
                    func: AggFunc::BoolAnd,
                    input_col: Some(0),
                    distinct: false,
                    filter_col: None,
                    alias: "a".into(),
                },
                AggregateSpec {
                    func: AggFunc::BoolOr,
                    input_col: Some(0),
                    distinct: false,
                    filter_col: None,
                    alias: "o".into(),
                },
            ],
        );
        for (i, name) in ["bool_and", "bool_or"].iter().enumerate() {
            let want = brow.get::<_, Option<&str>>(i).map(|s| s == "true");
            assert_eq!(
                cell_bool(&bout, i),
                want,
                "{name} diverged on seed {seed} over {bs:?}"
            );
            checked += 1;
        }

        let ischema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]));
        let ibatch = RecordBatch::try_new(
            Arc::clone(&ischema),
            vec![Arc::new(Int64Array::from(is.clone())) as ArrayRef],
        )
        .unwrap();
        let iout = run_aggregates(
            ischema,
            ibatch,
            [AggFunc::BitAnd, AggFunc::BitOr, AggFunc::BitXor]
                .into_iter()
                .enumerate()
                .map(|(i, func)| AggregateSpec {
                    func,
                    input_col: Some(0),
                    distinct: false,
                    filter_col: None,
                    alias: format!("b{i}"),
                })
                .collect(),
        );
        for (i, name) in ["bit_and", "bit_or", "bit_xor"].iter().enumerate() {
            let want: Option<i64> = irow
                .get::<_, Option<&str>>(i)
                .map(|s| s.parse().expect("bigint"));
            assert_eq!(
                cell_i64(&iout, i),
                want,
                "{name} diverged on seed {seed} over {is:?}"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 16 * 5);
    eprintln!("boolean_and_bitwise: {checked} values verified against live PostgreSQL");
}

// ─────────────────────────────────────────────────────────────────────────
// percent_rank / cume_dist / ntile
// ─────────────────────────────────────────────────────────────────────────

/// One column per window function, in `(percent_rank, cume_dist, ntile)`
/// order — `float8`, `float8`, `int4`, matching what Postgres declares for
/// the three.
type WindowColumns = (Vec<Option<f64>>, Vec<Option<f64>>, Vec<Option<i32>>);

/// Run the three window functions over one already-sorted `x` column.
fn run_windows(xs: &[i64], buckets: i64) -> WindowColumns {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("n", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(xs.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(vec![buckets; xs.len()])) as ArrayRef,
        ],
    )
    .unwrap();
    let spec = |func, alias: &str| WindowSpec {
        func,
        arg_col: None,
        offset_col: None,
        default_col: None,
        nth_col: None,
        frame: None,
        alias: alias.into(),
    };
    let mut op = WindowAgg::new(
        source(Arc::clone(&schema), vec![batch]),
        vec![],
        vec![OrderKey {
            column: 0,
            descending: false,
            nulls_first: true,
        }],
        vec![
            spec(WindowFunc::PercentRank, "pr"),
            spec(WindowFunc::CumeDist, "cd"),
            spec(WindowFunc::Ntile { buckets_col: 1 }, "nt"),
        ],
        usize::MAX,
    )
    .expect("WindowAgg::new");

    let mut pr = Vec::new();
    let mut cd = Vec::new();
    let mut nt = Vec::new();
    while let Some(b) = op.next_batch().expect("window operator must not error") {
        let p = b
            .column_by_name("pr")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let c = b
            .column_by_name("cd")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let n = b
            .column_by_name("nt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        pr.extend(p.iter());
        cd.extend(c.iter());
        nt.extend(n.iter());
    }
    (pr, cd, nt)
}

/// `percent_rank`/`cume_dist` over data **with ties**, which is where an
/// implementation that counts rows instead of peer groups diverges. `ntile`
/// is checked separately, on distinct values, because it splits peers by
/// physical position and PostgreSQL's row order within a peer group is not
/// specified — comparing it under ties would be comparing an arbitrary
/// choice, not a semantic.
#[test]
fn percent_rank_and_cume_dist_match_live_postgres_under_ties() {
    let Some((rt, client)) = connect("percent_rank_and_cume_dist") else {
        return;
    };

    let sql = "SELECT percent_rank() OVER (ORDER BY x)::text, \
                      cume_dist()    OVER (ORDER BY x)::text \
                 FROM unnest($1::int8[]) AS t(x) ORDER BY x";

    let mut checked = 0usize;
    for seed in 301..=316u64 {
        let mut rng = Lcg(seed);
        let rows = 1 + (seed % 17) as usize;
        // A small value range against a larger row count guarantees heavy
        // tying — the point of the fixture.
        let mut xs: Vec<i64> = (0..rows).map(|_| rng.below(5) as i64).collect();
        xs.sort_unstable();

        let pg = rt
            .block_on(client.query(sql, &[&xs]))
            .expect("window query");
        assert_eq!(pg.len(), rows);

        let (pr, cd, _) = run_windows(&xs, 1);
        for (i, row) in pg.iter().enumerate() {
            let want_pr = parse_pg_f64(row.get::<_, Option<&str>>(0));
            let want_cd = parse_pg_f64(row.get::<_, Option<&str>>(1));
            assert!(
                same_f64(pr[i], want_pr),
                "percent_rank row {i} diverged on seed {seed} over {xs:?}: \
                 basin {:?}, PostgreSQL {want_pr:?}",
                pr[i]
            );
            assert!(
                same_f64(cd[i], want_cd),
                "cume_dist row {i} diverged on seed {seed} over {xs:?}: \
                 basin {:?}, PostgreSQL {want_cd:?}",
                cd[i]
            );
            checked += 2;
        }
    }
    assert!(checked > 0);
    eprintln!("percent_rank/cume_dist: {checked} values verified against live PostgreSQL");
}

#[test]
fn ntile_matches_live_postgres_over_distinct_values() {
    let Some((rt, client)) = connect("ntile") else {
        return;
    };

    let sql = "SELECT ntile($2::int4) OVER (ORDER BY x) \
                 FROM unnest($1::int8[]) AS t(x) ORDER BY x";

    let mut checked = 0usize;
    for seed in 401..=420u64 {
        let mut rng = Lcg(seed);
        let rows = 1 + (seed % 23) as usize;
        // Distinct and strictly increasing, so PostgreSQL's row order inside
        // the window is fully determined and `ntile` has one right answer.
        let mut acc = 0i64;
        let xs: Vec<i64> = (0..rows)
            .map(|_| {
                acc += 1 + rng.below(3) as i64;
                acc
            })
            .collect();
        // Buckets deliberately range over "fewer than rows", "equal to rows"
        // and "more than rows" — the last is the empty-bucket case.
        let buckets = 1 + (seed % 9) as i32;

        let pg = rt
            .block_on(client.query(sql, &[&xs, &buckets]))
            .expect("ntile query");
        assert_eq!(pg.len(), rows);

        let (_, _, nt) = run_windows(&xs, buckets as i64);
        for (i, row) in pg.iter().enumerate() {
            let want: i32 = row.get(0);
            assert_eq!(
                nt[i],
                Some(want),
                "ntile({buckets}) row {i} diverged on seed {seed} over {rows} rows"
            );
            checked += 1;
        }
    }
    assert!(checked > 0);
    eprintln!("ntile: {checked} values verified against live PostgreSQL");
}
