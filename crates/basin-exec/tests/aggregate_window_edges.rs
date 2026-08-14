//! Differential test for the aggregate and window edges where "looks right"
//! and "is what PostgreSQL does" come apart.
//!
//! `statistical_aggregates.rs` covers the *values* the statistical family
//! computes. This file covers four things that file does not, each of which
//! has bitten this migration or was one fixture away from doing so:
//!
//! 1. **`DISTINCT` inside an aggregate changes the output ORDER.** PostgreSQL
//!    implements `DISTINCT` by sorting the aggregate's input, so
//!    `array_agg(DISTINCT x)` and `string_agg(DISTINCT s, d)` — the two
//!    aggregates in `aggregate.rs` whose output order is observable — come
//!    back sorted, not in arrival order. This was wrong until the fix that
//!    added this file, and every existing test agreed with PostgreSQL anyway
//!    because every fixture happened to arrive already sorted. The randomised
//!    data below does not.
//! 2. **Zero *accepted* rows is not zero rows.** `sum` -> NULL, `count` -> 0,
//!    `array_agg` -> NULL (not `{}`), `string_agg` -> NULL (not `''`) — and
//!    the same must hold when the rows exist but `FILTER (WHERE …)` rejects
//!    all of them, which is a different code path in this operator (the
//!    accumulator is constructed and then never updated).
//! 3. **`lag`/`lead`'s third argument.** The default is used only when the
//!    offset lands outside the partition; a row that exists but holds NULL
//!    yields NULL, not the default. `window.rs` implements this
//!    (`WindowSpec::default_col`) but no query can reach it today — see the
//!    note at the bottom of this file.
//! 4. **A `RANGE` frame over NULL order values.** The NULLs form one peer
//!    group, and a `RANGE` frame's CURRENT ROW means "through the end of my
//!    peer group", so every NULL row sees every other NULL row.
//!
//! # Skipping
//!
//! Requires `PG_DIFF_TEST_DSN`, the same convention as this crate's
//! `statistical_aggregates.rs` and `function_equivalence.rs`. Unset, every
//! test here prints why it checked nothing and returns.
//!
//! ```text
//! PG_DIFF_TEST_DSN='postgres://pc@127.0.0.1:5432/postgres' \
//!   cargo test -p basin-exec --test aggregate_window_edges
//! ```

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use basin_exec::aggregate::{AggFunc, AggregateSpec, HashAggregate};
use basin_exec::operator::Operator;
use basin_exec::scan::{Scan, VecBatchSource};
use basin_exec::window::{OrderKey, WindowAgg, WindowFunc, WindowSpec};

// ─────────────────────────────────────────────────────────────────────────
// Plumbing (same posture as statistical_aggregates.rs)
// ─────────────────────────────────────────────────────────────────────────

fn connect(who: &str) -> Option<(tokio::runtime::Runtime, tokio_postgres::Client)> {
    let dsn = match std::env::var("PG_DIFF_TEST_DSN") {
        Ok(d) if !d.is_empty() => d,
        _ => {
            eprintln!(
                "\n*** {who}: SKIPPED — PG_DIFF_TEST_DSN is unset, so NOTHING was checked \
                 against a live server this run. Set it to a libpq DSN (e.g. \
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

/// Fixed LCG rather than a `rand` dependency — a failure has to be
/// reproducible from the seed printed in its own assertion message.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 11) % n
    }
}

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

fn ungrouped(schema: SchemaRef, batch: RecordBatch, specs: Vec<AggregateSpec>) -> RecordBatch {
    let mut agg = HashAggregate::new(source(Arc::clone(&schema), vec![batch]), vec![], specs, usize::MAX)
        .expect("HashAggregate::new over a well-typed input");
    let out = agg
        .next_batch()
        .expect("aggregate must not error")
        .expect("an ungrouped aggregate always emits exactly one row");
    assert_eq!(out.num_rows(), 1);
    out
}

fn spec(func: AggFunc, input_col: Option<usize>, distinct: bool, alias: &str) -> AggregateSpec {
    AggregateSpec {
        func,
        input_col,
        distinct,
        filter_col: None,
        alias: alias.into(),
    }
}

/// The elements of a `List<Int64>` output cell, or `None` if the cell is SQL
/// NULL — the distinction `array_agg` over zero rows turns on.
fn list_i64(batch: &RecordBatch, col: usize) -> Option<Vec<Option<i64>>> {
    let list = batch
        .column(col)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("array_agg output is a ListArray");
    if list.is_null(0) {
        return None;
    }
    let values = list.value(0);
    let ints = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("List<Int64> child");
    Some((0..ints.len()).map(|i| (!ints.is_null(i)).then(|| ints.value(i))).collect())
}

fn text(batch: &RecordBatch, col: usize) -> Option<String> {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("text output");
    (!a.is_null(0)).then(|| a.value(0).to_string())
}

// ─────────────────────────────────────────────────────────────────────────
// 1. DISTINCT sorts
// ─────────────────────────────────────────────────────────────────────────

/// `array_agg(DISTINCT x)` element for element, over data whose arrival order
/// is deliberately not its sorted order.
#[test]
fn array_agg_distinct_matches_live_postgres_element_for_element() {
    let Some((rt, client)) = connect("array_agg_distinct_matches_live_postgres") else {
        return;
    };
    let mut checked = 0;
    for seed in 0..12u64 {
        let mut rng = Lcg(seed.wrapping_mul(0x9E3779B97F4A7C15));
        // Small value domain so duplicates are common, and ~1 in 6 NULL so
        // the "one NULL survives, sorted last" rule is exercised.
        let xs: Vec<Option<i64>> = (0..25)
            .map(|_| match rng.below(6) {
                0 => None,
                _ => Some(rng.below(12) as i64 - 6),
            })
            .collect();

        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(xs.clone())) as ArrayRef],
        )
        .unwrap();
        let out = ungrouped(
            Arc::clone(&schema),
            batch,
            vec![spec(AggFunc::ArrayAgg, Some(0), true, "a")],
        );

        let want: Option<Vec<Option<i64>>> = rt.block_on(async {
            client
                .query_one(
                    "select array_agg(distinct x) from unnest($1::bigint[]) as t(x)",
                    &[&xs],
                )
                .await
                .expect("array_agg(distinct)")
                .get(0)
        });

        assert_eq!(
            list_i64(&out, 0),
            want,
            "array_agg(DISTINCT x) diverged on seed {seed} over {xs:?}"
        );
        checked += 1;
    }
    assert_eq!(checked, 12);
    eprintln!("array_agg(DISTINCT): {checked} datasets verified against live PostgreSQL");
}

/// `string_agg(DISTINCT s, ',')`, same idea. A constant delimiter, because a
/// per-row delimiter under `DISTINCT` only has a defined answer when no two
/// rows share a value — see the unit test in `aggregate.rs` for that shape.
#[test]
fn string_agg_distinct_matches_live_postgres() {
    let Some((rt, client)) = connect("string_agg_distinct_matches_live_postgres") else {
        return;
    };
    // Lowercase ASCII only: PostgreSQL orders text by the database collation
    // (en_US.UTF-8 on the reference server) and this crate orders by bytes.
    // They agree here; mixed case or non-ASCII would be testing the
    // engine-wide collation gap rather than this fix.
    const WORDS: &[&str] = &["pear", "apple", "fig", "kiwi", "plum", "date"];
    let mut checked = 0;
    for seed in 0..12u64 {
        let mut rng = Lcg(seed.wrapping_mul(0xD1B54A32D192ED03));
        let ss: Vec<Option<String>> = (0..20)
            .map(|_| match rng.below(6) {
                0 => None,
                _ => Some(WORDS[rng.below(WORDS.len() as u64) as usize].to_string()),
            })
            .collect();

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(ss.clone())) as ArrayRef,
                Arc::new(StringArray::from(vec![","; ss.len()])) as ArrayRef,
            ],
        )
        .unwrap();
        let out = ungrouped(
            Arc::clone(&schema),
            batch,
            vec![AggregateSpec {
                func: AggFunc::StringAgg { delim_col: 1 },
                input_col: Some(0),
                distinct: true,
                filter_col: None,
                alias: "s".into(),
            }],
        );

        let want: Option<String> = rt.block_on(async {
            client
                .query_one(
                    "select string_agg(distinct s, ',') from unnest($1::text[]) as t(s)",
                    &[&ss],
                )
                .await
                .expect("string_agg(distinct)")
                .get(0)
        });

        assert_eq!(
            text(&out, 0),
            want,
            "string_agg(DISTINCT s, ',') diverged on seed {seed} over {ss:?}"
        );
        checked += 1;
    }
    assert_eq!(checked, 12);
    eprintln!("string_agg(DISTINCT): {checked} datasets verified against live PostgreSQL");
}

// ─────────────────────────────────────────────────────────────────────────
// 2. Zero accepted rows
// ─────────────────────────────────────────────────────────────────────────

/// Over an empty input, and over a non-empty input every row of which
/// `FILTER (WHERE …)` rejects, each aggregate must produce the same thing —
/// and for most of them that thing is NULL rather than an identity element.
#[test]
fn aggregates_over_zero_accepted_rows_match_live_postgres() {
    let Some((rt, client)) = connect("aggregates_over_zero_accepted_rows") else {
        return;
    };

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("d", DataType::Utf8, true),
        Field::new("keep", DataType::Boolean, true),
    ]));
    let specs = || {
        vec![
            spec(AggFunc::Sum, Some(0), false, "sum"),
            spec(AggFunc::Count, Some(0), false, "count"),
            spec(AggFunc::CountStar, None, false, "count_star"),
            spec(AggFunc::Avg, Some(0), false, "avg"),
            spec(AggFunc::Min, Some(0), false, "min"),
            spec(AggFunc::Max, Some(0), false, "max"),
            spec(AggFunc::ArrayAgg, Some(0), false, "array_agg"),
            AggregateSpec {
                func: AggFunc::StringAgg { delim_col: 1 },
                input_col: Some(1),
                distinct: false,
                filter_col: None,
                alias: "string_agg".into(),
            },
        ]
    };

    // (a) an input with no rows at all.
    let empty = RecordBatch::new_empty(Arc::clone(&schema));
    let from_empty = ungrouped(Arc::clone(&schema), empty, specs());

    // (b) three real rows, every one of them rejected by FILTER. Same
    // expected answer, different path: the accumulators exist and are never
    // updated, rather than never seeing a batch.
    let rows = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![",", ",", ","])) as ArrayRef,
            Arc::new(arrow_array::BooleanArray::from(vec![false, false, false])) as ArrayRef,
        ],
    )
    .unwrap();
    let filtered_specs: Vec<AggregateSpec> = specs()
        .into_iter()
        .map(|mut s| {
            s.filter_col = Some(2);
            s
        })
        .collect();
    let from_filter = ungrouped(Arc::clone(&schema), rows, filtered_specs);

    let row = rt.block_on(async {
        client
            .query_one(
                "select sum(x)::text, count(x)::text, count(*)::text, avg(x)::text, \
                 min(x)::text, max(x)::text, array_agg(x)::text, string_agg(d, ',')::text \
                 from (select 1::bigint as x, ','::text as d) v where false",
                &[],
            )
            .await
            .expect("zero-row aggregate row")
    });

    let names = [
        "sum",
        "count",
        "count(*)",
        "avg",
        "min",
        "max",
        "array_agg",
        "string_agg",
    ];
    for (i, name) in names.iter().enumerate() {
        let want: Option<String> = row.get(i);
        for (which, got) in [("empty input", &from_empty), ("FILTER rejects all", &from_filter)] {
            let is_null = got.column(i).is_null(0);
            assert_eq!(
                is_null,
                want.is_none(),
                "{name} over {which}: Basin null={is_null}, PostgreSQL null={}",
                want.is_none()
            );
            // The two counts are the ones with a non-NULL answer, and getting
            // them as 0 rather than NULL is the whole point.
            if let Some(w) = &want {
                let n = got
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("only the counts are non-NULL here, and both are bigint")
                    .value(0);
                assert_eq!(
                    n.to_string(),
                    *w,
                    "{name} over {which} diverged from PostgreSQL"
                );
            }
        }
    }
    eprintln!("zero-accepted-rows: 8 aggregates x 2 paths verified against live PostgreSQL");
}

// ─────────────────────────────────────────────────────────────────────────
// 3. lag/lead with an explicit default
// ─────────────────────────────────────────────────────────────────────────

/// `lag(x, 1, d)` / `lead(x, 1, d)`: the default appears only where the
/// offset leaves the partition. A row that exists but holds NULL gives NULL —
/// the distinction an implementation that treats "no value" and "outside the
/// partition" alike gets wrong, and the reason the fixture below puts a NULL
/// next to both edges.
#[test]
fn lag_lead_explicit_default_matches_live_postgres() {
    let Some((rt, client)) = connect("lag_lead_explicit_default") else {
        return;
    };
    let xs: Vec<Option<i64>> = vec![Some(7), None, Some(8), Some(9), None];
    const DEFAULT: i64 = -999;

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("off", DataType::Int64, true),
        Field::new("def", DataType::Int64, true),
        Field::new("i", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(xs.clone())) as ArrayRef,
            Arc::new(Int64Array::from(vec![1i64; xs.len()])) as ArrayRef,
            Arc::new(Int64Array::from(vec![DEFAULT; xs.len()])) as ArrayRef,
            Arc::new(Int64Array::from(
                (1..=xs.len() as i64).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();

    let offset_spec = |func, alias: &str| WindowSpec {
        func,
        arg_col: Some(0),
        offset_col: Some(1),
        default_col: Some(2),
        nth_col: None,
        frame: None,
        alias: alias.into(),
    };
    let mut op = WindowAgg::new(
        source(Arc::clone(&schema), vec![batch]),
        vec![],
        vec![OrderKey {
            column: 3,
            descending: false,
            nulls_first: false,
        }],
        vec![
            offset_spec(WindowFunc::Lag, "lag"),
            offset_spec(WindowFunc::Lead, "lead"),
        ],
        usize::MAX,
    )
    .expect("WindowAgg::new");

    let mut got_lag = Vec::new();
    let mut got_lead = Vec::new();
    while let Some(b) = op.next_batch().expect("window operator must not error") {
        for (name, out) in [("lag", &mut got_lag), ("lead", &mut got_lead)] {
            let a = b
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            out.extend(a.iter());
        }
    }

    let rows = rt.block_on(async {
        client
            .query(
                "select lag(x, 1, $2::bigint) over (order by i), \
                        lead(x, 1, $2::bigint) over (order by i) \
                 from unnest($1::bigint[]) with ordinality as t(x, i) order by i",
                &[&xs, &DEFAULT],
            )
            .await
            .expect("lag/lead with an explicit default")
    });
    let want_lag: Vec<Option<i64>> = rows.iter().map(|r| r.get(0)).collect();
    let want_lead: Vec<Option<i64>> = rows.iter().map(|r| r.get(1)).collect();

    assert_eq!(got_lag, want_lag, "lag(x, 1, {DEFAULT}) over {xs:?}");
    assert_eq!(got_lead, want_lead, "lead(x, 1, {DEFAULT}) over {xs:?}");
    eprintln!(
        "lag/lead defaults: {} rows verified against live PostgreSQL",
        want_lag.len()
    );
}

// ─────────────────────────────────────────────────────────────────────────
// 4. RANGE frame over NULL order values
// ─────────────────────────────────────────────────────────────────────────

/// `sum(x) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT
/// ROW)` where `k` holds NULLs. All the NULLs are ONE peer group and sort
/// last (PostgreSQL's `ASC` default), so every NULL row's frame is the whole
/// partition — the running total stops changing across them rather than
/// continuing to climb row by row.
///
/// The input is fed pre-sorted because `WindowAgg` sorts nothing; the
/// PostgreSQL side is given the same order explicitly.
#[test]
fn range_frame_over_null_order_values_matches_live_postgres() {
    let Some((rt, client)) = connect("range_frame_over_null_order_values") else {
        return;
    };
    // Sorted by k ascending, NULLs last — including a tie (two 20s), so the
    // peer-group rule is exercised on non-NULL values too.
    let ks: Vec<Option<i64>> = vec![Some(10), Some(20), Some(20), Some(30), None, None];
    let xs: Vec<Option<i64>> = vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)];

    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ks.clone())) as ArrayRef,
            Arc::new(Int64Array::from(xs.clone())) as ArrayRef,
        ],
    )
    .unwrap();

    let mut op = WindowAgg::new(
        source(Arc::clone(&schema), vec![batch]),
        vec![],
        vec![OrderKey {
            column: 0,
            descending: false,
            nulls_first: false,
        }],
        vec![WindowSpec {
            func: WindowFunc::Sum,
            arg_col: Some(1),
            offset_col: None,
            default_col: None,
            nth_col: None,
            // None means "not written in the SQL", which resolves to
            // PostgreSQL's default frame: RANGE UNBOUNDED PRECEDING TO
            // CURRENT ROW when there is an ORDER BY.
            frame: None,
            alias: "running".into(),
        }],
        usize::MAX,
    )
    .expect("WindowAgg::new");

    let mut got = Vec::new();
    while let Some(b) = op.next_batch().expect("window operator must not error") {
        let a = b
            .column_by_name("running")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        got.extend(a.iter());
    }

    let rows = rt.block_on(async {
        client
            .query(
                "select sum(x) over (order by k) \
                 from unnest($1::bigint[], $2::bigint[]) with ordinality as t(k, x, i) \
                 order by k nulls last, i",
                &[&ks, &xs],
            )
            .await
            .expect("default RANGE frame over NULL order values")
    });
    let want: Vec<Option<i64>> = rows.iter().map(|r| r.get(0)).collect();

    assert_eq!(
        got, want,
        "default RANGE frame over k={ks:?} x={xs:?} — NULLs are one peer group"
    );
    eprintln!(
        "RANGE over NULL peers: {} rows verified against live PostgreSQL",
        want.len()
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Reachability
//
// `lag_lead_explicit_default_matches_live_postgres` above verifies an
// operator feature that NO SQL can currently reach: `lead(x, n, default)` is
// pg_proc oid 3111 (`lag` is 3108), and neither has a `FuncSig` in
// `basin-pgtype`'s table nor an arm in `build.rs`'s `window_func_of`, so the
// three-argument spelling falls back to DataFusion before it ever reaches
// `window.rs`. That is exactly why the test is here: when the wiring lands,
// the semantics it wires up are already known-correct rather than
// assumed-correct. It is NOT evidence that `SELECT lead(n, 1, 0) OVER (…)`
// is served — the fallback probe is, and it still says otherwise.
// ─────────────────────────────────────────────────────────────────────────
