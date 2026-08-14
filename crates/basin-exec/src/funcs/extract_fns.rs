//! `extract(FIELD FROM value)` — `pg_proc` oids 6199-6204.
//!
//! # THIS MODULE IS NOT REGISTERED, AND MUST NOT BE UNTIL THE ABI GROWS
//!
//! Read `extract_fns.integration.md` beside this file before touching
//! anything here. The short version, because it is the whole point of the
//! slice:
//!
//! `extract` returns `numeric`, and the `Decimal128` **scale** of the result
//! is a function of the unit's *value* — measured on live PostgreSQL 18.2,
//! `second`/`epoch` are scale 6, `millisecond(s)` scale 3, everything else
//! scale 0, for `timestamp`, `timestamptz` and `interval` alike.
//! [`crate::eval::eval_extract`] can only insist on that because it receives
//! the **unevaluated** field argument and can read the literal off the `Expr`.
//!
//! This ABI hands implementations `&[ArrayRef]`, and
//! `crate::project::Project::new` fixes a projection's output schema by
//! evaluating each expression against a **zero-row probe batch**:
//!
//! ```text
//! let probe = RecordBatch::new_empty(Arc::clone(&input_schema));
//! let array = eval::eval(expr, &probe)?;          // <- schema comes from here
//! ```
//!
//! `eval_literal` broadcasts a literal to `batch.num_rows()`, so over that
//! probe the unit arrives as a **length-0** `StringArray` and the unit string
//! is simply gone. An implementation can then fail (the plan does not build)
//! or guess a scale (`RecordBatch::try_new` rejects the first real batch,
//! which reports a different scale). Both break `extract` in *every* query
//! that uses it — this is not an edge case, it is the ordinary path.
//!
//! `ScalarFunc::return_type` does not rescue it: it takes `&[PgType]`, which
//! is argument *types*, not argument *values*, and nothing calls it today
//! anyway. The fix is a plan-time constant side-channel — `invoke_bound`,
//! spelled out in §6 of the integration note — which carries `Datum`s lifted
//! straight off the `Expr` tree with nothing evaluated, and so cannot
//! reintroduce the double evaluation `&[ArrayRef]` exists to prevent (#151).
//!
//! Until that lands, `eval_scalar_fn`'s six-oid `match` arm stays exactly
//! where it is, and the impls below are prepared, not live.
//!
//! # Why they are written down anyway
//!
//! Because the *rest* of the port is settled and would otherwise have to be
//! rediscovered: the argument order, the fail-closed cases, the delegation
//! boundary, and the divergences measured on the way. When §5 and §6 of the
//! integration note land, wiring these six is three edits and no thinking.
//!
//! # Where the bodies are, and why
//!
//! Each impl delegates to [`crate::eval::eval_extract`], exactly as
//! [`crate::funcs::dt_fns`] delegates for the eleven `date_part`/`date_trunc`/
//! `to_char`/`age` oids, and for the same reason: a port moves *hosting* and
//! nothing else. Copying `parse_date_unit`, `DateUnit`, `extract_scale`,
//! `temporal_kind`, `temporal_readings` or `date_part_of` across would leave
//! two copies of Postgres's unit vocabulary in one crate, free to drift apart
//! — which is the failure the `date_part`/`extract` pair was built to avoid.
//!
//! # One implementation, six oids, dispatching on the Arrow type
//!
//! `eval_extract` reads the argument's actual Arrow type rather than trusting
//! the oid, and that is correct rather than a shortcut: overload resolution is
//! not right yet, so `extract(EPOCH FROM ts)` and `extract(YEAR FROM day)`
//! both reach `basin_pgtype::func::resolve` as `extract(unknown, unknown)` and
//! come back as the *same* oid. The oid is known-unreliable input; the
//! evaluated argument's type is ground truth. All six structs below therefore
//! have identical bodies and differ only in the oid they register under, which
//! is exactly what the deleted `match` arm expressed by listing six patterns
//! against one body.
//!
//! # Two of the six answer nothing, on both sides, on purpose
//!
//! `extract(… FROM time)` (6200), `extract(… FROM timetz)` (6201) and
//! `extract(… FROM interval)` (6204) are real PostgreSQL calls that Basin does
//! not implement: `temporal_kind` classifies only `Date32` and
//! `Timestamp(Microsecond, _)`, so all three fail closed with
//! `"extract on … is not implemented — only date, timestamp and timestamptz
//! are"`. That is unchanged here and deliberately so. In particular **6204 is
//! not wired up by adding an `Interval` arm to `temporal_kind`**: `extract`'s
//! body reads a *civil time*, while an interval has no position on the
//! calendar and Postgres computes it with a different C function
//! (`interval_part`) whose rules Basin reproduces separately in
//! `eval_date_part_interval` — the `quarter` branch on `months` rather than
//! `months % 12`, and the load-bearing summation order for `epoch`. Routing an
//! interval through `temporal_readings` would produce wrong answers, not
//! missing ones. See §5 of the integration note for the shape 6204 actually
//! needs; it is a new implementation, not a port.
//!
//! They are still listed here because they were on the `match` as one arm and
//! must leave it as one arm: hosting five sixths of a function and leaving the
//! sixth on the `match` makes the `match` a graveyard nobody can read.

use arrow_array::{Array, ArrayRef, StringArray};
use basin_pgtype::Oid;

use crate::eval::{self, downcast_array, EvalSession};
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// The refusal the deleted `match` arm raised, moved verbatim.
///
/// `eval_extract` used to build this itself when the field argument was not an
/// `Expr::Literal(Utf8)`. Once the unit arrives as an evaluated array that test
/// no longer exists, so the message moves here rather than being lost — it is
/// the only thing that tells a reader why a perfectly legal PostgreSQL call is
/// refused.
fn non_literal_unit() -> ExecError {
    ExecError::Internal(
        "extract with a non-literal field is not implemented — the result's numeric \
         scale depends on the field, and one Arrow array carries one scale"
            .into(),
    )
}

/// Recover the one unit string the whole array must agree on.
///
/// **This is the stopgap, and it is not the old test.** The deleted arm asked
/// the *plan* "is this argument a literal?"; this asks the *data* "is this
/// column constant?". The two differ in both directions, and both differences
/// are pinned in this module's tests:
///
/// * a computed constant (`extract(lower('YEAR') FROM ts)`) was refused and is
///   now accepted — which happens to agree with PostgreSQL, and is still a
///   change of acceptance set rather than a port;
/// * a **zero-row** batch was accepted and is now refused — which is the
///   blocker in this module's header, because `Project::new`'s schema probe is
///   a zero-row batch.
///
/// The `Expr::Literal` test is recovered exactly by the `invoke_bound`
/// side-channel in §6 of the integration note. Nothing else recovers it: by
/// the time an argument is an `ArrayRef`, "a literal" and "a computed
/// constant" are the same array.
fn literal_unit(units: &ArrayRef) -> Result<&str, ExecError> {
    let u = downcast_array::<StringArray>(units, "text")?;
    if u.is_empty() || u.is_null(0) {
        return Err(non_literal_unit());
    }
    let first = u.value(0);
    for i in 1..u.len() {
        if u.is_null(i) || u.value(i) != first {
            return Err(non_literal_unit());
        }
    }
    Ok(first)
}

/// The body all six oids share.
///
/// The value argument is fetched *before* the unit, which is the order the
/// deleted arm used (`let value = a(1)?;` and only then `args.first()`), so a
/// short call reports the same missing index it did before. Both are fetched
/// through [`arg`] so that a short call is reported as the planner bug it is
/// rather than panicking.
fn extract_one(
    args: &[ArrayRef],
    oid: u32,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let value = arg(args, 1, oid)?;
    let unit = arg(args, 0, oid)?;
    eval::eval_extract(literal_unit(unit)?, value, session)
}

/// `extract(text, date) -> numeric`, oid 6199.
///
/// Postgres's `extract_date`, which is **not** `date_part(text, date)`: that
/// one reaches the timestamp implementation by an implicit `date -> timestamp`
/// cast and so answers `date_part('hour', DATE '2024-03-05')` as `0`, while
/// `extract(hour FROM DATE '2024-03-05')` is
/// `ERROR: unit "hour" not supported for type date`. All eight refused units
/// are measured in §7.1 of the integration note; `extract_scale`'s `Date` arm
/// already reproduces exactly that set.
pub struct ExtractDate;

impl ScalarFunc for ExtractDate {
    fn oid(&self) -> Oid {
        Oid(6199)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6199, session)
    }
}

/// `extract(text, time) -> numeric`, oid 6200. **Fails closed** —
/// `temporal_kind` does not classify `Time64`. Unimplemented before this
/// module and unimplemented after it; see the module doc.
pub struct ExtractTime;

impl ScalarFunc for ExtractTime {
    fn oid(&self) -> Oid {
        Oid(6200)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6200, session)
    }
}

/// `extract(text, timetz) -> numeric`, oid 6201. **Fails closed**, as 6200
/// does, and for the same reason.
pub struct ExtractTimetz;

impl ScalarFunc for ExtractTimetz {
    fn oid(&self) -> Oid {
        Oid(6201)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6201, session)
    }
}

/// `extract(text, timestamp) -> numeric`, oid 6202.
///
/// Session-independent: a `timestamp without time zone` is already a civil
/// reading, so `session` is passed on only because one function serves all six
/// oids and the `timestamptz` one needs it.
pub struct ExtractTimestamp;

impl ScalarFunc for ExtractTimestamp {
    fn oid(&self) -> Oid {
        Oid(6202)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6202, session)
    }
}

/// `extract(text, timestamptz) -> numeric`, oid 6203. **Session-dependent**:
/// every field except `epoch` is read off the session-local rendering, and
/// `timezone`/`timezone_hour`/`timezone_minute` report that rendering's offset.
pub struct ExtractTimestamptz;

impl ScalarFunc for ExtractTimestamptz {
    fn oid(&self) -> Oid {
        Oid(6203)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6203, session)
    }
}

/// `extract(text, interval) -> numeric`, oid 6204. **Fails closed.**
///
/// On the server this agrees with `date_part(text, interval)` (oid 1172) on
/// every accepted unit's value and on every rejection, differing only in
/// returning `numeric` — at scale 6 for `second`/`epoch`, 3 for
/// `millisecond(s)` and 0 for everything else, re-measured in §3 of the
/// integration note. Basin cannot answer it yet: `temporal_kind` refuses an
/// `Interval(MonthDayNano)`, and the fix is **not** an extra arm there. See
/// the module doc.
pub struct ExtractInterval;

impl ScalarFunc for ExtractInterval {
    fn oid(&self) -> Oid {
        Oid(6204)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        extract_one(args, 6204, session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::builtins;
    use std::sync::Arc;

    use arrow_array::types::IntervalMonthDayNano;
    use arrow_array::{
        Date32Array, Decimal128Array, IntervalMonthDayNanoArray, TimestampMicrosecondArray,
    };
    use arrow_schema::DataType;

    /// The six oids this module prepares.
    const PREPARED: &[u32] = &[6199, 6200, 6201, 6202, 6203, 6204];

    fn text(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn ts(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
    }

    /// `2024-03-05 14:07:09.123456` in epoch microseconds, read off the server.
    const TS_2024_03_05: i64 = 1_709_647_629_123_456;

    /// `2024-03-05` as a `Date32` day number.
    const DAY_2024_03_05: i32 = 19_787;

    fn decimal(out: &ArrayRef) -> &Decimal128Array {
        out.as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("extract returns numeric")
    }

    /// **The status assertion.** These six are deliberately NOT registered:
    /// the unit's value decides the output scale, and `Project::new` fixes the
    /// output schema from a zero-row probe where the unit is unrecoverable.
    /// They must keep falling through to `eval_scalar_fn`'s `match`.
    ///
    /// When the `invoke_bound` side-channel lands and these are registered,
    /// this test flips to asserting `is_some()` — do not simply delete it.
    #[test]
    fn the_six_extract_oids_are_deliberately_not_registered_yet() {
        for oid in PREPARED {
            assert!(
                builtins().scalar(Oid(*oid)).is_none(),
                "oid {oid} must still fall through to the match: the registry ABI cannot \
                 carry the plan-time unit its numeric scale depends on"
            );
        }
    }

    /// A typo in one of six near-identical impls is exactly the mistake that
    /// survives review, and `register_scalar`'s check cannot catch it here
    /// because nothing is registered.
    #[test]
    fn each_impl_reports_its_own_oid() {
        let impls: [(&dyn ScalarFunc, u32); 6] = [
            (&ExtractDate, 6199),
            (&ExtractTime, 6200),
            (&ExtractTimetz, 6201),
            (&ExtractTimestamp, 6202),
            (&ExtractTimestamptz, 6203),
            (&ExtractInterval, 6204),
        ];
        for (f, oid) in impls {
            assert_eq!(f.oid(), Oid(oid));
        }
    }

    /// The scale is a function of the unit, which is the entire blocker.
    /// Measured on live PostgreSQL 18.2 for
    /// `timestamp '2024-03-05 14:07:09.123456'`:
    ///
    /// ```text
    /// extract('second', …) = 9.123456        scale 6
    /// extract('year',   …) = 2024            scale 0
    /// extract('epoch',  …) = 1709647629.123456   scale 6
    /// ```
    #[test]
    fn the_output_scale_depends_on_the_unit_not_on_the_argument_type() {
        let second = ExtractTimestamp
            .invoke(
                &[text(&[Some("second")]), ts(&[Some(TS_2024_03_05)])],
                &EvalSession::DEFAULT,
            )
            .expect("extract(second from timestamp)");
        assert_eq!(second.data_type(), &DataType::Decimal128(38, 6));
        assert_eq!(decimal(&second).value(0), 9_123_456);

        let year = ExtractTimestamp
            .invoke(
                &[text(&[Some("year")]), ts(&[Some(TS_2024_03_05)])],
                &EvalSession::DEFAULT,
            )
            .expect("extract(year from timestamp)");
        assert_eq!(year.data_type(), &DataType::Decimal128(38, 0));
        assert_eq!(decimal(&year).value(0), 2024);

        assert_ne!(
            second.data_type(),
            year.data_type(),
            "two calls to the same oid on the same column type produce two different \
             Arrow types — which is why the schema cannot be derived from a zero-row probe"
        );
    }

    /// **THE BLOCKER, pinned.** A zero-row batch is exactly what
    /// `Project::new` evaluates to decide the output schema, and over it the
    /// unit is gone: `eval_literal` broadcasts a literal to `num_rows()`, so
    /// `'second'` over zero rows is a length-0 `StringArray`.
    ///
    /// The `match` arm this module would replace answers this case correctly,
    /// because it reads the unit off the unevaluated `Expr`. That is the
    /// regression that keeps these six unregistered.
    #[test]
    fn an_empty_batch_cannot_recover_the_unit_which_is_the_blocker() {
        let err = ExtractTimestamp
            .invoke(&[text(&[]), ts(&[])], &EvalSession::DEFAULT)
            .expect_err(
                "a zero-row unit array carries no unit — if this ever starts \
                 succeeding, the ABI grew a plan-time constant and this whole \
                 module can be registered",
            );
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("non-literal field")),
            "got {err:?}"
        );
    }

    /// **KNOWN ACCEPTANCE-SET CHANGE**, in the other direction. The deleted
    /// arm refused any field that was not an `Expr::Literal`, including a
    /// computed constant such as `extract(lower('YEAR') FROM ts)`, which
    /// PostgreSQL accepts and answers `2024`. By the time the argument is an
    /// `ArrayRef` that distinction no longer exists, so this now succeeds.
    ///
    /// It is a divergence from the pre-port behaviour even though it agrees
    /// with the server, and it is recorded rather than celebrated: the
    /// `invoke_bound` side-channel restores the original refusal exactly.
    #[test]
    fn a_computed_constant_unit_is_now_accepted_where_the_match_refused_it() {
        let out = ExtractTimestamp
            .invoke(
                &[
                    text(&[Some("year"), Some("year")]),
                    ts(&[Some(TS_2024_03_05), Some(TS_2024_03_05)]),
                ],
                &EvalSession::DEFAULT,
            )
            .expect("a constant unit column is accepted by the stopgap");
        assert_eq!(decimal(&out).value(0), 2024);
    }

    /// A genuinely varying unit stays refused, as it must: PostgreSQL answers
    /// it with a per-row `dscale` (`2024` and `9.123456` in one column,
    /// measured), and one Arrow `Decimal128` carries one scale for the whole
    /// array. Refusing is fail-closed; guessing a scale would be a wrong
    /// answer to the wire.
    #[test]
    fn a_varying_unit_is_refused_because_one_array_carries_one_scale() {
        let err = ExtractTimestamp
            .invoke(
                &[
                    text(&[Some("year"), Some("second")]),
                    ts(&[Some(TS_2024_03_05), Some(TS_2024_03_05)]),
                ],
                &EvalSession::DEFAULT,
            )
            .expect_err("PostgreSQL answers this per row; Arrow cannot");
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("one Arrow array carries one scale")),
            "got {err:?}"
        );

        // A NULL unit is refused by the same gate rather than producing a NULL
        // row: the scale would still be undecidable.
        let null_unit = ExtractTimestamp
            .invoke(
                &[text(&[None]), ts(&[Some(TS_2024_03_05)])],
                &EvalSession::DEFAULT,
            )
            .expect_err("a NULL unit decides no scale either");
        assert!(matches!(null_unit, ExecError::Internal(_)), "got {null_unit:?}");
    }

    /// NULL value in, NULL out, and the output is as long as the input — the
    /// two invariants every hosted function owes, checked on the one oid that
    /// actually answers.
    #[test]
    fn a_null_value_yields_a_null_row_and_the_length_is_preserved() {
        let out = ExtractTimestamp
            .invoke(
                &[
                    text(&[Some("year"), Some("year"), Some("year")]),
                    ts(&[Some(TS_2024_03_05), None, Some(TS_2024_03_05)]),
                ],
                &EvalSession::DEFAULT,
            )
            .expect("extract over a column with a NULL");
        let d = decimal(&out);
        assert_eq!(d.len(), 3, "output length must match input length");
        assert!(d.is_null(1), "NULL in, NULL out");
        assert_eq!(d.value(0), 2024);
        assert_eq!(d.value(2), 2024);
    }

    /// `extract` on a `date` refuses every sub-day unit and every zone field,
    /// which is precisely where it parts company with `date_part(text, date)`.
    /// All eight messages measured on live PostgreSQL 18.2, e.g.
    /// `ERROR:  unit "hour" not supported for type date`.
    #[test]
    fn extract_from_a_date_refuses_the_eight_units_postgres_refuses() {
        let day: ArrayRef = Arc::new(Date32Array::from(vec![Some(DAY_2024_03_05)]));
        for unit in [
            "hour",
            "minute",
            "second",
            "millisecond",
            "microsecond",
            "timezone",
            "timezone_hour",
            "timezone_minute",
        ] {
            let err = ExtractDate
                .invoke(&[text(&[Some(unit)]), day.clone()], &EvalSession::DEFAULT)
                .expect_err("date has no sub-day resolution and no zone");
            assert!(
                err.to_string()
                    .contains(&format!("unit \"{unit}\" not supported for type date")),
                "extract('{unit}', date): got {err:?}"
            );
        }

        // The units it does answer, measured live, all at scale 0.
        for (unit, expected) in [
            ("epoch", 1_709_596_800_i128),
            ("julian", 2_460_375),
            ("doy", 65),
            ("dow", 2),
            ("isodow", 2),
            ("isoyear", 2024),
            ("year", 2024),
            ("quarter", 1),
            ("week", 10),
            ("decade", 202),
            ("century", 21),
            ("millennium", 3),
        ] {
            let out = ExtractDate
                .invoke(&[text(&[Some(unit)]), day.clone()], &EvalSession::DEFAULT)
                .unwrap_or_else(|e| panic!("extract('{unit}', date): {e:?}"));
            assert_eq!(out.data_type(), &DataType::Decimal128(38, 0));
            assert_eq!(decimal(&out).value(0), expected, "extract('{unit}', date)");
        }
    }

    /// **KNOWN DIVERGENCE, inherited from `parse_date_unit` and unchanged.**
    /// PostgreSQL accepts four short aliases Basin's vocabulary does not, and
    /// `extract` inherits the gap because it shares the parser with
    /// `date_part` (which `dt_fns.rs` already pins). Measured on 18.2 against
    /// `interval '3 days 4:05:06'`:
    ///
    /// ```text
    /// extract('m',  …) -> 5   (MINUTE, not month)
    /// extract('mm', …) -> 5   (MINUTE)
    /// extract('h',  …) -> 4   (HOUR)
    /// extract('j',  …) -> ERROR: unit "j" not supported for type interval
    ///                     (an alias for julian, so it lands in the REFUSED
    ///                      set — with the "not supported" message, not the
    ///                      "not recognized" one)
    /// ```
    ///
    /// Basin says `not recognized` for all four. Asserted here on a
    /// `timestamp`, because `interval` fails earlier for an unrelated reason
    /// (see [`extract_from_an_interval_still_fails_closed`]) and would hide it.
    #[test]
    fn extract_rejects_four_unit_aliases_postgres_accepts() {
        for unit in ["m", "mm", "h", "j"] {
            let err = ExtractTimestamp
                .invoke(
                    &[text(&[Some(unit)]), ts(&[Some(TS_2024_03_05)])],
                    &EvalSession::DEFAULT,
                )
                .expect_err(
                    "Basin does not know this alias; if this starts succeeding the \
                     parser was fixed and this test should assert the server's value",
                );
            assert!(
                err.to_string()
                    .contains(&format!("unit \"{unit}\" not recognized")),
                "extract('{unit}', timestamp): got {err:?}"
            );
        }
    }

    /// 6200/6201/6204 answer nothing, before and after this module.
    /// `temporal_kind` classifies only `Date32` and
    /// `Timestamp(Microsecond, _)`, so an interval argument is refused with an
    /// `Internal` "not implemented" rather than a wrong answer — which is the
    /// correct failure, since Postgres computes `extract(… FROM interval)` with
    /// `interval_part`, not with the civil-time rules `extract` uses here.
    #[test]
    fn extract_from_an_interval_still_fails_closed() {
        let iv: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
            IntervalMonthDayNano::new(14, 3, 14_706_789_123_000),
        )]));
        let err = ExtractInterval
            .invoke(&[text(&[Some("year")]), iv], &EvalSession::DEFAULT)
            .expect_err(
                "an interval is not a civil time; answering it from the timestamp \
                 rules would be a wrong answer, not a missing one",
            );
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("is not implemented")),
            "got {err:?}"
        );
    }

    /// Arity is guaranteed by resolution, so a short call is a planner bug and
    /// must say so rather than panicking on an index.
    #[test]
    fn a_missing_argument_is_reported_as_a_planner_bug() {
        let err = ExtractTimestamp
            .invoke(&[text(&[Some("year")])], &EvalSession::DEFAULT)
            .expect_err("one argument must fail");
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {err:?}"
        );

        let none = ExtractDate
            .invoke(&[], &EvalSession::DEFAULT)
            .expect_err("no arguments must fail");
        assert!(
            matches!(none, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {none:?}"
        );
    }
}
