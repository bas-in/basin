//! Date/time functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! Eleven `pg_proc` oids, moved off `eval_scalar_fn`'s hard-coded `match`
//! following the worked example in [`crate::funcs::str_fns`] — read that file
//! first; it states the contract this one obeys.
//!
//! ```text
//! 2058  age(timestamp, timestamp)          3846  make_date(int, int, int)
//! 2020  date_trunc(text, timestamp)        2021  date_part(text, timestamp)
//! 1217  date_trunc(text, timestamptz)      1171  date_part(text, timestamptz)
//! 1218  date_trunc(text, interval)         1384  date_part(text, date)
//! 2049  to_char(timestamp, text)           1172  date_part(text, interval)
//! 1770  to_char(timestamptz, text)
//! ```
//!
//! Every one of them already had behavioural tests in `eval.rs` that call
//! `eval`/`eval_with`, so those tests now exercise the registry path unchanged
//! — which is the evidence a port wants, and is why these eleven were picked
//! over the rest of the family.
//!
//! # This family is the session-dependent one
//!
//! [`EvalSession`] is not decoration here. Three of the eleven read the
//! `TimeZone` GUC out of it and answer differently per session:
//!
//! * `date_trunc(text, timestamptz)` truncates the session-local rendering,
//!   then re-derives the UTC offset for units of a day and larger (and only
//!   for those — see `unit_redetermines_zone`).
//! * `date_part(text, timestamptz)` reads every field except `epoch` off the
//!   session-local rendering, and answers `timezone`/`timezone_hour`/
//!   `timezone_minute` from that rendering's offset.
//! * `to_char(timestamptz, text)` renders in the session zone; both `to_char`
//!   oids are `provolatile = 's'` in `pg_proc` for exactly this reason.
//!
//! The other eight are pure functions of their arguments: a `timestamp`, a
//! `date` and an `interval` carry no zone, so there is nothing for a session
//! to change. That split is preserved here rather than papered over — a
//! `date_part('hour', ts)` that quietly consulted a zone would be wrong in
//! every session that is not UTC.
//!
//! What is deliberately NOT here is anything that needs the *clock*.
//! [`EvalSession::DEFAULT`] has none, and `now()` (oid 1299) has no `match`
//! arm to port: implementing it here would be new behaviour smuggled in under
//! a port, and unreachable from `eval()` besides. See the orphan list at the
//! top of `eval.rs`.
//!
//! # Where the bodies are, and why
//!
//! Each impl below delegates to the function in [`crate::eval`] that the
//! deleted `match` arm called. The algorithms are **not** copied across, on
//! purpose:
//!
//! * They share private machinery with functions that are still on the
//!   `match` — `parse_date_unit`, `DateUnit`, `truncate_local`,
//!   `date_part_of`, `temporal_readings`, `DATE_PATTERNS`. `extract`
//!   (6199-6204) still calls three of those. Copying the machinery here would
//!   leave two copies of Postgres's unit vocabulary in one crate, free to
//!   drift apart, which is precisely the failure the `date_part`/`extract`
//!   pair was built to avoid.
//! * A port must move behaviour, not rewrite it. Delegation moves *hosting*
//!   and nothing else, so a bisect over this commit can only ever blame the
//!   seam.
//!
//! The bodies follow when the whole family is hosted and the private helpers
//! can move as one piece. Until then this file is the boundary and `eval.rs`
//! is the implementation.
//!
//! # `extract` is not portable yet, and that is a finding
//!
//! `eval_extract` (oids 6199-6204) takes `Option<&Expr>` — the *unevaluated*
//! field argument — because the result's `numeric` scale depends on the unit
//! (`second`/`epoch` 6, `milliseconds` 3, everything else 0) and one Arrow
//! array carries one scale, so a non-literal field is refused. This ABI hands
//! implementations `&[ArrayRef]`, by which point "a literal" and "a computed
//! constant" are the same array and the refusal cannot be reproduced. Porting
//! `extract` therefore means *changing* its acceptance set, which is not a
//! port. It stays on the `match` until the ABI can express a plan-time
//! constant argument.

use arrow_array::ArrayRef;
use basin_pgtype::Oid;

use crate::eval::{self, EvalSession};
use crate::funcs::str_fns::arg;
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// The `(unit, value)` argument pair that seven of these functions take.
///
/// Both arguments are fetched through [`arg`], so a short call is reported as
/// the planner bug it is rather than panicking on an index.
fn unit_and_value(args: &[ArrayRef], oid: u32) -> Result<(&ArrayRef, &ArrayRef), ExecError> {
    Ok((arg(args, 0, oid)?, arg(args, 1, oid)?))
}

/// `age(timestamp, timestamp) -> interval`, `pg_proc` oid 2058.
///
/// Session-independent: both arguments are already civil wall-clock readings.
/// The three overloads that are NOT (`age(timestamptz, timestamptz)` 2059,
/// `age(timestamp)` 2059's one-argument sibling 1199, and 1386) are still
/// unimplemented on purpose — they need a rendering zone, and `eval.rs`'s
/// `the_session_dependent_age_overloads_are_left_unimplemented` pins that.
pub struct Age;

impl ScalarFunc for Age {
    fn oid(&self) -> Oid {
        Oid(2058)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (lhs, rhs) = unit_and_value(args, 2058)?;
        eval::eval_age(lhs, rhs)
    }
}

/// `date_trunc(text, timestamp) -> timestamp`, oid 2020.
///
/// Pure calendar arithmetic; a `timestamp without time zone` is already a
/// civil reading, so there is no zone to truncate in.
pub struct DateTruncTimestamp;

impl ScalarFunc for DateTruncTimestamp {
    fn oid(&self) -> Oid {
        Oid(2020)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 2020)?;
        eval::eval_date_trunc_timestamp(unit, value)
    }
}

/// `date_trunc(text, timestamptz) -> timestamptz`, oid 1217.
///
/// **Session-dependent.** The truncation happens on the session-local
/// rendering, and for units of a day and larger the UTC offset is re-derived
/// from the truncated local time — invisible except across a DST transition,
/// where getting it wrong is off by exactly the DST step.
pub struct DateTruncTimestamptz;

impl ScalarFunc for DateTruncTimestamptz {
    fn oid(&self) -> Oid {
        Oid(1217)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 1217)?;
        eval::eval_date_trunc_timestamptz(unit, value, session)
    }
}

/// `date_trunc(text, interval) -> interval`, oid 1218.
///
/// Session-independent, and deliberately not the timestamp arithmetic:
/// `date_trunc('century', interval '137 years 5 mons')` is `100 years`, not
/// the `101 years` Postgres's off-by-one century rule gives a date, because an
/// interval has no year 1 to count from. `week` is refused outright — see
/// [`date_trunc_interval_refuses_the_week_date_part_accepts`].
pub struct DateTruncInterval;

impl ScalarFunc for DateTruncInterval {
    fn oid(&self) -> Oid {
        Oid(1218)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 1218)?;
        eval::eval_date_trunc_interval(unit, value)
    }
}

/// `date_part(text, timestamp) -> float8`, oid 2021.
pub struct DatePartTimestamp;

impl ScalarFunc for DatePartTimestamp {
    fn oid(&self) -> Oid {
        Oid(2021)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 2021)?;
        eval::eval_date_part_timestamp(unit, value)
    }
}

/// `date_part(text, timestamptz) -> float8`, oid 1171.
///
/// **Session-dependent.** Every field except `epoch` is read off the
/// session-local rendering, and `timezone`/`timezone_hour`/`timezone_minute`
/// report that rendering's offset — which is not always a whole number of
/// hours (`Australia/Lord_Howe` is `+10:30`).
pub struct DatePartTimestamptz;

impl ScalarFunc for DatePartTimestamptz {
    fn oid(&self) -> Oid {
        Oid(1171)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 1171)?;
        eval::eval_date_part_timestamptz(unit, value, session)
    }
}

/// `date_part(text, date) -> float8`, oid 1384.
///
/// Postgres implements this by casting the `date` to a `timestamp`, which is
/// visible in its error messages: an unknown unit on a `date` is reported
/// against `timestamp without time zone`. Reproduced, not corrected.
pub struct DatePartDate;

impl ScalarFunc for DatePartDate {
    fn oid(&self) -> Oid {
        Oid(1384)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 1384)?;
        eval::eval_date_part_date(unit, value)
    }
}

/// `date_part(text, interval) -> float8`, oid 1172.
///
/// Postgres's `interval_part`, a C function in its own right — not
/// `date_part(text, timestamp)` with a conversion, and not `extract` with a
/// cast. Two of its rules cannot be recovered by reasoning and were measured:
/// `quarter` branches on `months` rather than `months % 12`, and `epoch`'s
/// summation order is load-bearing to one ulp.
pub struct DatePartInterval;

impl ScalarFunc for DatePartInterval {
    fn oid(&self) -> Oid {
        Oid(1172)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (unit, value) = unit_and_value(args, 1172)?;
        eval::eval_date_part_interval(unit, value)
    }
}

/// `to_char(timestamp, text) -> text`, oid 2049.
///
/// Also answers a `Date32` argument, which is not an extra overload: Postgres
/// has no `to_char(date, text)` row at all and reaches a datetime `to_char`
/// from a `date` by an implicit cast Basin's lowering does not insert, so the
/// widening to midnight happens inside the implementation.
pub struct ToCharTimestamp;

impl ScalarFunc for ToCharTimestamp {
    fn oid(&self) -> Oid {
        Oid(2049)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (value, format) = unit_and_value(args, 2049)?;
        eval::eval_to_char_datetime(value, format, session)
    }
}

/// `to_char(timestamptz, text) -> text`, oid 1770. **Session-dependent**: the
/// instant is rendered in the session's `TimeZone`.
///
/// One implementation serves both `to_char` oids, dispatching on the
/// argument's actual Arrow type rather than on the oid — which is what lets a
/// `date` argument work under either, since resolution picks 1770 for a
/// `date` on a live server (`basin_pgtype::func` records the `EXPLAIN
/// (VERBOSE)` output that settles it) while a bare literal lands on 2049.
pub struct ToCharTimestamptz;

impl ScalarFunc for ToCharTimestamptz {
    fn oid(&self) -> Oid {
        Oid(1770)
    }

    fn invoke(&self, args: &[ArrayRef], session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let (value, format) = unit_and_value(args, 1770)?;
        eval::eval_to_char_datetime(value, format, session)
    }
}

/// `make_date(year int, month int, day int) -> date`, oid 3846.
///
/// Year zero is rejected (Postgres has no year 0), a negative year is BC, and
/// a day the month does not have is Postgres's `date field value out of
/// range`. The accepted *range* of years is narrower than Postgres's — see
/// [`make_date_refuses_a_year_postgres_accepts`].
pub struct MakeDate;

impl ScalarFunc for MakeDate {
    fn oid(&self) -> Oid {
        Oid(3846)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let year = arg(args, 0, 3846)?;
        let month = arg(args, 1, 3846)?;
        let day = arg(args, 2, 3846)?;
        eval::eval_make_date(year, month, day)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::builtins;
    use std::sync::Arc;

    // `is_null`/`len`/`value` on the concrete arrays are `Array` trait
    // methods, not inherent ones.
    use arrow_array::types::IntervalMonthDayNano;
    use arrow_array::{
        Array, Date32Array, Float64Array, Int32Array, IntervalMonthDayNanoArray, StringArray,
        TimestampMicrosecondArray,
    };

    /// Every oid this module hosts. Kept as one list so the registry test and
    /// the reader see the same eleven.
    const HOSTED: &[u32] = &[
        2058, 2020, 1217, 1218, 2021, 1171, 1384, 1172, 2049, 1770, 3846,
    ];

    fn text(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn ts(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(values.to_vec()))
    }

    fn interval(months: i32, days: i32, nanos: i64) -> ArrayRef {
        Arc::new(IntervalMonthDayNanoArray::from(vec![Some(
            IntervalMonthDayNano::new(months, days, nanos),
        )]))
    }

    fn f64_at(out: &ArrayRef, i: usize) -> f64 {
        out.as_any()
            .downcast_ref::<Float64Array>()
            .expect("date_part returns float8")
            .value(i)
    }

    fn micros_at(out: &ArrayRef, i: usize) -> i64 {
        out.as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("date_trunc returns a timestamp")
            .value(i)
    }

    /// `2024-03-05 14:07:09.123456`, in microseconds since the epoch, read
    /// off the server: `extract(epoch from …) * 1000000` is
    /// 1709647629123456.
    const TS_2024_03_05: i64 = 1_709_647_629_123_456;

    /// The registry must actually be reached for all eleven oids. Without
    /// this, a port that forgot to register would still pass every
    /// behavioural test — the `match` arm would answer instead, and the port
    /// would look done while having changed nothing.
    #[test]
    fn every_ported_oid_is_hosted_by_the_registry() {
        for oid in HOSTED {
            assert!(
                builtins().scalar(Oid(*oid)).is_some(),
                "oid {oid} must be reachable through the registry, not only the match"
            );
        }
    }

    /// The hosted oid must equal the key it registers under, which
    /// `register_scalar` enforces — but a typo in one of eleven near-identical
    /// impls is exactly the mistake this catches early.
    #[test]
    fn each_impl_reports_the_oid_it_is_registered_under() {
        for oid in HOSTED {
            let hosted = builtins().scalar(Oid(*oid)).expect("hosted");
            assert_eq!(hosted.oid(), Oid(*oid));
        }
    }

    /// `date_trunc('day', TIMESTAMP '2024-03-05 14:07:09.123456')` is
    /// `2024-03-05 00:00:00` — 1709596800000000 micros, measured live.
    #[test]
    fn date_trunc_timestamp_truncates_and_is_strict() {
        let out = DateTruncTimestamp
            .invoke(
                &[
                    text(&[Some("day"), Some("day"), None]),
                    ts(&[Some(TS_2024_03_05), None, Some(TS_2024_03_05)]),
                ],
                &EvalSession::DEFAULT,
            )
            .expect("date_trunc over timestamp");
        assert_eq!(micros_at(&out, 0), 1_709_596_800_000_000);
        assert!(out.is_null(1), "NULL value in, NULL out");
        assert!(out.is_null(2), "NULL unit in, NULL out");
        assert_eq!(out.len(), 3, "output length must match input length");
    }

    /// The session's `TimeZone` decides the answer, and the two units below
    /// disagree about the offset in a way only a DST transition exposes.
    ///
    /// Measured on 18.2 with `SET timezone='Australia/Lord_Howe'` (a
    /// half-hour DST step) on the instant `2024-04-06 15:00:00+00`
    /// (1712415600000000), the first instant after that zone's 2024
    /// fall-back:
    ///
    /// ```text
    /// date_trunc('day',  …) = 2024-04-07 00:00:00+11  -> 1712408400000000
    /// date_trunc('hour', …) = 2024-04-07 01:30:00+11  -> 1712413800000000
    /// ```
    ///
    /// The `hour` answer keeps the *input's* `+10:30`, so its rendered
    /// minutes are not zero. An implementation that re-derives the offset for
    /// every unit answers `01:00:00+11` and is wrong.
    #[test]
    fn date_trunc_timestamptz_reads_the_session_zone() {
        let session = EvalSession::with_time_zone("Australia/Lord_Howe");
        let lord_howe = 1_712_415_600_000_000_i64;

        let day = DateTruncTimestamptz
            .invoke(&[text(&[Some("day")]), ts(&[Some(lord_howe)])], &session)
            .expect("date_trunc('day', timestamptz)");
        assert_eq!(micros_at(&day, 0), 1_712_408_400_000_000);

        let hour = DateTruncTimestamptz
            .invoke(&[text(&[Some("hour")]), ts(&[Some(lord_howe)])], &session)
            .expect("date_trunc('hour', timestamptz)");
        assert_eq!(micros_at(&hour, 0), 1_712_413_800_000_000);

        // The same call in the default (UTC) session is a different answer,
        // which is the whole point of `session` being a parameter.
        let utc = DateTruncTimestamptz
            .invoke(
                &[text(&[Some("day")]), ts(&[Some(lord_howe)])],
                &EvalSession::DEFAULT,
            )
            .expect("date_trunc('day', timestamptz) in UTC");
        assert_ne!(micros_at(&utc, 0), micros_at(&day, 0));
    }

    /// Measured in the same zone and on the same instant:
    /// `date_part('hour', …)` is 1, `timezone` is 37800 (`+10:30`) and
    /// `timezone_minute` is 30 — the case an implementation storing offsets
    /// in whole hours gets wrong.
    #[test]
    fn date_part_timestamptz_reads_the_session_zone() {
        let session = EvalSession::with_time_zone("Australia/Lord_Howe");
        let lord_howe = 1_712_415_600_000_000_i64;
        for (unit, expected) in [("hour", 1.0), ("timezone", 37800.0), ("timezone_minute", 30.0)] {
            let out = DatePartTimestamptz
                .invoke(&[text(&[Some(unit)]), ts(&[Some(lord_howe)])], &session)
                .unwrap_or_else(|e| panic!("date_part('{unit}', timestamptz): {e:?}"));
            assert_eq!(f64_at(&out, 0), expected, "date_part('{unit}', timestamptz)");
        }
    }

    /// `date_part('doy', DATE '2024-03-05')` is 65 and `epoch` is 1709596800
    /// — the `date` overload answers through the timestamp path, as Postgres
    /// does.
    #[test]
    fn date_part_date_answers_through_the_timestamp_path() {
        let day: ArrayRef = Arc::new(Date32Array::from(vec![Some(19_787)]));
        for (unit, expected) in [("doy", 65.0), ("epoch", 1_709_596_800.0), ("hour", 0.0)] {
            let out = DatePartDate
                .invoke(&[text(&[Some(unit)]), day.clone()], &EvalSession::DEFAULT)
                .unwrap_or_else(|e| panic!("date_part('{unit}', date): {e:?}"));
            assert_eq!(f64_at(&out, 0), expected, "date_part('{unit}', date)");
        }
    }

    /// The `quarter` rule that no reasoning produces: the branch is on
    /// `months`, not on `months % 12`. Measured live —
    /// `date_part('quarter', interval '-24 months')` is -1 while
    /// `date_part('quarter', interval '0')` is 1, and both have
    /// `months % 12 == 0`.
    #[test]
    fn date_part_interval_quarter_branches_on_months_not_on_the_remainder() {
        let minus_two_years = DatePartInterval
            .invoke(
                &[text(&[Some("quarter")]), interval(-24, 0, 0)],
                &EvalSession::DEFAULT,
            )
            .expect("date_part('quarter', interval)");
        assert_eq!(f64_at(&minus_two_years, 0), -1.0);

        let zero = DatePartInterval
            .invoke(
                &[text(&[Some("quarter")]), interval(0, 0, 0)],
                &EvalSession::DEFAULT,
            )
            .expect("date_part('quarter', interval '0')");
        assert_eq!(f64_at(&zero, 0), 1.0);
    }

    /// `week` is the unit where the two functions genuinely disagree, and the
    /// asymmetry is measured, not derived:
    ///
    /// ```text
    /// date_part ('week', interval '20 days') = 2
    /// date_trunc('week', interval '10 days') = ERROR:  unit "week" not
    ///                                          supported for type interval
    /// ```
    #[test]
    fn date_trunc_interval_refuses_the_week_date_part_accepts() {
        let accepted = DatePartInterval
            .invoke(
                &[text(&[Some("week")]), interval(0, 20, 0)],
                &EvalSession::DEFAULT,
            )
            .expect("date_part('week', interval) is legal");
        assert_eq!(f64_at(&accepted, 0), 2.0);

        let refused = DateTruncInterval
            .invoke(
                &[text(&[Some("week")]), interval(0, 10, 0)],
                &EvalSession::DEFAULT,
            )
            .expect_err("date_trunc('week', interval) is an error");
        assert!(
            refused.to_string().contains("not supported for type interval"),
            "expected Postgres's \"not supported\" message, got {refused:?}"
        );
    }

    /// `date_trunc('century', interval '137 years 5 mons')` is `100 years`:
    /// plain division on the month count, not the off-by-one century rule a
    /// date gets. Measured live.
    #[test]
    fn date_trunc_interval_uses_plain_division_for_the_year_scale_units() {
        let out = DateTruncInterval
            .invoke(
                &[text(&[Some("century")]), interval(137 * 12 + 5, 17, 0)],
                &EvalSession::DEFAULT,
            )
            .expect("date_trunc('century', interval)");
        let iv = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("date_trunc over interval returns an interval")
            .value(0);
        assert_eq!((iv.months, iv.days, iv.nanoseconds), (1200, 0, 0));
    }

    /// `age` borrows days from the EARLIER argument's month, which is what
    /// makes `age('2024-03-01', '2024-01-31')` one month and one day rather
    /// than one month and two.
    #[test]
    fn age_borrows_from_the_earlier_arguments_month() {
        // 2024-03-01 00:00:00 and 2024-01-31 00:00:00 as epoch micros.
        let march = 1_709_251_200_000_000_i64;
        let january = 1_706_659_200_000_000_i64;
        let out = Age
            .invoke(
                &[ts(&[Some(march), None]), ts(&[Some(january), Some(january)])],
                &EvalSession::DEFAULT,
            )
            .expect("age(timestamp, timestamp)");
        let arr = out
            .as_any()
            .downcast_ref::<IntervalMonthDayNanoArray>()
            .expect("age returns an interval");
        let iv = arr.value(0);
        assert_eq!((iv.months, iv.days, iv.nanoseconds), (1, 1, 0));
        assert!(arr.is_null(1), "age is strict in both arguments");
        assert_eq!(arr.len(), 2, "output length must match input length");
    }

    /// `to_char(TIMESTAMP '2024-03-05 14:07:09.123456', 'YYYY-MM-DD
    /// HH24:MI:SS')` is `2024-03-05 14:07:09`, measured live.
    #[test]
    fn to_char_renders_a_timestamp_and_is_strict_in_the_format() {
        let out = ToCharTimestamp
            .invoke(
                &[
                    ts(&[Some(TS_2024_03_05), Some(TS_2024_03_05)]),
                    text(&[Some("YYYY-MM-DD HH24:MI:SS"), None]),
                ],
                &EvalSession::DEFAULT,
            )
            .expect("to_char over timestamp");
        let s = out
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("to_char returns text");
        assert_eq!(s.value(0), "2024-03-05 14:07:09");
        assert!(s.is_null(1), "a NULL format is a NULL result");
        assert_eq!(s.len(), 2, "output length must match input length");
    }

    /// `make_date(2024, 3, 5)` is 19787 days from the epoch; year zero is
    /// refused with Postgres's own message; a negative year is BC.
    #[test]
    fn make_date_rejects_year_zero_and_accepts_bc() {
        let ints = |v: i32| -> ArrayRef { Arc::new(Int32Array::from(vec![Some(v)])) };

        let out = MakeDate
            .invoke(&[ints(2024), ints(3), ints(5)], &EvalSession::DEFAULT)
            .expect("make_date(2024, 3, 5)");
        assert_eq!(
            out.as_any()
                .downcast_ref::<Date32Array>()
                .expect("make_date returns a date")
                .value(0),
            19_787
        );

        let zero = MakeDate
            .invoke(&[ints(0), ints(1), ints(1)], &EvalSession::DEFAULT)
            .expect_err("year zero has no meaning in Postgres");
        assert!(
            zero.to_string()
                .contains("date field value out of range: 0-01-01"),
            "got {zero:?}"
        );

        // `make_date(-1, 1, 1)` is `0001-01-01 BC`, which is chrono's
        // astronomical year 0 — one day short of the common era.
        let bc = MakeDate
            .invoke(&[ints(-1), ints(1), ints(1)], &EvalSession::DEFAULT)
            .expect("a negative year is BC, not an error");
        assert_eq!(
            out.len(),
            1,
            "output length must match input length"
        );
        assert!(!bc.is_null(0), "make_date(-1, 1, 1) is a date, not NULL");
    }

    // ─── Divergences from live PostgreSQL 18.2 ──────────────────────────────
    //
    // Every assertion below states what Basin does AND what the server does.
    // All three predate this port and are unchanged by it: a port moves
    // behaviour, and fixing one of these here would make a later bisect blame
    // the seam instead of the arm.

    /// **KNOWN DIVERGENCE.** Postgres's unit vocabulary contains four
    /// single/double-letter aliases that `eval.rs`'s `parse_date_unit` does
    /// not, so Basin errors where the server answers. Brute-forced over every
    /// one- and two-letter unit name on 18.2:
    ///
    /// ```text
    /// date_part('m',  TIMESTAMP '2024-03-05 14:07:09') -> 7      (MINUTE)
    /// date_part('mm', TIMESTAMP '2024-03-05 14:07:09') -> 7      (MINUTE)
    /// date_part('h',  TIMESTAMP '2024-03-05 14:07:09') -> 14     (HOUR)
    /// date_part('j',  TIMESTAMP '2024-03-05 14:07:09') -> 2460375.588…
    /// ```
    ///
    /// `m` is minute, not month — the trap that makes guessing this table
    /// worse than measuring it. Note the parser cannot simply gain all four:
    /// `date_trunc('m', …)` IS accepted by the server (it truncates to the
    /// minute) while `date_trunc('mm', …)` is "not recognized", so `mm` is
    /// accepted by `date_part`/`extract` and refused by `date_trunc` — one
    /// shared table cannot express that, which is why this is filed rather
    /// than patched mid-port.
    #[test]
    fn date_part_rejects_four_unit_aliases_postgres_accepts() {
        for unit in ["m", "mm", "h", "j"] {
            let err = DatePartTimestamp
                .invoke(
                    &[text(&[Some(unit)]), ts(&[Some(TS_2024_03_05)])],
                    &EvalSession::DEFAULT,
                )
                .expect_err(
                    "Basin does not know this alias; if this call starts \
                     succeeding, the parser was fixed and this test should \
                     assert the server's value instead",
                );
            assert!(
                err.to_string()
                    .contains(&format!("unit \"{unit}\" not recognized")),
                "date_part('{unit}', timestamp): got {err:?}"
            );
        }
    }

    /// **KNOWN DIVERGENCE.** `make_date` converts through `chrono::NaiveDate`,
    /// whose year range is ±262143, while Postgres's `date` reaches
    /// 5874897-12-31 (and `Date32` could hold it — that limit is 2147483493
    /// days from the epoch, inside `i32`). Measured on 18.2:
    ///
    /// ```text
    /// make_date(262144, 1, 1)  -> 262144-01-01
    /// make_date(5874898, 1, 1) -> ERROR:  date out of range: 5874898-01-01
    /// make_date(-100000, 1, 1) -> ERROR:  date out of range: -99999-01-01
    /// ```
    ///
    /// Basin refuses the first with the *wrong message* (`date field value
    /// out of range`, which Postgres reserves for a bad field rather than an
    /// out-of-range date) and ACCEPTS the third, producing a date the server
    /// rejects. Both directions are wrong; neither is changed by this port.
    #[test]
    fn make_date_refuses_a_year_postgres_accepts() {
        let ints = |v: i32| -> ArrayRef { Arc::new(Int32Array::from(vec![Some(v)])) };

        let err = MakeDate
            .invoke(&[ints(262_144), ints(1), ints(1)], &EvalSession::DEFAULT)
            .expect_err("chrono cannot represent year 262144");
        assert!(
            err.to_string()
                .contains("date field value out of range: 262144-01-01"),
            "got {err:?}"
        );

        // The other direction: Postgres's minimum date is 4714-11-24 BC, so
        // this call is `ERROR: date out of range: -99999-01-01` on the
        // server and a perfectly good `Date32` here.
        let accepted = MakeDate
            .invoke(&[ints(-100_000), ints(1), ints(1)], &EvalSession::DEFAULT)
            .expect("chrono is happy with astronomical year -99999");
        assert!(
            !accepted.is_null(0),
            "Basin answers where PostgreSQL raises `date out of range`"
        );
    }

    /// **KNOWN DIVERGENCE.** For a unit that names a *field* rather than a
    /// resolution, `date_trunc(text, interval)` reports the wrong one of
    /// Postgres's two messages. Measured on 18.2:
    ///
    /// ```text
    /// date_trunc('epoch', interval '5 days') -> ERROR: unit "epoch" not
    ///                                           recognized for type interval
    /// date_trunc('dow',   interval '5 days') -> ERROR: unit "dow" not
    ///                                           recognized for type interval
    /// ```
    ///
    /// Basin says "not **supported**" for both, because
    /// `eval_date_trunc_interval` resolves the unit first and only then finds
    /// no truncation width. The distinction is Postgres's own and is
    /// preserved correctly by the *timestamp* overload, which is what makes
    /// this an interval-only slip. `timezone` on an interval is genuinely
    /// "not supported" on both sides, so only the field-naming units differ.
    #[test]
    fn date_trunc_interval_reports_not_supported_where_postgres_says_not_recognized() {
        for unit in ["epoch", "dow", "isoyear", "julian"] {
            let err = DateTruncInterval
                .invoke(
                    &[text(&[Some(unit)]), interval(0, 5, 0)],
                    &EvalSession::DEFAULT,
                )
                .expect_err("every unit here is refused by both implementations");
            assert!(
                err.to_string()
                    .contains(&format!("unit \"{unit}\" not supported for type interval")),
                "Basin says \"not supported\" where PostgreSQL says \"not \
                 recognized\" for {unit}: got {err:?}"
            );
        }

        // Unchanged and correct on both sides, for contrast.
        let tz = DateTruncInterval
            .invoke(
                &[text(&[Some("timezone")]), interval(0, 5, 0)],
                &EvalSession::DEFAULT,
            )
            .expect_err("timezone is not supported for an interval");
        assert!(
            tz.to_string()
                .contains("unit \"timezone\" not supported for type interval"),
            "got {tz:?}"
        );
    }

    /// Arity is guaranteed by resolution, so a short call is a planner bug
    /// and must say so rather than panicking on an index.
    #[test]
    fn a_missing_argument_is_reported_as_a_planner_bug() {
        let err = DatePartTimestamp
            .invoke(&[text(&[Some("hour")])], &EvalSession::DEFAULT)
            .expect_err("one argument must fail");
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {err:?}"
        );

        let short = MakeDate
            .invoke(&[], &EvalSession::DEFAULT)
            .expect_err("no arguments must fail");
        assert!(
            matches!(short, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {short:?}"
        );
    }
}
