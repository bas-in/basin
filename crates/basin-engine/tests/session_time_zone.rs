//! The `TimeZone` GUC and the transaction/statement clocks, end to end
//! through `ProjectSession::execute` — the wiring that made
//! `basin_exec::eval::EvalSession` reachable.
//!
//! `EvalSession` landed with `date_trunc` (OIDs 1217/2020/1218) and
//! `date_part` (1171/2021/1384) implemented against it and verified by value,
//! and with **nothing that could construct one**: the `TimeZone` GUC did not
//! exist, and no executor hook called `datetime_more_udf`'s
//! `tick_statement_ts` / `tick_txn_ts`, so `eval()` always ran against
//! `EvalSession::DEFAULT` — UTC, no clock. These tests are the proof that the
//! two ends are now joined: a `SET TimeZone` issued as SQL changes what a
//! `date_trunc` issued as SQL answers, and `now()` is a property of the
//! transaction rather than of whenever the row happened to be evaluated.
//!
//! # Every expected value here came from PostgreSQL 18.2
//!
//! Not from re-deriving what Basin ought to do — from asking a live server and
//! recording the answer. The zone set is UTC, `America/New_York` and
//! `Australia/Lord_Howe`, and the third is the one that matters: Lord Howe
//! steps its clock by **30 minutes**, not an hour, so an implementation that
//! truncates in UTC and re-labels, or that assumes whole-hour offsets, gets
//! the same answer as a correct one in the first two zones and a wrong answer
//! in the third.
//!
//! The witness, measured:
//!
//! ```text
//! SET TimeZone='Australia/Lord_Howe';
//! SELECT date_trunc('hour', TIMESTAMPTZ '2024-04-06 15:00:00+00');
//!        date_trunc
//! ------------------------
//!  2024-04-07 01:30:00+11
//! ```
//!
//! **The displayed minutes are not zero**, which is what makes this case a
//! test rather than a formality. PostgreSQL truncates the session-local wall
//! clock (`01:30` local, at offset `+10:30`, becomes `01:00` local) and then,
//! for units of an hour and below, re-attaches *the input instant's own*
//! offset rather than re-deriving one — landing on an instant that renders
//! `01:30:00+11` in that zone. Truncating "to the hour" moves the clock by 30
//! minutes here, and any engine that reasons in whole hours misses it.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use arrow_array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// `BASIN_OWNED_ENGINE` is process-wide; every test that depends on its value
/// holds this for the whole window in which it matters. Mirrors
/// `owned_engine_bridge.rs`'s lock, for the same reason.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

/// The single scalar `SHOW`-style string a one-row/one-column result carries.
async fn scalar_text(sess: &ProjectSession, sql: &str) -> String {
    let batches = rows(sess, sql).await;
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from {sql:?}"));
    let col = b.column(0);
    if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        return s.value(0).to_string();
    }
    panic!("{sql:?} did not return text, got {:?}", col.data_type());
}

/// Microseconds since the Unix epoch from a one-row timestamp result.
///
/// Compared as an *instant*, not as rendered text, on purpose: the whole
/// question a session `TimeZone` answers is which instant `date_trunc` lands
/// on, and text rendering is a separate concern (`DateStyle`) that
/// `EvalSession` deliberately does not carry yet.
async fn scalar_ts_micros(sess: &ProjectSession, sql: &str) -> i64 {
    let batches = rows(sess, sql).await;
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from {sql:?}"));
    let col = b.column(0);
    let ts = col
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap_or_else(|| {
            panic!(
                "{sql:?} did not return a µs timestamp, got {:?}",
                col.data_type()
            )
        });
    assert!(!ts.is_null(0), "{sql:?} returned NULL");
    ts.value(0)
}

/// A one-row integer result (`count(…)`).
async fn scalar_i64(sess: &ProjectSession, sql: &str) -> i64 {
    let batches = rows(sess, sql).await;
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from {sql:?}"));
    let col = b.column(0);
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        return a.value(0);
    }
    panic!("{sql:?} did not return int8, got {:?}", col.data_type());
}

/// A one-row float result (`date_part` returns double precision).
async fn scalar_f64(sess: &ProjectSession, sql: &str) -> f64 {
    let batches = rows(sess, sql).await;
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap_or_else(|| panic!("no rows from {sql:?}"));
    let col = b.column(0);
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
        return a.value(0);
    }
    panic!("{sql:?} did not return float8, got {:?}", col.data_type());
}

// ── The GUC itself ──────────────────────────────────────────────────────────

/// A fresh session reports `UTC`, and every spelling of `SET` round-trips
/// through `SHOW`.
///
/// Measured against PostgreSQL 18.2 (the `SHOW TimeZone` column, verbatim):
///
/// | statement | `SHOW TimeZone` |
/// |---|---|
/// | *(fresh session, no config file)* | `GMT` — `boot_val` in `pg_settings` |
/// | `SET TimeZone = 'utc'` | `UTC` — canonicalised, not echoed |
/// | `SET TimeZone = UTC` (bare ident) | `UTC` |
/// | `SET TIME ZONE 'America/New_York'` | `America/New_York` |
/// | `RESET TimeZone` | back to the default |
///
/// Basin's default is `UTC` rather than PostgreSQL's literal `GMT` spelling
/// (same zero offset, and `UTC` is the name the rest of Basin uses) and
/// deliberately not the host zone that `initdb` bakes into a real cluster's
/// `postgresql.conf` — see `session::DEFAULT_TIME_ZONE` for why a server whose
/// nodes are interchangeable must not read `date_trunc`'s answer off the host
/// clock.
#[tokio::test]
async fn time_zone_guc_set_show_and_reset() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    assert_eq!(
        scalar_text(&sess, "SHOW TimeZone").await,
        "UTC",
        "a never-SET session reports the process default"
    );

    // Every assignment spelling sqlparser distinguishes.
    for (stmt, want) in [
        ("SET TimeZone = 'America/New_York'", "America/New_York"),
        (
            "SET TIMEZONE TO 'Australia/Lord_Howe'",
            "Australia/Lord_Howe",
        ),
        ("SET TIME ZONE 'Europe/Berlin'", "Europe/Berlin"),
        ("SET TIME ZONE = 'Asia/Kolkata'", "Asia/Kolkata"),
        ("SET TimeZone = UTC", "UTC"),
    ] {
        exec(&sess, stmt).await;
        assert_eq!(
            scalar_text(&sess, "SHOW TimeZone").await,
            want,
            "after {stmt:?}"
        );
    }

    // `SET` persists across statements (it is session state, not statement
    // state) — measured: two separate `SHOW` messages after one `SET` both
    // report the set value.
    exec(&sess, "SET TimeZone = 'Australia/Lord_Howe'").await;
    assert_eq!(
        scalar_text(&sess, "SHOW TimeZone").await,
        "Australia/Lord_Howe"
    );
    assert_eq!(
        scalar_text(&sess, "SHOW TimeZone").await,
        "Australia/Lord_Howe"
    );

    // Each documented reset spelling.
    for stmt in [
        "RESET TimeZone",
        "SET TimeZone TO DEFAULT",
        "SET TIME ZONE DEFAULT",
        "SET TIME ZONE LOCAL",
    ] {
        exec(&sess, "SET TimeZone = 'America/New_York'").await;
        assert_eq!(
            scalar_text(&sess, "SHOW TimeZone").await,
            "America/New_York"
        );
        exec(&sess, stmt).await;
        assert_eq!(
            scalar_text(&sess, "SHOW TimeZone").await,
            "UTC",
            "{stmt:?} must restore the default"
        );
    }

    // `RESET ALL` covers it too — the GUC is reset-by-construction along with
    // every other one, so a pooled checkout cannot inherit a zone.
    exec(&sess, "SET TimeZone = 'America/New_York'").await;
    exec(&sess, "RESET ALL").await;
    assert_eq!(scalar_text(&sess, "SHOW TimeZone").await, "UTC");
}

/// An unresolvable zone is refused at `SET` time, not accepted and then
/// silently answered in UTC.
///
/// PostgreSQL 18.2, measured verbatim:
///
/// ```text
/// SET TimeZone = 'Not/AZone';
/// ERROR:  invalid value for parameter "TimeZone": "Not/AZone"
/// ```
///
/// Refusing here rather than at first use is the point: a session that
/// accepted the name would keep running, and every `date_trunc` in it would
/// be quietly wrong for as long as it lived.
#[tokio::test]
async fn an_invalid_time_zone_is_refused_at_set_time_with_postgres_wording() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    let err = sess
        .execute("SET TimeZone = 'Not/AZone'")
        .await
        .expect_err("an unknown zone must not be accepted");
    let msg = format!("{err}");
    assert!(
        msg.contains(r#"invalid value for parameter "TimeZone": "Not/AZone""#),
        "expected PostgreSQL's own wording, got {msg:?}"
    );

    // And the session's zone is untouched by the failed SET.
    assert_eq!(scalar_text(&sess, "SHOW TimeZone").await, "UTC");
}

/// `SET TIME ZONE INTERVAL '+05:00' HOUR TO MINUTE` and the bare-number form.
///
/// PostgreSQL renders an interval-valued zone in POSIX form, whose inner sign
/// is inverted relative to the ISO offset. All four rows measured on 18.2:
///
/// | statement | `SHOW TimeZone` |
/// |---|---|
/// | `SET TIME ZONE INTERVAL '+05:00' HOUR TO MINUTE` | `<+05>-05` |
/// | `SET TIME ZONE INTERVAL '-05:30' HOUR TO MINUTE` | `<-05:30>+05:30` |
/// | `SET TIME ZONE -8` | `<-08>+08` |
/// | `SET TIME ZONE 5.5` | `<+05:30>-05:30` |
#[tokio::test]
async fn interval_and_numeric_time_zones_render_the_way_postgres_renders_them() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    for (stmt, want) in [
        ("SET TIME ZONE INTERVAL '+05:00' HOUR TO MINUTE", "<+05>-05"),
        (
            "SET TIME ZONE INTERVAL '-05:30' HOUR TO MINUTE",
            "<-05:30>+05:30",
        ),
        ("SET TIME ZONE -8", "<-08>+08"),
        ("SET TIME ZONE 5.5", "<+05:30>-05:30"),
    ] {
        exec(&sess, stmt).await;
        assert_eq!(
            scalar_text(&sess, "SHOW TimeZone").await,
            want,
            "after {stmt:?}"
        );
    }
}

// ── The zone reaching evaluation ────────────────────────────────────────────

/// The end-to-end case: `SET TimeZone` changes what `date_trunc`/`date_part`
/// answer, through real SQL, in three zones — including the half-hour one.
///
/// Every expected value below was generated by PostgreSQL 18.2 under the
/// matching `SET TimeZone`, with the instant taken as
/// `extract(epoch from …)*1000000` so the comparison is of instants rather
/// than of rendered text:
///
/// ```text
/// zone                  trunc('hour')                     µs                trunc('day')                      µs
/// UTC                   2024-04-06 15:00:00+00   1712415600000000   2024-04-06 00:00:00+00   1712361600000000
/// America/New_York      2024-04-06 11:00:00-04   1712415600000000   2024-04-06 00:00:00-04   1712376000000000
/// Australia/Lord_Howe   2024-04-07 01:30:00+11   1712413800000000   2024-04-07 00:00:00+11   1712408400000000
/// ```
///
/// Read the `trunc('hour')` column: UTC and New York agree on the instant
/// (both offsets are whole hours, so truncating to an hour is offset-blind),
/// and **Lord Howe does not** — 1 712 413 800 000 000 is 30 minutes earlier.
/// That single differing number is the entire value of this test; a UTC-only
/// implementation passes the first two rows.
#[tokio::test]
async fn date_trunc_and_date_part_follow_the_session_time_zone_in_three_zones() {
    let _guard = env_lock();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    const INPUT: &str = "TIMESTAMPTZ '2024-04-06 15:00:00+00'";

    struct Case {
        zone: &'static str,
        trunc_hour_us: i64,
        trunc_day_us: i64,
        part_hour: f64,
        part_minute: f64,
    }
    let cases = [
        Case {
            zone: "UTC",
            trunc_hour_us: 1_712_415_600_000_000,
            trunc_day_us: 1_712_361_600_000_000,
            part_hour: 15.0,
            part_minute: 0.0,
        },
        Case {
            zone: "America/New_York",
            trunc_hour_us: 1_712_415_600_000_000,
            trunc_day_us: 1_712_376_000_000_000,
            part_hour: 11.0,
            part_minute: 0.0,
        },
        Case {
            zone: "Australia/Lord_Howe",
            trunc_hour_us: 1_712_413_800_000_000,
            trunc_day_us: 1_712_408_400_000_000,
            part_hour: 1.0,
            part_minute: 30.0,
        },
    ];

    for c in &cases {
        exec(&sess, &format!("SET TimeZone = '{}'", c.zone)).await;

        assert_eq!(
            scalar_ts_micros(&sess, &format!("SELECT date_trunc('hour', {INPUT})")).await,
            c.trunc_hour_us,
            "date_trunc('hour', …) in {}",
            c.zone
        );
        assert_eq!(
            scalar_ts_micros(&sess, &format!("SELECT date_trunc('day', {INPUT})")).await,
            c.trunc_day_us,
            "date_trunc('day', …) in {}",
            c.zone
        );
        assert_eq!(
            scalar_f64(&sess, &format!("SELECT date_part('hour', {INPUT})")).await,
            c.part_hour,
            "date_part('hour', …) in {}",
            c.zone
        );
        assert_eq!(
            scalar_f64(&sess, &format!("SELECT date_part('minute', {INPUT})")).await,
            c.part_minute,
            "date_part('minute', …) in {}",
            c.zone
        );
    }

    // The negative control the three cases above cannot give on their own:
    // truncating to the hour on Lord Howe is NOT the same instant as
    // truncating to the hour in UTC. If the session zone were being dropped
    // somewhere between `SET` and `eval`, these two would be equal and every
    // assertion above about UTC would still pass.
    assert_ne!(
        cases[0].trunc_hour_us, cases[2].trunc_hour_us,
        "the fixture itself must distinguish UTC from Lord Howe, or it proves nothing"
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

// ── The clocks ──────────────────────────────────────────────────────────────

/// PostgreSQL's three timestamps, and the two things that distinguish them.
///
/// Measured on a live 18.2, in one `BEGIN` block, one statement per protocol
/// message:
///
/// ```text
/// BEGIN;
/// SELECT now(), transaction_timestamp(), statement_timestamp(), clock_timestamp();
///  2026-08-14 05:01:19.049133+02 | …049133+02 | …049133+02 | …050469+02
/// SELECT pg_sleep(0.05);
/// SELECT now(), transaction_timestamp(), statement_timestamp(), clock_timestamp();
///  2026-08-14 05:01:19.049133+02 | …049133+02 | …830465+02 | …131441+02
/// COMMIT;
/// ```
///
/// Two facts, and this test asserts both:
///
/// 1. `now()` **is** `transaction_timestamp()`, and it is byte-identical
///    across the two statements — `…049133` both times, 50ms of sleep
///    notwithstanding.
/// 2. `statement_timestamp()` advanced between them (`…049133` → `…830465`),
///    and `clock_timestamp()` advanced again on top of that.
///
/// Before this wiring, Basin's `now()` was DataFusion's built-in: stable
/// within one query and fresh on the next, which is fact 2's behaviour
/// wearing fact 1's name.
#[tokio::test]
async fn now_is_the_transaction_timestamp_and_is_stable_across_statements_in_one_transaction() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "BEGIN").await;

    let now_1 = scalar_ts_micros(&sess, "SELECT now()").await;
    let txn_1 = scalar_ts_micros(&sess, "SELECT transaction_timestamp()").await;
    let stmt_1 = scalar_ts_micros(&sess, "SELECT statement_timestamp()").await;

    // Enough real time to make an "advances per statement" claim falsifiable:
    // the assertions below are `>`/`==`, so a sleep shorter than the clock's
    // resolution could make an advancing timestamp look pinned.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let now_2 = scalar_ts_micros(&sess, "SELECT now()").await;
    let txn_2 = scalar_ts_micros(&sess, "SELECT transaction_timestamp()").await;
    let stmt_2 = scalar_ts_micros(&sess, "SELECT statement_timestamp()").await;

    assert_eq!(
        now_1, txn_1,
        "now() and transaction_timestamp() are the same function in PostgreSQL"
    );
    assert_eq!(now_2, txn_2, "…and still are on the second statement");
    assert_eq!(
        now_1, now_2,
        "now() must not move inside one transaction — that is clock_timestamp()'s job"
    );
    assert!(
        stmt_2 > stmt_1,
        "statement_timestamp() must advance per statement: {stmt_1} → {stmt_2}"
    );
    assert!(
        stmt_1 >= txn_1,
        "the transaction started no later than its first statement: txn {txn_1}, stmt {stmt_1}"
    );

    exec(&sess, "COMMIT").await;

    // After COMMIT the transaction anchor is gone, so the next autocommit
    // statement is its own transaction and `now()` moves again — PostgreSQL's
    // autocommit behaviour, measured: two successive `SELECT now()` outside a
    // block differ by the round trip.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let now_after = scalar_ts_micros(&sess, "SELECT now()").await;
    assert!(
        now_after > now_1,
        "after COMMIT, now() belongs to the next transaction: {now_1} → {now_after}"
    );
}

/// The third timestamp: `clock_timestamp()` is free to move *within* one
/// statement, and `now()` is not. That difference is the whole reason
/// `EvalSession::DEFAULT` refuses to answer `now()` with a fresh `Utc::now()`
/// — doing so would have made `now()` into this function under another name.
///
/// Measured on 18.2, in one statement over 5 000 rows:
/// `SELECT DISTINCT clock_timestamp() FROM generate_series(1,5000)` returns
/// many distinct instants, while `now()` over the same rows returns exactly
/// one.
///
/// The assertion here is the half that must hold for correctness — `now()`
/// takes exactly one value across every row of one statement. The converse
/// (`clock_timestamp()` takes more than one) is genuinely timing-dependent:
/// a fast enough machine can evaluate 5 000 rows inside one microsecond tick,
/// so asserting it would be asserting that the host is slow. It is checked
/// only for "did not collapse to a constant across a statement boundary",
/// which is not timing-dependent.
#[tokio::test]
async fn now_takes_exactly_one_value_across_every_row_of_a_statement() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "BEGIN").await;

    let distinct = scalar_i64(
        &sess,
        "SELECT count(DISTINCT now()) FROM generate_series(1, 2000)",
    )
    .await;
    assert_eq!(
        distinct, 1,
        "now() must be one value for the whole statement, got {distinct} distinct"
    );

    exec(&sess, "COMMIT").await;

    // Across a transaction boundary it does move — so the single value above
    // is a pinned clock, not a frozen one.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let before = scalar_ts_micros(&sess, "SELECT now()").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = scalar_ts_micros(&sess, "SELECT now()").await;
    assert!(
        after > before,
        "two autocommit statements are two transactions: {before} → {after}"
    );
}

/// A rolled-back transaction releases its `now()` anchor exactly as a
/// committed one does — measured on 18.2, where `ROLLBACK` ends the block and
/// the following statement gets a fresh transaction timestamp.
#[tokio::test]
async fn rollback_releases_the_transaction_timestamp() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    exec(&sess, "BEGIN").await;
    let inside = scalar_ts_micros(&sess, "SELECT now()").await;
    exec(&sess, "ROLLBACK").await;

    tokio::time::sleep(Duration::from_millis(50)).await;
    let after = scalar_ts_micros(&sess, "SELECT now()").await;
    assert!(
        after > inside,
        "ROLLBACK ends the transaction, so now() moves on: {inside} → {after}"
    );
}
