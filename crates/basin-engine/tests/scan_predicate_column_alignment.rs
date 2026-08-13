//! The owned engine must filter on the column the predicate NAMES.
//!
//! # What this guards
//!
//! A scan's pushed filter carries its column as an index, and there are two
//! index spaces it could be written in: a position in the table, or a position
//! in the scan's own (post-pruning) projection. The logical layer uses the
//! second — projection pruning renumbers a scan's filters in step with the
//! projection it shrinks — and `basin-exec` used to read them as the first when
//! translating a predicate for `basin-storage` to push down.
//!
//! The two spaces coincide exactly when the projection is the identity `0..n`,
//! which it is for every hand-built plan and for `SELECT *`. So the defect was
//! invisible to unit tests and to any query that selected every column, and
//! showed up only on ordinary SQL that selects a subset — where storage
//! filtered on a DIFFERENT COLUMN and returned wrong rows silently.
//!
//! # Why the assertions are on values
//!
//! Which wrong column you land on depends on the SELECT list, because that is
//! what decides the projection. On the fixture below, `SELECT n FROM u WHERE
//! n > 7` came back completely UNFILTERED (it filtered on `uid`, every value of
//! which passes) while `SELECT uid FROM u WHERE n > 7` came back EMPTY (it
//! filtered on `k`, no value of which passes). One symptom has too many rows
//! and the other too few, so a test asserting "some rows came back" or even a
//! row count would have passed on one of them. The fixture is built so that the
//! same predicate gives a different answer on every column, and every assertion
//! below is on the row VALUES.
//!
//! # The oracle
//!
//! Expected values are PostgreSQL 18.2's, run against the identical fixture.
//! When `PG_DIFF_TEST_DSN` is set the test additionally re-derives them from a
//! live server, so the constants cannot rot; without it the constants still
//! assert, so a CI run with no PostgreSQL is a real test rather than a skip.

use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::RecordBatch;
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// `BASIN_OWNED_ENGINE` is process-wide; serialize every test that sets it,
/// exactly as `owned_engine_bridge.rs` does and for the same reason.
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

/// Render every row as `col|col|…`, type-agnostically, so one helper covers
/// int2/int4/int8/text/float8 and NULL renders distinguishably from `''`.
fn render(batches: &[RecordBatch]) -> Vec<String> {
    let opts = arrow::util::display::FormatOptions::default().with_null("NULL");
    let mut out = Vec::new();
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| arrow::util::display::ArrayFormatter::try_new(c.as_ref(), &opts).unwrap())
            .collect();
        for r in 0..b.num_rows() {
            out.push(
                fmts.iter()
                    .map(|f| f.value(r).to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    out
}

/// `uid`, `k` and `n` all differ under `> 7`: `uid` passes for every row, `k`
/// for none, `n` for exactly two. A predicate that reads the wrong one of the
/// three cannot produce the right rows by coincidence.
///
/// `k` is deliberately the column between `uid` and `n`, because that is the
/// position `SELECT uid FROM u WHERE n > 7` mis-resolved to.
const DDL: &str = "CREATE TABLE u (\
        uid BIGINT, \
        k INT, \
        n INT, \
        b BIGINT, \
        s TEXT, \
        f DOUBLE PRECISION, \
        nn INT)";

const DML: &str = "INSERT INTO u (uid, k, n, b, s, f, nn) VALUES \
     (10, 0, 7, 7, 'g', 7.0, NULL), \
     (11, 0, 8, 8, 'h', 8.0, 8), \
     (12, 0, 9, 9, 'i', 9.0, NULL)";

/// Sort rendered rows so the comparison is on the SET of rows each side
/// returned, not on emission order.
///
/// Deliberately done here rather than with `ORDER BY` in the SQL: an
/// `ORDER BY` the owned pipeline could not lower would send the whole
/// statement to the DataFusion fallback, and the test would then quietly be
/// comparing DataFusion against PostgreSQL — passing while proving nothing
/// about the engine it is meant to guard. `served_count` below is the other
/// half of that guard.
fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// Every (projection, predicate) pair, with PostgreSQL 18.2's answer.
#[allow(clippy::type_complexity)]
fn cases() -> Vec<(&'static str, Vec<&'static str>)> {
    vec![
        // ── int4, the column the report named — every operator ──────────
        ("SELECT uid FROM u WHERE n > 7", vec!["11", "12"]),
        ("SELECT uid FROM u WHERE n >= 8", vec!["11", "12"]),
        ("SELECT uid FROM u WHERE n < 9", vec!["10", "11"]),
        ("SELECT uid FROM u WHERE n <= 8", vec!["10", "11"]),
        ("SELECT uid FROM u WHERE n = 8", vec!["11"]),
        (
            "SELECT uid FROM u WHERE n <> 8",
            vec!["10", "12"],
        ),
        // ── the same predicates with the FILTER column projected. This is
        //    the half that came back unfiltered rather than empty. ────────
        ("SELECT n FROM u WHERE n > 7", vec!["8", "9"]),
        ("SELECT n FROM u WHERE n >= 8", vec!["8", "9"]),
        ("SELECT n FROM u WHERE n < 9", vec!["7", "8"]),
        ("SELECT n FROM u WHERE n <= 8", vec!["7", "8"]),
        ("SELECT n FROM u WHERE n = 8", vec!["8"]),
        ("SELECT n FROM u WHERE n <> 8", vec!["7", "9"]),
        // ── int8: reported as unaffected, and is NOT ─────────────────────
        ("SELECT uid FROM u WHERE b > 7", vec!["11", "12"]),
        ("SELECT b FROM u WHERE b > 7", vec!["8", "9"]),
        ("SELECT uid FROM u WHERE b = 8", vec!["11"]),
        ("SELECT b FROM u WHERE b < 9", vec!["7", "8"]),
        // ── text and float8: the operator oids storage does not push, so
        //    these were already right. Pinned so a later widening of the
        //    push list cannot break them unnoticed. ─────────────────────
        ("SELECT uid FROM u WHERE s > 'g'", vec!["11", "12"]),
        ("SELECT s FROM u WHERE s > 'g'", vec!["h", "i"]),
        ("SELECT uid FROM u WHERE s = 'h'", vec!["11"]),
        (
            "SELECT uid FROM u WHERE f > 7.0",
            vec!["11", "12"],
        ),
        ("SELECT f FROM u WHERE f > 7.0", vec!["8.0", "9.0"]),
        ("SELECT uid FROM u WHERE f <= 8.0", vec!["10", "11"]),
        // ── NULL handling, on a column that has them ─────────────────────
        (
            "SELECT uid FROM u WHERE nn IS NULL",
            vec!["10", "12"],
        ),
        ("SELECT uid FROM u WHERE nn IS NOT NULL", vec!["11"]),
        ("SELECT nn FROM u WHERE nn IS NOT NULL", vec!["8"]),
        // A NULL never satisfies a comparison — it must not leak in from
        // either side of the pushdown boundary.
        ("SELECT uid FROM u WHERE nn > 0", vec!["11"]),
        ("SELECT uid FROM u WHERE nn <> 8", vec![]),
        ("SELECT nn FROM u WHERE nn < 100", vec!["8"]),
        // ── projections that shift the predicate's position differently ──
        (
            "SELECT uid, n FROM u WHERE n > 7",
            vec!["11|8", "12|9"],
        ),
        (
            "SELECT n, uid FROM u WHERE n > 7",
            vec!["8|11", "9|12"],
        ),
        ("SELECT k FROM u WHERE n > 7", vec!["0", "0"]),
        (
            "SELECT s FROM u WHERE n > 7",
            vec!["h", "i"],
        ),
        // The identity projection — the one shape that was always right,
        // and the reason `SELECT *` smoke tests never caught this.
        (
            "SELECT * FROM u WHERE n > 7",
            vec!["11|0|8|8|h|8.0|8", "12|0|9|9|i|9.0|NULL"],
        ),
        // ── a conjunction over two different columns ────────────────────
        ("SELECT uid FROM u WHERE n > 7 AND b < 9", vec!["11"]),
        ("SELECT n FROM u WHERE n > 7 AND uid > 11", vec!["9"]),
    ]
}

/// The owned engine must return PostgreSQL's rows for every case.
#[tokio::test]
async fn owned_engine_filters_on_the_column_the_predicate_names() {
    let _guard = env_lock();
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, DDL).await;
    exec(&sess, DML).await;

    let mut failures = Vec::new();
    let mut not_served = Vec::new();
    for (sql, expected) in cases() {
        let served_before = eng.owned_engine_served_count();
        std::env::set_var("BASIN_OWNED_ENGINE", "1");
        let got = sorted(render(&rows(&sess, sql).await));
        std::env::remove_var("BASIN_OWNED_ENGINE");
        if eng.owned_engine_served_count() == served_before {
            not_served.push(sql);
        }
        let want = sorted(expected.iter().map(|s| s.to_string()).collect());
        if got != want {
            failures.push(format!("  {sql}\n    want {want:?}\n    got  {got:?}"));
        }
    }
    assert!(
        not_served.is_empty(),
        "these fell back to DataFusion, so the owned engine's answer was never \
         tested — the case list has drifted out of what the owned pipeline \
         serves and no longer guards it:\n  {}",
        not_served.join("\n  ")
    );
    assert!(
        failures.is_empty(),
        "owned engine disagreed with PostgreSQL 18.2 on {} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The same cases through the DataFusion path, so a divergence is attributed
/// to the engine that actually diverged rather than to "basin is wrong".
#[tokio::test]
async fn the_datafusion_path_agrees_with_postgres_on_the_same_cases() {
    let _guard = env_lock();
    std::env::remove_var("BASIN_OWNED_ENGINE");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, DDL).await;
    exec(&sess, DML).await;

    let mut failures = Vec::new();
    for (sql, expected) in cases() {
        let got = sorted(render(&rows(&sess, sql).await));
        let want = sorted(expected.iter().map(|s| s.to_string()).collect());
        if got != want {
            failures.push(format!("  {sql}\n    want {want:?}\n    got  {got:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "DataFusion path disagreed with PostgreSQL 18.2 on {} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Collapse the one difference that is presentation, not value: Arrow prints
/// a whole `float8` as `8.0`, PostgreSQL prints it as `8`. Nothing else is
/// normalized — a divergence in any digit, sign or NULL survives this.
fn canon(v: &str) -> String {
    match v.strip_suffix(".0") {
        Some(head) if head.parse::<i64>().is_ok() => head.to_string(),
        _ => v.to_string(),
    }
}

/// Re-derive every expected value from a live PostgreSQL, so the constants
/// above cannot quietly become "whatever basin happened to return".
///
/// Skips cleanly (passes) when `PG_DIFF_TEST_DSN` is unset — the same
/// convention `differential_pg.rs` uses — because the constants are already
/// asserted by the two tests above.
#[tokio::test]
async fn the_expected_values_are_what_a_live_postgres_returns() {
    let Ok(dsn) = std::env::var("PG_DIFF_TEST_DSN") else {
        return;
    };
    if dsn.trim().is_empty() {
        return;
    }
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("PG_DIFF_TEST_DSN connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let schema = format!("basin_scan_align_{}", std::process::id());
    client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; \
             SET search_path TO {schema};"
        ))
        .await
        .unwrap();
    client.batch_execute(DDL).await.unwrap();
    client.batch_execute(DML).await.unwrap();

    let mut failures = Vec::new();
    for (sql, expected) in cases() {
        // The SIMPLE query protocol returns every value already rendered as
        // text by the server, so the comparison is on what PostgreSQL prints
        // rather than on this client library's type mapping.
        let got = sorted(
            client
                .simple_query(sql)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|m| match m {
                    tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                        (0..r.len())
                            .map(|i| canon(r.get(i).unwrap_or("NULL")))
                            .collect::<Vec<_>>()
                            .join("|"),
                    ),
                    _ => None,
                })
                .collect(),
        );
        let want = sorted(
            expected
                .iter()
                .map(|v| v.split('|').map(canon).collect::<Vec<_>>().join("|"))
                .collect(),
        );
        if got != want {
            failures.push(format!("  {sql}\n    pinned {want:?}\n    PG     {got:?}"));
        }
    }
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    assert!(
        failures.is_empty(),
        "the pinned expectations are not PostgreSQL's answers on {} case(s):\n{}",
        failures.len(),
        failures.join("\n")
    );
}
