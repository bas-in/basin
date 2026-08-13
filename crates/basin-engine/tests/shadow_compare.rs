//! End-to-end guard for shadow-compare (`BASIN_OWNED_ENGINE_SHADOW_COMPARE`,
//! implemented in `owned_engine.rs` — see that module's "Shadow-compare"
//! section for the four decisions this pins).
//!
//! Shadow-compare re-runs every SELECT the owned engine serves through the
//! incumbent DataFusion path and diffs the two answers. That makes it a free
//! differential oracle over the whole test suite — but only if the diff is
//! actually discriminating. An oracle that reports nothing because it
//! compares nothing looks exactly like an oracle that reports nothing because
//! everything agrees, and only one of those is worth having. So these tests
//! come in two halves:
//!
//!  * **The wiring**, exercised the way a real client would (through
//!    `ProjectSession::execute`): flags off change nothing, flags on actually
//!    compare, and a statement that writes is never re-executed.
//!  * **The rules**, exercised through [`basin_engine::compare_shadow_results`]
//!    on results this file obtained *from the real engine* and then knowingly
//!    made differ. The flag-on path can only ever compare two answers that
//!    agree — both engines read the same committed files — so it cannot
//!    demonstrate that a real divergence would be caught, nor that a benign
//!    one would be ignored. Feeding the same comparison a shuffled row order,
//!    a changed value, and a perturbed float is what proves that.
//!
//! Modelled on `tests/owned_engine_bridge.rs` (the env-var discipline and
//! the engine/session fixtures) and `tests/fallback_histogram.rs` (the
//! counter-reading style).
//!
//! Both `BASIN_OWNED_ENGINE` and `BASIN_OWNED_ENGINE_SHADOW_COMPARE` are
//! process-wide, so every test here holds [`ENV_LOCK`] for the whole window
//! in which their values matter and clears them afterwards.

use std::sync::{Arc, Mutex, MutexGuard};

use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{compare_shadow_results, Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

const OWNED: &str = "BASIN_OWNED_ENGINE";
const SHADOW: &str = "BASIN_OWNED_ENGINE_SHADOW_COMPARE";

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Acquire [`ENV_LOCK`], recovering from poison — a genuine assertion failure
/// in one test must not cascade into every later one. Same rationale (and
/// same code) as `owned_engine_bridge.rs`.
fn env_lock() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Clear both flags. Called at the end of every test that sets them, so the
/// next test starts from the documented default (both OFF).
fn clear_flags() {
    std::env::remove_var(OWNED);
    std::env::remove_var(SHADOW);
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

async fn exec(sess: &ProjectSession, sql: &str) -> ExecResult {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"))
}

async fn rows(sess: &ProjectSession, sql: &str) -> ExecResult {
    match exec(sess, sql).await {
        result @ ExecResult::Rows { .. } => result,
        other => panic!("expected a result set from {sql:?}, got {other:?}"),
    }
}

fn row_count(result: &ExecResult) -> usize {
    match result {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("expected rows, got {other:?}"),
    }
}

/// One BIGINT column, one TEXT column, one DOUBLE PRECISION column and four
/// rows — enough for an order-sensitive comparison (distinct ids), an
/// exact-text comparison, and a float comparison.
async fn seed(sess: &ProjectSession) {
    exec(
        sess,
        "CREATE TABLE t (id BIGINT NOT NULL, name TEXT, amt DOUBLE PRECISION)",
    )
    .await;
    exec(
        sess,
        "INSERT INTO t VALUES (1,'alpha',1.5),(2,'beta',2.5),(3,'gamma',3.5),(4,'delta',4.5)",
    )
    .await;
}

// ───────────────────────── the wiring ─────────────────────────

/// The flag defaults OFF and is independent of `BASIN_OWNED_ENGINE` (module
/// docs, decision 4): the owned engine serving queries must not, on its own,
/// start running everything twice.
#[tokio::test]
async fn shadow_compare_is_off_unless_its_own_flag_is_set() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // Neither flag: nothing is served, so nothing is compared.
    rows(&sess, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(eng.owned_engine_shadow_compare_count(), 0);

    // Owned engine on, shadow-compare still off: served, still not compared.
    std::env::set_var(OWNED, "1");
    rows(&sess, "SELECT id, name FROM t ORDER BY id").await;
    assert!(
        eng.owned_engine_served_count() > 0,
        "the owned engine must actually be serving, or the next assertion is vacuous"
    );
    assert_eq!(
        eng.owned_engine_shadow_compare_count(),
        0,
        "turning on the owned engine must not turn on double execution"
    );
    assert_eq!(eng.owned_engine_shadow_compare_divergence_count(), 0);
    assert!(eng.owned_engine_shadow_compare_divergences().is_empty());

    clear_flags();
}

/// With both flags on, every served SELECT is compared — and on real data
/// read from real files by both engines, the answers agree.
///
/// The `served == compared` equality is the load-bearing part: it is what
/// says the oracle ran on everything it was supposed to, so the zero
/// divergence count below means "they agreed", not "nothing was checked".
///
/// The corpus is reads over a freshly inserted table. It stops short of
/// read-after-UPDATE on purpose and this is the one thing to know about this
/// test: shadow-compare DOES report a divergence there
/// (`row count differs: owned 6, DataFusion 3` — the owned scan path returns
/// every row twice after an UPDATE, reading files the catalog has
/// superseded), which is a real open bug in the owned engine's read path,
/// not in the oracle. Widening this corpus is the right move the day that is
/// fixed; asserting a nonzero divergence count here in the meantime would
/// enshrine the bug as expected behaviour.
#[tokio::test]
async fn both_flags_on_compares_every_served_select_and_finds_them_equal() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    std::env::set_var(OWNED, "1");
    std::env::set_var(SHADOW, "1");

    let corpus = [
        "SELECT id, name FROM t ORDER BY id",
        "SELECT id, name FROM t",
        "SELECT id FROM t WHERE id > 2",
        "SELECT amt FROM t ORDER BY amt DESC",
        "SELECT id, name, amt FROM t WHERE name = 'beta'",
        "SELECT id FROM t ORDER BY id LIMIT 2",
    ];
    for sql in corpus {
        rows(&sess, sql).await;
    }

    let served = eng.owned_engine_served_count();
    let compared = eng.owned_engine_shadow_compare_count();
    assert!(
        served > 0,
        "no query in the corpus was served by the owned engine — nothing was compared, \
         so this test proves nothing; fix the corpus rather than the assertion"
    );
    assert_eq!(
        served, compared,
        "every served SELECT must be shadow-compared; {served} served but {compared} compared"
    );
    assert_eq!(
        eng.owned_engine_shadow_compare_divergence_count(),
        0,
        "the two engines disagreed on real data: {:#?}",
        eng.owned_engine_shadow_compare_divergences()
    );

    clear_flags();
}

/// Decision 3, end to end: a statement that WRITES must never be re-executed.
///
/// Today's call site only ever reaches the bridge for `StmtKind::Select`, so
/// this cannot reach the guard through `execute()` — which is precisely why
/// the guard is structural and unit-tested directly in `owned_engine.rs`
/// (`shadow_target_accepts_only_select_nodes`,
/// `side_effect_free_accepts_read_only_ctes_and_set_op_arms`). What this
/// test adds is the observable consequence at the top: with both flags on, a
/// DML statement writes exactly once and is never counted as a comparison.
///
/// The verification reads deliberately run with the flags OFF, i.e. through
/// DataFusion. Not to be lenient — the counts asserted are exact — but to
/// keep the question single. `BASIN_OWNED_ENGINE` currently makes a read
/// after an UPDATE return every row twice (the owned scan path reads files
/// the catalog has already superseded; see the branch notes), so reading the
/// evidence for "did this write happen once?" through the owned engine would
/// answer a different question than the one this test asks.
#[tokio::test]
async fn dml_writes_exactly_once_and_is_never_shadow_compared() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    std::env::set_var(OWNED, "1");
    std::env::set_var(SHADOW, "1");

    let before_compared = eng.owned_engine_shadow_compare_count();
    exec(&sess, "INSERT INTO t VALUES (10,'ten',10.5)").await;
    exec(&sess, "INSERT INTO t VALUES (11,'eleven',11.5)").await;
    exec(&sess, "UPDATE t SET amt = amt + 1 WHERE id = 10").await;
    exec(&sess, "DELETE FROM t WHERE id = 11").await;
    assert_eq!(
        eng.owned_engine_shadow_compare_count(),
        before_compared,
        "a write must never be shadow-executed — that is a double-write"
    );

    clear_flags();

    // Four seeded rows + one surviving insert. A second execution of either
    // INSERT would show up here immediately.
    let all = rows(&sess, "SELECT id FROM t ORDER BY id").await;
    assert_eq!(row_count(&all), 5, "exactly one row survived the DML");

    let ten = rows(&sess, "SELECT id, amt FROM t WHERE id = 10").await;
    assert_eq!(
        row_count(&ten),
        1,
        "the inserted row must exist exactly once"
    );
    let gone = rows(&sess, "SELECT id FROM t WHERE id = 11").await;
    assert_eq!(row_count(&gone), 0, "the deleted row must be gone");
}

// ───────────────────────── the rules ─────────────────────────
//
// Every result below is produced by the real engine; the two sides of each
// comparison are two real queries whose answers differ in exactly one known
// way. `compare_shadow_results` is the same diff the flag-on path runs.

/// Decision 1: without a top-level `ORDER BY` the statement guarantees no row
/// order, so two engines emitting the same rows in different orders AGREE.
/// This is the false-positive the canonical sort exists to prevent — without
/// it nearly every `GROUP BY`/join/no-`ORDER BY` query would report.
#[tokio::test]
async fn differing_row_order_without_order_by_is_not_a_divergence() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    // The same four rows, twice, in opposite orders.
    let ascending = rows(&sess, "SELECT id, name FROM t ORDER BY id").await;
    let descending = rows(&sess, "SELECT id, name FROM t ORDER BY id DESC").await;
    assert_eq!(row_count(&ascending), 4);
    assert_eq!(row_count(&descending), 4);

    assert_eq!(
        compare_shadow_results("SELECT id, name FROM t", &ascending, &descending),
        None,
        "the statement has no ORDER BY, so row order is not part of its answer"
    );

    // Same two results, now claimed to come from a statement that DID ask for
    // an order: the identical pair must now be reported.
    let detail = compare_shadow_results(
        "SELECT id, name FROM t ORDER BY id",
        &ascending,
        &descending,
    )
    .expect("under ORDER BY, a different row order is a real disagreement");
    assert!(
        detail.contains("positional"),
        "the message should say why it compared positionally, got {detail:?}"
    );

    clear_flags();
}

/// A genuine value difference must survive the canonical sort — the sort
/// reorders rows, it must not launder them.
#[tokio::test]
async fn a_genuine_value_difference_is_reported_with_or_without_order_by() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let truth = rows(&sess, "SELECT id, name FROM t ORDER BY id").await;
    // Same shape, same row count, one column genuinely different.
    let wrong = rows(&sess, "SELECT id + 100, name FROM t ORDER BY id").await;
    assert_eq!(row_count(&truth), row_count(&wrong));

    for sql in [
        "SELECT id, name FROM t",
        "SELECT id, name FROM t ORDER BY id",
    ] {
        let detail = compare_shadow_results(sql, &truth, &wrong)
            .unwrap_or_else(|| panic!("{sql:?}: 1 vs 101 is a wrong answer, not a reordering"));
        assert!(
            detail.contains("column 0"),
            "{sql:?}: the report must name the offending column, got {detail:?}"
        );
    }

    // A text-only difference, to show the exactness applies off the numeric
    // path too.
    let upper = rows(&sess, "SELECT id, upper(name) FROM t ORDER BY id").await;
    assert!(
        compare_shadow_results("SELECT id, name FROM t", &truth, &upper).is_some(),
        "'alpha' and 'ALPHA' are different answers"
    );

    // And the control: the same query twice must always agree.
    let again = rows(&sess, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(
        compare_shadow_results("SELECT id, name FROM t ORDER BY id", &truth, &again),
        None
    );

    clear_flags();
}

/// Decision 2: a float difference at the accumulation-order/ULP level is
/// ignored; one big enough to be a wrong answer is not.
///
/// `0.1 + 0.2` vs `0.3` is the canonical IEEE-754 artefact — the two differ
/// in the last bit, which is exactly the kind of difference two engines
/// summing in different orders produce, and exactly what exact equality
/// would drown this mode in.
#[tokio::test]
async fn float_differences_inside_the_tolerance_are_ignored_and_outside_are_not() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    const SQL: &str = "SELECT x FROM t";

    let accumulated = rows(&sess, "SELECT 0.1::float8 + 0.2::float8 AS x").await;
    let exact = rows(&sess, "SELECT 0.3::float8 AS x").await;
    assert_eq!(
        compare_shadow_results(SQL, &accumulated, &exact),
        None,
        "0.1 + 0.2 vs 0.3 differs in the last bit — that is summation order, not a bug"
    );

    // Relative limb: a difference far past 1e-9 in absolute terms but well
    // inside 1e-9 relative to operands this large.
    let big = rows(&sess, "SELECT 1000000000000.0::float8 AS x").await;
    let big_nudged = rows(&sess, "SELECT 1000000000000.001::float8 AS x").await;
    assert_eq!(
        compare_shadow_results(SQL, &big, &big_nudged),
        None,
        "1e-3 out of 1e12 is 1e-15 relative — inside the tolerance"
    );

    // Outside: 1e-7 relative.
    let a = rows(&sess, "SELECT 1.5::float8 AS x").await;
    let b = rows(&sess, "SELECT 1.5000001::float8 AS x").await;
    let detail = compare_shadow_results(SQL, &a, &b)
        .expect("1e-7 relative is a hundred times the tolerance");
    assert!(detail.contains("column 0"), "got {detail:?}");

    // Well outside: an ordinary wrong number.
    assert!(compare_shadow_results(
        SQL,
        &rows(&sess, "SELECT 1.5::float8 AS x").await,
        &rows(&sess, "SELECT 2.5::float8 AS x").await
    )
    .is_some());

    clear_flags();
}

/// Decision 2's other half: NUMERIC is compared exactly. The difference here
/// is 1e-12 out of 1 — deep inside the float tolerance — and must still be
/// reported, because decimal arithmetic has no summation-order slack to
/// forgive.
#[tokio::test]
async fn numeric_gets_no_float_tolerance() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    const SQL: &str = "SELECT n FROM t";

    let a = rows(&sess, "SELECT 1.000000000000::NUMERIC(20,12) AS n").await;
    let b = rows(&sess, "SELECT 1.000000000001::NUMERIC(20,12) AS n").await;
    assert!(
        compare_shadow_results(SQL, &a, &b).is_some(),
        "a NUMERIC difference of 1e-12 is a real difference, however small"
    );

    let same = rows(&sess, "SELECT 1.000000000000::NUMERIC(20,12) AS n").await;
    assert_eq!(compare_shadow_results(SQL, &a, &same), None);

    clear_flags();
}

/// Shape differences are reported before any cell is looked at, and a
/// statement that is not a side-effect-free SELECT has no comparison at all.
#[tokio::test]
async fn shape_and_eligibility_failures_are_reported_not_silently_skipped() {
    let _guard = env_lock();
    clear_flags();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed(&sess).await;

    let four = rows(&sess, "SELECT id FROM t ORDER BY id").await;
    let two = rows(&sess, "SELECT id FROM t WHERE id > 2 ORDER BY id").await;
    let wide = rows(&sess, "SELECT id, name FROM t ORDER BY id").await;

    assert!(compare_shadow_results("SELECT id FROM t", &four, &two)
        .expect("4 rows vs 2 rows")
        .contains("row count differs"));
    assert!(compare_shadow_results("SELECT id FROM t", &four, &wide)
        .expect("1 column vs 2 columns")
        .contains("column count differs"));

    // Not a comparable statement — must say so rather than quietly returning
    // "they agree".
    assert!(
        compare_shadow_results("INSERT INTO t VALUES (99,'x',9.5)", &four, &four).is_some(),
        "a write has no shadow comparison; reporting None here would read as agreement"
    );
    assert!(
        compare_shadow_results(
            "WITH x AS (INSERT INTO t VALUES (99,'x',9.5) RETURNING id) SELECT * FROM x",
            &four,
            &four
        )
        .is_some(),
        "a data-modifying CTE roots at a SelectStmt but still writes"
    );
    assert!(compare_shadow_results("NOT SQL AT ALL", &four, &four).is_some());

    clear_flags();
}
