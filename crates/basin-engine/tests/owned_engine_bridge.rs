//! End-to-end guard for the `BASIN_OWNED_ENGINE` bridge (`owned_engine.rs`).
//!
//! Before this bridge existed, `basin-plan` (lowering + IR + optimizer) and
//! `basin-exec` (operators, real Vortex/Parquet reads) were a fully tested
//! pipeline that no client SELECT had ever reached. These tests exercise the
//! bridge exactly the way a real client would — through `ProjectSession::execute`
//! — and check the three properties the flag's safety depends on:
//!
//! 1. Flag off (the default) leaves `execute()` byte-for-byte unchanged and
//!    never touches either counter.
//! 2. Flag on serves a real, simple SELECT against a real table end to end,
//!    and its answer matches what the same SQL returns via DataFusion (flag
//!    off) — proving the bridge doesn't just "not crash" but returns the
//!    right rows.
//! 3. A construct the owned pipeline does not support (`GROUP BY ROLLUP`,
//!    per `basin-plan/src/lower/select.rs`'s documented scope) still returns
//!    the correct answer — it must fall back to DataFusion rather than
//!    error — and the fallback counter, not the served counter, is the one
//!    that advances.
//!
//! `BASIN_OWNED_ENGINE` is a process-wide env var, so every test that
//! touches it serializes on [`ENV_LOCK`] for the full window during which
//! its value matters, and restores it to unset afterwards — otherwise a
//! concurrently-running test in this same binary could observe the wrong
//! value.

use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Serializes every test in this binary that sets/reads `BASIN_OWNED_ENGINE`
/// — see the module docs.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Acquire [`ENV_LOCK`], recovering from poison rather than propagating it.
/// A test that panics while holding this lock (e.g. on a genuine, unrelated
/// assertion failure) would otherwise poison the mutex for the rest of the
/// binary, turning one real failure into a cascade of spurious ones in every
/// test that runs after it — `()` carries no invariant that a panic could
/// have left inconsistent, so recovering here loses nothing.
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

/// Flatten `(id, name)` batches — the two-column shape every test below
/// selects — into a plain `Vec` so assertions don't depend on batch
/// boundaries, and two runs of the *same* logical result (one served by
/// each engine) compare equal regardless of how each happened to batch rows.
fn flatten_id_name(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..b.num_rows() {
            let id = ids.value(r);
            let name = (!names.is_null(r)).then(|| names.value(r).to_string());
            out.push((id, name));
        }
    }
    out
}

/// Flatten two BIGINT columns, the shape a `GROUP BY ... count(*)` result
/// has. The first is nullable (a `ROLLUP` grand-total row carries a NULL
/// grouping key), the second is a count and never is.
fn flatten_two_i64(batches: &[RecordBatch]) -> Vec<(Option<i64>, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let counts = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            out.push((
                (!keys.is_null(r)).then(|| keys.value(r)),
                counts.value(r),
            ));
        }
    }
    out
}

async fn seed_table(sess: &ProjectSession) {
    exec(sess, "CREATE TABLE t (id BIGINT, name TEXT)").await;
    exec(
        sess,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma'), (4, 'delta')",
    )
    .await;
}

/// Property 1: with the flag unset, `execute()` never invokes the bridge at
/// all — both counters stay at zero and the query still returns the right
/// rows via the unchanged DataFusion path.
#[tokio::test]
async fn flag_off_by_default_leaves_behaviour_and_counters_untouched() {
    let _guard = env_lock();
    std::env::remove_var("BASIN_OWNED_ENGINE");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    let batches = rows(&sess, "SELECT id, name FROM t WHERE id > 1 ORDER BY id").await;
    let got = flatten_id_name(&batches);
    assert_eq!(
        got,
        vec![
            (2, Some("beta".to_string())),
            (3, Some("gamma".to_string())),
            (4, Some("delta".to_string())),
        ]
    );

    assert_eq!(
        eng.owned_engine_served_count(),
        0,
        "the bridge must not be attempted at all with the flag unset"
    );
    assert_eq!(
        eng.owned_engine_fallback_count(),
        0,
        "no attempt means no fallback either — flag-off is not 'always falls back'"
    );
}

/// Property 2: flag on serves a real `WHERE ... ORDER BY` SELECT end to end
/// through the owned pipeline, reading the same real Vortex/Parquet files
/// DataFusion would, and returns the identical answer.
#[tokio::test]
async fn flag_on_serves_a_simple_select_matching_datafusion() {
    let _guard = env_lock();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    const SQL: &str = "SELECT id, name FROM t WHERE id > 1 ORDER BY id";

    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned_batches = rows(&sess, SQL).await;
    let owned = flatten_id_name(&owned_batches);
    assert_eq!(
        eng.owned_engine_served_count(),
        1,
        "the owned pipeline must have actually served this query"
    );
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let df_batches = rows(&sess, SQL).await;
    let df = flatten_id_name(&df_batches);

    assert_eq!(
        owned, df,
        "the owned engine's answer must match DataFusion's for the same SQL"
    );
    assert_eq!(
        owned,
        vec![
            (2, Some("beta".to_string())),
            (3, Some("gamma".to_string())),
            (4, Some("delta".to_string())),
        ]
    );
    // The DataFusion-served re-run above must not have moved either counter:
    // the flag was off for it, so `execute()` never called the bridge.
    assert_eq!(eng.owned_engine_served_count(), 1);
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 3: a construct outside the owned pipeline's documented scope
/// (`GROUP BY ROLLUP (...)` — a grouping-set construct
/// `basin-plan/src/lower/select.rs` reports as `LowerError::Unsupported`)
/// must still produce the correct answer, via a silent fallback to
/// DataFusion, and record itself as a fallback rather than a served query or
/// a client-visible error.
///
/// This test used `SELECT DISTINCT ON (...)` until `basin-plan` grew
/// DISTINCT ON lowering, at which point the query started being SERVED and
/// the test failed — correctly, because its premise had expired. The
/// assertions below are unchanged in strength; only the construct they are
/// pointed at moved to one the pipeline genuinely does not implement yet.
/// The expected rows were checked against a live PostgreSQL 18.2:
/// `(1, 2), (2, 1), (NULL, 3)`.
#[tokio::test]
async fn unsupported_construct_falls_back_instead_of_erroring() {
    let _guard = env_lock();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, "CREATE TABLE d (id BIGINT, name TEXT)").await;
    exec(
        &sess,
        "INSERT INTO d (id, name) VALUES (1, 'x'), (1, 'x'), (2, 'y')",
    )
    .await;

    // ROLLUP must fall back, not error, and the fallback must still compute
    // the whole grouping set — the two per-id groups AND the grand total.
    let batches = rows(
        &sess,
        "SELECT id, count(*) FROM d GROUP BY ROLLUP (id) ORDER BY id",
    )
    .await;
    let got = flatten_two_i64(&batches);
    assert_eq!(
        got,
        vec![(Some(1), 2), (Some(2), 1), (None, 3)],
        "a fallback must still return the correct answer, grand total included"
    );

    assert_eq!(
        eng.owned_engine_served_count(),
        0,
        "an unsupported construct must not be reported as served"
    );
    assert_eq!(
        eng.owned_engine_fallback_count(),
        1,
        "the fallback counter is exactly the one that should move here"
    );

    // ROLLUP is a construct nothing downstream implements yet
    // (`LowerError::Unsupported`), not an ineligible table or a genuine
    // error — it must land in exactly the `unsupported` bucket, and the
    // histogram must still sum to the flat fallback count.
    let hist = eng.owned_engine_fallback_reason_counts();
    assert_eq!(hist.unsupported, 1);
    assert_eq!(hist.ineligible, 0);
    assert_eq!(hist.lowering_error, 0);
    assert_eq!(hist.build_error, 0);
    assert_eq!(hist.exec_error, 0);
    assert_eq!(hist.total(), eng.owned_engine_fallback_count());

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 4: a predicate under `NOT` — unresolvable by this bridge before
/// its resolvers were widened (`RealOperators` used to report `None` for
/// `"NOT"` on purpose) — is now served end to end, and matches DataFusion.
///
/// Deliberately a text comparison (`name = 'gamma'`), not a numeric one
/// (`id > 2`): `NOT` is never a shape `basin-exec::storage_source`'s
/// `expr_to_predicate` turns into a pushable `basin_storage::Predicate` (see
/// that function's own doc — it only recognises specific un-negated leaf
/// comparisons), so the whole predicate is evaluated Arrow-side via
/// `eval_unary`/`eval_binary` rather than at the storage layer. That is a
/// pre-existing, narrower gap unrelated to this task: comparing this bridge's
/// `int4`-typed integer literals (`lower_a_const` always lowers a bare
/// integer literal to `PgType::INT4`, regardless of the column's width — see
/// that function's own doc) against an `int8` column with no inserted cast
/// fails in Arrow (`cmp::gt` refuses `Int64 > Int32`) whenever the comparison
/// isn't storage-pushed — a text/text comparison has no such width to
/// mismatch, so it isolates the property this test exists to prove (`NOT`
/// itself now resolves and executes) from that unrelated, already-tracked
/// gap.
#[tokio::test]
async fn not_predicate_is_now_served_and_matches_datafusion() {
    let _guard = env_lock();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    const SQL: &str = "SELECT id, name FROM t WHERE NOT (name = 'gamma') ORDER BY id";

    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned_batches = rows(&sess, SQL).await;
    let owned = flatten_id_name(&owned_batches);
    assert_eq!(
        eng.owned_engine_served_count(),
        1,
        "NOT must now resolve and be served, not fall back"
    );
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let df_batches = rows(&sess, SQL).await;
    let df = flatten_id_name(&df_batches);

    assert_eq!(
        owned, df,
        "the owned engine's NOT must match DataFusion's for the same SQL"
    );
    assert_eq!(
        owned,
        vec![
            (1, Some("alpha".to_string())),
            (2, Some("beta".to_string())),
            (4, Some("delta".to_string())),
        ]
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 5: a scalar function — resolvable by neither of the pre-widening
/// resolvers at all (only 5 aggregate names were wired) — used in both the
/// target list and the `WHERE` clause is now served end to end, matching
/// DataFusion.
///
/// Both `WHERE` arms are `=` comparisons, not `LIKE` and not `id = 4`:
/// `LIKE`'s pattern operand is not exempted from `eval_like`'s ordinary
/// `eval()` call in `basin-exec::eval` the way `eval_binary` exempts an
/// untyped-literal `=`/`<`/`>` operand (see `eval_binary`'s own
/// `is_unknown_literal` handling) — a `LIKE 'b%'` pattern stays
/// `PgType::UNKNOWN` (Postgres's own rule for a bare string literal, per
/// `lower_a_const`) all the way to `eval`, which fails it outright. That is
/// a real, pre-existing, unrelated gap in `LIKE` specifically, not something
/// this task's widened resolvers touch, so this test avoids it entirely
/// rather than exercise it incidentally. `OR` is unpushable to
/// `basin_storage::Predicate` either way, so this still evaluates Arrow-side
/// and still proves scalar functions serve there — see
/// `not_predicate_is_now_served_and_matches_datafusion`'s doc comment for
/// the sibling reasoning on numeric literals.
#[tokio::test]
async fn scalar_function_in_select_list_and_where_is_now_served_and_matches_datafusion() {
    let _guard = env_lock();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    const SQL: &str = "SELECT id, upper(name) FROM t WHERE lower(name) = 'beta' \
                        OR upper(name) = 'DELTA' ORDER BY id";

    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned_batches = rows(&sess, SQL).await;
    let owned = flatten_id_name(&owned_batches);
    assert_eq!(
        eng.owned_engine_served_count(),
        1,
        "lower()/upper() must now resolve and be served, not fall back"
    );
    assert_eq!(eng.owned_engine_fallback_count(), 0);

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let df_batches = rows(&sess, SQL).await;
    let df = flatten_id_name(&df_batches);

    assert_eq!(
        owned, df,
        "the owned engine's scalar-function answer must match DataFusion's"
    );
    assert_eq!(
        owned,
        vec![
            (2, Some("BETA".to_string())),
            (4, Some("DELTA".to_string())),
        ]
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 6: a function that now *resolves* against the real `pg_proc`
/// table (this bridge's widened `RealFunctions`) but that
/// `basin-exec::eval` does not implement execution for yet (`sqrt` is not in
/// its scalar-function dispatch table) must still return the correct answer
/// — build succeeds, execution fails cleanly, and the fallback is filed
/// under the `exec_error` bucket specifically, not `unsupported`, proving
/// the histogram distinguishes "nothing resolved" from "resolved, but nothing
/// downstream runs it yet".
#[tokio::test]
async fn resolved_but_unimplemented_scalar_function_falls_back_via_exec_error_bucket() {
    let _guard = env_lock();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    let batches = rows(&sess, "SELECT id FROM t WHERE sqrt(id) > 1.5 ORDER BY id").await;
    let mut got = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            got.push(ids.value(r));
        }
    }
    assert_eq!(
        got,
        vec![3, 4],
        "a fallback must still return the correct answer"
    );

    assert_eq!(
        eng.owned_engine_served_count(),
        0,
        "an unimplemented-in-eval function must not be reported as served"
    );
    assert_eq!(eng.owned_engine_fallback_count(), 1);
    let hist = eng.owned_engine_fallback_reason_counts();
    assert_eq!(
        hist.exec_error, 1,
        "resolved-then-failed-at-runtime must be exec_error, not unsupported"
    );
    assert_eq!(hist.unsupported, 0);
    assert_eq!(hist.ineligible, 0);
    assert_eq!(hist.total(), 1);

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 7: an in-progress explicit transaction is an `Ineligible`
/// decline (checked by this bridge itself, before lowering ever runs), not
/// an error of any kind — and it must be attributed to the `ineligible`
/// bucket like every other per-table/per-session safety gate.
#[tokio::test]
async fn active_transaction_falls_back_via_ineligible_bucket() {
    let _guard = env_lock();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    seed_table(&sess).await;

    exec(&sess, "BEGIN").await;
    let batches = rows(&sess, "SELECT id, name FROM t WHERE id > 1 ORDER BY id").await;
    exec(&sess, "COMMIT").await;

    let got = flatten_id_name(&batches);
    assert_eq!(
        got,
        vec![
            (2, Some("beta".to_string())),
            (3, Some("gamma".to_string())),
            (4, Some("delta".to_string())),
        ]
    );

    assert_eq!(eng.owned_engine_served_count(), 0);
    assert_eq!(eng.owned_engine_fallback_count(), 1);
    let hist = eng.owned_engine_fallback_reason_counts();
    assert_eq!(hist.ineligible, 1);
    assert_eq!(hist.unsupported, 0);
    assert_eq!(hist.total(), 1);

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 8: the point of wiring `basin_plan::opt::optimize_default` into
/// this bridge (rather than lowering straight into `basin_exec::build::build`,
/// or the hand-picked 2-rule subset this bridge used to call) — proved
/// against real storage, not just against rows.
///
/// Three files with disjoint `id` ranges, one per `INSERT` statement (each
/// statement writes straight to cold storage and mints its own file — no
/// hot-tier buffering in this shardless test harness, so nothing here trips
/// the bridge's hot-tier eligibility gate). A predicate that only the third
/// file's rows can satisfy is served by the owned engine; `storage`'s own
/// read counters must show exactly one file opened.
///
/// Without filter pushdown, lowering leaves `id > 250` sitting in a `Filter`
/// node *above* an unfiltered `Scan` (see `select.rs`'s `build_range_var`:
/// every scan starts with `filters: vec![]`) — `StorageTableResolver::open`
/// would then see no predicate at all and open all three files. A test that
/// only checked the returned rows would pass either way, since the rules are
/// answer-preserving by design; this checks the I/O the rules exist to save.
#[tokio::test]
async fn optimize_default_pushes_the_filter_so_storage_prunes_files_not_just_rows() {
    let _guard = env_lock();
    std::env::set_var("BASIN_OWNED_ENGINE", "1");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(&sess, "CREATE TABLE pruned (id BIGINT, name TEXT)").await;

    for (lo, hi) in [(0i64, 100i64), (100i64, 200i64), (200i64, 300i64)] {
        exec(
            &sess,
            &format!(
                "INSERT INTO pruned (id, name) VALUES ({lo}, 'lo'), ({}, 'hi')",
                hi - 1
            ),
        )
        .await;
    }

    let before = eng.config().storage.read_counters().snapshot();
    let batches = rows(&sess, "SELECT id FROM pruned WHERE id > 250 ORDER BY id").await;
    let after = eng.config().storage.read_counters().snapshot();

    assert_eq!(
        eng.owned_engine_served_count(),
        1,
        "must be served by the owned engine, not fall back to DataFusion"
    );

    let mut ids = Vec::new();
    for b in &batches {
        let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            ids.push(arr.value(r));
        }
    }
    assert_eq!(ids, vec![299], "only id=299 satisfies id > 250");

    let delta = after.delta(&before);
    assert_eq!(
        delta.files_opened, 1,
        "optimize_default's filter pushdown must land `id > 250` in Scan::filters so \
         storage-side stats pruning skips the two files whose id range can't satisfy \
         it — opening all three would mean the predicate never reached the scan"
    );

    std::env::remove_var("BASIN_OWNED_ENGINE");
}

/// Property 9: proves projection pruning specifically, the other half of
/// `optimize_default`'s job — `SELECT id FROM wide WHERE id > 0` over a table
/// with an extra unreferenced column must still return the right answer with
/// the optimizer wired in, matching what the bridge produced before this
/// task (the same fixture as property 2, widened by a column nothing in this
/// query touches). This is deliberately still a row-count assertion, not a
/// counter one — see `optimize_default_prunes_the_projection_and_pushes_the_filter_into_the_scan`
/// in `owned_engine.rs`'s own unit tests for the plan-shape assertion that
/// actually pins projection pruning; this test's job is just to confirm the
/// answer is unchanged with an unreferenced column in the mix, end to end.
#[tokio::test]
async fn optimize_default_leaves_the_answer_correct_with_an_unreferenced_column() {
    let _guard = env_lock();

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();
    exec(
        &sess,
        "CREATE TABLE wide (id BIGINT, name TEXT, extra TEXT)",
    )
    .await;
    exec(
        &sess,
        "INSERT INTO wide (id, name, extra) VALUES \
         (1, 'alpha', 'x'), (2, 'beta', 'y'), (3, 'gamma', 'z')",
    )
    .await;

    const SQL: &str = "SELECT id FROM wide WHERE id > 1 ORDER BY id";

    std::env::set_var("BASIN_OWNED_ENGINE", "1");
    let owned_batches = rows(&sess, SQL).await;
    assert_eq!(eng.owned_engine_served_count(), 1);
    let mut owned_ids = Vec::new();
    for b in &owned_batches {
        let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            owned_ids.push(arr.value(r));
        }
    }

    std::env::remove_var("BASIN_OWNED_ENGINE");
    let df_batches = rows(&sess, SQL).await;
    let mut df_ids = Vec::new();
    for b in &df_batches {
        let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            df_ids.push(arr.value(r));
        }
    }

    assert_eq!(
        owned_ids, df_ids,
        "projection pruning must not change the answer"
    );
    assert_eq!(owned_ids, vec![2, 3]);

    std::env::remove_var("BASIN_OWNED_ENGINE");
}
