//! Integration test: ADR 0027 Phase 4 — promoted JSONB shadow column served
//! by the `fast_select` read path (bypassing DataFusion).
//!
//! ## What this verifies
//!
//! Once a JSONB path is promoted and every live data file carries the
//! materialised `__promoted$col$key` shadow column, a query of the shape
//! `SELECT payload->>'key' FROM t [WHERE <simple predicate>]` is served by
//! `fast_select` reading the shadow column directly — no per-row UDF dispatch,
//! no DataFusion plan.
//!
//! Three properties, all expressed against the always-correct DataFusion / UDF
//! answer (`json_get_text(payload, 'key')`):
//!
//! * **parity** — when all files are backfilled the fast path returns
//!   row-identical results to the UDF path;
//! * **mixed-file correctness guard** — when SOME files predate the promotion
//!   (shadow column absent) the fast path must NOT null-fill the missing column;
//!   it falls back to DataFusion and returns the correct extracted values;
//! * **routing proof** — the engine's `promoted_fast_select_count()` advances
//!   only when the shadow-column fast path actually engages (and stays put when
//!   the guard forces a fallback).

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// Build a shard + engine pair backed by a temp local filesystem.
async fn open_shard_engine() -> (
    TempDir,
    TempDir,
    Arc<dyn Wal>,
    Storage,
    Shard,
    Engine,
    ProjectId,
) {
    let storage_dir = TempDir::new().unwrap();
    let wal_dir = TempDir::new().unwrap();

    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(storage_dir.path()).unwrap()),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wal_dir.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );

    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog: catalog.clone(),
        shard: Some(shard.clone()),
    });

    let project = ProjectId::new();
    catalog.create_namespace(&project).await.unwrap();

    (storage_dir, wal_dir, wal, storage, shard, engine, project)
}

/// Execute `SELECT id, <expr> FROM ...` (no ORDER BY, so the query qualifies
/// for the `fast_select` path) and return the `<expr>` string values ordered by
/// `id` in the harness.  Sorting in Rust (rather than via SQL `ORDER BY`, which
/// would force the DataFusion path) keeps the result deterministic while
/// allowing the fast path to engage.
async fn query_id_string_col(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Vec<Option<String>> {
    let result = sess.execute(sql).await.unwrap();
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows result, got {:?}", other),
    };
    let mut rows: Vec<(i64, Option<String>)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("expected Int64 id column");
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("expected Utf8 value column");
        for i in 0..batch.num_rows() {
            let v = if vals.is_null(i) {
                None
            } else {
                Some(vals.value(i).to_string())
            };
            rows.push((ids.value(i), v));
        }
    }
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, v)| v).collect()
}

/// Parity + routing: after promotion AND a flush that backfills every file with
/// the shadow column, `SELECT payload->>'event_type' FROM events ORDER BY id`
/// returns row-identical results to the `json_get_text` UDF path AND is served
/// by the fast path (the promoted-fast-select counter advances).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_fast_select_parity_and_routing() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();

    // Insert rows, THEN promote, THEN flush so the flush materialises the shadow
    // column into the (only) cold-tier file via backfill.  After this every live
    // file carries `__promoted$payload$event_type`.
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (1, '{\"event_type\": \"login\",  \"user\": \"alice\"}'),\
           (2, '{\"event_type\": 42,        \"user\": \"bob\"}'),\
           (3, '{\"user\": \"carol\"}'),\
           (4, '{\"event_type\": null,      \"user\": \"dave\"}'),\
           (5, '{\"event_type\": \"logout\", \"user\": \"eve\"}')",
    )
    .await
    .unwrap();

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // The expected answer (== what json_get_text computes from the payloads).
    let expected = vec![
        Some("login".to_string()),
        Some("42".to_string()),
        None,
        None,
        Some("logout".to_string()),
    ];

    // Snapshot the counter, run the promoted `->>` query (no ORDER BY → eligible
    // for fast_select), and assert it advanced — i.e. the shadow-column fast
    // path served the query, not DataFusion.
    let before = engine.promoted_fast_select_count();
    let promoted_vals =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after = engine.promoted_fast_select_count();

    assert!(
        after > before,
        "promoted fast-select counter must advance (before={before}, after={after}) — \
         the shadow-column fast path should have served `payload->>'event_type'`"
    );
    assert_eq!(
        promoted_vals, expected,
        "promoted fast-select results must equal the json_get_text answer.\n\
         promoted: {promoted_vals:?}\n\
         expected: {expected:?}"
    );

    // Same query with a simple WHERE predicate on a real column also fast-paths.
    let before2 = engine.promoted_fast_select_count();
    let filtered = query_id_string_col(
        &sess,
        "SELECT id, payload->>'event_type' FROM events WHERE id = 1",
    )
    .await;
    let after2 = engine.promoted_fast_select_count();
    assert!(
        after2 > before2,
        "filtered promoted `->>` should also fast-path"
    );
    assert_eq!(filtered, vec![Some("login".to_string())]);

    wal.close().await.unwrap();
}

/// Mixed-file correctness guard: some files predate the promotion (no shadow
/// column) and some are written after.  Reading the shadow column from a
/// pre-promotion file would null-fill — a WRONG answer.  The guard must detect
/// the missing column on at least one file and fall back to the DataFusion /
/// UDF path, which returns the correct extracted values for ALL rows (no
/// spurious NULLs), and must NOT bump the promoted-fast-select counter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_fast_select_mixed_files_guard() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();

    let table = TableName::new("events").unwrap();

    // ── File #1: written + flushed BEFORE promotion → no shadow column. ──────
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (1, '{\"event_type\": \"login\",  \"user\": \"alice\"}'),\
           (2, '{\"user\": \"bob\"}'),\
           (3, '{\"event_type\": \"signup\", \"user\": \"carol\"}')",
    )
    .await
    .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Promote the path now — file #1 already exists without the shadow column.
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "event_type")
        .await
        .unwrap();

    // ── File #2: written + flushed AFTER promotion → carries shadow column. ──
    sess.execute(
        "INSERT INTO events (id, payload) VALUES \
           (4, '{\"event_type\": \"purchase\", \"user\": \"dave\"}'),\
           (5, '{\"event_type\": null,        \"user\": \"eve\"}'),\
           (6, '{\"event_type\": 99,          \"user\": \"frank\"}')",
    )
    .await
    .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Sanity: confirm the file set is genuinely MIXED — at least one file lacks
    // the shadow column in its catalog `column_stats`, at least one has it.
    let shadow = "__promoted$payload$event_type";
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let files = meta.live_data_files();
    let with = files
        .iter()
        .filter(|f| f.column_stats.contains_key(shadow))
        .count();
    let without = files
        .iter()
        .filter(|f| !f.column_stats.contains_key(shadow))
        .count();
    assert!(
        with >= 1 && without >= 1,
        "test requires a MIXED file set: with_shadow={with}, without_shadow={without}, \
         files={}",
        files.len()
    );
    // Belt-and-braces: list_data_files agrees there are ≥2 files.
    let raw_files = storage.list_data_files(&project, &table).await.unwrap();
    assert!(
        raw_files.len() >= 2,
        "expected ≥2 files, got {}",
        raw_files.len()
    );

    // Expected answer over ALL rows (== json_get_text from the payloads).
    let expected = vec![
        Some("login".to_string()),
        None, // row 2: absent key
        Some("signup".to_string()),
        Some("purchase".to_string()),
        None, // row 5: JSON null
        Some("99".to_string()),
    ];

    // The promoted `->>` query against the MIXED file set: the guard must NOT
    // rewrite to the shadow column (a missing column on file #1 would null-fill
    // rows 1/3), so the query is served via the UDF / DataFusion path and the
    // promoted fast-select counter must NOT advance.
    let before = engine.promoted_fast_select_count();
    let promoted_vals =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after = engine.promoted_fast_select_count();

    // Correctness: identical to the expected answer — NO spurious NULLs.
    assert_eq!(
        promoted_vals, expected,
        "mixed-file promoted `->>` must equal the json_get_text answer (no null-fill).\n\
         promoted: {promoted_vals:?}\n\
         expected: {expected:?}"
    );
    // Routing: the shadow-column fast path must NOT have served this query.
    assert_eq!(
        after, before,
        "promoted fast-select counter must NOT advance when the file set is mixed \
         (guard must leave the value as a json_get_text UDF call)"
    );

    // After backfilling the pre-promotion file, every file carries the shadow
    // column and the SAME query now DOES fast-path — still with correct values.
    let rewritten = shard
        .run_promoted_column_backfill_sweep(&project, &table)
        .await
        .unwrap();
    assert!(
        rewritten >= 1,
        "backfill sweep should rewrite ≥1 file, got {rewritten}"
    );

    let before3 = engine.promoted_fast_select_count();
    let promoted_after_backfill =
        query_id_string_col(&sess, "SELECT id, payload->>'event_type' FROM events").await;
    let after3 = engine.promoted_fast_select_count();
    assert!(
        after3 > before3,
        "after backfill the promoted `->>` query should fast-path (counter must advance)"
    );
    assert_eq!(
        promoted_after_backfill, expected,
        "post-backfill fast-path results must still equal the expected answer"
    );

    wal.close().await.unwrap();
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE-equality + GROUP BY rewrite coverage (the two 1M full-scan shapes:
// `WHERE payload->>'k' = 'literal'` and `GROUP BY payload->>'k'`).
// ─────────────────────────────────────────────────────────────────────────────

/// Execute a single-scalar query (`SELECT COUNT(*) …`) and return the i64.
async fn scalar_i64(sess: &basin_engine::ProjectSession, sql: &str) -> i64 {
    let result = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows result for {sql}, got {other:?}"),
    };
    for batch in &batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = batch.column(0);
        let a = col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| {
                panic!("expected Int64 scalar for {sql}, got {:?}", col.data_type())
            });
        return a.value(0);
    }
    panic!("empty scalar result for {sql}");
}

/// Read row `i` of a string-typed column tolerating the Utf8 / LargeUtf8 /
/// Utf8View encodings the fast path and the DataFusion/Vortex path emit.
fn opt_string_at(col: &dyn Array, i: usize) -> Option<String> {
    use arrow_array::cast::AsArray;
    if col.is_null(i) {
        return None;
    }
    match col.data_type() {
        arrow_schema::DataType::Utf8 => Some(col.as_string::<i32>().value(i).to_string()),
        arrow_schema::DataType::LargeUtf8 => Some(col.as_string::<i64>().value(i).to_string()),
        arrow_schema::DataType::Utf8View => Some(col.as_string_view().value(i).to_string()),
        other => panic!("expected a string-typed column, got {other:?}"),
    }
}

/// Execute a `(key, COUNT)` GROUP BY query and return the groups sorted by
/// key (NULL group first) so two queries can be compared deterministically.
async fn group_counts(
    sess: &basin_engine::ProjectSession,
    sql: &str,
) -> Vec<(Option<String>, i64)> {
    let result = sess
        .execute(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}: {e}"));
    let batches = match result {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows result for {sql}, got {other:?}"),
    };
    let mut rows: Vec<(Option<String>, i64)> = Vec::new();
    for batch in &batches {
        let keys = batch.column(0);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap_or_else(|| {
                panic!(
                    "expected Int64 count column for {sql}, got {:?}",
                    batch.column(1).data_type()
                )
            });
        for i in 0..batch.num_rows() {
            rows.push((opt_string_at(keys.as_ref(), i), counts.value(i)));
        }
    }
    rows.sort();
    rows
}

/// Seed the twin-key dataset: every row carries the SAME value under
/// `cat_promoted` (which the test promotes) and `cat_plain` (never promoted),
/// so any query on the promoted key has a semantically-identical unrewritten
/// twin on the plain key.
///
/// NULL-semantics rows are included deliberately: a JSON `null` value, a
/// missing key, and a NULL payload row — `payload->>'k'` is SQL NULL for all
/// three, so equality excludes them and `IS NULL` / the NULL GROUP BY group
/// counts exactly 3.
async fn seed_twin_key_events(sess: &basin_engine::ProjectSession) {
    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();
    sess.execute(
        r#"INSERT INTO events (id, payload) VALUES
           (1, '{"cat_promoted":"alpha","cat_plain":"alpha"}'),
           (2, '{"cat_promoted":"alpha","cat_plain":"alpha"}'),
           (3, '{"cat_promoted":"alpha","cat_plain":"alpha"}'),
           (4, '{"cat_promoted":"beta","cat_plain":"beta"}'),
           (5, '{"cat_promoted":"beta","cat_plain":"beta"}'),
           (6, '{"cat_promoted":null,"cat_plain":null}'),
           (7, '{"other":1}'),
           (8, NULL)"#,
    )
    .await
    .unwrap();
}

/// WHERE-equality rewrite: `payload->>'cat_promoted' = 'alpha'` must ride the
/// shadow column (promoted fast-select counter advances) and return exactly
/// the same count as the unrewritten twin on the never-promoted key —
/// including the NULL/missing-key rows, which equality excludes on BOTH
/// forms.  `IS NULL` parity is asserted too (missing key + JSON null + NULL
/// payload all surface as SQL NULL in the shadow column — see
/// `materialize_promoted_columns` / `extract_promoted_value`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_where_equality_parity_and_routing() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();
    seed_twin_key_events(&sess).await;

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "cat_promoted")
        .await
        .unwrap();
    // The flush backfills the shadow column into the (pre-promotion) tail
    // rows, so every live file carries `__promoted$payload$cat_promoted`.
    shard.flush_to_parquet().await.unwrap();

    // Unrewritten twin first (cat_plain is never promoted → UDF path).
    let twin_eq = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_plain' = 'alpha'",
    )
    .await;
    assert_eq!(twin_eq, 3, "twin sanity: three 'alpha' rows");

    // Promoted form: must be REWRITTEN to the shadow column and served by the
    // fast path (counter advances), with the identical count.
    let before = engine.promoted_fast_select_count();
    let promoted_eq = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' = 'alpha'",
    )
    .await;
    let after = engine.promoted_fast_select_count();
    assert_eq!(
        promoted_eq, twin_eq,
        "WHERE-equality on the promoted key must equal the unrewritten twin"
    );
    assert!(
        after > before,
        "promoted WHERE-equality must engage the shadow-column fast path \
         (before={before}, after={after})"
    );

    // IS NULL parity: missing key (id 7) + JSON null (id 6) + NULL payload
    // (id 8) — three rows on BOTH forms.
    let twin_null = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_plain' IS NULL",
    )
    .await;
    let promoted_null = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' IS NULL",
    )
    .await;
    assert_eq!(twin_null, 3, "twin sanity: three NULL-extracting rows");
    assert_eq!(
        promoted_null, twin_null,
        "IS NULL on the promoted key must equal the unrewritten twin \
         (shadow column stores SQL NULL for missing key / JSON null / NULL row)"
    );

    wal.close().await.unwrap();
}

/// GROUP BY rewrite parity: grouping on the promoted key (both the explicit
/// `GROUP BY payload->>'k'` form and the `GROUP BY 1` ordinal form) must
/// produce the identical `(key, count)` groups as the unrewritten twin —
/// including the NULL group that collects the JSON-null / missing-key /
/// NULL-payload rows.  This shape always rides DataFusion (the fast path
/// rejects GROUP BY), so it also proves the rewritten `__promoted$…` column
/// resolves in the DataFusion-registered schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_group_by_rewrite_parity() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();
    seed_twin_key_events(&sess).await;

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "cat_promoted")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    let twin = group_counts(
        &sess,
        "SELECT payload->>'cat_plain' AS k, COUNT(*) AS c FROM events \
         GROUP BY payload->>'cat_plain'",
    )
    .await;
    assert_eq!(
        twin,
        vec![
            (None, 3),
            (Some("alpha".to_string()), 3),
            (Some("beta".to_string()), 2),
        ],
        "twin sanity: alpha=3, beta=2, NULL group=3"
    );

    let promoted_explicit = group_counts(
        &sess,
        "SELECT payload->>'cat_promoted' AS k, COUNT(*) AS c FROM events \
         GROUP BY payload->>'cat_promoted'",
    )
    .await;
    assert_eq!(
        promoted_explicit, twin,
        "explicit GROUP BY on the promoted key must equal the unrewritten twin"
    );

    let promoted_ordinal = group_counts(
        &sess,
        "SELECT payload->>'cat_promoted' AS k, COUNT(*) AS c FROM events GROUP BY 1",
    )
    .await;
    assert_eq!(
        promoted_ordinal, twin,
        "GROUP BY 1 over the promoted key must equal the unrewritten twin"
    );

    wal.close().await.unwrap();
}

/// Regression for the fast-path fallback SQL: with a PRIMARY KEY table, a
/// hot-tier UPDATE leaves a dirty overlay entry in the memtable; the promoted
/// WHERE-equality plan then reaches `fast_select`, whose shadow gate declines
/// (cold-only shadow reads cannot see the overlay) and delegates to the
/// DataFusion path.  Before the `fallback_sql` fix that delegation re-used the
/// user's ORIGINAL `->>` text, which DataFusion cannot plan — the conservative
/// fallback became a hard error.  This test asserts the query SUCCEEDS and
/// reflects the post-UPDATE values (the overlay row moved from 'alpha' to
/// 'beta' and both counts shift accordingly, matching the unrewritten twin).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_where_equality_survives_dirty_overlay() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    // PRIMARY KEY so the UPDATE below can take the hot-tier overlay path.
    sess.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, payload JSONB)")
        .await
        .unwrap();
    sess.execute(
        r#"INSERT INTO events (id, payload) VALUES
           (1, '{"cat_promoted":"alpha","cat_plain":"alpha"}'),
           (2, '{"cat_promoted":"alpha","cat_plain":"alpha"}'),
           (3, '{"cat_promoted":"beta","cat_plain":"beta"}'),
           (4, '{"other":1}')"#,
    )
    .await
    .unwrap();

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "cat_promoted")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Move row 1 from 'alpha' to 'beta' (both keys, to keep the twin in
    // lockstep). Whether this lands as a hot overlay or a CoW rewrite, the
    // subsequent reads must reflect it; the hot-overlay case is the one that
    // exercises the fallback.
    sess.execute(
        r#"UPDATE events SET payload = '{"cat_promoted":"beta","cat_plain":"beta"}' WHERE id = 1"#,
    )
    .await
    .unwrap();

    // No routing assertions here: depending on which UPDATE path engaged, the
    // promoted read may ride the fast path, decline the rewrite, or take the
    // fallback — every one of those must produce the SAME counts.
    let promoted_alpha = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' = 'alpha'",
    )
    .await;
    let twin_alpha = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_plain' = 'alpha'",
    )
    .await;
    assert_eq!(
        twin_alpha, 1,
        "post-UPDATE twin sanity: one 'alpha' row left"
    );
    assert_eq!(
        promoted_alpha, twin_alpha,
        "promoted WHERE-equality must see the overlay write (no stale shadow read, no \
         fallback plan error)"
    );

    let promoted_beta = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' = 'beta'",
    )
    .await;
    let twin_beta = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_plain' = 'beta'",
    )
    .await;
    assert_eq!(twin_beta, 2, "post-UPDATE twin sanity: two 'beta' rows");
    assert_eq!(
        promoted_beta, twin_beta,
        "promoted/beta count must match the twin"
    );

    wal.close().await.unwrap();
}

/// Incomplete-shadow decline: a fresh INSERT sitting in the shard tail (not
/// yet materialised into a cold file) must make the WHERE-equality rewrite
/// DECLINE — the query answers via the UDF path (correct, fresh row included,
/// fast-select counter unchanged).  Once the tail drains, the same query
/// re-engages the shadow fast path with the same answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_where_equality_declines_on_pending_tail() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, _storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();
    seed_twin_key_events(&sess).await;

    let table = TableName::new("events").unwrap();
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "cat_promoted")
        .await
        .unwrap();
    shard.flush_to_parquet().await.unwrap();

    // Fresh write BEFORE any materialisation: lands in the shard tail.
    sess.execute(
        r#"INSERT INTO events (id, payload) VALUES
           (9, '{"cat_promoted":"alpha","cat_plain":"alpha"}')"#,
    )
    .await
    .unwrap();

    // The rewrite gate must decline (pending tail) — counter stays put — and
    // the UDF/DataFusion path must count the fresh row.
    let before = engine.promoted_fast_select_count();
    let with_tail = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' = 'alpha'",
    )
    .await;
    let after = engine.promoted_fast_select_count();
    assert_eq!(
        with_tail, 4,
        "fresh tail row must be visible (3 seeded + 1 new)"
    );
    assert_eq!(
        after, before,
        "with un-flushed tail rows the promoted rewrite must DECLINE \
         (fast-select counter must not advance)"
    );

    // The DataFusion read above flushed the tail (exec_select flushes before
    // planning), and the compactor backfills shadow columns for promoted
    // paths — so the SAME query now rides the fast path with the same count.
    let before2 = engine.promoted_fast_select_count();
    let drained = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'cat_promoted' = 'alpha'",
    )
    .await;
    let after2 = engine.promoted_fast_select_count();
    assert_eq!(drained, 4, "post-drain count unchanged");
    assert!(
        after2 > before2,
        "once the tail drained the promoted WHERE-equality must fast-path again"
    );

    wal.close().await.unwrap();
}

/// End-to-end pruning proof at small scale: three cold files with DISJOINT
/// category values, promoted + backfilled via the sweep.  After the sweep,
/// (a) every live file carries the shadow column in `column_stats` AND a
/// writer-computed bloom for it (the sweep previously committed an EMPTY
/// bloom map), and (b) the equality literal prunes to ≤1 file opened
/// (`files_opened` drops vs. the file count).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promoted_eq_prunes_files_after_backfill_sweep() {
    basin_common::telemetry::try_init_for_tests();

    let (_sd, _wd, wal, storage, shard, engine, project) = open_shard_engine().await;
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT NOT NULL, payload JSONB)")
        .await
        .unwrap();

    // Three INSERT+flush cycles → three cold files, each holding ONE category.
    for (base, cat) in [(0i64, "alpha"), (100, "beta"), (200, "gamma")] {
        let rows: Vec<String> = (0..10)
            .map(|i| format!(r#"({}, '{{"category":"{cat}"}}')"#, base + i))
            .collect();
        sess.execute(&format!(
            "INSERT INTO events (id, payload) VALUES {}",
            rows.join(", ")
        ))
        .await
        .unwrap();
        shard.flush_to_parquet().await.unwrap();
    }

    let table = TableName::new("events").unwrap();
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    let n_files = meta.live_data_files().len();
    assert!(n_files >= 3, "expected ≥3 live files, got {n_files}");

    // Promote AFTER all files exist — none carries the shadow column yet, so
    // the rewrite declines and the UDF path answers (correct, no fast-path).
    engine
        .config()
        .catalog
        .promote_jsonb_path(&project, &table, "payload", "category")
        .await
        .unwrap();
    let before_udf = engine.promoted_fast_select_count();
    let pre_sweep = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'category' = 'gamma'",
    )
    .await;
    assert_eq!(pre_sweep, 10, "pre-sweep UDF count");
    assert_eq!(
        engine.promoted_fast_select_count(),
        before_udf,
        "pre-sweep the rewrite must decline (files lack the shadow column)"
    );

    // Backfill sweep: rewrites every file with the shadow column AND blooms.
    let rewritten = shard
        .run_promoted_column_backfill_sweep(&project, &table)
        .await
        .unwrap();
    assert!(
        rewritten >= 3,
        "sweep should rewrite all pre-promotion files, got {rewritten}"
    );

    let shadow = "__promoted$payload$category";
    let meta = engine
        .config()
        .catalog
        .load_table(&project, &table)
        .await
        .unwrap();
    for f in meta.live_data_files() {
        assert!(
            f.column_stats.contains_key(shadow),
            "post-sweep file {} must carry shadow column stats",
            f.path
        );
        assert!(
            f.bloom_filters.contains_key(shadow),
            "post-sweep file {} must carry a bloom for the shadow column \
             (the sweep previously committed an EMPTY bloom map)",
            f.path
        );
    }

    // Equality on the promoted key: fast path + pruning. 'gamma' lives in ONE
    // file; stats/bloom on the shadow column must rule the other files out.
    let counters_before = storage.read_counters().snapshot();
    let fast_before = engine.promoted_fast_select_count();
    let post_sweep = scalar_i64(
        &sess,
        "SELECT COUNT(*) FROM events WHERE payload->>'category' = 'gamma'",
    )
    .await;
    let fast_after = engine.promoted_fast_select_count();
    let delta = storage.read_counters().snapshot().delta(&counters_before);

    assert_eq!(post_sweep, 10, "post-sweep count must match the UDF answer");
    assert!(
        fast_after > fast_before,
        "post-sweep the promoted WHERE-equality must ride the shadow fast path"
    );
    assert!(
        (delta.files_opened as usize) < n_files,
        "equality literal must prune: opened {} of {} files (expected < all)",
        delta.files_opened,
        n_files
    );
    assert!(
        delta.files_opened <= 2,
        "'gamma' lives in one file; expected ≤2 opens (1 data + slack), got {}",
        delta.files_opened
    );

    wal.close().await.unwrap();
}
