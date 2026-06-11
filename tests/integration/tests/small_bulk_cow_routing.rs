//! Small multi-row structural UPDATEs route through the hot-tier overlay
//! (fast path) instead of the cold copy-on-write whole-file rewrite.
//!
//! Background: the single-row PK-eq UPDATE fast path
//! (`hot_tier_update_by_pk`) already serves `jsonb_set(...)` and `CASE` RHS
//! because `apply_assignments` evaluates any allowlisted row-local expression
//! via the same DataFusion evaluator the cold path uses. This suite covers the
//! *multi-key* extension:
//!
//!   * `try_resolve_fast_path_update` now accepts a small key SET resolved by
//!     PROBE — it runs `SELECT * FROM t [WHERE <predicate>] LIMIT N+1` through
//!     the overlay-aware fast-SELECT machinery; ≤ N matched keys route through
//!     `hot_tier_update_by_pk` (which already takes `&[RowKey]`), N+1 falls to
//!     cold CoW exactly as before. The probe carries the full row images as
//!     the write step's pre-image source (no second cold read).
//!   * the cap N is `delta_update_max_keys()` — default 10 000, overridable
//!     per statement with `BASIN_DELTA_UPDATE_MAX_KEYS`. The raised cap is
//!     budget-guarded: after the pre-images are gathered the fast path
//!     reserves their bytes against the project memtable budget and DECLINES
//!     to the cold CoW path on `HardCapReached`.
//!   * a no-WHERE `UPDATE t SET …` routes through the same probe
//!     (`match_simple_select` admits the zero-predicate select): a small
//!     table's whole row set rides the overlay, a >cap table falls to cold.
//!   * the RMW allowlist (`rmw_rhs_is_fast_path_eligible`) gained an explicit
//!     Immutable / row-local FUNCTION allowlist: `jsonb_set`, `jsonb_insert`,
//!     `jsonb_strip_nulls`, `coalesce` (NOW() stays excluded).
//!
//! ## Detecting which path ran
//!
//! When the fast path fires, the engine writes a `MemRowValue::Update` per
//! matched PK into the process-wide `MemTableRegistry`. The cold CoW path
//! rewrites Parquet/Vortex files and leaves the registry untouched. So we probe
//! `engine.memtable_registry()` for `Update` entries — exactly the strategy
//! `fastpath_gates.rs` uses — and, where the contract is "zero replacement
//! writes", we additionally assert the catalog's live data-file set (and
//! snapshot id) is unchanged: a cold CoW `commit_replace` MUST change it.
//!
//! ## Env discipline
//!
//! Some tests mutate routing-relevant process env
//! (`BASIN_DELTA_UPDATE_MAX_KEYS`, `BASIN_HOTTIER_FASTPATH_DISABLE`). Env is
//! process-global and the engine reads these per statement, so mutators take
//! the `ENV_LOCK` WRITE lock for their whole duration and restore the prior
//! value; every other test takes the READ lock (they may run concurrently
//! with each other but never against a mutator). Same pattern as
//! `row_tier_residency.rs`.
//!
//! Drives `ProjectSession::execute` directly (no pgwire, `shard: None`).

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array, LargeBinaryArray, StringArray};
use basin_catalog::InMemoryCatalog;
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_hottier::{MemRowValue, RowKey};
use object_store::local::LocalFileSystem;
use serde_json::Value;
use tempfile::TempDir;

// ── Env serialization (see module docs) ─────────────────────────────────────

static ENV_LOCK: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

fn restore_env(key: &str, prev: Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

// ── Harness ─────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

// ── Registry probe ──────────────────────────────────────────────────────────

fn row_key_i64(v: i64) -> RowKey {
    RowKey::builder().append_i64(v).finish()
}

/// True iff the registry holds a `MemRowValue::Update` for `pk`.
fn registry_has_update(eng: &Engine, project: &ProjectId, table: &TableName, pk: i64) -> bool {
    let Some(entry) = eng.memtable_registry().get(project, table) else {
        return false;
    };
    matches!(
        entry.memtable.get(&row_key_i64(pk)),
        Some(MemRowValue::Update { .. })
    )
}

/// Count of `Update` overlay entries for `(project, table)`.
fn registry_update_count(eng: &Engine, project: &ProjectId, table: &TableName) -> usize {
    let Some(entry) = eng.memtable_registry().get(project, table) else {
        return 0;
    };
    entry
        .memtable
        .snapshot()
        .iter()
        .filter(|(_, v)| matches!(v, MemRowValue::Update { .. }))
        .count()
}

// ── Read helpers ────────────────────────────────────────────────────────────

async fn count_all(sess: &ProjectSession, table: &str) -> i64 {
    match sess
        .execute(&format!("SELECT COUNT(*) FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        other => panic!("COUNT(*) got {other:?}"),
    }
}

/// Single-cell Int64 result of an arbitrary aggregate query — for comparing
/// PK-omitting fast-path read shapes (e.g. `SELECT SUM(v)`) against the cold
/// twin / hand-computed expectations.
async fn int_scalar(sess: &ProjectSession, sql: &str) -> i64 {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        other => panic!("{sql} got {other:?}"),
    }
}

/// All Int64 values of the FIRST output column of `sql`, across batches, in
/// scan order. Used for non-PK projection scans (`SELECT v FROM t`) whose
/// read set omits the PK column — the caller sorts before comparing.
async fn i64_column(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("column must be Int64");
                for i in 0..b.num_rows() {
                    out.push(arr.value(i));
                }
            }
            out
        }
        other => panic!("{sql} got {other:?}"),
    }
}

async fn fetch_i64(sess: &ProjectSession, table: &str, col: &str, pk: i64) -> Option<i64> {
    let sql = format!("SELECT {col} FROM {table} WHERE id = {pk}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("col must be Int64");
                return Some(arr.value(0));
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

async fn fetch_text(sess: &ProjectSession, table: &str, col: &str, pk: i64) -> Option<String> {
    let sql = format!("SELECT {col} FROM {table} WHERE id = {pk}");
    match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let arr = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("col must be Utf8");
                return Some(arr.value(0).to_string());
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

/// Decode the JSONB `payload` document for one `id`.
async fn payload_for_id(sess: &ProjectSession, table: &str, id: i64) -> Value {
    let sql = format!("SELECT payload FROM {table} WHERE id = {id}");
    let batches = match sess.execute(&sql).await {
        Ok(ExecResult::Rows { batches, .. }) => batches,
        other => panic!("non-rows for {sql}: {other:?}"),
    };
    for b in &batches {
        if b.num_rows() == 0 {
            continue;
        }
        let col = b.column(0);
        if col.is_null(0) {
            panic!("payload NULL for id={id}");
        }
        if let Some(arr) = col.as_any().downcast_ref::<LargeBinaryArray>() {
            return serde_json::from_slice(arr.value(0)).expect("decode binary payload");
        }
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            return serde_json::from_str(arr.value(0)).expect("decode text payload");
        }
        panic!("payload unexpected type {:?}", col.data_type());
    }
    panic!("no payload row for id={id}");
}

/// Sorted live data-file paths plus the current snapshot id for `(project,
/// table)`, straight from the catalog. The delta (overlay) UPDATE path commits
/// nothing, so both must be UNCHANGED across it; a cold CoW UPDATE runs
/// `commit_replace` (new snapshot, replacement file paths), so both change.
/// This is the "zero replacement files" detector.
async fn cold_tier_state(eng: &Engine, project: &ProjectId, table: &TableName) -> (u64, Vec<String>) {
    let meta = eng
        .config()
        .catalog
        .load_table(project, table)
        .await
        .unwrap();
    let mut paths: Vec<String> = meta
        .live_data_files()
        .iter()
        .map(|f| f.path.clone())
        .collect();
    paths.sort();
    (meta.current_snapshot.0, paths)
}

/// Full `(id, v, status)` content of a `seed_status`-shaped table, ordered by
/// id — for byte-for-byte delta-vs-cold twin comparison.
async fn all_status_rows(sess: &ProjectSession, table: &str) -> Vec<(i64, i64, String)> {
    let sql = format!("SELECT id, v, status FROM {table} ORDER BY id");
    let batches = match sess.execute(&sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("all_status_rows got {other:?}"),
    };
    let mut out = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let vs = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sts = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), vs.value(i), sts.value(i).to_string()));
        }
    }
    out
}

// ── Seed helpers ────────────────────────────────────────────────────────────

/// `(id BIGINT PRIMARY KEY, payload JSONB)` with `n` rows; payload is
/// `{"category":"c","score":<id>}`. One multi-row INSERT → one committed file.
async fn seed_events(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload JSONB)"),
    )
    .await;
    let vals: String = (1i64..=n)
        .map(|k| format!(r#"({k}, '{{"category":"c","score":{k}}}')"#))
        .collect::<Vec<_>>()
        .join(",");
    exec(sess, &format!("INSERT INTO {table} (id, payload) VALUES {vals}")).await;
}

/// `(id BIGINT PRIMARY KEY, v BIGINT NOT NULL, status TEXT NOT NULL)` with `n`
/// rows `(i, i*10, "old")`.
async fn seed_status(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, status TEXT NOT NULL)"
        ),
    )
    .await;
    let vals: String = (1i64..=n)
        .map(|k| format!("({k}, {}, 'old')", k * 10))
        .collect::<Vec<_>>()
        .join(",");
    exec(
        sess,
        &format!("INSERT INTO {table} (id, v, status) VALUES {vals}"),
    )
    .await;
}

// ── 1. jsonb_set over a PK range routes to the overlay ──────────────────────

/// `UPDATE events SET payload = jsonb_set(payload, '{score}', '999') WHERE id < 11`
/// (10 matching rows) takes the fast path: the registry holds 10 Update entries,
/// every matched row's nested value is rewritten, non-matching rows untouched,
/// and COUNT(*) is stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_set_pk_range_routes_fast_path() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("events").unwrap();
    seed_events(&sess, "events", 50).await;

    exec(
        &sess,
        r#"UPDATE events SET payload = jsonb_set(payload, '{score}', '999') WHERE id < 11"#,
    )
    .await;

    // Fast path fired: one Update overlay entry per matched PK (1..=10).
    assert_eq!(
        registry_update_count(&eng, &project, &table),
        10,
        "10-row jsonb_set range UPDATE must write 10 overlay entries (probe fast path)"
    );
    for pk in 1..=10 {
        assert!(
            registry_has_update(&eng, &project, &table, pk),
            "overlay must hold Update for id={pk}"
        );
    }

    // Values correct on matched rows, untouched elsewhere.
    for pk in 1..=10 {
        let doc = payload_for_id(&sess, "events", pk).await;
        assert_eq!(doc["score"], serde_json::json!(999), "id={pk} score rewritten");
        assert_eq!(doc["category"], serde_json::json!("c"), "category preserved");
    }
    let untouched = payload_for_id(&sess, "events", 11).await;
    assert_eq!(untouched["score"], serde_json::json!(11), "id=11 untouched");

    // COUNT stable — UPDATE never changes cardinality.
    assert_eq!(count_all(&sess, "events").await, 50, "COUNT stable after UPDATE");
}

// ── 2. CASE conditional over a PK range ─────────────────────────────────────

/// `UPDATE t SET status = CASE WHEN v > 50 THEN 'hi' ELSE 'lo' END WHERE id < 10`
/// routes to the overlay and applies the per-row conditional correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn case_conditional_pk_range_routes_fast_path() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 30).await;

    exec(
        &sess,
        "UPDATE t SET status = CASE WHEN v > 50 THEN 'hi' ELSE 'lo' END WHERE id < 10",
    )
    .await;

    // 9 matched rows (id 1..=9) → 9 overlay entries.
    assert_eq!(
        registry_update_count(&eng, &project, &table),
        9,
        "CASE range UPDATE must write 9 overlay entries"
    );

    // id=5 has v=50 → not > 50 → 'lo'; id=6 has v=60 → 'hi'.
    assert_eq!(fetch_text(&sess, "t", "status", 5).await.as_deref(), Some("lo"));
    assert_eq!(fetch_text(&sess, "t", "status", 6).await.as_deref(), Some("hi"));
    // id=10 outside WHERE → untouched.
    assert_eq!(fetch_text(&sess, "t", "status", 10).await.as_deref(), Some("old"));
    assert_eq!(count_all(&sess, "t").await, 30, "COUNT stable");
}

// ── 3. Non-PK predicate routes via probe ────────────────────────────────────

/// `UPDATE t SET v = 7777 WHERE status = 'flag'` where the probe-evaluated
/// predicate matches 5 rows → fast path, correct values. The predicate is fully
/// consumed by the probe (it returns the matching PKs), so no post-probe
/// residual re-evaluation is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_pk_predicate_routes_via_probe() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 30).await;

    // Mark 5 rows (id 3,6,9,12,15) with status='flag' via the existing fast
    // path (single-row PK-eq), then bulk-update by the non-PK predicate.
    for pk in [3, 6, 9, 12, 15] {
        exec(&sess, &format!("UPDATE t SET status = 'flag' WHERE id = {pk}")).await;
    }

    exec(&sess, "UPDATE t SET v = 7777 WHERE status = 'flag'").await;

    // All 5 flagged rows now carry v=7777 (overlay entries present).
    for pk in [3, 6, 9, 12, 15] {
        assert!(
            registry_has_update(&eng, &project, &table, pk),
            "id={pk} must have an overlay Update after non-PK probe UPDATE"
        );
        assert_eq!(fetch_i64(&sess, "t", "v", pk).await, Some(7777), "id={pk} v rewritten");
    }
    // A non-flagged row keeps its seeded value.
    assert_eq!(fetch_i64(&sess, "t", "v", 4).await, Some(40), "id=4 untouched");
    assert_eq!(count_all(&sess, "t").await, 30, "COUNT stable");
}

// ── 4. >cap matches fall to cold CoW ────────────────────────────────────────

/// A WHERE matching more than the delta cap must fall to the cold
/// copy-on-write path: NO overlay entries, a NEW catalog snapshot (the cold
/// rewrite commits replacement files), and values still correct. The cap is
/// pinned to 64 via `BASIN_DELTA_UPDATE_MAX_KEYS` (default is 10 000) so the
/// boundary is exercised with a small seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_cap_falls_to_cold() {
    let _env = ENV_LOCK.write().await;
    let prev = std::env::var("BASIN_DELTA_UPDATE_MAX_KEYS").ok();
    std::env::set_var("BASIN_DELTA_UPDATE_MAX_KEYS", "64");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 200).await;
    let before = cold_tier_state(&eng, &project, &table).await;

    // id <= 65 → 65 rows, exceeds the pinned cap of 64 → cold path.
    exec(&sess, "UPDATE t SET v = 1 WHERE id <= 65").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "65 matches exceed the delta cap; UPDATE must fall to cold CoW (no overlay)"
    );
    let after = cold_tier_state(&eng, &project, &table).await;
    assert_ne!(
        before, after,
        "cold CoW must commit replacement files (snapshot/file set must change)"
    );

    // Values still correct via the cold rewrite.
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(1), "id=1 rewritten by cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 65).await, Some(1), "id=65 rewritten by cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 66).await, Some(660), "id=66 untouched");
    assert_eq!(count_all(&sess, "t").await, 200, "COUNT stable");

    restore_env("BASIN_DELTA_UPDATE_MAX_KEYS", prev);
}

/// Exactly the cap (pinned to 64 rows) still takes the fast path — the LIMIT
/// N+1 probe returns 64 keys, which is ≤ cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exactly_cap_routes_fast_path() {
    let _env = ENV_LOCK.write().await;
    let prev = std::env::var("BASIN_DELTA_UPDATE_MAX_KEYS").ok();
    std::env::set_var("BASIN_DELTA_UPDATE_MAX_KEYS", "64");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 200).await;

    // id <= 64 → exactly 64 rows.
    exec(&sess, "UPDATE t SET v = 2 WHERE id <= 64").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        64,
        "exactly cap (64) matches must take the fast path"
    );
    assert_eq!(fetch_i64(&sess, "t", "v", 64).await, Some(2), "id=64 rewritten via overlay");
    assert_eq!(fetch_i64(&sess, "t", "v", 65).await, Some(650), "id=65 untouched");

    restore_env("BASIN_DELTA_UPDATE_MAX_KEYS", prev);
}

// ── 5. Mixed with prior overlays (RMW accumulation across statements) ────────

/// A prior single-row overlay UPDATE is visible to the probe + read-before-
/// write of a subsequent multi-key range UPDATE (overlay > cold precedence),
/// so the second UPDATE's RMW accumulates on top of the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_with_prior_overlay_accumulates() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 20).await;

    // First: single-row overlay UPDATE sets id=3.v = 1000.
    exec(&sess, "UPDATE t SET v = 1000 WHERE id = 3").await;
    assert!(registry_has_update(&eng, &project, &table, 3), "prior overlay present");

    // Then: a multi-key range RMW `v = v + 1` over id < 6. id=3 must read the
    // OVERLAY value (1000), not the stale cold base (30), → 1001.
    exec(&sess, "UPDATE t SET v = v + 1 WHERE id < 6").await;

    assert_eq!(
        fetch_i64(&sess, "t", "v", 3).await,
        Some(1001),
        "range RMW must accumulate on the prior overlay value (1000 + 1)"
    );
    // A row not previously overlaid reads the cold base: id=1 → 10 + 1 = 11.
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(11), "id=1 cold base + 1");
    assert_eq!(count_all(&sess, "t").await, 20, "COUNT stable");
}

// ── 6. jsonb_set on a MISSING path (create-missing) ─────────────────────────

/// `jsonb_set` adding a key that does not yet exist, over a small PK range,
/// routes to the overlay and creates the key on every matched row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonb_set_missing_path_routes_fast_path() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("events").unwrap();
    seed_events(&sess, "events", 20).await;

    // `tier` does not exist in the seeded docs; create_missing defaults true.
    exec(
        &sess,
        r#"UPDATE events SET payload = jsonb_set(payload, '{tier}', '"gold"') WHERE id BETWEEN 2 AND 5"#,
    )
    .await;

    // BETWEEN 2 AND 5 → 4 rows.
    assert_eq!(
        registry_update_count(&eng, &project, &table),
        4,
        "BETWEEN range jsonb_set must write 4 overlay entries"
    );
    for pk in 2..=5 {
        let doc = payload_for_id(&sess, "events", pk).await;
        assert_eq!(doc["tier"], serde_json::json!("gold"), "id={pk} tier created");
        assert_eq!(doc["score"], serde_json::json!(pk), "id={pk} score preserved");
    }
    // Outside the range: no `tier` key.
    let untouched = payload_for_id(&sess, "events", 6).await;
    assert!(untouched.get("tier").is_none(), "id=6 must not gain tier");
}

// ── 7. RETURNING with a multi-key probe UPDATE ──────────────────────────────

/// `UPDATE ... WHERE id < 6 RETURNING id, v` over the probe path returns one
/// projected row per matched key and still writes the overlay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returning_with_multi_key_probe() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 20).await;

    let res = sess
        .execute("UPDATE t SET v = 4242 WHERE id < 6 RETURNING id, v")
        .await
        .expect("UPDATE … RETURNING (multi-key) must succeed");

    // 5 matched rows → 5 overlay entries.
    assert_eq!(
        registry_update_count(&eng, &project, &table),
        5,
        "RETURNING multi-key UPDATE must still write 5 overlay entries"
    );

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => panic!("RETURNING must produce rows"),
    };
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5, "RETURNING must surface one row per matched key");

    // Every returned v must be the post-image value 4242.
    for b in &batches {
        let v = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v Int64");
        for i in 0..b.num_rows() {
            assert_eq!(v.value(i), 4242, "RETURNING v must be the post-image");
        }
    }
}

// ── 8. Non-allowlisted function stays on the cold path ──────────────────────

/// `upper(status)` is deterministic but deliberately NOT on the RMW function
/// allowlist (only `jsonb_set` / `jsonb_insert` / `jsonb_strip_nulls` /
/// `coalesce` are), so a SET with that RHS stays on the cold CoW path even over
/// a small PK range: no overlay entries, but the value is still written.
/// Guards against the closed function allowlist accidentally widening.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_allowlisted_function_stays_cold() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 20).await;

    exec(&sess, "UPDATE t SET status = upper(status) WHERE id < 5").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "upper() is not on the RMW function allowlist; UPDATE must stay cold (no overlay)"
    );
    // Value still written by the cold rewrite.
    assert_eq!(
        fetch_text(&sess, "t", "status", 1).await.as_deref(),
        Some("OLD"),
        "cold path must still apply upper(status)"
    );
    assert_eq!(count_all(&sess, "t").await, 20, "COUNT stable");
}

/// `coalesce` IS on the allowlist: a small-range `SET v = coalesce(v, 0)`
/// routes to the overlay. Confirms the positive direction of the function
/// allowlist (deterministic, Immutable, row-local).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesce_function_routes_fast_path() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 20).await;

    exec(&sess, "UPDATE t SET v = coalesce(v, 0) + 1 WHERE id < 4").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        3,
        "coalesce(...) is allowlisted; small-range UPDATE must take the fast path"
    );
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(11), "coalesce(10,0)+1 = 11");
}

// ── 9. Raised cap: 200-key UPDATE routes delta with zero replacement files ──

/// 200 matched keys — far beyond the historical 64-key cap, well under the
/// 10 000 default — must route through the overlay: 200 `Update` entries, the
/// catalog snapshot and live data-file set UNCHANGED (zero replacement
/// writes), and every value correct (RMW reads the pre-image carried by the
/// probe itself).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_hundred_keys_route_delta_zero_replacement_files() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 300).await;
    let before = cold_tier_state(&eng, &project, &table).await;

    exec(&sess, "UPDATE t SET v = v + 1 WHERE id <= 200").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        200,
        "200-key UPDATE (≤ 10k delta cap) must write one overlay entry per key"
    );
    let after = cold_tier_state(&eng, &project, &table).await;
    assert_eq!(
        before, after,
        "delta UPDATE must commit ZERO replacement files (snapshot + live file set unchanged)"
    );

    // RMW correctness across the whole range + the boundary.
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(11), "10 + 1");
    assert_eq!(fetch_i64(&sess, "t", "v", 137).await, Some(1371), "1370 + 1");
    assert_eq!(fetch_i64(&sess, "t", "v", 200).await, Some(2001), "2000 + 1");
    assert_eq!(fetch_i64(&sess, "t", "v", 201).await, Some(2010), "id=201 untouched");
    assert_eq!(count_all(&sess, "t").await, 300, "COUNT stable");
}

// ── 10. Memory guard: exhausted hard cap declines to cold ───────────────────

/// With the project's memtable budget reserved to the brim, the fast path's
/// post-gather `try_reserve_bytes` reports `HardCapReached` and the UPDATE
/// must DECLINE to the cold CoW path: zero overlay entries, a new catalog
/// snapshot (replacement files), and a correct result.
///
/// The budget is exhausted directly through the registry (the same
/// `try_reserve_bytes` API the engine uses) rather than via a tiny
/// `BASIN_MEMTABLE_HARD_CAP` env, so no engine-construction env race exists.
/// The 200-row single-statement seed stays above the write-through residency
/// row cap (128), so the registry holds NO clean bytes the guard's internal
/// evict-clean could reclaim — the decline is deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_cap_exhausted_falls_cold() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 200).await;
    let before = cold_tier_state(&eng, &project, &table).await;

    // Exhaust this project's budget: top the counter up to exactly the hard
    // cap (relative to whatever the engine already holds), so any further
    // reservation (the UPDATE's staged override bytes) must be refused.
    let registry = eng.memtable_registry();
    let hard_cap = registry.config().project_hard_cap_bytes;
    let fill = hard_cap - registry.project_bytes(&project);
    let _ = registry.try_reserve_bytes(&project, fill);
    assert_eq!(
        registry.project_bytes(&project),
        hard_cap,
        "budget must be reserved to exactly the hard cap"
    );

    exec(&sess, "UPDATE t SET v = v + 1 WHERE id <= 50").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "HardCapReached must decline the overlay write — no Update entries"
    );
    let after = cold_tier_state(&eng, &project, &table).await;
    assert_ne!(
        before, after,
        "the declined UPDATE must have run as a cold CoW (replacement files committed)"
    );

    // Result correct via the cold rewrite.
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(11), "10 + 1 via cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 50).await, Some(501), "500 + 1 via cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 51).await, Some(510), "id=51 untouched");
    assert_eq!(count_all(&sess, "t").await, 200, "COUNT stable");

    // Hygiene: hand the synthetic reservation back.
    registry.release_bytes(&project, fill);
}

// ── 11. No-WHERE UPDATE: small table routes delta, identical to cold twin ───

/// `UPDATE t SET v = v + 7` (no WHERE) on a 40-row table routes through the
/// overlay (40 Update entries, zero replacement files) and its result is
/// byte-identical to the SAME statement forced down the cold path
/// (`BASIN_HOTTIER_FASTPATH_DISABLE=1`) on an identically seeded twin engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_where_small_table_routes_delta_matches_cold_twin() {
    let _env = ENV_LOCK.write().await;
    // Pin the fast path ON for the delta engine even under an inherited env.
    let prev_kill = std::env::var("BASIN_HOTTIER_FASTPATH_DISABLE").ok();
    std::env::remove_var("BASIN_HOTTIER_FASTPATH_DISABLE");

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let eng_a = engine_in(&dir_a);
    let eng_b = engine_in(&dir_b);
    let sess_a = open(&eng_a).await;
    let sess_b = open(&eng_b).await;
    let project_a = sess_a.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess_a, "t", 40).await;
    seed_status(&sess_b, "t", 40).await;
    let before_a = cold_tier_state(&eng_a, &project_a, &table).await;

    // Delta engine: fast path on.
    exec(&sess_a, "UPDATE t SET v = v + 7").await;
    assert_eq!(
        registry_update_count(&eng_a, &project_a, &table),
        40,
        "no-WHERE UPDATE on a small table must route every row through the overlay"
    );
    let after_a = cold_tier_state(&eng_a, &project_a, &table).await;
    assert_eq!(
        before_a, after_a,
        "no-WHERE delta UPDATE must commit zero replacement files"
    );

    // PK-omitting fast-path reads WITH the overlay live. `SELECT SUM(v)` /
    // `SELECT v FROM t` build a minimal read set ({v}) that does not include
    // the PK — yet the merge-on-read suppression keys on the PK, so the read
    // layer must fetch it or every overridden cold row survives AND its
    // override row is appended (SUM = old_sum + new_sum; scans return 80
    // rows). The full-scan compare further down does NOT cover this class:
    // `... ORDER BY id` without LIMIT routes to DataFusion (whose scan
    // augments the projection), and it projects the PK anyway.
    let expected_sum: i64 = (1..=40i64).map(|k| k * 10 + 7).sum();
    assert_eq!(
        int_scalar(&sess_a, "SELECT SUM(v) FROM t").await,
        expected_sum,
        "SUM(v) over a live overlay must suppress the overridden cold rows"
    );
    assert_eq!(
        int_scalar(&sess_a, "SELECT COUNT(*) FROM t").await,
        40,
        "COUNT(*) over a live overlay must stay at the row count"
    );
    let mut vs = i64_column(&sess_a, "SELECT v FROM t").await;
    vs.sort_unstable();
    assert_eq!(
        vs,
        (1..=40i64).map(|k| k * 10 + 7).collect::<Vec<_>>(),
        "a non-PK projection scan must return exactly the post-UPDATE values"
    );

    // Cold twin: kill switch forces the historical cold CoW route.
    std::env::set_var("BASIN_HOTTIER_FASTPATH_DISABLE", "1");
    exec(&sess_b, "UPDATE t SET v = v + 7").await;
    restore_env("BASIN_HOTTIER_FASTPATH_DISABLE", prev_kill);
    assert_eq!(
        registry_update_count(&eng_b, &sess_b.project(), &table),
        0,
        "kill switch must keep the twin on the cold path (no overlay)"
    );

    // Row-for-row identical results.
    let rows_a = all_status_rows(&sess_a, "t").await;
    let rows_b = all_status_rows(&sess_b, "t").await;
    assert_eq!(rows_a.len(), 40, "twin tables must keep all 40 rows");
    assert_eq!(
        rows_a, rows_b,
        "no-WHERE delta UPDATE must be byte-identical to the cold twin"
    );
}

// ── 12. No-WHERE UPDATE on a >cap table falls to cold ───────────────────────

/// A no-WHERE UPDATE whose table holds MORE live rows than the delta cap must
/// fall to the cold CoW path exactly as before the routing change: zero
/// overlay entries, replacement files committed, every row updated. The cap is
/// pinned to 25 via `BASIN_DELTA_UPDATE_MAX_KEYS` so a 30-row table is "big".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_where_over_cap_falls_to_cold() {
    let _env = ENV_LOCK.write().await;
    let prev = std::env::var("BASIN_DELTA_UPDATE_MAX_KEYS").ok();
    std::env::set_var("BASIN_DELTA_UPDATE_MAX_KEYS", "25");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 30).await;
    let before = cold_tier_state(&eng, &project, &table).await;

    exec(&sess, "UPDATE t SET v = 1").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "30 rows exceed the pinned cap of 25; the no-WHERE UPDATE must go cold"
    );
    let after = cold_tier_state(&eng, &project, &table).await;
    assert_ne!(before, after, "cold CoW must commit replacement files");
    for pk in [1, 15, 30] {
        assert_eq!(fetch_i64(&sess, "t", "v", pk).await, Some(1), "id={pk} rewritten");
    }
    assert_eq!(count_all(&sess, "t").await, 30, "COUNT stable");

    restore_env("BASIN_DELTA_UPDATE_MAX_KEYS", prev);
}

// ── 13. UPDATE 0 on an empty table ───────────────────────────────────────────

/// `UPDATE t SET v = 1` (no WHERE) and a WHERE variant on an EMPTY table must
/// both report `UPDATE 0`, error-free, with zero overlay entries — the probe
/// resolves zero matched keys and that is a valid fast-path outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_zero_on_empty_table() {
    let _env = ENV_LOCK.read().await;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, status TEXT NOT NULL)",
    )
    .await;

    let res = sess
        .execute("UPDATE t SET v = 1")
        .await
        .expect("no-WHERE UPDATE on an empty table must not error");
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 0"),
        "no-WHERE UPDATE on empty table must report UPDATE 0, got {res:?}"
    );

    let res = sess
        .execute("UPDATE t SET v = 1 WHERE v = 99")
        .await
        .expect("WHERE UPDATE on an empty table must not error");
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 0"),
        "WHERE UPDATE on empty table must report UPDATE 0, got {res:?}"
    );

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "UPDATE 0 must leave no overlay entries"
    );
    assert_eq!(count_all(&sess, "t").await, 0, "table stays empty");
}
