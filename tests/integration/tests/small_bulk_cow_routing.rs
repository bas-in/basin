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
//!     PROBE — it runs `SELECT <pk> FROM t WHERE <predicate> LIMIT N+1` through
//!     the overlay-aware fast-SELECT machinery; ≤ N matched keys route through
//!     `hot_tier_update_by_pk` (which already takes `&[RowKey]`), N+1 falls to
//!     cold CoW exactly as before.
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
//! `fastpath_gates.rs` uses.
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

/// A WHERE matching more than the small-bulk cap (64) must fall to the cold
/// copy-on-write path: NO overlay entries, but values still correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_cap_falls_to_cold() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed_status(&sess, "t", 200).await;

    // id <= 65 → 65 rows, exceeds the cap of 64 → cold path.
    exec(&sess, "UPDATE t SET v = 1 WHERE id <= 65").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        "65 matches exceed the small-bulk cap; UPDATE must fall to cold CoW (no overlay)"
    );

    // Values still correct via the cold rewrite.
    assert_eq!(fetch_i64(&sess, "t", "v", 1).await, Some(1), "id=1 rewritten by cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 65).await, Some(1), "id=65 rewritten by cold path");
    assert_eq!(fetch_i64(&sess, "t", "v", 66).await, Some(660), "id=66 untouched");
    assert_eq!(count_all(&sess, "t").await, 200, "COUNT stable");
}

/// Exactly the cap (64 rows) still takes the fast path — the LIMIT N+1 probe
/// returns 64 keys, which is ≤ cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exactly_cap_routes_fast_path() {
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
}

// ── 5. Mixed with prior overlays (RMW accumulation across statements) ────────

/// A prior single-row overlay UPDATE is visible to the probe + read-before-
/// write of a subsequent multi-key range UPDATE (overlay > cold precedence),
/// so the second UPDATE's RMW accumulates on top of the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_with_prior_overlay_accumulates() {
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
