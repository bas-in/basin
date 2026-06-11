//! Gate matrix tests: prove that each guard in `dml_mutate.rs` routes DELETE
//! and UPDATE to the slow CoW path when its condition holds.
//!
//! ## How we detect "slow path fired"
//!
//! When the **fastpath** fires, `dml_mutate.rs` writes a `MemRowValue::Tombstone`
//! (DELETE) or `MemRowValue::Update` (UPDATE) into the process-wide
//! `MemTableRegistry`. When the **slow CoW path** fires, the registry is
//! NOT written — the rewrite happens directly on cold-tier Parquet files.
//!
//! Detection strategy: after the mutation, probe
//! `engine.memtable_registry().get(project, table)` and check whether the
//! registry entry holds any Tombstone or Update entries for the mutated PK.
//! If the entry is empty (or the slot for the PK is absent / contains only a
//! `Row` value), the slow path ran.
//!
//! ## Gates tested (one test per gate)
//!
//! 1. `tx_active`          — inside BEGIN/COMMIT, fastpath must NOT fire.
//! 2. `returning_present`  — DELETE/UPDATE … RETURNING * goes slow.
//! 3. `multi_col_pk`       — composite PK goes slow.
//! 4. `rls_enabled`        — RLS goes slow.
//! 5. `soft_delete_column` — soft-delete column goes slow.
//! 6. `audit_table`        — AUDIT TO … goes slow.
//! 7. `update_reactor`     — UPDATE reactor registered → slow.
//! 8. `predicate_not_pk_eq`— non-PK WHERE predicate → slow.
//!
//! All tests also perform a functional SELECT after the mutation to confirm
//! correctness on the slow path.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use basin_catalog::{InMemoryCatalog, ReactorDef, ReactorOps};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_hottier::{MemRowValue, RowKey};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Env isolation ─────────────────────────────────────────────────────────────

/// Process-wide mutex so env-var flips are serialised. Held across every await
/// so parallel tests in this binary never observe a half-configured gate.
static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(v) => unsafe { std::env::set_var(self.key, v) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

/// Enable `key=1` for the test duration; restore prior value on drop.
fn set_env(key: &'static str) -> EnvGuard {
    let prev = std::env::var(key).ok();
    unsafe { std::env::set_var(key, "1") };
    EnvGuard { key, prev }
}

// ── Engine helper ─────────────────────────────────────────────────────────────

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

// ── Registry probe helpers ────────────────────────────────────────────────────

/// Build the `RowKey` for a BIGINT PK value.
fn row_key_i64(v: i64) -> RowKey {
    RowKey::builder().append_i64(v).finish()
}

/// True iff the registry entry for `(project, table)` holds a `Tombstone`
/// for `pk`.
fn registry_has_tombstone(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    pk: i64,
) -> bool {
    let Some(entry) = engine.memtable_registry().get(project, table) else {
        return false;
    };
    let key = row_key_i64(pk);
    matches!(entry.memtable.get(&key), Some(MemRowValue::Tombstone))
}

/// True iff the registry entry for `(project, table)` holds an `Update`
/// for `pk`.
fn registry_has_update(
    engine: &Engine,
    project: &ProjectId,
    table: &TableName,
    pk: i64,
) -> bool {
    let Some(entry) = engine.memtable_registry().get(project, table) else {
        return false;
    };
    let key = row_key_i64(pk);
    matches!(entry.memtable.get(&key), Some(MemRowValue::Update { .. }))
}

// ── Seed helper ───────────────────────────────────────────────────────────────

/// Seed `(id BIGINT PRIMARY KEY, v BIGINT)` with `n` rows `(i, i * 10)`.
///
/// Uses a single multi-row VALUES INSERT so all rows land in one committed
/// Vortex file — avoids file-count fragmentation that can confuse the
/// slow-path CoW reader when a DELETE inside a transaction reads cold files.
async fn seed(sess: &ProjectSession, table: &str, n: i64) {
    exec(
        sess,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL)"),
    )
    .await;
    let vals: String = (1i64..=n)
        .map(|k| format!("({k}, {})", k * 10))
        .collect::<Vec<_>>()
        .join(",");
    exec(
        sess,
        &format!("INSERT INTO {table} (id, v) VALUES {vals}"),
    )
    .await;
}

/// `SELECT COUNT(*) FROM t` → i64.
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

/// `SELECT v FROM t WHERE id = pk` → Some(value) or None.
async fn fetch_v(sess: &ProjectSession, table: &str, pk: i64) -> Option<i64> {
    let sql = format!("SELECT v FROM {table} WHERE id = {pk}");
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
                    .expect("v must be Int64");
                return Some(arr.value(0));
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

// ── Gate 1: tx_active ────────────────────────────────────────────────────────

/// Inside BEGIN/COMMIT, DELETE/UPDATE must NOT write a tombstone/update to
/// the memtable. After ROLLBACK the original row must be visible (proves the
/// slow path ran — registry tombstones can't be rolled back).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_tx_active_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    exec(&sess, "BEGIN").await;
    exec(&sess, "DELETE FROM t WHERE id = 3").await;

    // Inside the tx: the registry must NOT hold a tombstone for pk=3 because
    // the fastpath is gated off by tx_active.
    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 3);
    assert!(
        !has_tombstone,
        "registry must NOT hold a tombstone while inside BEGIN/COMMIT; \
         if it does, the tx_active gate regressed — a ROLLBACK would leak the tombstone"
    );

    // Rollback: the row must be visible again (slow path).
    exec(&sess, "ROLLBACK").await;
    let n_after_rollback = fetch_v(&sess, "t", 3).await;
    assert_eq!(
        n_after_rollback,
        Some(30),
        "DELETE inside BEGIN/ROLLBACK must not remove the row; \
         the row must be visible with its original value after ROLLBACK"
    );
}

/// Inside BEGIN/COMMIT, UPDATE must NOT write to the registry. After ROLLBACK
/// the original value is restored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_tx_active_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    exec(&sess, "BEGIN").await;
    exec(&sess, "UPDATE t SET v = 99999 WHERE id = 2").await;

    let has_update = registry_has_update(&eng, &project, &table, 2);
    assert!(
        !has_update,
        "registry must NOT hold an Update entry while inside BEGIN/COMMIT; \
         the tx_active gate must be blocking the fastpath"
    );

    exec(&sess, "ROLLBACK").await;
    let v = fetch_v(&sess, "t", 2).await;
    assert_eq!(
        v,
        Some(20),
        "UPDATE inside BEGIN/ROLLBACK must restore the original value; \
         got {v:?}"
    );
}

// ── Gate 2: returning_present ─────────────────────────────────────────────────

/// `DELETE … RETURNING *` must go through slow CoW (returns the deleted row).
/// No tombstone in registry; the returned row content proves the slow path ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_returning_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    let res = sess
        .execute("DELETE FROM t WHERE id = 4 RETURNING id, v")
        .await
        .expect("DELETE RETURNING must succeed");

    // Registry must NOT have a tombstone — slow CoW path ran.
    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 4);
    assert!(
        !has_tombstone,
        "registry must NOT hold a tombstone after DELETE … RETURNING; \
         the returning_present gate must block the fastpath"
    );

    // The returned content proves the slow path projected the row.
    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => panic!("DELETE RETURNING must return rows"),
    };
    let total_returned: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_returned, 1,
        "DELETE RETURNING must return exactly 1 row; got {total_returned}"
    );

    // Functional check: the row is gone after the DELETE.
    let n = fetch_v(&sess, "t", 4).await;
    assert_eq!(
        n, None,
        "deleted row must not be visible after DELETE RETURNING"
    );
}

/// RETURNING gate split: plain column lists ride the fastpath (the post-image
/// is already in hand — see `returning_is_fast_path_eligible`), while
/// expression RETURNING still requires the slow CoW path's DataFusion context.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_returning_update_plain_fast_expression_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    let res = sess
        .execute("UPDATE t SET v = 5555 WHERE id = 1 RETURNING id, v")
        .await
        .expect("UPDATE RETURNING must succeed");

    let has_update = registry_has_update(&eng, &project, &table, 1);
    assert!(
        has_update,
        "plain-column RETURNING must take the fastpath (Update registry entry)"
    );

    // Expression RETURNING stays on the slow path: no registry override.
    sess.execute("UPDATE t SET v = 7 WHERE id = 2 RETURNING v + 1")
        .await
        .expect("expression RETURNING must succeed via CoW");
    assert!(
        !registry_has_update(&eng, &project, &table, 2),
        "expression RETURNING must NOT take the fastpath"
    );
    let v2 = fetch_v(&sess, "t", 2).await;
    assert_eq!(v2, Some(7), "expression-RETURNING UPDATE must apply the value");

    let batches = match res {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => panic!("UPDATE RETURNING must return rows"),
    };
    let total_returned: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_returned, 1, "UPDATE RETURNING must return 1 row");

    // Functional: updated value visible.
    let v = fetch_v(&sess, "t", 1).await;
    assert_eq!(v, Some(5555), "UPDATE RETURNING must apply the new value");
}

// ── Gate 3: multi_col_pk ──────────────────────────────────────────────────────

/// Table with `PRIMARY KEY (a, b)` — DELETE/UPDATE must not use fastpath.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_multi_col_pk_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("mc").unwrap();

    exec(
        &sess,
        "CREATE TABLE mc (a BIGINT, b BIGINT, v BIGINT, PRIMARY KEY (a, b))",
    )
    .await;
    for k in 1i64..=5 {
        exec(&sess, &format!("INSERT INTO mc (a, b, v) VALUES ({k}, {k}, {})", k * 10)).await;
    }

    exec(&sess, "DELETE FROM mc WHERE a = 2 AND b = 2").await;

    // Multi-col PK → no RowKey encoding for composite keys → slow path.
    // The registry entry should not exist or have no tombstone.
    let entry = eng.memtable_registry().get(&project, &table);
    if let Some(e) = entry {
        let snap = e.memtable.snapshot();
        let has_any_tombstone = snap
            .iter()
            .any(|(_, v)| matches!(v, MemRowValue::Tombstone));
        assert!(
            !has_any_tombstone,
            "multi-col PK DELETE must not write any tombstones to the registry; \
             the multi_col_pk gate must block the fastpath"
        );
    }

    // Functional: the row is gone.
    let n = count_all(&sess, "mc").await;
    assert_eq!(n, 4, "DELETE on multi-col PK must remove the row; count={n}");
}

/// Table with composite PK — UPDATE must go slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_multi_col_pk_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("mc").unwrap();

    exec(
        &sess,
        "CREATE TABLE mc (a BIGINT, b BIGINT, v BIGINT, PRIMARY KEY (a, b))",
    )
    .await;
    for k in 1i64..=5 {
        exec(&sess, &format!("INSERT INTO mc (a, b, v) VALUES ({k}, {k}, {})", k * 10)).await;
    }

    exec(&sess, "UPDATE mc SET v = 9999 WHERE a = 3 AND b = 3").await;

    let entry = eng.memtable_registry().get(&project, &table);
    if let Some(e) = entry {
        let snap = e.memtable.snapshot();
        let has_any_update = snap
            .iter()
            .any(|(_, v)| matches!(v, MemRowValue::Update { .. }));
        assert!(
            !has_any_update,
            "multi-col PK UPDATE must not write any Update entries to the registry"
        );
    }

    // Functional: new value visible.
    let sql = "SELECT v FROM mc WHERE a = 3 AND b = 3";
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), 9999, "UPDATE on composite PK must apply");
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}

// ── Gate 4: rls_enabled ───────────────────────────────────────────────────────

/// RLS-enabled table → fastpath skipped for DELETE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_rls_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    exec(&sess, "ALTER TABLE t ENABLE ROW LEVEL SECURITY").await;
    // Permissive policy: allow all rows where id > 0 (always true for our data).
    exec(
        &sess,
        "CREATE POLICY allow_all ON t FOR ALL TO PUBLIC USING (id > 0)",
    )
    .await;

    exec(&sess, "DELETE FROM t WHERE id = 2").await;

    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 2);
    assert!(
        !has_tombstone,
        "RLS-enabled table DELETE must not write a tombstone; \
         the rls_enabled gate must block the fastpath"
    );

    // Functional: row deleted.
    let n = count_all(&sess, "t").await;
    assert_eq!(n, 4, "DELETE on RLS table must still remove the row");
}

/// RLS-enabled table → fastpath skipped for UPDATE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_rls_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    exec(&sess, "ALTER TABLE t ENABLE ROW LEVEL SECURITY").await;
    // Permissive policy: allow all rows where id > 0 (always true for our data).
    exec(
        &sess,
        "CREATE POLICY allow_all ON t FOR ALL TO PUBLIC USING (id > 0)",
    )
    .await;

    exec(&sess, "UPDATE t SET v = 1234 WHERE id = 1").await;

    let has_update = registry_has_update(&eng, &project, &table, 1);
    assert!(
        !has_update,
        "RLS-enabled table UPDATE must not write an Update entry; \
         the rls_enabled gate must block the fastpath"
    );

    let v = fetch_v(&sess, "t", 1).await;
    assert_eq!(v, Some(1234), "UPDATE on RLS table must still apply");
}

// ── Gate 5: soft_delete_column ────────────────────────────────────────────────

/// Table with a SOFT DELETE column → fastpath skipped.
///
/// The `SOFT DELETE` column attribute is declared inline in CREATE TABLE as:
///   `deleted_at TIMESTAMPTZ SOFT DELETE`
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_soft_delete_column_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, \
         deleted_at TIMESTAMPTZ SOFT DELETE)",
    )
    .await;
    for k in 1i64..=5 {
        exec(
            &sess,
            &format!("INSERT INTO t (id, v) VALUES ({k}, {})", k * 10),
        )
        .await;
    }

    exec(&sess, "DELETE FROM t WHERE id = 3").await;

    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 3);
    assert!(
        !has_tombstone,
        "soft-delete table DELETE must not write a tombstone; \
         the soft_delete_column gate must block the fastpath and go through \
         exec_soft_delete (which stamps deleted_at = now())"
    );

    // Functional: soft-DELETE stamps deleted_at; row is not visible to normal SELECT.
    let n = count_all(&sess, "t").await;
    assert_eq!(n, 4, "soft-DELETE must hide the row from COUNT(*); got {n}");
}

/// Soft-delete table → UPDATE fastpath skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_soft_delete_column_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, \
         deleted_at TIMESTAMPTZ SOFT DELETE)",
    )
    .await;
    for k in 1i64..=5 {
        exec(
            &sess,
            &format!("INSERT INTO t (id, v) VALUES ({k}, {})", k * 10),
        )
        .await;
    }

    exec(&sess, "UPDATE t SET v = 8888 WHERE id = 2").await;

    let has_update = registry_has_update(&eng, &project, &table, 2);
    assert!(
        !has_update,
        "soft-delete table UPDATE must not write an Update entry; \
         the soft_delete_column gate must block the fastpath"
    );

    let v = fetch_v(&sess, "t", 2).await;
    assert_eq!(v, Some(8888), "UPDATE on soft-delete table must still apply");
}

// ── Gate 6: audit_table ───────────────────────────────────────────────────────

/// Table with `AUDIT TO <name>` → fastpath skipped for DELETE.
///
/// `AUDIT TO <ident>` is a trailing clause in CREATE TABLE.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_audit_table_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) AUDIT TO t_audit",
    )
    .await;
    for k in 1i64..=5 {
        exec(
            &sess,
            &format!("INSERT INTO t (id, v) VALUES ({k}, {})", k * 10),
        )
        .await;
    }

    exec(&sess, "DELETE FROM t WHERE id = 1").await;

    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 1);
    assert!(
        !has_tombstone,
        "audit-table DELETE must not write a tombstone; \
         the audit_table gate must block the fastpath so audit rows are written"
    );

    // Functional: row is gone.
    let n = count_all(&sess, "t").await;
    assert_eq!(n, 4, "DELETE on audit table must remove the row; count={n}");
}

/// Audit table → UPDATE fastpath skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_audit_table_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();

    exec(
        &sess,
        "CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) AUDIT TO t_audit",
    )
    .await;
    for k in 1i64..=5 {
        exec(
            &sess,
            &format!("INSERT INTO t (id, v) VALUES ({k}, {})", k * 10),
        )
        .await;
    }

    exec(&sess, "UPDATE t SET v = 7777 WHERE id = 4").await;

    let has_update = registry_has_update(&eng, &project, &table, 4);
    assert!(
        !has_update,
        "audit-table UPDATE must not write an Update entry; \
         the audit_table gate must block the fastpath"
    );

    let v = fetch_v(&sess, "t", 4).await;
    assert_eq!(v, Some(7777), "UPDATE on audit table must still apply");
}

// ── Gate 7: update_reactor ────────────────────────────────────────────────────

/// Table with a configured UPDATE reactor → fastpath skipped.
///
/// We register the reactor directly via the catalog API (same as the engine
/// does when `ALTER TABLE … REACT ON UPDATE …` is executed) so there is no
/// dependency on the SQL surface for reactor registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_update_reactor_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    // Register a DELETE reactor directly on the catalog.
    // The body uses SELECT 1 (a no-op that requires no target table) so the
    // reactor fires and returns without a side-effect table dependency.
    let def = ReactorDef {
        project,
        table: table.clone(),
        name: "del_reactor".to_string(),
        ops: ReactorOps::DELETE,
        when_predicate: None,
        body: "SELECT 1".to_string(),
    };
    eng.config()
        .catalog
        .register_reactor(def)
        .await
        .expect("register_reactor must succeed");

    exec(&sess, "DELETE FROM t WHERE id = 5").await;

    let has_tombstone = registry_has_tombstone(&eng, &project, &table, 5);
    assert!(
        !has_tombstone,
        "DELETE with a registered DELETE reactor must not write a tombstone; \
         the update_reactor gate must route to the slow CoW path"
    );

    let n = count_all(&sess, "t").await;
    assert_eq!(n, 4, "DELETE with reactor must still remove the row; count={n}");
}

/// Table with an UPDATE reactor → UPDATE fastpath skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_update_reactor_update_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 5).await;

    let def = ReactorDef {
        project,
        table: table.clone(),
        name: "upd_reactor".to_string(),
        ops: ReactorOps::UPDATE,
        when_predicate: None,
        body: "SELECT 1".to_string(),
    };
    eng.config()
        .catalog
        .register_reactor(def)
        .await
        .expect("register_reactor must succeed");

    exec(&sess, "UPDATE t SET v = 6666 WHERE id = 3").await;

    let has_update = registry_has_update(&eng, &project, &table, 3);
    assert!(
        !has_update,
        "UPDATE with a registered UPDATE reactor must not write an Update entry; \
         the update_reactor gate must route to the slow CoW path"
    );

    let v = fetch_v(&sess, "t", 3).await;
    assert_eq!(v, Some(6666), "UPDATE with reactor must still apply the value");
}

// ── Gate 8: predicate_not_pk_eq ───────────────────────────────────────────────

/// `DELETE … WHERE other_col = 5` (non-PK predicate) must go slow path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_predicate_not_pk_eq_delete_goes_slow() {
    let _g = ENV_LOCK.lock().await;
    let _del = set_env("BASIN_HOTTIER_DELETE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 10).await;

    // WHERE predicate on non-PK column.
    exec(&sess, "DELETE FROM t WHERE v = 50").await;

    // Registry must have no tombstones (slow path ran).
    let entry = eng.memtable_registry().get(&project, &table);
    if let Some(e) = entry {
        let snap = e.memtable.snapshot();
        let has_any_tombstone = snap
            .iter()
            .any(|(_, v)| matches!(v, MemRowValue::Tombstone));
        assert!(
            !has_any_tombstone,
            "non-PK DELETE must not write any tombstones; \
             predicate_not_pk_eq gate must block the fastpath"
        );
    }

    // Functional: the row with v=50 (id=5) is gone.
    let n = count_all(&sess, "t").await;
    assert_eq!(n, 9, "DELETE WHERE v=50 must remove 1 row; count={n}");
}

/// Count `MemRowValue::Update` entries in the registry for `table`.
fn registry_update_count(eng: &Engine, project: &ProjectId, table: &TableName) -> usize {
    match eng.memtable_registry().get(project, table) {
        Some(e) => e
            .memtable
            .snapshot()
            .iter()
            .filter(|(_, v)| matches!(v, MemRowValue::Update { .. }))
            .count(),
        None => 0,
    }
}

/// `UPDATE … WHERE other_col = 20` (non-PK predicate) now routes the matched
/// SMALL set (≤ SMALL_BULK_FASTPATH_CAP) through the hot-tier overlay via the
/// resolve-by-probe path (the probe evaluates the full predicate, returns the
/// matching PK, and the overlay write applies to exactly that key). The old
/// "non-PK predicate must go slow" contract is obsolete: a single-row non-PK
/// match must now produce an overlay Update with the correct value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_predicate_not_pk_eq_update_small_set_routes_overlay() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    seed(&sess, "t", 10).await;

    // Non-PK equality predicate matching exactly one row (id=2, v=20).
    exec(&sess, "UPDATE t SET v = 9999 WHERE v = 20").await;

    // NEW contract: the small matched set is routed through the overlay, so
    // exactly one Update entry is written.
    assert_eq!(
        registry_update_count(&eng, &project, &table),
        1,
        "small non-PK UPDATE must write exactly one overlay Update (probe fast path)"
    );

    // Functional: the row with v=20 (id=2) now has v=9999, read back through
    // the overlay-aware read path.
    let v = fetch_v(&sess, "t", 2).await;
    assert_eq!(v, Some(9999), "UPDATE WHERE v=20 must apply to id=2");
    // An unmatched row is untouched.
    assert_eq!(fetch_v(&sess, "t", 1).await, Some(10), "id=1 untouched");
}

/// A non-PK predicate matching MORE than the delta-update key cap must NOT
/// route through the overlay — it falls to the cold copy-on-write path
/// (whole-file rewrite), so the registry holds zero Update entries. The cap
/// default is now 10k (BASIN_DELTA_UPDATE_MAX_KEYS); this gate pins the
/// over-cap behavior, so it pins the cap at the historical 64 to keep its
/// 70-row predicate over the limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_predicate_not_pk_eq_update_large_set_goes_cold() {
    let _g = ENV_LOCK.lock().await;
    let _upd = set_env("BASIN_HOTTIER_UPDATE_FASTPATH");
    let prev_cap = std::env::var("BASIN_DELTA_UPDATE_MAX_KEYS").ok();
    unsafe { std::env::set_var("BASIN_DELTA_UPDATE_MAX_KEYS", "64") };
    let _cap = EnvGuard {
        key: "BASIN_DELTA_UPDATE_MAX_KEYS",
        prev: prev_cap,
    };

    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    let table = TableName::new("t").unwrap();
    // 100 rows: v = id*10, so v ranges 10..=1000.
    seed(&sess, "t", 100).await;

    // Non-PK range predicate matching 70 rows (v in 10..=700 → id 1..=70),
    // which exceeds the 64-row cap → cold CoW, no overlay.
    exec(&sess, "UPDATE t SET v = 0 WHERE v <= 700").await;

    assert_eq!(
        registry_update_count(&eng, &project, &table),
        0,
        ">cap non-PK UPDATE must fall to cold CoW; no overlay Update entries"
    );

    // Functional: the 70 matched rows were rewritten (cold path), the rest are
    // untouched.
    assert_eq!(fetch_v(&sess, "t", 70).await, Some(0), "id=70 rewritten via cold CoW");
    assert_eq!(fetch_v(&sess, "t", 71).await, Some(710), "id=71 untouched");
    assert_eq!(count_all(&sess, "t").await, 100, "COUNT stable");
}
