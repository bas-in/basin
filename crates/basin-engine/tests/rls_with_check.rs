//! BUG #133: Row-Level Security `WITH CHECK` / `USING` enforcement on
//! INSERT and UPDATE.
//!
//! Before the fix, `CREATE POLICY ... WITH CHECK (expr)` predicates were
//! parsed and stored but never consulted on writes — a row violating the
//! policy was silently persisted (silent data leak). PostgreSQL rejects it
//! with SQLSTATE `42501` and message
//! `new row violates row-level security policy for table "<t>"`.
//!
//! Each test opens a `ProjectSession` and runs SQL through the same
//! `execute()` entry point a pgwire connection would hit. We assert:
//!
//! - WITH CHECK true  → INSERT ok.
//! - WITH CHECK false → 42501 (`BasinError::RlsViolation`).
//! - UPDATE moving a row OUT of WITH CHECK → 42501.
//! - UPDATE keeping the row valid → ok.
//! - Table without RLS enabled → policy is inert (write unaffected).
//! - WITH CHECK absent → USING is used as the write predicate (PG fallback).
//! - Multiple PERMISSIVE policies combine with OR.
//! - RLS enabled with no applicable policy → default-deny on write.
//!
//! Role/owner simplification: basin has no table-owner / role-membership
//! model, so WITH CHECK is enforced for every writer (equivalent to PG's
//! `FORCE ROW LEVEL SECURITY` for all roles) — the fail-closed direction.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

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

async fn admin(eng: &Engine, p: ProjectId) -> ProjectSession {
    eng.open_session(p).await.unwrap()
}

/// Assert an error is the RLS 42501-class violation.
fn assert_rls_violation(err: &BasinError, ctx: &str) {
    match err {
        BasinError::RlsViolation(msg) => {
            assert!(
                msg.contains("violates row-level security policy"),
                "{ctx}: unexpected RLS message: {msg}"
            );
        }
        other => panic!("{ctx}: expected RlsViolation (42501), got {other:?}"),
    }
}

// 1. WITH CHECK true → INSERT ok; WITH CHECK false → 42501. -----------------

#[tokio::test]
async fn insert_with_check_true_ok_false_rejected() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE orders (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    s.execute(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await
    .unwrap();

    // Satisfies WITH CHECK (amount > 0): allowed.
    s.execute("INSERT INTO orders VALUES (1, 10)")
        .await
        .expect("amount>0 must satisfy WITH CHECK");

    // Violates WITH CHECK: must be rejected with 42501.
    let err = s
        .execute("INSERT INTO orders VALUES (2, -5)")
        .await
        .expect_err("amount=-5 violates WITH CHECK; must be rejected");
    assert_rls_violation(&err, "insert violating WITH CHECK");

    // The rejected row must NOT have been persisted (no silent leak).
    let res = s
        .execute("SELECT id FROM orders ORDER BY id")
        .await
        .unwrap();
    let ids = collect_i64(&res, "id");
    assert_eq!(ids, vec![1], "rejected row -5 must not be persisted");
}

// 2. UPDATE moving a row OUT of WITH CHECK → 42501; valid UPDATE → ok. -------

#[tokio::test]
async fn update_out_of_with_check_rejected_valid_ok() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE t (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO t VALUES (1, 100)").await.unwrap();
    s.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    s.execute("CREATE POLICY p ON t FOR ALL TO PUBLIC USING (true) WITH CHECK (amount > 0)")
        .await
        .unwrap();

    // UPDATE that keeps the row inside WITH CHECK: allowed.
    s.execute("UPDATE t SET amount = 50 WHERE id = 1")
        .await
        .expect("post-image amount=50 satisfies WITH CHECK");

    // UPDATE that moves the row OUT of WITH CHECK: rejected with 42501.
    let err = s
        .execute("UPDATE t SET amount = -1 WHERE id = 1")
        .await
        .expect_err("post-image amount=-1 violates WITH CHECK");
    assert_rls_violation(&err, "update out of WITH CHECK");

    // The row must retain its last valid value (50), not -1.
    let res = s
        .execute("SELECT amount FROM t WHERE id = 1")
        .await
        .unwrap();
    let amts = collect_i64(&res, "amount");
    assert_eq!(amts, vec![50], "rejected UPDATE must not mutate the row");
}

// 3. Table WITHOUT RLS enabled → policy is inert. ---------------------------

#[tokio::test]
async fn no_rls_enabled_policy_is_inert() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE t (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    // Policy exists but ROW LEVEL SECURITY is never enabled.
    s.execute("CREATE POLICY p ON t FOR ALL TO PUBLIC USING (true) WITH CHECK (amount > 0)")
        .await
        .unwrap();

    // Would violate WITH CHECK, but RLS is disabled → write proceeds.
    s.execute("INSERT INTO t VALUES (1, -99)")
        .await
        .expect("RLS disabled: WITH CHECK must not be enforced");
    let res = s.execute("SELECT id FROM t").await.unwrap();
    assert_eq!(collect_i64(&res, "id"), vec![1]);
}

// 4. WITH CHECK absent → USING used as the write predicate (PG fallback). ----

#[tokio::test]
async fn with_check_absent_falls_back_to_using() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE t (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    // No WITH CHECK — USING (amount >= 0) must govern writes too.
    s.execute("CREATE POLICY p ON t FOR ALL TO PUBLIC USING (amount >= 0)")
        .await
        .unwrap();

    s.execute("INSERT INTO t VALUES (1, 0)")
        .await
        .expect("amount>=0 satisfies USING-as-WITH-CHECK fallback");
    let err = s
        .execute("INSERT INTO t VALUES (2, -1)")
        .await
        .expect_err("amount=-1 violates USING fallback");
    assert_rls_violation(&err, "USING fallback on INSERT");
}

// 5. Multiple PERMISSIVE policies combine with OR. --------------------------

#[tokio::test]
async fn multiple_permissive_policies_or_semantics() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE t (id BIGINT NOT NULL, kind BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    // Two permissive policies: a row passes if it satisfies EITHER.
    s.execute("CREATE POLICY p1 ON t FOR ALL TO PUBLIC USING (true) WITH CHECK (kind = 1)")
        .await
        .unwrap();
    s.execute("CREATE POLICY p2 ON t FOR ALL TO PUBLIC USING (true) WITH CHECK (kind = 2)")
        .await
        .unwrap();

    // kind=1 satisfies p1.
    s.execute("INSERT INTO t VALUES (1, 1)")
        .await
        .expect("kind=1 satisfies p1 (OR semantics)");
    // kind=2 satisfies p2.
    s.execute("INSERT INTO t VALUES (2, 2)")
        .await
        .expect("kind=2 satisfies p2 (OR semantics)");
    // kind=3 satisfies neither → rejected.
    let err = s
        .execute("INSERT INTO t VALUES (3, 3)")
        .await
        .expect_err("kind=3 satisfies neither permissive policy");
    assert_rls_violation(&err, "multiple permissive OR");

    let res = s.execute("SELECT id FROM t ORDER BY id").await.unwrap();
    assert_eq!(collect_i64(&res, "id"), vec![1, 2]);
}

// 6. RLS enabled, no applicable policy → default-deny on write. -------------

#[tokio::test]
async fn rls_enabled_no_policy_default_deny_on_write() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE t (id BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    // No policy declared at all: PG default-deny — no row may be written.
    let err = s
        .execute("INSERT INTO t VALUES (1)")
        .await
        .expect_err("RLS on + no policy must default-deny writes");
    assert_rls_violation(&err, "default-deny no policy");
}

// 7. INSERT ... SELECT is subject to WITH CHECK too. ------------------------

#[tokio::test]
async fn insert_select_enforces_with_check() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let s = admin(&eng, ProjectId::new()).await;

    s.execute("CREATE TABLE src (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("INSERT INTO src VALUES (1, 5), (2, -7)")
        .await
        .unwrap();
    s.execute("CREATE TABLE dst (id BIGINT NOT NULL, amount BIGINT NOT NULL)")
        .await
        .unwrap();
    s.execute("ALTER TABLE dst ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    s.execute("CREATE POLICY p ON dst FOR ALL TO PUBLIC USING (true) WITH CHECK (amount > 0)")
        .await
        .unwrap();

    // The -7 row violates WITH CHECK; the whole statement must be rejected.
    let err = s
        .execute("INSERT INTO dst SELECT id, amount FROM src")
        .await
        .expect_err("INSERT...SELECT must enforce WITH CHECK on materialised rows");
    assert_rls_violation(&err, "insert-select WITH CHECK");
}

// --- helper ----------------------------------------------------------------

fn collect_i64(res: &basin_engine::ExecResult, col: &str) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    let batches = match res {
        basin_engine::ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected Rows, got {other:?}"),
    };
    let mut out = Vec::new();
    for b in batches {
        let arr = b
            .column_by_name(col)
            .unwrap_or_else(|| panic!("column {col} missing"))
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 column");
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out
}
