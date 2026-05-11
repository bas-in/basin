//! Integration tests: `auth.uid()` works in RLS policies end-to-end.
//!
//! These tests use the engine directly (no pgwire / REST boot) so they run
//! without an external Postgres or SMTP relay. The `AuthContext` is built
//! from a UUID, matching exactly what the pgwire JWT path does in production.
//!
//! Scenarios covered:
//!
//! 1. **auth.uid() in USING clause**: `owner_id = auth.uid()` filters rows
//!    so user A sees only their rows and user B sees only theirs.
//! 2. **Anonymous session**: `auth.uid()` IS NULL → no rows visible under the
//!    `owner_id = auth.uid()` policy (NULL = anything is NULL, not TRUE).
//! 3. **auth.role() returns 'authenticated'** for JWT sessions.
//! 4. **auth.role() returns 'anon'** for anonymous sessions.
//! 5. **Cross-user isolation via auth.uid()**: neither user can see the
//!    other's rows even through UNION ALL or a CTE.
//! 6. **Disable RLS**: both users see all rows again after
//!    `ALTER TABLE … DISABLE ROW LEVEL SECURITY`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use arrow_array::Array;
use basin_catalog::InMemoryCatalog;
use basin_common::TenantId;
use basin_engine::{AuthContext, Engine, EngineConfig, ExecResult};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Engine factory
// ---------------------------------------------------------------------------

fn make_engine(dir: &TempDir) -> Engine {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Count the number of data rows in an `ExecResult`.
fn row_count(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Extract the (non-null) string value at column 0, row 0 of the first batch.
/// Returns `None` if the batch is empty or the value is null.
fn first_string(res: ExecResult) -> Option<String> {
    match res {
        ExecResult::Rows { batches, .. } => {
            let batch = batches.first()?;
            if batch.num_rows() == 0 {
                return None;
            }
            let col = batch.column(0);
            if col.is_null(0) {
                return None;
            }
            let arr = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("column 0 must be String");
            Some(arr.value(0).to_owned())
        }
        ExecResult::Empty { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Core RLS-with-auth.uid() scenario:
///
/// 1. Create `items (id, owner_id UUID, payload TEXT)`.
/// 2. CREATE POLICY using `owner_id = auth_uid()`.
/// 3. User A inserts one row with `owner_id = user_a_id`.
/// 4. User B inserts one row with `owner_id = user_b_id`.
/// 5. User A SELECT → 1 row (their own).
/// 6. User B SELECT → 1 row (their own).
/// 7. Anonymous SELECT → 0 rows (auth_uid() IS NULL, NULL = UUID is NULL).
#[tokio::test]
async fn rls_with_auth_uid_filters_per_user() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let tenant = TenantId::new();

    let user_a_id = Uuid::new_v4();
    let user_b_id = Uuid::new_v4();

    // Admin session: schema setup.
    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute(
            "CREATE TABLE items (\
                id BIGINT NOT NULL, \
                owner_id TEXT NOT NULL, \
                payload TEXT NOT NULL\
            )",
        )
        .await
        .unwrap();

    // Insert as admin (no auth ctx → no RLS interference during seeding).
    admin
        .execute(&format!(
            "INSERT INTO items VALUES (1, '{user_a_id}', 'alice-payload')"
        ))
        .await
        .unwrap();
    admin
        .execute(&format!(
            "INSERT INTO items VALUES (2, '{user_b_id}', 'bob-payload')"
        ))
        .await
        .unwrap();

    // Enable RLS with an auth.uid() policy.
    admin
        .execute("ALTER TABLE items ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own_rows ON items FOR ALL TO PUBLIC \
             USING (owner_id = auth_uid())",
        )
        .await
        .unwrap();

    // Open authenticated session for user A.
    let ctx_a = AuthContext::from_jwt(
        user_a_id,
        "authenticated",
        serde_json::json!({"user_id": user_a_id.to_string()}),
    );
    let sess_a = engine
        .open_session_with_auth(tenant, user_a_id.to_string(), ctx_a)
        .await
        .unwrap();

    // Open authenticated session for user B.
    let ctx_b = AuthContext::from_jwt(
        user_b_id,
        "authenticated",
        serde_json::json!({"user_id": user_b_id.to_string()}),
    );
    let sess_b = engine
        .open_session_with_auth(tenant, user_b_id.to_string(), ctx_b)
        .await
        .unwrap();

    // Anonymous session (no JWT).
    let sess_anon = engine.open_session(tenant).await.unwrap();

    // 5. User A sees exactly 1 row.
    let a_rows = row_count(sess_a.execute("SELECT * FROM items").await.unwrap());
    assert_eq!(a_rows, 1, "user A must see exactly 1 row under RLS; got {a_rows}");

    // 6. User B sees exactly 1 row.
    let b_rows = row_count(sess_b.execute("SELECT * FROM items").await.unwrap());
    assert_eq!(b_rows, 1, "user B must see exactly 1 row under RLS; got {b_rows}");

    // 7. Anonymous sees 0 rows.
    let anon_rows = row_count(sess_anon.execute("SELECT * FROM items").await.unwrap());
    assert_eq!(
        anon_rows, 0,
        "anonymous session must see 0 rows (auth_uid() IS NULL); got {anon_rows}"
    );
}

/// `auth.uid()` (schema-qualified form) and `auth_uid()` (flat form) must
/// produce the same non-NULL UUID for authenticated sessions.
#[tokio::test]
async fn auth_uid_schema_form_equals_flat_form() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let user_id = Uuid::new_v4();

    let auth = AuthContext::from_jwt(
        user_id,
        "authenticated",
        serde_json::json!({"user_id": user_id.to_string()}),
    );
    let sess = engine
        .open_session_with_auth(TenantId::new(), user_id.to_string(), auth)
        .await
        .unwrap();

    let flat = first_string(sess.execute("SELECT auth_uid()").await.unwrap())
        .expect("auth_uid() must return non-NULL for authenticated session");
    let schema_form = first_string(sess.execute("SELECT auth.uid()").await.unwrap())
        .expect("auth.uid() must return non-NULL for authenticated session");

    assert_eq!(
        flat, schema_form,
        "auth_uid() and auth.uid() must return identical values"
    );
    assert_eq!(flat, user_id.to_string(), "auth_uid must match the session user_id");
}

/// `auth.uid()` returns NULL for anonymous sessions; `auth.role()` returns
/// `'anon'`.
#[tokio::test]
async fn auth_uid_null_and_role_anon_for_anonymous_session() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let sess = engine.open_session(TenantId::new()).await.unwrap();

    // auth_uid() IS NULL
    let uid_result = sess.execute("SELECT auth_uid()").await.unwrap();
    match uid_result {
        ExecResult::Rows { batches, .. } => {
            let arr = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            assert!(arr.is_null(0), "auth_uid() must be NULL for anonymous session");
        }
        other => panic!("expected Rows, got {other:?}"),
    }

    // auth.role() = 'anon'
    let role = first_string(sess.execute("SELECT auth.role()").await.unwrap())
        .expect("auth.role() must not be NULL");
    assert_eq!(role, "anon", "anonymous session must have role 'anon'");
}

/// Authenticated session: `auth.role()` returns `'authenticated'`.
#[tokio::test]
async fn auth_role_authenticated_for_jwt_session() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let user_id = Uuid::new_v4();
    let auth = AuthContext::from_jwt(user_id, "authenticated", serde_json::json!({}));
    let sess = engine
        .open_session_with_auth(TenantId::new(), "alice", auth)
        .await
        .unwrap();

    let role = first_string(sess.execute("SELECT auth.role()").await.unwrap())
        .expect("auth.role() must not be NULL");
    assert_eq!(role, "authenticated");
}

/// RLS using `auth.uid()` in a USING clause does not leak across users via
/// `UNION ALL`.
#[tokio::test]
async fn rls_auth_uid_union_cannot_bypass() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let tenant = TenantId::new();
    let user_a_id = Uuid::new_v4();
    let user_b_id = Uuid::new_v4();

    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute(
            "CREATE TABLE secrets (\
                id BIGINT NOT NULL, \
                owner_id TEXT NOT NULL, \
                secret_val TEXT NOT NULL\
            )",
        )
        .await
        .unwrap();
    admin
        .execute(&format!(
            "INSERT INTO secrets VALUES (1, '{user_a_id}', 'secret-a'), (2, '{user_b_id}', 'secret-b')"
        ))
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE secrets ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own_secrets ON secrets FOR ALL TO PUBLIC \
             USING (owner_id = auth_uid())",
        )
        .await
        .unwrap();

    let ctx_b = AuthContext::from_jwt(user_b_id, "authenticated", serde_json::json!({}));
    let sess_b = engine
        .open_session_with_auth(tenant, user_b_id.to_string(), ctx_b)
        .await
        .unwrap();

    // UNION ALL: both arms query the same RLS-protected table — user B should
    // see their own row twice, never user A's row.
    let union_rows = row_count(
        sess_b
            .execute(
                "SELECT secret_val FROM secrets \
                 UNION ALL \
                 SELECT secret_val FROM secrets",
            )
            .await
            .unwrap(),
    );
    // User B owns 1 row → UNION ALL of two filtered scans = 2 rows.
    assert_eq!(
        union_rows, 2,
        "UNION ALL under RLS auth.uid() must see 2 (B's row doubled), got {union_rows}"
    );

    // Verify user A's secret is not in the result.
    let union_res = sess_b
        .execute(
            "SELECT secret_val FROM secrets \
             UNION ALL \
             SELECT secret_val FROM secrets",
        )
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = union_res {
        for batch in &batches {
            let arr = batch
                .column_by_name("secret_val")
                .expect("secret_val column")
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("string array");
            for i in 0..arr.len() {
                assert_ne!(
                    arr.value(i),
                    "secret-a",
                    "user A's secret must not appear in user B's UNION result"
                );
            }
        }
    }
}

/// Disabling RLS makes all rows visible to all sessions again.
#[tokio::test]
async fn rls_disabled_all_rows_visible() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let tenant = TenantId::new();
    let user_a_id = Uuid::new_v4();
    let user_b_id = Uuid::new_v4();

    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute(
            "CREATE TABLE widgets (\
                id BIGINT NOT NULL, \
                owner_id TEXT NOT NULL\
            )",
        )
        .await
        .unwrap();
    admin
        .execute(&format!(
            "INSERT INTO widgets VALUES (1, '{user_a_id}'), (2, '{user_b_id}')"
        ))
        .await
        .unwrap();

    // Enable RLS + auth.uid() policy.
    admin
        .execute("ALTER TABLE widgets ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own_widget ON widgets FOR ALL TO PUBLIC \
             USING (owner_id = auth_uid())",
        )
        .await
        .unwrap();

    let ctx_a = AuthContext::from_jwt(user_a_id, "authenticated", serde_json::json!({}));
    let sess_a = engine
        .open_session_with_auth(tenant, user_a_id.to_string(), ctx_a)
        .await
        .unwrap();

    // With RLS on, user A sees 1 row.
    assert_eq!(
        row_count(sess_a.execute("SELECT * FROM widgets").await.unwrap()),
        1
    );

    // Disable RLS → user A now sees both rows.
    admin
        .execute("ALTER TABLE widgets DISABLE ROW LEVEL SECURITY")
        .await
        .unwrap();

    assert_eq!(
        row_count(sess_a.execute("SELECT * FROM widgets").await.unwrap()),
        2,
        "after DISABLE ROW LEVEL SECURITY, all rows must be visible"
    );

    // Anonymous session also sees both rows after RLS is disabled.
    let sess_anon = engine.open_session(tenant).await.unwrap();
    assert_eq!(
        row_count(sess_anon.execute("SELECT * FROM widgets").await.unwrap()),
        2,
        "anonymous session must see all rows when RLS is disabled"
    );
}

/// RLS with auth.uid() in a CTE: the policy is applied inside the CTE scan,
/// not bypassed by the outer query.
#[tokio::test]
async fn rls_auth_uid_cte_cannot_bypass() {
    let dir = TempDir::new().unwrap();
    let engine = make_engine(&dir);
    let tenant = TenantId::new();
    let user_a_id = Uuid::new_v4();
    let user_b_id = Uuid::new_v4();

    let admin = engine.open_session(tenant).await.unwrap();
    admin
        .execute(
            "CREATE TABLE notes (\
                id BIGINT NOT NULL, \
                owner_id TEXT NOT NULL, \
                body TEXT NOT NULL\
            )",
        )
        .await
        .unwrap();
    admin
        .execute(&format!(
            "INSERT INTO notes VALUES (1, '{user_a_id}', 'note-a'), (2, '{user_b_id}', 'note-b')"
        ))
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE notes ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own_note ON notes FOR ALL TO PUBLIC USING (owner_id = auth_uid())",
        )
        .await
        .unwrap();

    let ctx_b = AuthContext::from_jwt(user_b_id, "authenticated", serde_json::json!({}));
    let sess_b = engine
        .open_session_with_auth(tenant, user_b_id.to_string(), ctx_b)
        .await
        .unwrap();

    // CTE wrapping the table scan — RLS must apply inside the CTE.
    let cte_rows = row_count(
        sess_b
            .execute(
                "WITH all_notes AS (SELECT body FROM notes) \
                 SELECT body FROM all_notes",
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        cte_rows, 1,
        "CTE must not bypass auth_uid() RLS; expected 1, got {cte_rows}"
    );

    // Confirm user A's note is not reachable.
    let cte_res = sess_b
        .execute(
            "WITH all_notes AS (SELECT body FROM notes) \
             SELECT body FROM all_notes",
        )
        .await
        .unwrap();
    if let ExecResult::Rows { batches, .. } = cte_res {
        for batch in &batches {
            let arr = batch
                .column_by_name("body")
                .expect("body column")
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("string array");
            for i in 0..arr.len() {
                assert_ne!(
                    arr.value(i),
                    "note-a",
                    "user A's note must not appear via CTE under auth_uid() RLS"
                );
            }
        }
    }
}
