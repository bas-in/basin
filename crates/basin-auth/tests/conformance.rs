//! Integration conformance tests for `AuthStore`.
//!
//! This file exercises the full `AuthStore` contract against the
//! `PostgresAuthStore` implementation. The same invariants are exercised
//! against `EngineAuthStore` by the unit tests in
//! `services/basin-server/src/engine_auth_store.rs`.
//!
//! ## Running
//!
//! ```bash
//! # Postgres path (gracefully skipped when env var is absent):
//! BASIN_AUTH_TEST_POSTGRES_URL="host=127.0.0.1 port=5432 user=pc dbname=postgres" \
//!   cargo test -p basin-auth --test conformance
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use basin_auth::store::postgres::PostgresAuthStore;
use basin_auth::store::AuthStore;
use basin_auth::project_credentials::parse_project_from_pgwire_user;
use basin_auth::tokens;
use basin_common::{BasinError, ProjectId};
use chrono::Utc;
use tokio_postgres::NoTls;
use ulid::Ulid;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Bcrypt helper (cost 4 is the minimum; keeps tests fast)
// ---------------------------------------------------------------------------

const TEST_BCRYPT_COST: u32 = 4;

fn bcrypt_hash(plain: &str) -> String {
    bcrypt::hash(plain, TEST_BCRYPT_COST).expect("bcrypt hash")
}

// ---------------------------------------------------------------------------
// Postgres connection helpers
// ---------------------------------------------------------------------------

fn pg_dsn() -> Option<String> {
    // Canonical env var per Phase 5.10 spec; legacy alias for compat.
    std::env::var("BASIN_AUTH_TEST_POSTGRES_URL")
        .ok()
        .or_else(|| std::env::var("BASIN_TEST_PG_DSN").ok())
}

fn unique_schema() -> String {
    format!("basin_conform_{}", Ulid::new().to_string().to_lowercase())
}

/// A RAII guard that drops the test schema when the test finishes.
struct SchemaGuard {
    schema: String,
    dsn: String,
}

impl Drop for SchemaGuard {
    fn drop(&mut self) {
        let schema = self.schema.clone();
        let dsn = self.dsn.clone();
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(_) => return,
            };
            rt.block_on(async {
                let connect = tokio::time::timeout(
                    Duration::from_secs(2),
                    tokio_postgres::connect(&dsn, NoTls),
                )
                .await;
                let (client, conn) = match connect {
                    Ok(Ok(pair)) => pair,
                    _ => return,
                };
                let _driver = tokio::spawn(async move {
                    let _ = conn.await;
                });
                let _ = client
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await;
            });
        })
        .join();
    }
}

/// Try to connect to Postgres and stand up a fresh schema.
/// Returns `None` gracefully when the env var is unset or Postgres is
/// unreachable.
async fn try_connect() -> Option<(Arc<dyn AuthStore>, SchemaGuard)> {
    let dsn = pg_dsn()?;
    let schema = unique_schema();

    let connect =
        tokio::time::timeout(Duration::from_secs(5), tokio_postgres::connect(&dsn, NoTls)).await;

    let (client, conn) = match connect {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            eprintln!("conformance[pg]: connect failed ({e}), skipping");
            return None;
        }
        Err(_) => {
            eprintln!("conformance[pg]: connect timed out, skipping");
            return None;
        }
    };

    tokio::spawn(async move {
        let _ = conn.await;
    });

    let store = Arc::new(PostgresAuthStore::new(client, schema.clone()));
    store
        .migrate(&schema)
        .await
        .expect("conformance migration must succeed");

    let guard = SchemaGuard { schema, dsn };
    Some((store as Arc<dyn AuthStore>, guard))
}

// ---------------------------------------------------------------------------
// Conformance suite — shared by PG and Engine test entry points
// ---------------------------------------------------------------------------

/// Run all invariants against `store`. The store must already have been
/// migrated.
pub async fn run_conformance_suite(store: Arc<dyn AuthStore>) {
    user_uniqueness_per_project(&store).await;
    cross_project_same_email(&store).await;
    email_token_single_use(&store).await;
    refresh_rotation(&store).await;
    refresh_blanket_revocation(&store).await;
    api_key_lifecycle(&store).await;
    project_credential_self_routing(&store).await;
    project_credential_malformed_inputs();
    session_settings_upsert(&store).await;
}

// ---------------------------------------------------------------------------
// 1. User uniqueness per project
// ---------------------------------------------------------------------------

/// Invariant: `(project, email)` pairs are unique; the same email in two
/// different projects must succeed.
async fn user_uniqueness_per_project(store: &Arc<dyn AuthStore>) {
    let project_a = ProjectId::new();
    let email = format!("unique-{}@conform.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");

    // First insert → Ok.
    let uid_a = Uuid::new_v4();
    store
        .create_user(&project_a, &email, &hash, uid_a)
        .await
        .expect("first create_user must succeed");

    // Duplicate (project_a, email) → Err.
    let dup = store
        .create_user(&project_a, &email, &hash, Uuid::new_v4())
        .await;
    assert!(
        dup.is_err(),
        "duplicate (project, email) must fail, got Ok"
    );
}

// ---------------------------------------------------------------------------
// 2. Cross-project same email
// ---------------------------------------------------------------------------

/// Invariant: the same email CAN be registered in two different projects.
async fn cross_project_same_email(store: &Arc<dyn AuthStore>) {
    let project_a = ProjectId::new();
    let project_b = ProjectId::new();
    let email = format!("cross-{}@conform.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");

    let uid_a = Uuid::new_v4();
    store
        .create_user(&project_a, &email, &hash, uid_a)
        .await
        .expect("create in project_a");

    let uid_b = Uuid::new_v4();
    store
        .create_user(&project_b, &email, &hash, uid_b)
        .await
        .expect("same email in project_b must succeed (different project)");

    // find_user_by_email must isolate by project.
    let found_a = store
        .find_user_by_email(&project_a, &email)
        .await
        .expect("find in project_a")
        .expect("user must exist in project_a");
    assert_eq!(found_a.user_id, uid_a);

    let found_b = store
        .find_user_by_email(&project_b, &email)
        .await
        .expect("find in project_b")
        .expect("user must exist in project_b");
    assert_eq!(found_b.user_id, uid_b);

    // Wrong project returns None.
    let not_found = store
        .find_user_by_email(&ProjectId::new(), &email)
        .await
        .expect("find in unrelated project must not error");
    assert!(not_found.is_none(), "user must not be visible in wrong project");
}

// ---------------------------------------------------------------------------
// 3. Single-use tokens
// ---------------------------------------------------------------------------

/// Invariant: a token can be consumed exactly once.
async fn email_token_single_use(store: &Arc<dyn AuthStore>) {
    let project = ProjectId::new();
    let user_id = Uuid::new_v4();
    let email = format!("token-{}@conform.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&project, &email, &hash, user_id)
        .await
        .expect("create_user");

    let (_, token_hash) = tokens::generate();
    let expires_at = Utc::now() + chrono::Duration::minutes(30);

    store
        .insert_email_token(&project, user_id, &token_hash, "verify", expires_at)
        .await
        .expect("insert_email_token");

    // First consume → 1 row.
    let n1 = store
        .consume_email_token(&project, &token_hash)
        .await
        .expect("first consume");
    assert_eq!(n1, 1, "first consume must update 1 row");

    // Second consume → 0 rows (already consumed).
    let n2 = store
        .consume_email_token(&project, &token_hash)
        .await
        .expect("second consume must not error");
    assert_eq!(n2, 0, "second consume must return 0 (already consumed)");
}

// ---------------------------------------------------------------------------
// 4. Refresh rotation
// ---------------------------------------------------------------------------

/// Invariant: `insert_refresh_revocation` is idempotent (ON CONFLICT DO
/// NOTHING) but the jti remains in the list after a duplicate call.
async fn refresh_rotation(store: &Arc<dyn AuthStore>) {
    let user_id = Uuid::new_v4();
    let email = format!("refresh-{}@conform.test", Uuid::new_v4());
    let project = ProjectId::new();
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&project, &email, &hash, user_id)
        .await
        .expect("create_user");

    let jti_a = format!("jti-a-{}", Uuid::new_v4());
    let jti_b = format!("jti-b-{}", Uuid::new_v4());
    let expires = Utc::now() + chrono::Duration::days(30);

    // Insert jti_a → 1 row.
    let n = store
        .insert_refresh_revocation(&jti_a, user_id, expires)
        .await
        .expect("insert jti_a");
    assert_eq!(n, 1, "first revocation must insert 1 row");

    // jti_a must appear in the list.
    let rows = store
        .list_refresh_revocations(user_id)
        .await
        .expect("list after jti_a");
    assert!(
        rows.iter().any(|r| r.token_hash == jti_a),
        "jti_a must appear in revocation list"
    );

    // Insert jti_b.
    store
        .insert_refresh_revocation(&jti_b, user_id, expires)
        .await
        .expect("insert jti_b");

    // Both must appear.
    let rows2 = store
        .list_refresh_revocations(user_id)
        .await
        .expect("list after jti_b");
    assert!(rows2.iter().any(|r| r.token_hash == jti_a), "jti_a still present");
    assert!(rows2.iter().any(|r| r.token_hash == jti_b), "jti_b present");

    // Duplicate insert of jti_a → 0 rows (ON CONFLICT DO NOTHING), no error.
    let n2 = store
        .insert_refresh_revocation(&jti_a, user_id, expires)
        .await
        .expect("duplicate insert must not error");
    assert_eq!(n2, 0, "duplicate must return 0 (ON CONFLICT DO NOTHING)");

    // jti_a still in list.
    let rows3 = store
        .list_refresh_revocations(user_id)
        .await
        .expect("list after idempotent insert");
    assert!(
        rows3.iter().any(|r| r.token_hash == jti_a),
        "jti_a must remain after idempotent re-insert"
    );
}

// ---------------------------------------------------------------------------
// 5. Blanket revocation sentinel
// ---------------------------------------------------------------------------

/// Invariant: `BLANKET:<user_id>` sentinel appears in the revocation list and
/// is idempotent on upsert.
async fn refresh_blanket_revocation(store: &Arc<dyn AuthStore>) {
    let user_id = Uuid::new_v4();
    let email = format!("blanket-{}@conform.test", Uuid::new_v4());
    let project = ProjectId::new();
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&project, &email, &hash, user_id)
        .await
        .expect("create_user");

    let blanket_key = format!("BLANKET:{user_id}");
    let expires = Utc::now() + chrono::Duration::days(30);

    // First upsert.
    store
        .upsert_blanket_revocation(&blanket_key, user_id, expires)
        .await
        .expect("first upsert_blanket_revocation");

    // Sentinel must appear.
    let rows = store
        .list_refresh_revocations(user_id)
        .await
        .expect("list after blanket upsert");
    assert!(
        rows.iter().any(|r| r.token_hash == blanket_key),
        "BLANKET sentinel must appear in list_refresh_revocations"
    );

    // Second upsert must not error and must not duplicate.
    store
        .upsert_blanket_revocation(&blanket_key, user_id, expires)
        .await
        .expect("second upsert must not error");

    let rows2 = store
        .list_refresh_revocations(user_id)
        .await
        .expect("list after second upsert");
    let count = rows2.iter().filter(|r| r.token_hash == blanket_key).count();
    assert_eq!(count, 1, "duplicate upsert must not create two sentinel rows");
}

// ---------------------------------------------------------------------------
// 6. API key lifecycle
// ---------------------------------------------------------------------------

/// Invariant: insert → find (not revoked) → revoke → find (revoked_at set).
/// Revoking via a different project returns NotFound.
async fn api_key_lifecycle(store: &Arc<dyn AuthStore>) {
    let project = ProjectId::new();
    let user_id = Uuid::new_v4();
    let email = format!("apikey-{}@conform.test", Uuid::new_v4());
    let hash = bcrypt_hash("testpassword");
    store
        .create_user(&project, &email, &hash, user_id)
        .await
        .ok(); // user existence is needed for FK in Postgres; not enforced in engine

    let raw_key = format!("basin_{project}_testkey{}", Uuid::new_v4());
    let key_sha = {
        use sha2::{Digest, Sha256};
        hex::encode(Sha256::digest(raw_key.as_bytes()))
    };
    let key_bcrypt = bcrypt_hash(&raw_key);

    let (id, created_at) = store
        .insert_api_key(&project, user_id, "conform-key", &key_sha, &key_bcrypt)
        .await
        .expect("insert_api_key");
    assert!(id > 0, "api key id must be positive");
    assert!(
        (Utc::now() - created_at).num_seconds().abs() < 5,
        "created_at must be recent"
    );

    // find_api_keys_by_hash → not revoked.
    let rows = store
        .find_api_keys_by_hash(&key_sha)
        .await
        .expect("find_api_keys_by_hash");
    assert!(!rows.is_empty(), "must return the inserted key");
    let row = rows.iter().find(|r| r.id == id).expect("row by id");
    assert!(row.revoked_at.is_none(), "key must not be revoked initially");
    assert_eq!(row.user_id, user_id);

    // Revoke.
    store
        .revoke_api_key(&project, id)
        .await
        .expect("revoke_api_key");

    // find after revoke → revoked_at set.
    let rows2 = store
        .find_api_keys_by_hash(&key_sha)
        .await
        .expect("find after revoke");
    let row2 = rows2.iter().find(|r| r.id == id).expect("row by id");
    assert!(row2.revoked_at.is_some(), "revoked_at must be set");

    // Revoke from wrong project → NotFound.
    let wrong = store
        .revoke_api_key(&ProjectId::new(), id)
        .await
        .expect_err("wrong project must fail");
    assert!(
        matches!(wrong, BasinError::NotFound(_)),
        "expected NotFound, got {wrong:?}"
    );
}

// ---------------------------------------------------------------------------
// 7. Self-routing credential parsing
// ---------------------------------------------------------------------------

/// Invariant: `pgwire_user` in the new format embeds the project id, which
/// `parse_project_from_pgwire_user` recovers. The credential is round-tripped
/// through the store to verify the insert and lookup path.
async fn project_credential_self_routing(store: &Arc<dyn AuthStore>) {
    let project = ProjectId::new();
    let pgwire_user = format!("{}_{:08x}", project, 0xdeadbeef_u32);
    let hash = bcrypt_hash("routingpwd");

    store
        .insert_project_credential(&project, &pgwire_user, &hash, "basin")
        .await
        .expect("insert_project_credential");

    // Positive: parse recovers the project id.
    let parsed = parse_project_from_pgwire_user(&pgwire_user)
        .expect("valid pgwire_user must parse");
    assert_eq!(
        parsed,
        project.to_string().as_str(),
        "parsed project must match original"
    );

    // `find_project_credential` returns the row.
    let row = store
        .find_project_credential(&pgwire_user)
        .await
        .expect("find_project_credential must not error")
        .expect("credential must exist after insert");
    assert_eq!(row.project_id, project.to_string());
    assert_eq!(row.dbname, "basin");
}

// ---------------------------------------------------------------------------
// 8. Malformed credential inputs — pure parsing, no store needed
// ---------------------------------------------------------------------------

/// Invariant: `parse_project_from_pgwire_user` returns `None` for all
/// malformed inputs — no panic, no error, no unwrap.
fn project_credential_malformed_inputs() {
    let bad: &[&str] = &[
        "",
        "short",
        "project_a1b2c3d4",                     // legacy format
        "01JBAS1NAVTH00000000000000",            // 26 chars, no underscore
        "01JBAS1NAVTH00000000000000-suffix",     // wrong separator
        "01JBAS1NAVTH00000000000000_",           // underscore at 26 but nothing after
        "../etc/passwd",                          // path injection
        "'; DROP TABLE users; --",               // SQL injection
    ];

    for input in bad {
        assert!(
            parse_project_from_pgwire_user(input).is_none(),
            "malformed input {input:?} must return None"
        );
    }
}

// ---------------------------------------------------------------------------
// 9. Session settings upsert
// ---------------------------------------------------------------------------

/// Invariant: upsert is idempotent; re-upserting updates the value without
/// duplicating the row.
async fn session_settings_upsert(store: &Arc<dyn AuthStore>) {
    let project = ProjectId::new();
    let user = Uuid::new_v4();

    store
        .upsert_session_setting(&project, user, "timezone", "UTC")
        .await
        .expect("first upsert");

    let m1: HashMap<String, String> = store
        .list_session_settings(&project, user)
        .await
        .expect("list after first upsert");
    assert_eq!(m1.get("timezone").map(|s| s.as_str()), Some("UTC"));
    assert_eq!(m1.len(), 1);

    store
        .upsert_session_setting(&project, user, "timezone", "America/New_York")
        .await
        .expect("second upsert");

    let m2: HashMap<String, String> = store
        .list_session_settings(&project, user)
        .await
        .expect("list after second upsert");
    assert_eq!(
        m2.get("timezone").map(|s| s.as_str()),
        Some("America/New_York"),
        "value must update"
    );
    assert_eq!(m2.len(), 1, "upsert must not duplicate");

    store
        .upsert_session_setting(&project, user, "language", "en")
        .await
        .expect("upsert language");
    let m3: HashMap<String, String> = store
        .list_session_settings(&project, user)
        .await
        .expect("list after third upsert");
    assert_eq!(m3.len(), 2, "two distinct keys must produce two entries");
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

/// Run the full conformance suite against `PostgresAuthStore`.
///
/// Gracefully skipped when `BASIN_AUTH_TEST_POSTGRES_URL` (or the legacy
/// `BASIN_TEST_PG_DSN`) is not set or when Postgres is unreachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_auth_store_conformance() {
    let Some((store, _guard)) = try_connect().await else {
        eprintln!("conformance[pg]: BASIN_AUTH_TEST_POSTGRES_URL not set — skipping");
        return;
    };
    eprintln!("conformance[pg]: running suite against PostgresAuthStore");
    run_conformance_suite(store).await;
    eprintln!("conformance[pg]: all assertions passed");
}
