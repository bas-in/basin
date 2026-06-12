//! Security regression — Tier-1 isolation on the upsert and import write paths.
//!
//! Two concerns the isolation audit flagged as untested:
//!
//! 1. **upsert under RLS** (`upsert_respects_rls_owner_policy`): an
//!    `INSERT … ON CONFLICT DO UPDATE` must not let a user mutate a row whose
//!    RLS policy hides it. PostgreSQL evaluates the policy's USING clause for
//!    the conflicting row; an attacker who guesses another user's key must not
//!    be able to overwrite that row through the upsert fast path.
//!
//! 2. **import isolation** (`import_shaped_bulk_write_cannot_cross_projects`):
//!    `basinctl import-from-postgres` writes through ordinary project-scoped
//!    pgwire sessions (bulk INSERT / COPY into the connected project). This
//!    pins that the write path the importer uses is confined to its project —
//!    project B can observe NOTHING an importer wrote into project A, across
//!    catalog, data, and information_schema. (The end-to-end CLI run needs live
//!    endpoints and stays `#[ignore]`d in `services/basinctl/tests`; this test
//!    pins the engine-level confinement that run depends on, with no network.)

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{AuthContext, Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use uuid::Uuid;
use tempfile::TempDir;

#[allow(clippy::type_complexity)]
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(storage.clone(), catalog.clone(), wal.clone()));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal)
}

fn row_count(res: ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { .. } => 0,
    }
}

/// Return the single Int64 `v` for `id` as seen by `sess` (None if no row is
/// visible to that session).
async fn read_v(sess: &ProjectSession, id: i64) -> Option<i64> {
    let res = sess
        .execute(&format!("SELECT v FROM items WHERE id = {id}"))
        .await
        .unwrap();
    match res {
        ExecResult::Rows { batches, .. } => {
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .expect("v is Int64");
                if !col.is_null(0) {
                    return Some(col.value(0));
                }
            }
            None
        }
        ExecResult::Empty { .. } => None,
    }
}

// ---------------------------------------------------------------------------
// 1. Upsert under RLS: an upsert must not overwrite a row hidden by the policy.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_respects_rls_owner_policy() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let project = ProjectId::new();

    let user_a = Uuid::new_v4();
    let user_b = Uuid::new_v4();

    // Admin (no auth ctx) seeds two rows, one per owner, then enables RLS.
    let admin = eng.open_session(project).await.unwrap();
    admin
        .execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, owner_id TEXT NOT NULL, v BIGINT NOT NULL)",
        )
        .await
        .unwrap();
    admin
        .execute(&format!(
            "INSERT INTO items VALUES (1, '{user_a}', 10), (2, '{user_b}', 20)"
        ))
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE items ENABLE ROW LEVEL SECURITY")
        .await
        .unwrap();
    admin
        .execute(
            "CREATE POLICY own_rows ON items FOR ALL TO PUBLIC \
             USING (owner_id = auth_uid()) WITH CHECK (owner_id = auth_uid())",
        )
        .await
        .unwrap();

    // User A's session.
    let ctx_a = AuthContext::from_jwt(
        user_a,
        "authenticated",
        serde_json::json!({ "user_id": user_a.to_string() }),
    );
    let sess_a = eng
        .open_session_with_auth(project, user_a.to_string(), ctx_a)
        .await
        .unwrap();

    // A sees only its own row.
    assert_eq!(read_v(&sess_a, 1).await, Some(10), "A sees its own row");
    assert_eq!(read_v(&sess_a, 2).await, None, "A cannot see B's row under RLS");

    // ATTACK: A upserts on B's primary key, trying to overwrite B's value.
    // The conflicting row (id=2) is hidden from A by the policy, so the upsert
    // must NOT silently overwrite B's value. Acceptable outcomes:
    //   * a typed error (policy/check violation), OR
    //   * it inserts/updates only within A's own visibility,
    // but NEVER B's v=20 turned into the attacker's value.
    let attack = sess_a
        .execute(
            "INSERT INTO items (id, owner_id, v) VALUES (2, 'attacker', 999) \
             ON CONFLICT (id) DO UPDATE SET v = 999",
        )
        .await;
    println!("[sec_upsert] cross-owner upsert outcome: {attack:?}");

    // Verify that B's row is intact. RLS is enabled with an `auth_uid()`
    // policy and Basin applies RLS to every session (no owner/service bypass),
    // so we read as USER B — the only principal the policy lets see id=2. If
    // A's upsert had overwritten or dropped B's row, B's own SELECT would no
    // longer return v=20.
    let ctx_b = AuthContext::from_jwt(
        user_b,
        "authenticated",
        serde_json::json!({ "user_id": user_b.to_string() }),
    );
    let sess_b = eng
        .open_session_with_auth(project, user_b.to_string(), ctx_b)
        .await
        .unwrap();
    let b_v_after = {
        let res = sess_b
            .execute("SELECT v FROM items WHERE id = 2")
            .await
            .unwrap();
        match res {
            ExecResult::Rows { batches, .. } => batches
                .iter()
                .filter(|b| b.num_rows() > 0)
                .map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::Int64Array>()
                        .unwrap()
                        .value(0)
                })
                .next(),
            ExecResult::Empty { .. } => None,
        }
    };
    assert_eq!(
        b_v_after,
        Some(20),
        "SECURITY: an RLS-hidden row (id=2, owner B) was overwritten by A's upsert — \
         RLS bypass on the ON CONFLICT path (got {b_v_after:?})"
    );
}

// ---------------------------------------------------------------------------
// 2. Import isolation: the importer's bulk-write path cannot cross projects.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_shaped_bulk_write_cannot_cross_projects() {
    let (_sd, _wd, eng, _sh, _bg, _wal) = build().await;
    let pa = ProjectId::new();
    let pb = ProjectId::new();
    let sa = eng.open_session(pa).await.unwrap();
    let sb = eng.open_session(pb).await.unwrap();

    // Project A: an "import" — CREATE TABLE + a bulk multi-row INSERT, exactly
    // the shape `basinctl import-from-postgres` drives over a project-scoped
    // pgwire session.
    sa.execute("CREATE TABLE imported (id BIGINT PRIMARY KEY, secret TEXT NOT NULL)")
        .await
        .unwrap();
    let mut bulk = String::from("INSERT INTO imported VALUES ");
    for i in 0..500i64 {
        if i > 0 {
            bulk.push(',');
        }
        bulk.push_str(&format!("({i}, 'a-secret-{i}')"));
    }
    sa.execute(&bulk).await.unwrap();
    assert_eq!(
        row_count(sa.execute("SELECT * FROM imported").await.unwrap()),
        500,
        "A's import landed"
    );

    // Project B must see NOTHING from A's import.
    // (a) the table does not exist in B's namespace.
    let b_select = sb.execute("SELECT * FROM imported").await;
    assert!(
        b_select.is_err(),
        "SECURITY: project B can read project A's imported table: {b_select:?}"
    );
    // (b) B cannot see it in information_schema.
    let b_info = sb
        .execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'imported'",
        )
        .await
        .unwrap();
    assert_eq!(
        row_count(b_info),
        0,
        "SECURITY: project A's imported table leaked into project B's information_schema"
    );
    // (c) B creating a same-named table sees only its own (empty) data, never
    // A's 500 rows — the namespaces are fully separate.
    sb.execute("CREATE TABLE imported (id BIGINT PRIMARY KEY, secret TEXT NOT NULL)")
        .await
        .unwrap();
    assert_eq!(
        row_count(sb.execute("SELECT * FROM imported").await.unwrap()),
        0,
        "SECURITY: project B's same-named table surfaced project A's imported rows"
    );
    // A still sees exactly its 500 rows (B's create did not collide).
    assert_eq!(
        row_count(sa.execute("SELECT * FROM imported").await.unwrap()),
        500,
        "project A's import must be unaffected by B's same-named table"
    );
}
