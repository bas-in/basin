//! Regression guard: UUID-primary-key constraint enforcement must work on
//! Vortex cold-storage tables.
//!
//! Vortex has no FixedSizeBinary(N) encoder, so it stores UUID columns as
//! Decimal256(39,0) on disk. The cold-scan constraint paths (enforce_pk_on_insert
//! / enforce_unique_on_insert) call `read_file_with_options` *without* a catalog
//! schema, which means the restore-exec and restamp chains never run. Before the
//! fix, `scalar_to_canonical_string` hit the `other => Err(...)` branch for
//! Decimal256(39,0) UUID columns and produced an `Internal` error on any INSERT
//! into a Vortex table with a UUID primary key where cold data already existed.
//!
//! After the fix the cold-tier scan decodes Decimal256(39,0) as UUID bytes,
//! compares them to the incoming batch, and produces the correct
//! `UniqueViolation` on a duplicate or accepts the INSERT for a fresh UUID.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── test engine ──────────────────────────────────────────────────────────────

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

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec_ok(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed [{sql}]: {e}"));
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<arrow_array::RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows for [{sql}], got {other:?}"),
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Every regular file under `root` with the given extension.
fn files_with_ext(root: &std::path::Path, ext: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(ext))
            {
                out.push(p.to_string_lossy().into_owned());
            }
        }
    }
    out
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// INSERT a row, flush it to a Vortex cold file, then INSERT the same UUID again.
/// Must produce UniqueViolation, not Internal.
///
/// The default file format is Vortex; no `WITH (basin.file_format='...')` needed.
#[tokio::test]
async fn uuid_pk_dedup_across_cold_vortex_file() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE items (id UUID PRIMARY KEY, label TEXT)",
    )
    .await;

    let uuid_a = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    let uuid_b = "b1ffcd00-ad1c-5f09-cc7e-7cc0ce491b22";

    // First INSERT: writes a cold Vortex file.
    exec_ok(
        &sess,
        &format!("INSERT INTO items (id, label) VALUES ('{uuid_a}', 'first')"),
    )
    .await;

    // On-disk proof: the table's data files must be Vortex.
    let vortex_files = files_with_ext(dir.path(), ".vortex");
    assert!(
        !vortex_files.is_empty(),
        "expected cold .vortex file after first INSERT; got none"
    );

    // Second INSERT with a *different* UUID must succeed (cold scan finds no
    // duplicate; previously this was an Internal error because the cold scan
    // returned Decimal256(39,0) for the UUID column but
    // scalar_to_canonical_string only handled FixedSizeBinary(16)).
    exec_ok(
        &sess,
        &format!("INSERT INTO items (id, label) VALUES ('{uuid_b}', 'second')"),
    )
    .await;

    // Both rows should be readable.
    let batches = rows(&sess, "SELECT id, label FROM items ORDER BY label").await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 2,
        "both rows must be present after two distinct INSERTs"
    );

    // Duplicate INSERT of uuid_a must produce UniqueViolation.
    let err = sess
        .execute(&format!(
            "INSERT INTO items (id, label) VALUES ('{uuid_a}', 'dup')"
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::UniqueViolation(_)),
        "duplicate UUID PK from cold Vortex file must produce UniqueViolation; got {err:?}"
    );
}

/// UUID PK enforcement must also work for UNIQUE constraints (not just PRIMARY KEY),
/// ensuring the `enforce_unique_on_insert` cold-scan path handles Decimal256(39,0).
#[tokio::test]
async fn uuid_unique_dedup_across_cold_vortex_file() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;

    exec_ok(
        &sess,
        "CREATE TABLE orders (seq BIGINT PRIMARY KEY, ref_id UUID UNIQUE)",
    )
    .await;

    let uuid_x = "c2001122-3344-5566-7788-99aabbccddee";
    let uuid_y = "d3112233-4455-6677-8899-aabbccddeeff";

    exec_ok(
        &sess,
        &format!("INSERT INTO orders (seq, ref_id) VALUES (1, '{uuid_x}')"),
    )
    .await;

    // On-disk proof: cold Vortex file written.
    let vortex_files = files_with_ext(dir.path(), ".vortex");
    assert!(
        !vortex_files.is_empty(),
        "expected cold .vortex file after first INSERT; got none"
    );

    // Different UUID must be accepted.
    exec_ok(
        &sess,
        &format!("INSERT INTO orders (seq, ref_id) VALUES (2, '{uuid_y}')"),
    )
    .await;

    // Duplicate UUID in the UNIQUE column must produce UniqueViolation.
    let err = sess
        .execute(&format!(
            "INSERT INTO orders (seq, ref_id) VALUES (3, '{uuid_x}')"
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::UniqueViolation(_)),
        "duplicate UUID in UNIQUE column from cold Vortex file must produce UniqueViolation; got {err:?}"
    );
}
