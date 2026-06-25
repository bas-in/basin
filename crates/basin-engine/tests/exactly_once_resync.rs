//! Exactly-once-under-retry regression tests.
//!
//! Background: the durable barrier (#34) made the engine no-LOSS — a committed
//! batch survives a crash. But durability is not the same as exactly-once. If a
//! client connection drops mid-COPY and the client RESYNCs by re-sending rows
//! it cannot prove were committed, the engine must not end up with duplicate
//! rows.
//!
//! These tests pin down Basin's exactly-once-under-retry contract:
//!
//!   * PRIMARY KEY / UNIQUE tables: a re-sent row that was already committed is
//!     rejected with a unique violation (existing-row enforcement reads ALL
//!     files via the object-store LIST, so it catches a duplicate written by a
//!     PRIOR statement, not just one inside the same batch). The table ends up
//!     with exactly one copy of each key regardless of how many times a client
//!     re-sends it.
//!
//!   * `INSERT ... ON CONFLICT (col) DO NOTHING`: the recommended client-
//!     agnostic resync shape. A re-send is SILENTLY absorbed (no error, no
//!     duplicate) — the conflict filter drops already-present keys before the
//!     unique check runs. This is what a retry-safe loader should issue.
//!
//!   * Keyless (no-PK, no-UNIQUE) tables: the engine has NO key to dedup on, so
//!     a blind re-send DOES duplicate rows. This is the documented limitation
//!     (see `docs/durability.md` / `CAPABILITIES.md`): keyless bulk loads must
//!     declare a key (or use ON CONFLICT) to be retry-safe. The test below
//!     locks that behavior in so it cannot silently change.

use std::sync::Arc;

use arrow_array::Int64Array;
use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
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

/// Total rows across all batches of a SELECT.
async fn count(sess: &ProjectSession, table: &str) -> usize {
    match sess
        .execute(&format!("SELECT * FROM {table}"))
        .await
        .unwrap()
    {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        other => panic!("unexpected: {other:?}"),
    }
}

/// Distinct values of an Int64 column across all batches of a SELECT.
async fn distinct_i64(sess: &ProjectSession, sql: &str, col: &str) -> Vec<i64> {
    let batches = match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("unexpected: {other:?}"),
    };
    let mut out: Vec<i64> = Vec::new();
    for b in &batches {
        let arr = b
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i));
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

// ── PRIMARY KEY: re-send of a committed row is rejected, no duplicate ────────

#[tokio::test]
async fn pk_resync_resend_is_rejected_no_duplicate() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, payload BIGINT NOT NULL)")
        .await
        .unwrap();

    // "First send" — committed.
    sess.execute("INSERT INTO events VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    // Connection drops; the client cannot prove the commit landed and RESYNCs
    // by re-sending an OVERLAPPING batch (ids 2,3 already committed; 4 is new).
    // Because the existing-row PK check reads all committed files, the re-send
    // is rejected as a whole — the engine never admits the duplicate 2,3.
    let err = sess
        .execute("INSERT INTO events VALUES (2, 200), (3, 300), (4, 400)")
        .await
        .unwrap_err();
    assert!(
        matches!(err, BasinError::UniqueViolation(_)),
        "expected the resync re-send to be rejected as a unique violation, got {err:?}"
    );

    // Final state: exactly the originally committed rows, no duplicates. The
    // rejected re-send was all-or-nothing (matches PG INSERT statement atomicity).
    assert_eq!(count(&sess, "events").await, 3, "no duplicate rows admitted");
    assert_eq!(
        distinct_i64(&sess, "SELECT id FROM events", "id").await,
        vec![1, 2, 3],
        "keys are exactly-once"
    );
}

// ── ON CONFLICT DO NOTHING: the retry-safe resync shape — silently absorbed ──

#[tokio::test]
async fn on_conflict_do_nothing_resync_is_exactly_once() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute("CREATE TABLE events (id BIGINT PRIMARY KEY, payload BIGINT NOT NULL)")
        .await
        .unwrap();

    // A retry-safe loader issues ON CONFLICT DO NOTHING so a resync re-send is
    // idempotent rather than an error.
    sess.execute("INSERT INTO events VALUES (1, 100), (2, 200), (3, 300) ON CONFLICT (id) DO NOTHING")
        .await
        .unwrap();

    // RESYNC: re-send the overlapping batch (2,3 already committed) plus new 4.
    // DO NOTHING drops the already-present keys; 4 is admitted. No error.
    sess.execute("INSERT INTO events VALUES (2, 200), (3, 300), (4, 400) ON CONFLICT (id) DO NOTHING")
        .await
        .unwrap();

    // A second, fully-overlapping resync (a paranoid client retrying again):
    // every key already present → 0 rows admitted, still no error.
    sess.execute("INSERT INTO events VALUES (1, 100), (2, 200), (3, 300), (4, 400) ON CONFLICT (id) DO NOTHING")
        .await
        .unwrap();

    assert_eq!(
        count(&sess, "events").await,
        4,
        "every key stored exactly once across repeated resyncs"
    );
    assert_eq!(
        distinct_i64(&sess, "SELECT id FROM events", "id").await,
        vec![1, 2, 3, 4],
        "keys are exactly-once"
    );
}

// ── Composite PK behaves the same under resync ───────────────────────────────

#[tokio::test]
async fn composite_pk_resync_exactly_once_via_on_conflict() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    sess.execute(
        "CREATE TABLE readings (sensor BIGINT NOT NULL, ts BIGINT NOT NULL, v BIGINT NOT NULL, \
         PRIMARY KEY (sensor, ts))",
    )
    .await
    .unwrap();

    sess.execute(
        "INSERT INTO readings VALUES (1, 10, 5), (1, 11, 6) \
         ON CONFLICT (sensor, ts) DO NOTHING",
    )
    .await
    .unwrap();

    // Resync re-send overlapping (1,11) + new (1,12).
    sess.execute(
        "INSERT INTO readings VALUES (1, 11, 6), (1, 12, 7) \
         ON CONFLICT (sensor, ts) DO NOTHING",
    )
    .await
    .unwrap();

    assert_eq!(count(&sess, "readings").await, 3, "composite key exactly-once");
}

// ── Documented limitation: keyless re-send DOES duplicate ───────────────────
//
// This is intentionally NOT a "fix me" — it documents why a key (PK / UNIQUE)
// or ON CONFLICT is REQUIRED for retry-safety. The engine has no key to dedup a
// keyless re-send against; a blind resync of a keyless table duplicates rows.
// Locked in so the keyless contract can't silently drift.

#[tokio::test]
async fn keyless_blind_resend_duplicates_documents_the_limitation() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = eng.open_session(ProjectId::new()).await.unwrap();

    // No PRIMARY KEY, no UNIQUE — a pure append table.
    sess.execute("CREATE TABLE log (id BIGINT NOT NULL, payload BIGINT NOT NULL)")
        .await
        .unwrap();

    sess.execute("INSERT INTO log VALUES (1, 100), (2, 200), (3, 300)")
        .await
        .unwrap();

    // Blind resync re-send of the same rows. With no key, the engine cannot
    // tell these are re-sends — they are admitted again.
    sess.execute("INSERT INTO log VALUES (2, 200), (3, 300), (4, 400)")
        .await
        .unwrap();

    // 3 + 3 = 6 physical rows; ids 2 and 3 are duplicated. This is the gap a
    // keyless loader must close client-side (declare a key, or ON CONFLICT).
    assert_eq!(
        count(&sess, "log").await,
        6,
        "keyless re-send duplicates: this is why retry-safety needs a key"
    );
}
