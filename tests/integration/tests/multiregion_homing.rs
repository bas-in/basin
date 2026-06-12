//! Multi-region homing — commits 8/9/10 (ADR 0009).
//!
//! Verifies the per-region write gate, the `basin.read_tier` GUC, and the
//! `WriteForwarder` seam against an in-process engine over `InMemoryCatalog`.
//!
//! ## Why these tests are region-agnostic
//!
//! `basin_common::local_region()` reads `BASIN_REGION` exactly once and caches
//! it process-wide (OnceLock), so a test cannot flip the *local* region after
//! the first read. Instead every test reads `local_region()` and pins the
//! project's `home_region` RELATIVE to it:
//!
//!   * home == local_region()        → at home (writes/primary reads allowed),
//!   * home == "<local>__elsewhere"  → non-home (writes/primary reads rejected,
//!                                     lagging reads allowed),
//!   * home == None                  → legacy/unpinned (local everywhere).
//!
//! This makes the suite deterministic regardless of how the harness sets (or
//! leaves unset) `BASIN_REGION`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;

use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog, ProjectMetadata};
use basin_common::{local_region, BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ExecResult, ForwardContext, WriteForwarder};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

fn build_engine(dir: &TempDir) -> (Engine, Arc<InMemoryCatalog>) {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let cat = Arc::new(InMemoryCatalog::new());
    let catalog: Arc<dyn basin_catalog::Catalog> = cat.clone();
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (engine, cat)
}

/// A region string guaranteed != local_region().
fn foreign_region() -> String {
    format!("{}__elsewhere", local_region())
}

async fn pin_home(cat: &InMemoryCatalog, project: &ProjectId, home: Option<String>) {
    cat.set_project_metadata(
        project,
        ProjectMetadata {
            byo_bucket: None,
            home_region: home,
        },
    )
    .await
    .unwrap();
}

/// Mock forwarder: records the forwarded SQL + context and returns a sentinel
/// success result so the test can prove the engine delegated rather than
/// raising WrongRegion.
struct MockForwarder {
    seen: std::sync::Mutex<Vec<(String, String, String)>>, // (sql, home_region, current_user)
}

impl MockForwarder {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seen: std::sync::Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl WriteForwarder for MockForwarder {
    async fn forward_write(&self, ctx: ForwardContext<'_>) -> Result<ExecResult, BasinError> {
        self.seen.lock().unwrap().push((
            ctx.sql.to_string(),
            ctx.home_region.to_string(),
            ctx.current_user.to_string(),
        ));
        Ok(ExecResult::Empty {
            tag: "FORWARDED".into(),
        })
    }
}

fn tag_of(r: &ExecResult) -> String {
    match r {
        ExecResult::Empty { tag } => tag.clone(),
        ExecResult::Rows { .. } => "<rows>".into(),
    }
}

// ---------------------------------------------------------------------------
// Commit 8 — write gate
// ---------------------------------------------------------------------------

/// A write to a project homed in THIS region is accepted.
#[tokio::test]
async fn home_region_write_accepted() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();
    pin_home(&cat, &project, Some(local_region().to_string())).await;

    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();
    // The write (INSERT) succeeds in the home region.
    s.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
    let r = s.execute("SELECT count(*) FROM t").await.unwrap();
    // One row inserted, readable in the home region.
    assert!(matches!(r, ExecResult::Rows { .. }));
}

/// A write to a project homed ELSEWHERE, with no forwarder, raises WrongRegion
/// with BOTH region names in the message.
#[tokio::test]
async fn non_home_write_rejected_with_region_names() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();
    // Create the table while homed locally, THEN re-pin elsewhere, so the
    // table exists and only the region gate (not a missing-table error) fires.
    pin_home(&cat, &project, Some(local_region().to_string())).await;
    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();

    let foreign = foreign_region();
    pin_home(&cat, &project, Some(foreign.clone())).await;

    let err = s
        .execute("INSERT INTO t (id) VALUES (1)")
        .await
        .unwrap_err();
    match err {
        BasinError::WrongRegion(msg) => {
            assert!(msg.contains(&foreign), "message names home region: {msg}");
            assert!(
                msg.contains(local_region()),
                "message names local region: {msg}"
            );
        }
        other => panic!("expected WrongRegion, got {other:?}"),
    }

    // An UPDATE/DELETE/DDL is gated the same way.
    let err2 = s.execute("DROP TABLE t").await.unwrap_err();
    assert!(matches!(err2, BasinError::WrongRegion(_)));
}

/// Legacy / unpinned project (home_region == None) is treated as local: writes
/// and reads both succeed regardless of region.
#[tokio::test]
async fn legacy_project_treated_as_local() {
    let dir = TempDir::new().unwrap();
    let (engine, _cat) = build_engine(&dir);
    let project = ProjectId::new();
    // No set_project_metadata at all ⇒ get returns default ⇒ home_region None.

    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();
    s.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();
    let r = s.execute("SELECT count(*) FROM t").await.unwrap();
    assert!(matches!(r, ExecResult::Rows { .. }));
}

// ---------------------------------------------------------------------------
// Commit 9 — read tier
// ---------------------------------------------------------------------------

/// `SET` / `SHOW basin.read_tier` round-trips; `RESET ALL` restores the default.
#[tokio::test]
async fn read_tier_set_show_reset() {
    let dir = TempDir::new().unwrap();
    let (engine, _cat) = build_engine(&dir);
    let s = engine.open_session(ProjectId::new()).await.unwrap();

    // Default is 'primary'.
    let shown = s.execute("SHOW basin.read_tier").await.unwrap();
    assert_eq!(read_first_string(&shown), "primary");

    s.execute("SET basin.read_tier = 'lagging'").await.unwrap();
    let shown = s.execute("SHOW basin.read_tier").await.unwrap();
    assert_eq!(read_first_string(&shown), "lagging");

    // RESET ALL restores the default tier.
    s.execute("RESET ALL").await.unwrap();
    let shown = s.execute("SHOW basin.read_tier").await.unwrap();
    assert_eq!(read_first_string(&shown), "primary");

    // A bad value errors rather than silently downgrading consistency.
    let err = s.execute("SET basin.read_tier = 'banana'").await.unwrap_err();
    assert!(matches!(err, BasinError::InvalidSchema(_)));
}

/// A `lagging` read of a NON-home project is served (flushed/cold data), not
/// rejected. The data was flushed while the project was homed locally, then the
/// project re-pinned elsewhere — modelling S3-replicated cold data visible in a
/// non-home region.
#[tokio::test]
async fn lagging_read_non_home_serves_flushed_data() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();

    // Write + read while homed locally.
    pin_home(&cat, &project, Some(local_region().to_string())).await;
    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();
    s.execute("INSERT INTO t (id) VALUES (1), (2), (3)")
        .await
        .unwrap();

    // Re-pin elsewhere: this session is now in a non-home region.
    pin_home(&cat, &project, Some(foreign_region())).await;

    // Lagging tier: read is allowed despite being non-home.
    s.execute("SET basin.read_tier = 'lagging'").await.unwrap();
    let r = s.execute("SELECT count(*) FROM t").await.unwrap();
    assert!(
        matches!(r, ExecResult::Rows { .. }),
        "lagging read on a non-home project must serve flushed data, not reject"
    );
}

/// A `primary` (default) read of a NON-home project is rejected with
/// WrongRegion — only the home region has the live tail.
#[tokio::test]
async fn primary_read_non_home_rejected() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();

    pin_home(&cat, &project, Some(local_region().to_string())).await;
    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();

    pin_home(&cat, &project, Some(foreign_region())).await;

    // Default tier is 'primary' — a non-home primary read must reject.
    let err = s.execute("SELECT count(*) FROM t").await.unwrap_err();
    match err {
        BasinError::WrongRegion(msg) => {
            assert!(msg.contains("primary-tier read"), "msg: {msg}");
            assert!(msg.contains(&foreign_region()), "msg names home: {msg}");
        }
        other => panic!("expected WrongRegion for primary read, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Commit 10 — forwarder seam
// ---------------------------------------------------------------------------

/// With a forwarder registered, a non-home write delegates to it (returns the
/// forwarder's result) instead of raising WrongRegion.
#[tokio::test]
async fn forwarder_registered_delegates_non_home_write() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();

    pin_home(&cat, &project, Some(local_region().to_string())).await;
    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();

    let foreign = foreign_region();
    pin_home(&cat, &project, Some(foreign.clone())).await;

    // Register the mock forwarder.
    let fwd = MockForwarder::new();
    engine.attach_write_forwarder(fwd.clone());

    // The non-home INSERT now forwards instead of erroring.
    let r = s.execute("INSERT INTO t (id) VALUES (9)").await.unwrap();
    assert_eq!(tag_of(&r), "FORWARDED");

    let seen = fwd.seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "forwarder called exactly once");
    let (sql, home, _user) = &seen[0];
    assert!(sql.contains("INSERT INTO t"), "forwarded sql: {sql}");
    assert_eq!(home, &foreign, "forwarded to the home region");
}

/// A non-home write INSIDE an explicit transaction is NOT forwarded (cross-
/// region transactional semantics are a non-goal) — it fails loud even with a
/// forwarder registered.
#[tokio::test]
async fn non_home_write_in_txn_not_forwarded() {
    let dir = TempDir::new().unwrap();
    let (engine, cat) = build_engine(&dir);
    let project = ProjectId::new();

    pin_home(&cat, &project, Some(local_region().to_string())).await;
    let s = engine.open_session(project).await.unwrap();
    s.execute("CREATE TABLE t (id INT)").await.unwrap();
    pin_home(&cat, &project, Some(foreign_region())).await;

    let fwd = MockForwarder::new();
    engine.attach_write_forwarder(fwd.clone());

    s.execute("BEGIN").await.unwrap();
    let err = s
        .execute("INSERT INTO t (id) VALUES (1)")
        .await
        .unwrap_err();
    assert!(matches!(err, BasinError::WrongRegion(_)));
    assert!(
        fwd.seen.lock().unwrap().is_empty(),
        "an in-transaction write must not be forwarded"
    );
}

// ---------------------------------------------------------------------------
// Catalog round-trip — InMemory backend (Postgres covered by metadata serde
// unit test in basin-catalog)
// ---------------------------------------------------------------------------

/// `home_region` set on `ProjectMetadata` survives a set/get round-trip on the
/// in-memory catalog (and an unset field defaults to None).
#[tokio::test]
async fn home_region_catalog_round_trip_in_memory() {
    let cat = InMemoryCatalog::new();
    let project = ProjectId::new();

    // Default: no metadata ⇒ home_region None.
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert_eq!(got.home_region, None);

    // Round-trip a pinned region.
    cat.set_project_metadata(
        &project,
        ProjectMetadata {
            byo_bucket: None,
            home_region: Some("us-east-1".to_string()),
        },
    )
    .await
    .unwrap();
    let got = cat.get_project_metadata(&project).await.unwrap();
    assert_eq!(got.home_region.as_deref(), Some("us-east-1"));
}

fn read_first_string(r: &ExecResult) -> String {
    match r {
        ExecResult::Rows { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .map(|a| a.value(0).to_string())
            })
            .next()
            .unwrap_or_default(),
        ExecResult::Empty { .. } => String::new(),
    }
}
