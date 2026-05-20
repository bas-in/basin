//! Integration test: refcount-aware data-file GC.
//!
//! Covers two Phase 6 partial items that share one root mechanism:
//!
//! * **PITR physical GC** — after `rollback_to_snapshot`, files written by
//!   discarded snapshots have refcount 0 and appear in `gc_orphaned_files`.
//!   Files referenced by the surviving snapshot remain live.
//!
//! * **Fork-share safety** — `fork_table` shares data files by reference.
//!   Dropping the source table (or rolling it back) must NOT orphan files
//!   that the fork still references.  The GC sweep respects cross-table
//!   liveness within the same project.
//!
//! * **Cross-project isolation** — a path with the same name in two different
//!   projects is accounted for independently; one project's GC does not see
//!   the other project's live set.

#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::sync::Arc;

use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId};
use basin_common::{ProjectId, TableName};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_file(path: &str) -> DataFileRef {
    DataFileRef {
        path: path.to_string(),
        size_bytes: 1024,
        row_count: 100,
        column_stats: BTreeMap::new(),
        bloom_filters: BTreeMap::new(),
        hll_sketches: BTreeMap::new(),
        tdigest_sketches: BTreeMap::new(),
    }
}

fn tbl(name: &str) -> TableName {
    TableName::new(name).unwrap()
}

fn schema() -> arrow_schema::Schema {
    use arrow_schema::{DataType, Field};
    arrow_schema::Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, true),
    ])
}

// ---------------------------------------------------------------------------
// Test 1: PITR rollback → orphaned files physically gone, live files kept
// ---------------------------------------------------------------------------

/// After rolling back to snapshot 1, files written by snapshots 2 and 3
/// must appear as orphans; the file from snapshot 1 must remain live.
#[tokio::test]
async fn pitr_rollback_gc_identifies_orphans() {
    let cat = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = tbl("events");

    cat.create_namespace(&project).await.unwrap();
    cat.create_table(&project, &table, &schema()).await.unwrap();

    // Snapshot 1: file "events/snap1.parquet"
    cat.append_data_files(
        &project,
        &table,
        SnapshotId::GENESIS,
        vec![make_file("events/snap1.parquet")],
    )
    .await
    .unwrap();

    // Snapshot 2: file "events/snap2.parquet"
    cat.append_data_files(
        &project,
        &table,
        SnapshotId(1),
        vec![make_file("events/snap2.parquet")],
    )
    .await
    .unwrap();

    // Snapshot 3: file "events/snap3.parquet"
    cat.append_data_files(
        &project,
        &table,
        SnapshotId(2),
        vec![make_file("events/snap3.parquet")],
    )
    .await
    .unwrap();

    // PITR: roll back to snapshot 1.
    let meta = cat
        .rollback_to_snapshot(&project, &table, SnapshotId(1))
        .await
        .unwrap();
    assert_eq!(meta.current_snapshot, SnapshotId(1));
    assert_eq!(meta.snapshots.len(), 2, "only GENESIS and snap1 survive");

    // GC sweep.
    let report = cat.gc_orphaned_files(&project, &table).await.unwrap();

    // snap2 and snap3 must be orphaned.
    assert!(
        report
            .orphaned_paths
            .contains(&"events/snap2.parquet".to_string()),
        "snap2 must be orphaned after rollback, got {:?}",
        report.orphaned_paths
    );
    assert!(
        report
            .orphaned_paths
            .contains(&"events/snap3.parquet".to_string()),
        "snap3 must be orphaned after rollback, got {:?}",
        report.orphaned_paths
    );

    // snap1 must remain live.
    assert!(
        report
            .live_paths
            .contains(&"events/snap1.parquet".to_string()),
        "snap1 must remain live, got {:?}",
        report.live_paths
    );
    assert!(
        !report
            .orphaned_paths
            .contains(&"events/snap1.parquet".to_string()),
        "snap1 must NOT be orphaned, got {:?}",
        report.orphaned_paths
    );

    println!(
        "[pitr_rollback_gc] orphaned={:?} live={:?}",
        report.orphaned_paths, report.live_paths
    );
}

// ---------------------------------------------------------------------------
// Test 2: Fork shares files → dropping original does not delete fork's files
// ---------------------------------------------------------------------------

/// Fork inherits file references from source.  Rolling back the source to
/// GENESIS removes files from source's live set, but the fork still
/// references them.  The GC sweep must report them as live, not orphaned.
#[tokio::test]
async fn fork_share_safety_gc_respects_fork_refs() {
    let cat = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let src = tbl("orders_src");
    let dst = tbl("orders_fork");

    cat.create_namespace(&project).await.unwrap();
    cat.create_table(&project, &src, &schema()).await.unwrap();

    // Two appends into source.
    cat.append_data_files(
        &project,
        &src,
        SnapshotId::GENESIS,
        vec![make_file("orders/file_a.parquet")],
    )
    .await
    .unwrap();
    cat.append_data_files(
        &project,
        &src,
        SnapshotId(1),
        vec![make_file("orders/file_b.parquet")],
    )
    .await
    .unwrap();

    // Fork: dst inherits file_a and file_b by reference.
    cat.fork_table(&project, &src, &dst).await.unwrap();

    // Verify fork sees both files in its live set.
    let fork_meta = cat.load_table(&project, &dst).await.unwrap();
    let fork_live: Vec<_> = fork_meta
        .live_data_files()
        .into_iter()
        .map(|f| f.path)
        .collect();
    assert!(
        fork_live.contains(&"orders/file_a.parquet".to_string()),
        "fork must see file_a: {fork_live:?}"
    );
    assert!(
        fork_live.contains(&"orders/file_b.parquet".to_string()),
        "fork must see file_b: {fork_live:?}"
    );

    // Rollback source to GENESIS — file_a and file_b drop from source's live.
    cat.rollback_to_snapshot(&project, &src, SnapshotId::GENESIS)
        .await
        .unwrap();

    // GC on source: shared files must NOT be orphaned because the fork holds refs.
    let report = cat.gc_orphaned_files(&project, &src).await.unwrap();

    assert!(
        !report
            .orphaned_paths
            .contains(&"orders/file_a.parquet".to_string()),
        "file_a must not be orphaned while fork holds ref: {:?}",
        report.orphaned_paths
    );
    assert!(
        !report
            .orphaned_paths
            .contains(&"orders/file_b.parquet".to_string()),
        "file_b must not be orphaned while fork holds ref: {:?}",
        report.orphaned_paths
    );

    println!(
        "[fork_share_safety] orphaned={:?} live={:?}",
        report.orphaned_paths, report.live_paths
    );
}

// ---------------------------------------------------------------------------
// Test 3: Cross-project fork safety
// ---------------------------------------------------------------------------

/// A path appearing in project B's live set does not protect the same-named
/// path in project A after project A's rollback.  Projects are fully isolated.
#[tokio::test]
async fn cross_project_gc_isolation() {
    let cat = Arc::new(InMemoryCatalog::new());
    let proj_a = ProjectId::new();
    let proj_b = ProjectId::new();
    let tbl_a = tbl("xp_a");
    let tbl_b = tbl("xp_b");
    let shared_name = "shared/data.parquet";

    cat.create_namespace(&proj_a).await.unwrap();
    cat.create_namespace(&proj_b).await.unwrap();
    cat.create_table(&proj_a, &tbl_a, &schema()).await.unwrap();
    cat.create_table(&proj_b, &tbl_b, &schema()).await.unwrap();

    cat.append_data_files(
        &proj_a,
        &tbl_a,
        SnapshotId::GENESIS,
        vec![make_file(shared_name)],
    )
    .await
    .unwrap();
    cat.append_data_files(
        &proj_b,
        &tbl_b,
        SnapshotId::GENESIS,
        vec![make_file(shared_name)],
    )
    .await
    .unwrap();

    // Rollback project A to GENESIS — path becomes orphaned for proj_a.
    cat.rollback_to_snapshot(&proj_a, &tbl_a, SnapshotId::GENESIS)
        .await
        .unwrap();

    // GC for project A: path is orphaned.
    let report_a = cat.gc_orphaned_files(&proj_a, &tbl_a).await.unwrap();
    assert!(
        report_a
            .orphaned_paths
            .contains(&shared_name.to_string()),
        "proj_a GC must orphan path after rollback: {:?}",
        report_a.orphaned_paths
    );

    // GC for project B: path is still live (independent of proj_a's decision).
    let report_b = cat.gc_orphaned_files(&proj_b, &tbl_b).await.unwrap();
    assert!(
        report_b.live_paths.contains(&shared_name.to_string()),
        "proj_b GC must keep path live: {:?}",
        report_b.live_paths
    );
    assert!(
        !report_b.orphaned_paths.contains(&shared_name.to_string()),
        "proj_b must NOT orphan path: {:?}",
        report_b.orphaned_paths
    );

    println!(
        "[cross_project_gc] proj_a orphaned={:?}, proj_b live={:?}",
        report_a.orphaned_paths, report_b.live_paths
    );
}

// ---------------------------------------------------------------------------
// Test 4: file_refcount tracks shared references correctly
// ---------------------------------------------------------------------------

/// `file_refcount` reflects the number of tables (source + forks) whose live
/// set contains the path.  After dropping all references, refcount reaches 0.
#[tokio::test]
async fn file_refcount_tracks_fork_and_drop() {
    let cat = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let src = tbl("rc_src");
    let fork1 = tbl("rc_fork1");
    let fork2 = tbl("rc_fork2");

    cat.create_namespace(&project).await.unwrap();
    cat.create_table(&project, &src, &schema()).await.unwrap();

    cat.append_data_files(
        &project,
        &src,
        SnapshotId::GENESIS,
        vec![make_file("rc/file.parquet")],
    )
    .await
    .unwrap();

    // Initial refcount: only source.
    let rc0 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc0, 1, "refcount should be 1 (source only)");

    // Fork once → refcount 2.
    cat.fork_table(&project, &src, &fork1).await.unwrap();
    let rc1 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc1, 2, "refcount should be 2 (source + fork1)");

    // Fork again → refcount 3.
    cat.fork_table(&project, &src, &fork2).await.unwrap();
    let rc2 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc2, 3, "refcount should be 3 (source + fork1 + fork2)");

    // Drop source → refcount 2.
    cat.drop_table(&project, &src).await.unwrap();
    let rc3 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc3, 2, "refcount should be 2 after source drop");

    // Drop fork1 → refcount 1.
    cat.drop_table(&project, &fork1).await.unwrap();
    let rc4 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc4, 1, "refcount should be 1 after fork1 drop");

    // Drop fork2 → refcount 0.
    cat.drop_table(&project, &fork2).await.unwrap();
    let rc5 = cat
        .file_refcount(&project, "rc/file.parquet")
        .await
        .unwrap();
    assert_eq!(rc5, 0, "refcount should be 0 after all forks dropped");

    println!("[file_refcount_tracks] all assertions passed");
}
