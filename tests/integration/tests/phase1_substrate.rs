//! Phase 1 acceptance test.
//!
//! TASK.md calls out: "1M rows × 100 projects, round-trip + cross-project
//! isolation". This test wires `basin-storage` and `basin-catalog` together
//! against a `LocalFileSystem`-backed object store and exercises:
//!
//! * Project-scoped writes through both layers (storage produces the Parquet
//!   file, catalog records the new snapshot).
//! * Per-project reads that return only the calling project's rows.
//! * Snapshot monotonicity (each project's `current_snapshot` advances past
//!   `GENESIS` after one commit).
//! * Predicate pushdown (point lookup by `id` returns exactly one row).
//! * `list_tables` for project A never mentions project B's tables.
//!
//! Total volume is 100 projects × 10_000 rows = 1_000_000 rows. Writes fan
//! out across projects via `JoinSet` so wall time stays in the seconds range
//! even under `cargo test` (debug profile).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId, SnapshotOperation};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::task::JoinSet;

const PROJECT_COUNT: usize = 100;
const ROWS_PER_PROJECT: usize = 10_000;
const TABLE_NAME: &str = "events";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("project_marker", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(project: &ProjectId, rows: usize) -> RecordBatch {
    let ids: Int64Array = (0..rows as i64).collect();
    let marker_str = project.to_string();
    let markers: StringArray = (0..rows).map(|_| Some(marker_str.as_str())).collect();
    let payloads_owned: Vec<String> = (0..rows).map(|i| format!("p-{i}")).collect();
    let payloads: StringArray = payloads_owned.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(ids), Arc::new(markers), Arc::new(payloads)],
    )
    .unwrap()
}

fn storage_in(dir: &TempDir) -> Storage {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    Storage::new(StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_substrate_one_million_rows_one_hundred_projects() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let catalog = Arc::new(InMemoryCatalog::new());
    let table = TableName::new(TABLE_NAME).unwrap();
    let part = PartitionKey::default_key();

    let projects: Vec<ProjectId> = (0..PROJECT_COUNT).map(|_| ProjectId::new()).collect();

    // Phase A: provision every project's namespace + table in parallel.
    let mut prov: JoinSet<()> = JoinSet::new();
    for project in &projects {
        let cat = Arc::clone(&catalog);
        let project = *project;
        let table = table.clone();
        let sch = schema();
        prov.spawn(async move {
            cat.create_namespace(&project).await.unwrap();
            cat.create_table(&project, &table, &sch).await.unwrap();
        });
    }
    while let Some(r) = prov.join_next().await {
        r.unwrap();
    }

    // Phase B: write 10k rows per project, then commit the resulting data file
    // to the catalog. Run all 100 in parallel.
    let mut writers: JoinSet<(ProjectId, String, u64)> = JoinSet::new();
    for project in &projects {
        let storage = storage.clone();
        let cat = Arc::clone(&catalog);
        let project = *project;
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let batch = build_batch(&project, ROWS_PER_PROJECT);
            let df = storage
                .write_batch(&project, &table, &part, &batch)
                .await
                .unwrap();
            assert_eq!(df.row_count, ROWS_PER_PROJECT as u64);
            assert!(
                df.path.as_ref().contains(&format!("projects/{project}/")),
                "data file path missing project prefix: {}",
                df.path
            );

            let file_path = df.path.as_ref().to_owned();
            let bytes = df.size_bytes;
            let rows = df.row_count;

            let meta = cat
                .append_data_files(
                    &project,
                    &table,
                    SnapshotId::GENESIS,
                    vec![DataFileRef {
                        path: file_path.clone(),
                        size_bytes: bytes,
                        row_count: rows,
                        column_stats: df.column_stats.clone(),
                        hll_sketches: ::std::collections::BTreeMap::new(),
                        tdigest_sketches: ::std::collections::BTreeMap::new(),
                    }],
                )
                .await
                .unwrap();
            assert_ne!(meta.current_snapshot, SnapshotId::GENESIS);
            (project, file_path, rows)
        });
    }
    let mut written: Vec<(ProjectId, String, u64)> = Vec::with_capacity(PROJECT_COUNT);
    while let Some(r) = writers.join_next().await {
        written.push(r.unwrap());
    }
    assert_eq!(written.len(), PROJECT_COUNT);
    let total_rows: u64 = written.iter().map(|(_, _, r)| *r).sum();
    assert_eq!(total_rows as usize, PROJECT_COUNT * ROWS_PER_PROJECT);

    // Phase C: verify snapshot history per project — exactly two entries
    // (Genesis + one Append), `current` matches the Append, file count is 1.
    for project in &projects {
        let snaps = catalog.list_snapshots(project, &table).await.unwrap();
        assert_eq!(snaps.len(), 2, "project {project}: expected 2 snapshots");
        assert_eq!(snaps[0].summary.operation, SnapshotOperation::Genesis);
        assert_eq!(snaps[1].summary.operation, SnapshotOperation::Append);
        assert_eq!(snaps[1].data_files.len(), 1);
        assert_eq!(snaps[1].summary.added_rows, ROWS_PER_PROJECT as u64);

        let meta = catalog.load_table(project, &table).await.unwrap();
        assert_eq!(meta.current_snapshot, snaps[1].id);
        assert_eq!(meta.format_version, 2);
    }

    // Phase D: per-project full read. Each project returns exactly its own rows
    // and every row carries its own marker — a single leak across projects is
    // a P0 invariant violation per the build prompt's security model.
    let mut readers: JoinSet<(ProjectId, usize)> = JoinSet::new();
    for project in &projects {
        let storage = storage.clone();
        let table = table.clone();
        let project = *project;
        readers.spawn(async move {
            let mut stream = storage
                .read(&project, &table, ReadOptions::default())
                .await
                .unwrap();
            let mut rows = 0usize;
            let expected_marker = project.to_string();
            while let Some(b) = stream.next().await {
                let b = b.unwrap();
                rows += b.num_rows();
                let markers = b
                    .column_by_name("project_marker")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..markers.len() {
                    assert_eq!(
                        markers.value(i),
                        expected_marker,
                        "project {project}: row {i} carried marker {}, expected {}",
                        markers.value(i),
                        expected_marker
                    );
                }
            }
            (project, rows)
        });
    }
    while let Some(r) = readers.join_next().await {
        let (project, rows) = r.unwrap();
        assert_eq!(
            rows, ROWS_PER_PROJECT,
            "project {project} read {rows} rows, expected {ROWS_PER_PROJECT}"
        );
    }

    // Phase E: cross-project `list_tables` — each project sees exactly one
    // table; project A's list never references project B's table. (Same name,
    // different keys — the catalog must scope by project.)
    for project in &projects {
        let listed = catalog.list_tables(project).await.unwrap();
        assert_eq!(listed.len(), 1, "project {project}: expected 1 table");
        assert_eq!(listed[0], table);
    }

    // Phase F: predicate pushdown smoke test on one project. A point query for
    // `id = 42` returns exactly one row, and that row carries the right
    // marker. `list_data_files` for the project returns exactly one file.
    let probe = projects[0];
    let files = storage.list_data_files(&probe, &table).await.unwrap();
    assert_eq!(files.len(), 1);

    let mut stream = storage
        .read(
            &probe,
            &table,
            ReadOptions {
                projection: Some(vec!["id".into(), "project_marker".into()]),
                filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(42))],
                partition: None,
            },
        )
        .await
        .unwrap();
    let mut hit_rows = 0usize;
    while let Some(b) = stream.next().await {
        let b = b.unwrap();
        hit_rows += b.num_rows();
        if b.num_rows() > 0 {
            let ids = b
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..ids.len() {
                assert_eq!(ids.value(i), 42);
            }
            let markers = b
                .column_by_name("project_marker")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..markers.len() {
                assert_eq!(markers.value(i), probe.to_string());
            }
        }
    }
    assert_eq!(hit_rows, 1, "expected exactly one row for id = 42");

    // Phase G: stale-snapshot append must conflict. Re-using `GENESIS` after
    // we've already advanced past it is the textbook lost-update case.
    let stale_err = catalog
        .append_data_files(
            &probe,
            &table,
            SnapshotId::GENESIS,
            vec![DataFileRef {
                path: "projects/whatever/x.parquet".into(),
                size_bytes: 1,
                row_count: 1,
                column_stats: Default::default(),
                hll_sketches: ::std::collections::BTreeMap::new(),
                tdigest_sketches: ::std::collections::BTreeMap::new(),
            }],
        )
        .await
        .expect_err("expected CommitConflict on stale snapshot id");
    assert!(
        matches!(stale_err, basin_common::BasinError::CommitConflict(_)),
        "expected CommitConflict, got {stale_err:?}"
    );
}
