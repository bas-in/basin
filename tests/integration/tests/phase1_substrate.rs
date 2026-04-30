//! Phase 1 acceptance test.
//!
//! TASK.md calls out: "1M rows × 100 tenants, round-trip + cross-tenant
//! isolation". This test wires `basin-storage` and `basin-catalog` together
//! against a `LocalFileSystem`-backed object store and exercises:
//!
//! * Tenant-scoped writes through both layers (storage produces the Parquet
//!   file, catalog records the new snapshot).
//! * Per-tenant reads that return only the calling tenant's rows.
//! * Snapshot monotonicity (each tenant's `current_snapshot` advances past
//!   `GENESIS` after one commit).
//! * Predicate pushdown (point lookup by `id` returns exactly one row).
//! * `list_tables` for tenant A never mentions tenant B's tables.
//!
//! Total volume is 100 tenants × 10_000 rows = 1_000_000 rows. Writes fan
//! out across tenants via `JoinSet` so wall time stays in the seconds range
//! even under `cargo test` (debug profile).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog, SnapshotId, SnapshotOperation};
use basin_common::{PartitionKey, TableName, TenantId};
use basin_storage::{Predicate, ReadOptions, ScalarValue, Storage, StorageConfig};
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;
use tokio::task::JoinSet;

const TENANT_COUNT: usize = 100;
const ROWS_PER_TENANT: usize = 10_000;
const TABLE_NAME: &str = "events";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tenant_marker", DataType::Utf8, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(tenant: &TenantId, rows: usize) -> RecordBatch {
    let ids: Int64Array = (0..rows as i64).collect();
    let marker_str = tenant.to_string();
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
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase1_substrate_one_million_rows_one_hundred_tenants() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let storage = storage_in(&dir);
    let catalog = Arc::new(InMemoryCatalog::new());
    let table = TableName::new(TABLE_NAME).unwrap();
    let part = PartitionKey::default_key();

    let tenants: Vec<TenantId> = (0..TENANT_COUNT).map(|_| TenantId::new()).collect();

    // Phase A: provision every tenant's namespace + table in parallel.
    let mut prov: JoinSet<()> = JoinSet::new();
    for tenant in &tenants {
        let cat = Arc::clone(&catalog);
        let tenant = *tenant;
        let table = table.clone();
        let sch = schema();
        prov.spawn(async move {
            cat.create_namespace(&tenant).await.unwrap();
            cat.create_table(&tenant, &table, &sch).await.unwrap();
        });
    }
    while let Some(r) = prov.join_next().await {
        r.unwrap();
    }

    // Phase B: write 10k rows per tenant, then commit the resulting data file
    // to the catalog. Run all 100 in parallel.
    let mut writers: JoinSet<(TenantId, String, u64)> = JoinSet::new();
    for tenant in &tenants {
        let storage = storage.clone();
        let cat = Arc::clone(&catalog);
        let tenant = *tenant;
        let table = table.clone();
        let part = part.clone();
        writers.spawn(async move {
            let batch = build_batch(&tenant, ROWS_PER_TENANT);
            let df = storage
                .write_batch(&tenant, &table, &part, &batch)
                .await
                .unwrap();
            assert_eq!(df.row_count, ROWS_PER_TENANT as u64);
            assert!(
                df.path.as_ref().contains(&format!("tenants/{tenant}/")),
                "data file path missing tenant prefix: {}",
                df.path
            );

            let file_path = df.path.as_ref().to_owned();
            let bytes = df.size_bytes;
            let rows = df.row_count;

            let meta = cat
                .append_data_files(
                    &tenant,
                    &table,
                    SnapshotId::GENESIS,
                    vec![DataFileRef {
                        path: file_path.clone(),
                        size_bytes: bytes,
                        row_count: rows,
                    }],
                )
                .await
                .unwrap();
            assert_ne!(meta.current_snapshot, SnapshotId::GENESIS);
            (tenant, file_path, rows)
        });
    }
    let mut written: Vec<(TenantId, String, u64)> = Vec::with_capacity(TENANT_COUNT);
    while let Some(r) = writers.join_next().await {
        written.push(r.unwrap());
    }
    assert_eq!(written.len(), TENANT_COUNT);
    let total_rows: u64 = written.iter().map(|(_, _, r)| *r).sum();
    assert_eq!(total_rows as usize, TENANT_COUNT * ROWS_PER_TENANT);

    // Phase C: verify snapshot history per tenant — exactly two entries
    // (Genesis + one Append), `current` matches the Append, file count is 1.
    for tenant in &tenants {
        let snaps = catalog.list_snapshots(tenant, &table).await.unwrap();
        assert_eq!(snaps.len(), 2, "tenant {tenant}: expected 2 snapshots");
        assert_eq!(snaps[0].summary.operation, SnapshotOperation::Genesis);
        assert_eq!(snaps[1].summary.operation, SnapshotOperation::Append);
        assert_eq!(snaps[1].data_files.len(), 1);
        assert_eq!(snaps[1].summary.added_rows, ROWS_PER_TENANT as u64);

        let meta = catalog.load_table(tenant, &table).await.unwrap();
        assert_eq!(meta.current_snapshot, snaps[1].id);
        assert_eq!(meta.format_version, 2);
    }

    // Phase D: per-tenant full read. Each tenant returns exactly its own rows
    // and every row carries its own marker — a single leak across tenants is
    // a P0 invariant violation per the build prompt's security model.
    let mut readers: JoinSet<(TenantId, usize)> = JoinSet::new();
    for tenant in &tenants {
        let storage = storage.clone();
        let table = table.clone();
        let tenant = *tenant;
        readers.spawn(async move {
            let mut stream = storage
                .read(&tenant, &table, ReadOptions::default())
                .await
                .unwrap();
            let mut rows = 0usize;
            let expected_marker = tenant.to_string();
            while let Some(b) = stream.next().await {
                let b = b.unwrap();
                rows += b.num_rows();
                let markers = b
                    .column_by_name("tenant_marker")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for i in 0..markers.len() {
                    assert_eq!(
                        markers.value(i),
                        expected_marker,
                        "tenant {tenant}: row {i} carried marker {}, expected {}",
                        markers.value(i),
                        expected_marker
                    );
                }
            }
            (tenant, rows)
        });
    }
    while let Some(r) = readers.join_next().await {
        let (tenant, rows) = r.unwrap();
        assert_eq!(
            rows, ROWS_PER_TENANT,
            "tenant {tenant} read {rows} rows, expected {ROWS_PER_TENANT}"
        );
    }

    // Phase E: cross-tenant `list_tables` — each tenant sees exactly one
    // table; tenant A's list never references tenant B's table. (Same name,
    // different keys — the catalog must scope by tenant.)
    for tenant in &tenants {
        let listed = catalog.list_tables(tenant).await.unwrap();
        assert_eq!(listed.len(), 1, "tenant {tenant}: expected 1 table");
        assert_eq!(listed[0], table);
    }

    // Phase F: predicate pushdown smoke test on one tenant. A point query for
    // `id = 42` returns exactly one row, and that row carries the right
    // marker. `list_data_files` for the tenant returns exactly one file.
    let probe = tenants[0];
    let files = storage.list_data_files(&probe, &table).await.unwrap();
    assert_eq!(files.len(), 1);

    let mut stream = storage
        .read(
            &probe,
            &table,
            ReadOptions {
                projection: Some(vec!["id".into(), "tenant_marker".into()]),
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
                .column_by_name("tenant_marker")
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
                path: "tenants/whatever/x.parquet".into(),
                size_bytes: 1,
                row_count: 1,
            }],
        )
        .await
        .expect_err("expected CommitConflict on stale snapshot id");
    assert!(
        matches!(stale_err, basin_common::BasinError::CommitConflict(_)),
        "expected CommitConflict, got {stale_err:?}"
    );
}
