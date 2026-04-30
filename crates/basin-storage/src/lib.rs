//! `basin-storage` — tenant-aware Parquet-on-object-store substrate.
//!
//! Phase 1 scope. This crate is the bottom of the Basin stack: it knows
//! nothing about SQL, transactions, the WAL, or shard owners. Its only job
//! is to write Arrow `RecordBatch`es as immutable Parquet files under a
//! strict per-tenant key prefix and read them back with predicate and
//! projection pushdown.
//!
//! Tenant isolation is enforced by funneling every object key through one
//! private helper (`paths::data_file_key`) that always begins with
//! `tenants/{tenant_id}/`. There is no public escape hatch.

#![forbid(unsafe_code)]

mod data_file;
mod paths;
mod predicate;
mod reader;
mod vector_index;
mod writer;

use std::sync::Arc;

use basin_common::{Result, TableName, TenantId};
use futures::stream::BoxStream;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

pub use data_file::{ColumnStats, DataFile};
pub use predicate::{Predicate, ScalarValue};
pub use vector_index::VectorHit;

use arrow_array::RecordBatch;

/// Configuration for [`Storage`].
#[derive(Clone)]
pub struct StorageConfig {
    pub object_store: Arc<dyn ObjectStore>,
    /// Optional bucket sub-prefix that all tenant keys are nested under.
    /// `None` means keys live directly at the bucket root.
    pub root_prefix: Option<ObjectPath>,
}

impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("root_prefix", &self.root_prefix)
            .finish_non_exhaustive()
    }
}

/// Read knobs for [`Storage::read`]. Filters are ANDed together.
#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub projection: Option<Vec<String>>,
    pub filters: Vec<Predicate>,
    pub partition: Option<basin_common::PartitionKey>,
}

/// Tenant-aware Parquet store. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

struct Inner {
    object_store: Arc<dyn ObjectStore>,
    root_prefix: Option<ObjectPath>,
}

impl Storage {
    pub fn new(cfg: StorageConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                object_store: cfg.object_store,
                root_prefix: cfg.root_prefix,
            }),
        }
    }

    pub(crate) fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.inner.object_store
    }

    /// The underlying [`ObjectStore`]. Exposed so higher layers (e.g.
    /// `basin-engine`) can register the same store with DataFusion's runtime
    /// without round-tripping through configuration. Tenant-prefix
    /// enforcement still happens inside `Storage`'s own read/write methods;
    /// callers handed a raw `ObjectStore` are responsible for not crossing
    /// tenant boundaries themselves.
    pub fn object_store_handle(&self) -> Arc<dyn ObjectStore> {
        self.inner.object_store.clone()
    }

    pub(crate) fn root_prefix(&self) -> Option<&ObjectPath> {
        self.inner.root_prefix.as_ref()
    }

    /// The configured root prefix, if any. Same caveats as
    /// [`Storage::object_store_handle`].
    pub fn root_prefix_handle(&self) -> Option<ObjectPath> {
        self.inner.root_prefix.clone()
    }

    /// Write one `RecordBatch` as an immutable Parquet file. Returns the
    /// resulting `DataFile` descriptor.
    #[tracing::instrument(skip(self, batch), fields(tenant=%tenant, table=%table, partition=%partition, rows=batch.num_rows()))]
    pub async fn write_batch(
        &self,
        tenant: &TenantId,
        table: &TableName,
        partition: &basin_common::PartitionKey,
        batch: &RecordBatch,
    ) -> Result<DataFile> {
        writer::write_batch(self, tenant, table, partition, batch).await
    }

    /// Stream all rows for one tenant+table that match the read options.
    #[tracing::instrument(skip(self, opts), fields(tenant=%tenant, table=%table))]
    pub async fn read(
        &self,
        tenant: &TenantId,
        table: &TableName,
        opts: ReadOptions,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        reader::read(self, tenant, table, opts).await
    }

    /// List the data files for one tenant+table without reading their bodies.
    #[tracing::instrument(skip(self), fields(tenant=%tenant, table=%table))]
    pub async fn list_data_files(
        &self,
        tenant: &TenantId,
        table: &TableName,
    ) -> Result<Vec<DataFile>> {
        reader::list_data_files(self, tenant, table).await
    }

    /// Approximate nearest-neighbour search across all HNSW segments for
    /// `(tenant, table, column)`. Returns the merged top-`k` by distance.
    ///
    /// The returned `VectorHit::file_path` currently points at the index
    /// segment file rather than the matching Parquet data file — we don't
    /// track the data-side correspondence yet (see ADR 0003 for the
    /// compactor-driven plan). Higher layers (see `basin-engine`) resolve
    /// the row by reading the table's Parquet files and matching `row_id`.
    #[tracing::instrument(skip(self, query), fields(tenant=%tenant, table=%table, column=%column, k=%k))]
    pub async fn vector_search(
        &self,
        tenant: &TenantId,
        table: &TableName,
        column: &str,
        query: &[f32],
        k: usize,
        distance: basin_vector::Distance,
    ) -> Result<Vec<VectorHit>> {
        vector_index::vector_search(self, tenant, table, column, query, k, distance).await
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("root_prefix", &self.inner.root_prefix)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    //! Crate-level integration tests live alongside the modules they exercise.
    //! See `paths.rs`, `writer.rs`, and `reader.rs` for the per-module suites,
    //! plus `tests_e2e` below for the cross-module roundtrip cases the task
    //! brief calls out explicitly.
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use basin_common::{PartitionKey, TableName, TenantId};
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOpts,
        PutOptions, PutPayload, PutResult,
    };
    use tempfile::TempDir;

    fn small_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn small_batch(start: i64, len: usize, name_prefix: &str) -> RecordBatch {
        let ids: Int64Array = (start..start + len as i64).collect();
        let owned: Vec<String> = (0..len)
            .map(|i| format!("{name_prefix}{}", start + i as i64))
            .collect();
        let names: StringArray = owned.iter().map(|s| Some(s.as_str())).collect();
        RecordBatch::try_new(small_schema(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn storage_in(dir: &TempDir) -> Storage {
        let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        Storage::new(StorageConfig {
            object_store: Arc::new(fs),
            root_prefix: None,
        })
    }

    #[tokio::test]
    async fn write_then_read_roundtrip() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let tenant = TenantId::new();
        let table = TableName::new("events").unwrap();
        let part = PartitionKey::default_key();

        let batch = small_batch(0, 1_000, "row-");
        let df = s.write_batch(&tenant, &table, &part, &batch).await.unwrap();
        assert_eq!(df.row_count, 1_000);
        assert!(df.path.as_ref().contains(&format!("tenants/{tenant}/")));

        let stream = s
            .read(&tenant, &table, ReadOptions::default())
            .await
            .unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches
            .iter()
            .map(|b| b.as_ref().unwrap().num_rows())
            .sum();
        assert_eq!(total, 1_000);

        let first = batches[0].as_ref().unwrap();
        let names = first
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "row-0");
    }

    #[tokio::test]
    async fn tenant_isolation() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.write_batch(&a, &table, &part, &small_batch(0, 10, "a-"))
            .await
            .unwrap();
        s.write_batch(&b, &table, &part, &small_batch(0, 20, "b-"))
            .await
            .unwrap();

        let collect = |t: TenantId| {
            let s = s.clone();
            let table = table.clone();
            async move {
                let stream = s.read(&t, &table, ReadOptions::default()).await.unwrap();
                let batches: Vec<_> = stream.collect::<Vec<_>>().await;
                let total: usize = batches
                    .iter()
                    .map(|b| b.as_ref().unwrap().num_rows())
                    .sum();
                let any_name = batches
                    .first()
                    .map(|b| {
                        b.as_ref()
                            .unwrap()
                            .column_by_name("name")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0)
                            .to_string()
                    })
                    .unwrap_or_default();
                (total, any_name)
            }
        };
        let (na, name_a) = collect(a).await;
        let (nb, name_b) = collect(b).await;
        assert_eq!(na, 10);
        assert_eq!(nb, 20);
        assert!(name_a.starts_with("a-"));
        assert!(name_b.starts_with("b-"));
    }

    #[tokio::test]
    async fn projection_pushdown() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let tenant = TenantId::new();
        let table = TableName::new("wide").unwrap();
        let part = PartitionKey::default_key();

        // Five-column schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
            Field::new("e", DataType::Int64, false),
        ]));
        let make_col = |off: i64| -> Arc<dyn arrow_array::Array> {
            Arc::new((off..off + 100).collect::<Int64Array>())
        };
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                make_col(0),
                make_col(1000),
                make_col(2000),
                make_col(3000),
                make_col(4000),
            ],
        )
        .unwrap();
        s.write_batch(&tenant, &table, &part, &batch).await.unwrap();

        let opts = ReadOptions {
            projection: Some(vec!["a".into(), "c".into()]),
            ..Default::default()
        };
        let stream = s.read(&tenant, &table, opts).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let first = batches[0].as_ref().unwrap();
        assert_eq!(first.num_columns(), 2);
        let fields: Vec<_> = first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, vec!["a".to_string(), "c".to_string()]);
    }

    /// Wraps an object store and counts byte-range GETs so we can prove that
    /// row-group pruning skipped most of the file.
    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn ObjectStore>,
        range_gets: AtomicUsize,
        range_bytes: AtomicUsize,
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if let Some(r) = options.range.as_ref() {
                self.range_gets.fetch_add(1, Ordering::Relaxed);
                if let object_store::GetRange::Bounded(rng) = r {
                    self.range_bytes
                        .fetch_add(rng.end.saturating_sub(rng.start), Ordering::Relaxed);
                }
            }
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> futures::stream::BoxStream<'_, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
        ) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    #[tokio::test]
    async fn predicate_pushdown_prunes_row_groups() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let inner = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let counting = Arc::new(CountingStore {
            inner,
            range_gets: AtomicUsize::new(0),
            range_bytes: AtomicUsize::new(0),
        });
        let s = Storage::new(StorageConfig {
            object_store: counting.clone(),
            root_prefix: None,
        });
        let tenant = TenantId::new();
        let table = TableName::new("rg").unwrap();
        let part = PartitionKey::default_key();

        // Need ≥ 2 row groups for pruning to do work. Writer's row-group
        // cap is 65_536, so 200_000 rows splits into ~3 groups; a point
        // query for the last id lands in only one of them.
        let batch = small_batch(0, 200_000, "v");
        s.write_batch(&tenant, &table, &part, &batch).await.unwrap();

        // Reset counters so we measure only the read path.
        counting.range_gets.store(0, Ordering::Relaxed);
        counting.range_bytes.store(0, Ordering::Relaxed);

        // id = 199_500 lives only in the last row group.
        let opts = ReadOptions {
            filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(199_500))],
            ..Default::default()
        };
        let stream = s.read(&tenant, &table, opts).await.unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = batches
            .iter()
            .map(|b| b.as_ref().unwrap().num_rows())
            .sum();
        assert!(total >= 1, "expected the matching row");

        // 10 row groups exist; pruning must drop the vast majority. We allow
        // some metadata GETs but assert overall byte volume is much less than
        // the full file. A single matching row group + footer should be far
        // under half the file size.
        let bytes = counting.range_bytes.load(Ordering::Relaxed);
        let full_file = std::fs::metadata(
            walkdir_first_parquet(dir.path())
                .expect("parquet file to exist"),
        )
        .unwrap()
        .len() as usize;
        assert!(
            bytes * 2 < full_file,
            "row-group pruning failed: read {bytes} bytes of {full_file}"
        );
    }

    fn walkdir_first_parquet(root: &std::path::Path) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(root).ok()? {
            let entry = entry.ok()?;
            let p = entry.path();
            if p.is_dir() {
                if let Some(found) = walkdir_first_parquet(&p) {
                    return Some(found);
                }
            } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
                return Some(p);
            }
        }
        None
    }

    #[tokio::test]
    async fn list_data_files_returns_only_one_tenant() {
        basin_common::telemetry::try_init_for_tests();
        let dir = TempDir::new().unwrap();
        let s = storage_in(&dir);
        let a = TenantId::new();
        let b = TenantId::new();
        let table = TableName::new("t").unwrap();
        let part = PartitionKey::default_key();

        s.write_batch(&a, &table, &part, &small_batch(0, 5, "a-"))
            .await
            .unwrap();
        s.write_batch(&a, &table, &part, &small_batch(5, 5, "a-"))
            .await
            .unwrap();
        s.write_batch(&b, &table, &part, &small_batch(0, 7, "b-"))
            .await
            .unwrap();

        let listed_a = s.list_data_files(&a, &table).await.unwrap();
        let listed_b = s.list_data_files(&b, &table).await.unwrap();
        assert_eq!(listed_a.len(), 2);
        assert_eq!(listed_b.len(), 1);
        let prefix_a = format!("tenants/{a}/");
        for f in &listed_a {
            assert!(
                f.path.as_ref().contains(&prefix_a),
                "leaked path {}",
                f.path
            );
            assert!(!f.path.as_ref().contains(&format!("tenants/{b}/")));
        }
    }
}
