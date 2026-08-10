//! LIKE-prefix (`Predicate::StartsWith`) zone-map pruning, end to end.
//!
//! Investigation context (the "100k LIKE prefix spike"): a
//! `WHERE col LIKE 'x%'` query is lowered to `Predicate::StartsWith` and
//! pushed into the storage layer. The storage layer already converts a
//! prefix to a lex range `[prefix, prefix_end)` and uses the per-file and
//! per-row-group min/max zone maps to skip groups that provably cannot
//! contain the prefix — the same shape PostgreSQL's btree prefix scan
//! uses. See `predicate::prune_starts_with` (file/catalog level) and
//! `reader::prune_starts_with_row_group` (Parquet row-group level).
//!
//! These tests pin TWO facts with GENERIC prefixes (no bench literal):
//!
//!   1. When a prefix is *confined* to a subset of row groups (the data is
//!      laid out so each group covers a disjoint key range), the zone-map
//!      range prune skips the non-matching groups
//!      (`row_groups_pruned_by_stats > 0`). This is the win.
//!
//!   2. When the matching rows are *uniformly interleaved* across every
//!      row group — `value = bucket[i % N]` so a given bucket appears in
//!      EVERY group — the per-group min/max straddles the prefix in every
//!      group, so zone maps provably cannot skip anything
//!      (`row_groups_pruned_by_stats == 0`). This is NOT a pruning bug: a
//!      group that genuinely contains a matching row must be read. This is
//!      the documented root cause of the benchmark's scale-dependent LIKE
//!      prefix spike — the bench's status column is `bucket[i % 4]`, so the
//!      matched prefix lives in every row group and every per-scale file,
//!      leaving zone maps nothing to prune. The cost is intrinsic to a
//!      uniformly-interleaved low-cardinality column, not a missing prune.
//!
//! The fairness rule: every assertion below keys on the predicate
//! STRUCTURE (a prefix `StartsWith` predicate) and uses synthetic prefixes
//! ("alpha", "mango", "row-…", "k…"). No bench-specific value or column
//! name appears anywhere.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_storage::{FileFormat, Predicate, ReadOptions, Storage, StorageConfig, WriteOptions};
use futures::stream::{BoxStream, StreamExt};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// Object store that counts read-path RPCs so file-level pruning is
/// observable (a pruned file is never opened => no GET against it).
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    get_count: AtomicUsize,
    range_get_count: AtomicUsize,
    head_count: AtomicUsize,
    list_count: AtomicUsize,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            get_count: AtomicUsize::new(0),
            range_get_count: AtomicUsize::new(0),
            head_count: AtomicUsize::new(0),
            list_count: AtomicUsize::new(0),
        })
    }

    fn total_body_io(&self) -> usize {
        self.get_count.load(Ordering::Relaxed)
            + self.range_get_count.load(Ordering::Relaxed)
            + self.head_count.load(Ordering::Relaxed)
    }

    fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.range_get_count.store(0, Ordering::Relaxed);
        self.head_count.store(0, Ordering::Relaxed);
        self.list_count.store(0, Ordering::Relaxed);
    }
}

#[async_trait]
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
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.head_count.fetch_add(1, Ordering::Relaxed);
        } else if options.range.is_some() {
            self.range_get_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.get_count.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.list_count.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]))
}

fn batch_from_labels(start_id: i64, labels: &[String]) -> RecordBatch {
    let ids: Int64Array = (start_id..start_id + labels.len() as i64).collect();
    let label_arr: StringArray = labels.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(label_arr)]).unwrap()
}

async fn build() -> (Arc<CountingStore>, Storage, ProjectId, TableName) {
    let inner = Arc::new(InMemory::new());
    let counting = CountingStore::new(inner);
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        // Caches off so every fetch shows up against the wrapped store and
        // row-group pruning is measured on a cold read each time.
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let project = ProjectId::new();
    let table = TableName::new("labels").unwrap();
    catalog
        .create_table(&project, &table, &schema())
        .await
        .unwrap();
    storage.attach_catalog(catalog);
    (counting, storage, project, table)
}

async fn count_rows(stream: BoxStream<'static, basin_common::Result<RecordBatch>>) -> usize {
    let mut s = stream;
    let mut n = 0usize;
    while let Some(b) = s.next().await {
        n += b.unwrap().num_rows();
    }
    n
}

/// Force several Parquet row groups in ONE file by capping the row-group
/// size, with each group covering a disjoint sorted key range. A prefix
/// that lands inside only one group's `[min, max]` must prune the rest via
/// the zone-map range derived from `[prefix, prefix_end)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn starts_with_prunes_row_groups_when_prefix_is_clustered() {
    basin_common::telemetry::try_init_for_tests();
    let (_counting, storage, project, table) = build().await;
    let part = PartitionKey::default_key();

    // 4 disjoint, sorted alphabetic clusters of 50 labels each. Capping the
    // row group at 50 rows yields one row group per cluster, so each group's
    // min/max brackets exactly one letter family.
    //   group 0: "alpha000".."alpha049"
    //   group 1: "kilo000" .."kilo049"
    //   group 2: "mango000".."mango049"
    //   group 3: "zulu000" .."zulu049"
    let families = ["alpha", "kilo", "mango", "zulu"];
    let mut labels: Vec<String> = Vec::new();
    for fam in families {
        for i in 0..50 {
            labels.push(format!("{fam}{i:03}"));
        }
    }
    let batch = batch_from_labels(0, &labels);
    storage
        .write_batch_with_options(
            &project,
            &table,
            &part,
            &batch,
            &WriteOptions {
                file_format: FileFormat::Parquet,
                max_row_group_size: Some(50),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Cold read so the row-group prune actually runs on this read.
    storage.read_counters().reset();
    let opts = ReadOptions {
        filters: vec![Predicate::StartsWith {
            column: "label".into(),
            prefix: "mango".into(),
            case_insensitive: false,
        }],
        ..Default::default()
    };
    let rows = count_rows(storage.read(&project, &table, opts).await.unwrap()).await;
    assert_eq!(rows, 50, "every 'mango' row must survive the filter");

    let c = storage.read_counters().snapshot();
    assert_eq!(
        c.row_groups_considered, 4,
        "fixture must produce exactly four row groups; got {c:?}"
    );
    // The "mango" prefix sits between "kilo…"(g1) and "zulu…"(g3): groups 0,
    // 1, 3 are provably outside [mango, mangp) and must be pruned by the
    // zone-map range. Only group 2 survives.
    assert_eq!(
        c.row_groups_pruned_by_stats, 3,
        "prefix range prune must skip the 3 non-matching groups; got {c:?}"
    );
    assert_eq!(
        c.row_groups_scanned, 1,
        "only the one group whose min/max brackets 'mango' is scanned; got {c:?}"
    );
}

/// The lower-bound leg of the range prune: a prefix lexicographically ABOVE
/// every group's max prunes all groups whose max < prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn starts_with_prunes_groups_below_prefix() {
    basin_common::telemetry::try_init_for_tests();
    let (_counting, storage, project, table) = build().await;
    let part = PartitionKey::default_key();

    let families = ["alpha", "kilo", "mango", "zulu"];
    let mut labels: Vec<String> = Vec::new();
    for fam in families {
        for i in 0..50 {
            labels.push(format!("{fam}{i:03}"));
        }
    }
    storage
        .write_batch_with_options(
            &project,
            &table,
            &part,
            &batch_from_labels(0, &labels),
            &WriteOptions {
                file_format: FileFormat::Parquet,
                max_row_group_size: Some(50),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    storage.read_counters().reset();
    // "zulu" is the lex-largest family: groups alpha/kilo/mango all have
    // max < "zulu", so the `max < prefix` leg prunes them.
    let opts = ReadOptions {
        filters: vec![Predicate::StartsWith {
            column: "label".into(),
            prefix: "zulu".into(),
            case_insensitive: false,
        }],
        ..Default::default()
    };
    let rows = count_rows(storage.read(&project, &table, opts).await.unwrap()).await;
    assert_eq!(rows, 50);

    let c = storage.read_counters().snapshot();
    assert_eq!(c.row_groups_pruned_by_stats, 3, "got {c:?}");
    assert_eq!(c.row_groups_scanned, 1, "got {c:?}");
}

/// ROOT-CAUSE DOCUMENTATION for the benchmark spike.
///
/// When the matched value is uniformly interleaved (`bucket[i % N]`), the
/// matching prefix appears in EVERY row group, so each group's min/max
/// straddles the prefix and the range prune can skip nothing — exactly the
/// benchmark's `status = bucket[i % 4]` layout. The full scan is intrinsic
/// to the data distribution, not a defect in the prefix-pruning code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interleaved_prefix_cannot_be_pruned_documents_bench_spike() {
    basin_common::telemetry::try_init_for_tests();
    let (_counting, storage, project, table) = build().await;
    let part = PartitionKey::default_key();

    // 200 rows, label = bucket[i % 4] with a per-row suffix so each group's
    // min/max spans the full bucket alphabet. Buckets chosen so the target
    // prefix ("kilo") is neither the global min nor max — it sits strictly
    // inside [alpha, zulu] in every group, the worst case for zone maps.
    let buckets = ["alpha", "kilo", "mango", "zulu"];
    let labels: Vec<String> = (0..200)
        .map(|i| format!("{}{:04}", buckets[(i % 4) as usize], i))
        .collect();
    storage
        .write_batch_with_options(
            &project,
            &table,
            &part,
            &batch_from_labels(0, &labels),
            &WriteOptions {
                file_format: FileFormat::Parquet,
                max_row_group_size: Some(50), // 4 groups of 50 interleaved rows
                ..Default::default()
            },
        )
        .await
        .unwrap();

    storage.read_counters().reset();
    let opts = ReadOptions {
        filters: vec![Predicate::StartsWith {
            column: "label".into(),
            prefix: "kilo".into(),
            case_insensitive: false,
        }],
        ..Default::default()
    };
    let rows = count_rows(storage.read(&project, &table, opts).await.unwrap()).await;
    assert_eq!(rows, 50, "one in four rows matches 'kilo'");

    let c = storage.read_counters().snapshot();
    assert_eq!(c.row_groups_considered, 4, "got {c:?}");
    // The crux: every group contains "kilo" rows AND non-"kilo" rows, so
    // every group's [min,max] straddles [kilo, kilp). Zone maps PROVABLY
    // cannot skip a group that holds a matching row — pruning is 0 by
    // design, and the engine must read all four groups. This is the
    // intrinsic cost the benchmark observes; not a missing prune.
    assert_eq!(
        c.row_groups_pruned_by_stats, 0,
        "interleaved low-cardinality data is unprunable by zone maps; got {c:?}"
    );
    assert_eq!(c.row_groups_scanned, 4, "got {c:?}");
}

/// File-level prefix pruning via the catalog stats path: disjoint files,
/// the prefix confined to one => the other files' bodies are never opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn starts_with_prunes_whole_files_by_catalog_stats() {
    basin_common::telemetry::try_init_for_tests();
    let (counting, storage, project, table) = build().await;
    let part = PartitionKey::default_key();

    // Four disjoint single-family files. Only the "mango" file can satisfy
    // a `LIKE 'mango%'` prefix; the other three are pruned at file
    // granularity before their bodies are read.
    let families = ["alpha", "kilo", "mango", "zulu"];
    for (fi, fam) in families.iter().enumerate() {
        let labels: Vec<String> = (0..50).map(|i| format!("{fam}{i:03}")).collect();
        storage
            .write_batch_with_options(
                &project,
                &table,
                &part,
                &batch_from_labels((fi as i64) * 50, &labels),
                &WriteOptions {
                    file_format: FileFormat::Parquet,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    // Baseline: a no-predicate read opens every file body. Measure its
    // body I/O so the pruned read can be compared against it directly
    // (a relative bound is robust to per-backend footer/HEAD accounting,
    // unlike a brittle absolute count).
    counting.reset();
    let base_rows = count_rows(
        storage
            .read(&project, &table, ReadOptions::default())
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(base_rows, 200, "baseline must read all four files");
    let baseline_io = counting.total_body_io();

    // Pruned read: only the "mango" file can satisfy `LIKE 'mango%'`; the
    // other three are dropped at file granularity by the prefix range
    // prune over per-file min/max stats, so their bodies are never opened.
    counting.reset();
    storage.read_counters().reset();
    let opts = ReadOptions {
        filters: vec![Predicate::StartsWith {
            column: "label".into(),
            prefix: "mango".into(),
            case_insensitive: false,
        }],
        ..Default::default()
    };
    let rows = count_rows(storage.read(&project, &table, opts).await.unwrap()).await;
    assert_eq!(rows, 50, "only the 'mango' file's rows match");
    let pruned_io = counting.total_body_io();

    // Row-group counters are the unambiguous proof: only the surviving
    // file's single row group is ever scanned; the three pruned files
    // contribute zero scanned (and zero considered) groups because their
    // bodies are not opened at all.
    let c = storage.read_counters().snapshot();
    assert_eq!(
        c.row_groups_scanned, 1,
        "exactly one file's row group should be scanned after file prune; got {c:?}"
    );
    // And the body I/O is strictly below the all-files baseline.
    assert!(
        pruned_io < baseline_io,
        "prefix file-pruning did no work: pruned_io={pruned_io} not below baseline_io={baseline_io}"
    );
}
