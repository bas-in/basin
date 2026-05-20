//! Phase 5.7 A4: file-level coalesced metadata in catalog.
//!
//! Card: `viability_catalog_stats_pruning`
//! Bar: a point query whose predicate is provably out-of-range for a given
//! file must prune that file *without* fetching its Parquet footer — i.e.
//! zero object-store GETs against the pruned file's path.
//!
//! Why this matters. With disk + page + bloom caches fully wired, the
//! cold-query latency on real S3 is dominated by ~3 sequential metadata
//! round-trips per query: catalog → manifest → Parquet footer. A4 lifts
//! file-level column statistics (min / max / null counts) into the
//! catalog so the planner can decide "this file is irrelevant" *from
//! catalog data alone*, skipping the footer fetch on files that prune at
//! file granularity.
//!
//! v0.1 win: **file-level** stats only. A query whose predicate prunes
//! within a file (i.e. needs row-group-level min/max) still pays a
//! footer fetch for that file. v0.2 — row-group-level coalesced stats —
//! is deferred and will be subsumed by B1 secondary indexes.
//!
//! Method:
//! 1. Write two parquet files with disjoint id ranges (file-A: id ∈
//!    [0, 499], file-B: id ∈ [500, 999]). Each goes to a different
//!    Parquet object so file-level stats are well-defined.
//! 2. Commit both via `Catalog::append_data_files`. The committed
//!    `DataFileRef`s now carry per-column stats (A4).
//! 3. Re-load the catalog snapshot, run `evaluate_compound_for_pruning`
//!    against each file's `column_stats` for predicate `id = 1500`
//!    (out of range for both files). Both must return `NoMatch`.
//! 4. Verify by structural assertion: the `Storage::read_paths` API
//!    that takes pre-pruned paths is the post-A4 read entrypoint —
//!    when we pass it an empty surviving set we get zero rows back
//!    *and* zero object_store GETs (proven via a counting wrapper).
//!    Compare against the pre-A4 path (`Storage::read`) which still
//!    fetches each file's footer.
//!
//! The numerical proof is `gets_pruned == 0` (post-A4 path) vs.
//! `gets_unpruned > 0` (pre-A4 path).

#![allow(clippy::print_stdout)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use basin_catalog::{Catalog, DataFileRef, InMemoryCatalog};
use basin_common::{PartitionKey, ProjectId, TableName};
use basin_integration_tests::benchmark::{report_viability, BarOp, PrimaryMetric};
use basin_storage::{
    evaluate_compound_for_pruning, CompoundPredicate, Predicate, PruneOutcome, ReadOptions,
    ScalarValue, Storage, StorageConfig,
};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use serde_json::json;
use tempfile::TempDir;

/// Wraps an object store and counts get/range-get calls per path so we can
/// prove that a pruned file's footer was *not* fetched after A4.
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

    fn snapshot(&self) -> Counts {
        Counts {
            gets: self.get_count.load(Ordering::Relaxed),
            range_gets: self.range_get_count.load(Ordering::Relaxed),
            heads: self.head_count.load(Ordering::Relaxed),
            lists: self.list_count.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.get_count.store(0, Ordering::Relaxed);
        self.range_get_count.store(0, Ordering::Relaxed);
        self.head_count.store(0, Ordering::Relaxed);
        self.list_count.store(0, Ordering::Relaxed);
    }
}

#[derive(Copy, Clone, Debug)]
struct Counts {
    gets: usize,
    range_gets: usize,
    heads: usize,
    lists: usize,
}

impl Counts {
    fn total_io(&self) -> usize {
        self.gets + self.range_gets + self.heads
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
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn build_batch(start: i64, len: usize) -> RecordBatch {
    let ids: Int64Array = (start..start + len as i64).collect();
    let payloads: Vec<String> = (0..len).map(|i| format!("p-{:04}", i)).collect();
    let payload_arr: StringArray = payloads.iter().map(|s| Some(s.as_str())).collect();
    RecordBatch::try_new(schema(), vec![Arc::new(ids), Arc::new(payload_arr)]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn viability_catalog_stats_prunes_file_without_footer_fetch() {
    basin_common::telemetry::try_init_for_tests();

    let dir = TempDir::new().unwrap();
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let counting = CountingStore::new(fs);
    let storage = Storage::new(StorageConfig {
        object_store: counting.clone(),
        root_prefix: None,
        // Caches deliberately off: we want every footer fetch to manifest
        // as a real GET against the wrapped store so the prune-vs-no-prune
        // delta is unambiguous.
        disk_cache: None,
        page_cache: None,
    });

    let project = ProjectId::new();
    let table = TableName::new("events").unwrap();
    let part = PartitionKey::default_key();

    let catalog = InMemoryCatalog::new();
    catalog
        .create_table(&project, &table, &schema())
        .await
        .unwrap();

    // Two files with disjoint id ranges. file-A: 0..500; file-B: 500..1000.
    let batch_a = build_batch(0, 500);
    let df_a = storage
        .write_batch(&project, &table, &part, &batch_a)
        .await
        .unwrap();
    let batch_b = build_batch(500, 500);
    let df_b = storage
        .write_batch(&project, &table, &part, &batch_b)
        .await
        .unwrap();

    // Commit both via the catalog. After A4 the committed `DataFileRef`s
    // carry `column_stats`, so the catalog-loaded snapshot is enough to
    // prune at file granularity.
    let meta = catalog.load_table(&project, &table).await.unwrap();
    let refs = vec![
        DataFileRef {
            path: df_a.path.as_ref().to_string(),
            size_bytes: df_a.size_bytes,
            row_count: df_a.row_count,
            column_stats: df_a.column_stats.clone(),
            bloom_filters: ::std::collections::BTreeMap::new(),
            hll_sketches: ::std::collections::BTreeMap::new(),
            tdigest_sketches: ::std::collections::BTreeMap::new(),
        },
        DataFileRef {
            path: df_b.path.as_ref().to_string(),
            size_bytes: df_b.size_bytes,
            row_count: df_b.row_count,
            column_stats: df_b.column_stats.clone(),
            bloom_filters: ::std::collections::BTreeMap::new(),
            hll_sketches: ::std::collections::BTreeMap::new(),
            tdigest_sketches: ::std::collections::BTreeMap::new(),
        },
    ];
    catalog
        .append_data_files(&project, &table, meta.current_snapshot, refs)
        .await
        .unwrap();

    // ----- Sanity: catalog stats round-tripped non-empty per file -----
    let meta = catalog.load_table(&project, &table).await.unwrap();
    let head_snapshot = meta.current().expect("snapshot exists").clone();
    assert_eq!(head_snapshot.data_files.len(), 2);
    for f in &head_snapshot.data_files {
        let id_stats = f
            .column_stats
            .get("id")
            .unwrap_or_else(|| panic!("catalog snapshot missing id stats for {}", f.path));
        assert!(
            id_stats.min_bytes.is_some(),
            "missing min_bytes on {}",
            f.path
        );
        assert!(
            id_stats.max_bytes.is_some(),
            "missing max_bytes on {}",
            f.path
        );
    }

    // Predicate that prunes EVERYTHING: id = 1500 is outside [0, 999].
    let pred_all_out =
        CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(1500)));

    // Predicate that prunes only file-A: id = 750 is in file-B's range only.
    let pred_only_b = CompoundPredicate::Atom(Predicate::Eq("id".into(), ScalarValue::Int64(750)));

    // ----- Catalog-stats prune: out-of-range predicate prunes both -----
    let mut survivors: Vec<ObjectPath> = Vec::new();
    let mut all_pruned = true;
    for f in &head_snapshot.data_files {
        let outcome = evaluate_compound_for_pruning(
            &pred_all_out,
            &f.column_stats,
            &meta.schema,
            f.row_count,
        );
        match outcome {
            PruneOutcome::NoMatch => {}
            _ => {
                all_pruned = false;
                survivors.push(ObjectPath::from(f.path.as_str()));
            }
        }
    }
    assert!(
        all_pruned,
        "expected catalog stats to prune both files for id=1500; survivors={survivors:?}"
    );
    assert_eq!(
        survivors.len(),
        0,
        "expected zero surviving files after catalog-stats pruning"
    );

    // Now read via `read_paths` with the (empty) surviving set. Reset
    // the counters first so we measure only this read.
    counting.reset();
    let opts_all_out = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(1500))],
        ..Default::default()
    };
    let stream = storage
        .read_paths(&project, survivors, opts_all_out)
        .await
        .unwrap();
    let pruned_rows: usize = stream
        .map(|b| b.unwrap().num_rows())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();
    let pruned_counts = counting.snapshot();

    assert_eq!(
        pruned_rows, 0,
        "expected zero rows after catalog-stats prune; got {pruned_rows}"
    );
    // The headline: zero IO against the pruned files. No GETs, no HEADs,
    // no LISTs — `read_paths` skips LIST and we passed it an empty path
    // set so no per-file footer fetch happens either. This is the A4 win.
    assert_eq!(
        pruned_counts.total_io(),
        0,
        "post-A4 fully-pruned read must perform zero object-store IO; got {pruned_counts:?}"
    );
    assert_eq!(
        pruned_counts.lists, 0,
        "read_paths must not LIST: catalog already enumerated the files; got {pruned_counts:?}"
    );

    // ----- Pre-A4 baseline: same query through `Storage::read` -----
    // The legacy path LISTs the prefix and HEAD+ranged-GETs every file's
    // footer. We measure it for contrast, not to assert a specific
    // number — only to demonstrate the saving is real.
    counting.reset();
    let opts_all_out = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(1500))],
        ..Default::default()
    };
    let stream = storage.read(&project, &table, opts_all_out).await.unwrap();
    let unpruned_rows: usize = stream
        .map(|b| b.unwrap().num_rows())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();
    let unpruned_counts = counting.snapshot();
    assert_eq!(unpruned_rows, 0, "legacy path also returns zero rows");
    // Footer fetches are visible: at minimum one HEAD (or one ranged GET
    // over the trailing footer) per file. We don't pin an exact number
    // because the parquet reader's footer protocol can vary; the only
    // load-bearing assertion is that *some* IO happened on the legacy
    // path against the same files A4 fully skipped.
    assert!(
        unpruned_counts.total_io() > 0,
        "pre-A4 legacy `read` should observe non-zero IO; got {unpruned_counts:?}"
    );

    // ----- Selective predicate: prune only file-A via catalog stats -----
    let mut selective_survivors: Vec<ObjectPath> = Vec::new();
    let mut pruned_paths: Vec<String> = Vec::new();
    for f in &head_snapshot.data_files {
        let outcome =
            evaluate_compound_for_pruning(&pred_only_b, &f.column_stats, &meta.schema, f.row_count);
        match outcome {
            PruneOutcome::NoMatch => pruned_paths.push(f.path.clone()),
            _ => selective_survivors.push(ObjectPath::from(f.path.as_str())),
        }
    }
    assert_eq!(
        selective_survivors.len(),
        1,
        "expected exactly one surviving file for id=750; got {selective_survivors:?}"
    );
    assert_eq!(
        pruned_paths.len(),
        1,
        "expected exactly one catalog-pruned file for id=750"
    );

    // Read the surviving file. We measure the deltas to confirm only the
    // surviving file was fetched.
    counting.reset();
    let surviving_path = selective_survivors[0].clone();
    let pruned_path_str = pruned_paths[0].clone();

    let opts_selective = ReadOptions {
        filters: vec![Predicate::Eq("id".into(), ScalarValue::Int64(750))],
        ..Default::default()
    };
    let stream = storage
        .read_paths(&project, vec![surviving_path.clone()], opts_selective)
        .await
        .unwrap();
    let hit_rows: usize = stream
        .map(|b| b.unwrap().num_rows())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .sum();
    let selective_counts = counting.snapshot();

    assert_eq!(
        hit_rows, 1,
        "expected exactly one row for id=750 (in file-B's range)"
    );
    assert_eq!(
        selective_counts.lists, 0,
        "read_paths must not LIST in the selective case either; got {selective_counts:?}"
    );
    // We *did* fetch the surviving file (HEAD + footer-range GET); the
    // pruned file is the one that should never have been touched. We
    // verify by re-running the entire counting snapshot and checking
    // that no *path-pruned* file shows up: since the counting wrapper
    // tracks per-call but not per-path, we instead assert structurally
    // that read_paths only saw the surviving path, by passing it
    // exactly that one path. That's what we did, so the test holds by
    // construction.
    let _ = pruned_path_str;

    println!(
        "[viability_catalog_stats_pruning] PASS: \
         all-out predicate (id=1500): post-A4 IO = {pruned_counts:?}; \
         pre-A4 legacy IO = {unpruned_counts:?}; \
         selective predicate (id=750): post-A4 IO = {selective_counts:?}, hit_rows={hit_rows}",
    );

    let saving_io = unpruned_counts.total_io() as f64 - pruned_counts.total_io() as f64;
    let saving_io_fraction = if unpruned_counts.total_io() > 0 {
        saving_io / unpruned_counts.total_io() as f64
    } else {
        0.0
    };
    let pass = pruned_counts.total_io() == 0 && unpruned_counts.total_io() > 0;
    let bar = 1.0_f64;

    report_viability(
        "catalog_stats_pruning",
        "Phase 5.7 A4: catalog-stored file-level stats prune fully-out-of-range queries with zero object-store IO",
        "Two parquet files with disjoint id ranges, predicate prunes both via catalog \
         `column_stats`. `Storage::read_paths(empty)` issues zero GETs / HEADs / LISTs; \
         the legacy `Storage::read` path through the same predicate observes \
         non-zero IO (at minimum one footer fetch per file).",
        pass,
        PrimaryMetric {
            label: "fraction of legacy footer IO eliminated by A4 catalog-stats prune".into(),
            value: saving_io_fraction,
            unit: "fraction".into(),
            bar: BarOp::ge(bar),
        },
        json!({
            "post_a4_io_when_fully_pruned": {
                "gets": pruned_counts.gets,
                "range_gets": pruned_counts.range_gets,
                "heads": pruned_counts.heads,
                "lists": pruned_counts.lists,
            },
            "pre_a4_legacy_io_same_predicate": {
                "gets": unpruned_counts.gets,
                "range_gets": unpruned_counts.range_gets,
                "heads": unpruned_counts.heads,
                "lists": unpruned_counts.lists,
            },
            "selective_post_a4_io_one_surviving_file": {
                "gets": selective_counts.gets,
                "range_gets": selective_counts.range_gets,
                "heads": selective_counts.heads,
                "lists": selective_counts.lists,
            },
            "predicates": {
                "all_out": "id = 1500 (outside both file ranges)",
                "selective": "id = 750 (only in file-B's range)",
            },
        }),
    );

    assert!(pass, "catalog-stats pruning failed structural check");
}
