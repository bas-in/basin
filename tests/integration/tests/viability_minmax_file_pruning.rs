//! General per-file min/max pruning for an integer-range `WHERE` (task #17).
//!
//! Card: `viability_minmax_file_pruning`
//!
//! Heavy analytical (OLAP) queries that are NOT served by the metadata
//! fast-aggregate path must scan data files from object storage. For a
//! selective `WHERE col >= A AND col < B` on an Int64 column, the engine should
//! prune every data file whose catalog `column_stats` [min,max] provably lies
//! outside [A,B) BEFORE issuing any object-store GET — so a selective query
//! reads only the overlapping files instead of tripping a full scan (and the
//! cloud HTTP proxy timeout) over remote storage.
//!
//! What we test (NON-partitioned table — partition pruning does not apply):
//!
//! 1. Ten INSERTs land ten Parquet files, each holding a disjoint, contiguous
//!    `id` band (file k → ids [k*100, k*100+100)).
//! 2. `SELECT ... WHERE id >= 250 AND id < 350` overlaps only files 2 and 3.
//!    The CountingStore proves the other eight files are NEVER GET'd.
//! 3. The result rows are exactly the rows in [250,350) — pruning is
//!    correctness-exact (DataFusion re-applies the predicate over survivors).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Array, Int64Array};
use async_trait::async_trait;
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tempfile::TempDir;

/// Object store that records the set of object keys touched by GETs, so the
/// test can assert that pruned files are never fetched.
#[derive(Debug)]
struct KeyTrackingStore {
    inner: Arc<dyn ObjectStore>,
    get_keys: std::sync::Mutex<Vec<String>>,
    get_count: AtomicUsize,
}

impl std::fmt::Display for KeyTrackingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeyTrackingStore")
    }
}

impl KeyTrackingStore {
    fn new(inner: Arc<dyn ObjectStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            get_keys: std::sync::Mutex::new(Vec::new()),
            get_count: AtomicUsize::new(0),
        })
    }

    fn reset(&self) {
        self.get_keys.lock().unwrap().clear();
        self.get_count.store(0, Ordering::Relaxed);
    }

    /// Distinct data-file keys touched by any GET since the last `reset`.
    fn touched_data_files(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .get_keys
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.ends_with(".parquet") || k.ends_with(".vortex"))
            .cloned()
            .collect();
        v.sort();
        v.dedup();
        v
    }
}

#[async_trait]
impl ObjectStore for KeyTrackingStore {
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
        self.get_count.fetch_add(1, Ordering::Relaxed);
        self.get_keys
            .lock()
            .unwrap()
            .push(location.as_ref().to_string());
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
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
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

fn engine_with_tracking_store(dir: &TempDir) -> (Engine, Arc<KeyTrackingStore>) {
    let fs = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let tracking = KeyTrackingStore::new(fs);
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: tracking.clone(),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let engine = Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    });
    (engine, tracking)
}

async fn select_ids(sess: &ProjectSession, sql: &str) -> Vec<i64> {
    let res = sess.execute(sql).await.unwrap();
    let mut out = Vec::new();
    if let ExecResult::Rows { batches, .. } = res {
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i));
                }
            }
        }
    }
    out.sort_unstable();
    out
}

const FILES: i64 = 10;
const BAND: i64 = 100;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minmax_prune_skips_non_overlapping_files() {
    basin_common::telemetry::try_init_for_tests();
    // Default config (prune ON) — make the intent explicit and immune to a
    // stray env var in the test harness.
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();

    // One INSERT per file → 10 files, each a disjoint contiguous id band:
    // file k holds ids [k*100, k*100+100).
    for k in 0..FILES {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in 0..BAND {
            if i > 0 {
                sql.push(',');
            }
            let id = k * BAND + i;
            sql.push_str(&format!("({id}, 'p-{id}')"));
        }
        sess.execute(&sql).await.unwrap();
    }

    let table = basin_common::TableName::new("t").unwrap();
    let listed = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();
    assert_eq!(
        listed.len() as i64,
        FILES,
        "expected {FILES} data files, got {}",
        listed.len()
    );

    // Selective query: ids [250, 350). Overlaps ONLY file 2 ([200,300)) and
    // file 3 ([300,400)). The other 8 files must never be GET'd.
    tracking.reset();
    let got = select_ids(&sess, "SELECT id FROM t WHERE id >= 250 AND id < 350").await;
    let expected: Vec<i64> = (250..350).collect();
    assert_eq!(got, expected, "pruned scan must return the exact rows");

    let touched = tracking.touched_data_files();
    eprintln!(
        "minmax_file_pruning: touched {} data files for a 2-band query (of {FILES})",
        touched.len()
    );
    assert!(
        touched.len() <= 2,
        "expected at most 2 overlapping files to be GET'd, got {}: {:?}",
        touched.len(),
        touched
    );
    assert!(
        touched.len() >= 1,
        "the overlapping files must actually be read"
    );

    // Opt-out flag restores the un-pruned behaviour: every file is fetched.
    std::env::set_var("BASIN_DISABLE_MINMAX_PRUNE", "1");
    // A fresh session/engine so the once-cached flag is read anew is not
    // possible (the OnceLock is process-wide); instead just assert correctness
    // is preserved under whatever the flag resolves to in THIS process: the
    // result rows are identical regardless of pruning.
    let got2 = select_ids(&sess, "SELECT id FROM t WHERE id >= 250 AND id < 350").await;
    assert_eq!(got2, expected, "results invariant to the prune flag");
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minmax_prune_full_overlap_reads_all_and_is_correct() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, _tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    sess.execute("CREATE TABLE t (id BIGINT NOT NULL, payload TEXT NOT NULL)")
        .await
        .unwrap();
    for k in 0..FILES {
        let mut sql = String::from("INSERT INTO t VALUES ");
        for i in 0..BAND {
            if i > 0 {
                sql.push(',');
            }
            let id = k * BAND + i;
            sql.push_str(&format!("({id}, 'p-{id}')"));
        }
        sess.execute(&sql).await.unwrap();
    }

    // A wide range covering every band must return every row — pruning must
    // never drop a file that overlaps the predicate.
    let got = select_ids(&sess, "SELECT id FROM t WHERE id >= 0 AND id < 1000").await;
    let expected: Vec<i64> = (0..(FILES * BAND)).collect();
    assert_eq!(got, expected, "wide range must keep all overlapping files");

    // A range matching nothing returns zero rows (correct empty set).
    let none = select_ids(&sess, "SELECT id FROM t WHERE id >= 100000 AND id < 100001").await;
    assert!(none.is_empty(), "out-of-domain range returns no rows");
}
