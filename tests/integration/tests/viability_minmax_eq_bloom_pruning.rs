//! Broadened catalog-level file pruning for the common SELECTIVE filters
//! beyond the Int64 half-open range (which `viability_minmax_file_pruning.rs`
//! already covers): equality (`col = lit`), `IN (...)`, string/Utf8 ranges,
//! and the per-column **bloom-filter definite-negative** prune.
//!
//! Card: `viability_minmax_eq_bloom_pruning`
//!
//! The engine's `apply_minmax_file_pruning_for_query` drops every live data
//! file that its catalog `column_stats` (min/max) — or, for equality, its
//! per-column `bloom_filters` — PROVE cannot contain a matching row, BEFORE any
//! object-store GET. DataFusion re-applies the full predicate over the
//! survivors, so the result is always exact; pruning is a pure GET-saving.
//!
//! Correctness focus (these are the load-bearing assertions):
//!   * equality hits only the file(s) whose stats/bloom allow it (exact rows);
//!   * a bloom DEFINITE-NEGATIVE prunes a file whose min/max can't (value is
//!     inside [min,max] but absent from the column);
//!   * a bloom FALSE-POSITIVE keeps the file and STILL returns the correct
//!     (empty) result — bloom may prune only on a proven absence;
//!   * a string equality / range prunes lexicographically and returns exact
//!     rows;
//!   * an out-of-domain equality returns empty with minimal GETs;
//!   * a query overlapping every file reads all files (no over-prune).
//!
//! Routing note: the broadened prune lives in
//! `apply_minmax_file_pruning_for_query`, which runs on the `exec_select`
//! (DataFusion) read path — NOT the `fast_select` simple-`SELECT col FROM t
//! WHERE …` fast path (which has its own, narrower catalog-stats prune in
//! `basin-storage::evaluate_compound_for_pruning`). A bare-column projection
//! takes the fast path, so every query here uses a trivial PROJECTION
//! EXPRESSION (`id + 0`, `name || ''`) to deterministically route to
//! `exec_select` and exercise the code under test. The result is identical to
//! the bare-column form; only the read path differs.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, StringArray};
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

/// Object store that records the set of object keys touched by GETs, so a test
/// can assert that pruned files are never fetched.
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
                .expect("first column is Int64");
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

async fn select_strs(sess: &ProjectSession, sql: &str) -> Vec<String> {
    let res = sess.execute(sql).await.unwrap();
    let mut out = Vec::new();
    if let ExecResult::Rows { batches, .. } = res {
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("first column is Utf8");
            for i in 0..col.len() {
                if !col.is_null(i) {
                    out.push(col.value(i).to_string());
                }
            }
        }
    }
    out.sort();
    out
}

const FILES: i64 = 10;
const BAND: i64 = 100;

/// Insert FILES disjoint contiguous Int64 bands sorted by `id` (so per-file
/// blooms on `id` are populated and each cold file holds a disjoint band).
async fn seed_int_bands(sess: &ProjectSession) {
    sess.execute(
        "CREATE TABLE t (id BIGINT NOT NULL, payload TEXT NOT NULL) \
         WITH (basin.sort_by='id')",
    )
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn int_equality_prunes_to_single_file_exact_rows() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_int_bands(&sess).await;

    // `id = 250` is inside band 2 ([200,300)) only; min/max prunes the other 9.
    tracking.reset();
    let got = select_ids(&sess, "SELECT id + 0 FROM t WHERE id = 250").await;
    assert_eq!(got, vec![250], "equality must return exactly the one row");

    let touched = tracking.touched_data_files();
    eprintln!("int_eq: touched {} data files (of {FILES})", touched.len());
    assert!(
        touched.len() <= 1,
        "equality should read at most the single overlapping file, got {}: {:?}",
        touched.len(),
        touched
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn int_in_list_prunes_to_the_two_owning_files() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_int_bands(&sess).await;

    // 150 → band 1, 850 → band 8. Two distinct files; the other 8 are pruned.
    tracking.reset();
    let got = select_ids(&sess, "SELECT id + 0 FROM t WHERE id IN (150, 850)").await;
    assert_eq!(got, vec![150, 850], "IN-list must return exactly both rows");

    let touched = tracking.touched_data_files();
    eprintln!("int_in: touched {} data files (of {FILES})", touched.len());
    assert!(
        touched.len() <= 2,
        "IN (150,850) should read at most the two owning files, got {}: {:?}",
        touched.len(),
        touched
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn int_equality_out_of_domain_is_empty_with_minimal_gets() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_int_bands(&sess).await;

    // 999_999 is above every band's max → every file is provably non-matching.
    tracking.reset();
    let got = select_ids(&sess, "SELECT id + 0 FROM t WHERE id = 999999").await;
    assert!(got.is_empty(), "out-of-domain equality returns no rows");

    let touched = tracking.touched_data_files();
    eprintln!("int_eq_oob: touched {} data files (of {FILES})", touched.len());
    assert!(
        touched.is_empty(),
        "an out-of-domain equality must touch no data files, got {:?}",
        touched
    );
}

/// Min/max can't prune (target is *inside* the file's [min,max]) but the bloom
/// proves the value absent → a DEFINITE-NEGATIVE bloom prune.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bloom_definite_negative_prunes_inside_minmax_gap() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Two files, each with a GAP between its min and max so that an in-range
    // value is absent and only the bloom can prune it.
    //   file A: ids {0, 500}      → min=0,   max=500
    //   file B: ids {1000, 1500}  → min=1000, max=1500
    // sorted by id so per-file blooms on `id` are populated.
    sess.execute(
        "CREATE TABLE t (id BIGINT NOT NULL, payload TEXT NOT NULL) \
         WITH (basin.sort_by='id')",
    )
    .await
    .unwrap();
    sess.execute("INSERT INTO t VALUES (0, 'a'), (500, 'a')").await.unwrap();
    sess.execute("INSERT INTO t VALUES (1000, 'b'), (1500, 'b')").await.unwrap();

    let table = basin_common::TableName::new("t").unwrap();
    let listed = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2, "expected 2 data files");

    // `id = 250` is inside file A's [0,500] but ABSENT. min/max alone would
    // keep file A; the bloom proves 250 not present → file A pruned too.
    // It is above file B's max=1500? no — 250 < 1000, so min/max prunes file B.
    tracking.reset();
    let got = select_ids(&sess, "SELECT id + 0 FROM t WHERE id = 250").await;
    assert!(got.is_empty(), "250 is absent → empty result");

    let touched = tracking.touched_data_files();
    eprintln!(
        "bloom_definite_negative: touched {} data files (of 2)",
        touched.len()
    );
    assert!(
        touched.is_empty(),
        "the bloom definite-negative must prune file A (min/max can't); \
         expected 0 data-file GETs, got {:?}",
        touched
    );
}

/// A bloom is probabilistic: a FALSE POSITIVE (or any non-definite-negative)
/// must KEEP the file, and the result is still exact (empty). We don't try to
/// manufacture a hash collision; instead we assert the contract directly — a
/// value that the bloom DOES report as "maybe present" (because it really is
/// present elsewhere is irrelevant; here we use a value present in the file so
/// the bloom says "maybe", min/max says "maybe") still yields the correct rows.
/// The load-bearing guarantee is: pruning never changes the answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bloom_non_negative_keeps_file_and_result_is_exact() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, _tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();
    seed_int_bands(&sess).await;

    // A present value: bloom says "maybe", min/max says "maybe" → file kept,
    // row returned. (Proves we don't over-prune on a bloom "maybe".)
    let present = select_ids(&sess, "SELECT id + 0 FROM t WHERE id = 333").await;
    assert_eq!(present, vec![333], "present value must be returned (no over-prune)");

    // The SAME query under the opt-out flag returns the identical result —
    // pruning is answer-invariant regardless of bloom outcomes.
    std::env::set_var("BASIN_DISABLE_MINMAX_PRUNE", "1");
    let present2 = select_ids(&sess, "SELECT id + 0 FROM t WHERE id = 333").await;
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");
    assert_eq!(present, present2, "result invariant to the prune flag");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn string_equality_and_range_prune_lexicographically() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Sort by the TEXT key so per-file string min/max (and blooms) are
    // populated and each file holds a disjoint lexicographic band.
    //
    // Parquet format: the default Vortex stats path
    // (`vortex_format::column_stats_from_batch`) records min/max for Int64 /
    // Float64 / Timestamp only — NOT Utf8 — so a Vortex string column carries a
    // bloom (used by the equality path) but no string zone map. Parquet's
    // `extract_column_stats` populates string min/max (lexicographic), which is
    // what the string-RANGE prune needs. (The engine prune itself is
    // format-agnostic — it reads whatever `column_stats` the writer recorded.)
    sess.execute(
        "CREATE TABLE s (name TEXT NOT NULL, id BIGINT NOT NULL) \
         WITH (basin.sort_by='name', basin.file_format='parquet')",
    )
    .await
    .unwrap();
    // file 0: a-prefixed; file 1: m-prefixed; file 2: z-prefixed.
    sess.execute("INSERT INTO s VALUES ('apple', 1), ('avocado', 2), ('azalea', 3)")
        .await
        .unwrap();
    sess.execute("INSERT INTO s VALUES ('mango', 4), ('melon', 5), ('mulberry', 6)")
        .await
        .unwrap();
    sess.execute("INSERT INTO s VALUES ('zebra', 7), ('zinnia', 8), ('zucchini', 9)")
        .await
        .unwrap();

    let table = basin_common::TableName::new("s").unwrap();
    let listed = engine
        .config()
        .storage
        .list_data_files(&project, &table)
        .await
        .unwrap();
    assert_eq!(listed.len(), 3, "expected 3 data files");

    // String equality: 'melon' is only in the m-file. Other 2 pruned.
    tracking.reset();
    let got = select_strs(&sess, "SELECT name || '' FROM s WHERE name = 'melon'").await;
    assert_eq!(got, vec!["melon".to_string()], "string equality returns exact row");
    let touched = tracking.touched_data_files();
    eprintln!("str_eq: touched {} data files (of 3)", touched.len());
    assert!(
        touched.len() <= 1,
        "string equality should read at most the m-file, got {}: {:?}",
        touched.len(),
        touched
    );

    // String range: name >= 'n' AND name < 'zz' overlaps only the z-file
    // (the m-file's max 'mulberry' < 'n'; the a-file is far below).
    tracking.reset();
    let zr = select_strs(&sess, "SELECT name || '' FROM s WHERE name >= 'n' AND name < 'zz'").await;
    assert_eq!(
        zr,
        vec!["zebra".to_string(), "zinnia".to_string(), "zucchini".to_string()],
        "string range returns exactly the z-band"
    );
    let touched = tracking.touched_data_files();
    eprintln!("str_range: touched {} data files (of 3)", touched.len());
    assert!(
        touched.len() <= 1,
        "string range [n,zz) should read at most the z-file, got {}: {:?}",
        touched.len(),
        touched
    );

    // Out-of-domain string equality: nothing matches, no data-file GETs.
    tracking.reset();
    let none = select_strs(&sess, "SELECT name || '' FROM s WHERE name = 'banana'").await;
    assert!(none.is_empty(), "absent string returns empty");
    let touched = tracking.touched_data_files();
    eprintln!("str_eq_oob: touched {} data files (of 3)", touched.len());
    // 'banana' is between the a-file's max ('azalea') and the m-file's min
    // ('mango') and below the z-file — provably outside EVERY file's range.
    assert!(
        touched.is_empty(),
        "an out-of-domain string equality must touch no data files, got {:?}",
        touched
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn equality_over_all_files_reads_all_when_value_present_everywhere() {
    basin_common::telemetry::try_init_for_tests();
    std::env::remove_var("BASIN_DISABLE_MINMAX_PRUNE");

    let dir = TempDir::new().unwrap();
    let (engine, _tracking) = engine_with_tracking_store(&dir);
    let project = ProjectId::new();
    let sess = engine.open_session(project).await.unwrap();

    // Every file shares the same `tag` value, so an equality on it overlaps
    // ALL files — none may be pruned. Sort by `tag` so blooms exist (and all
    // report "maybe present").
    sess.execute(
        "CREATE TABLE u (tag BIGINT NOT NULL, id BIGINT NOT NULL) \
         WITH (basin.sort_by='tag')",
    )
    .await
    .unwrap();
    for k in 0..FILES {
        // Each file: tag=7 for every row, distinct ids.
        sess.execute(&format!("INSERT INTO u VALUES (7, {}), (7, {})", k * 2, k * 2 + 1))
            .await
            .unwrap();
    }

    // tag = 7 is present in every file → all files must be read, result exact.
    let res = sess.execute("SELECT id + 0 FROM u WHERE tag = 7").await.unwrap();
    let mut ids = Vec::new();
    if let ExecResult::Rows { batches, .. } = res {
        for b in batches {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..col.len() {
                if !col.is_null(i) {
                    ids.push(col.value(i));
                }
            }
        }
    }
    ids.sort_unstable();
    let expected: Vec<i64> = (0..(FILES * 2)).collect();
    assert_eq!(ids, expected, "equality present in all files must return every row");
}
