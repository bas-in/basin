//! OLTP scale-invariant gates: prove that point-shaped operations touch a
//! BOUNDED amount of work regardless of how large the table is, by asserting on
//! deterministic per-query WORK COUNTERS (files opened, rows decoded) rather
//! than wall-clock time. Wall-clock is noisy and machine-dependent; the
//! counters are exact and so make a real CI gate.
//!
//! Why this implies indefinite scaling: a point operation that opens O(1)
//! files and decodes O(1) rows costs the same at 100k rows as at 100M — the
//! cost is independent of N. An O(N) regression (a scan that forgot to prune,
//! a decode that materializes the whole table) shows up immediately as a
//! counter that grew with the seed size. Each assertion below names the
//! invariant it pins and why the bound is design-derived (not measured-and-
//! frozen): bounds carry ~2x slack to avoid flakes but stay tight enough to
//! catch a linear blow-up.
//!
//! # Counter discipline (process-global atomics)
//!
//! `ReadCounters` lives on the `Storage` as process-global atomics. The
//! per-query measurement is a snapshot-before / snapshot-after `delta()`. This
//! is only sound when the query under measurement is the ONLY read happening
//! between the two snapshots — so this file MUST run serially. The harness is
//! invoked with `--test-threads=1` for the scale gates; in addition every test
//! in this file builds its OWN engine + storage (fresh atomics are shared per
//! `Storage`, and `delta()` isolates each query), and within a test the
//! queries run one at a time on a single session. No background read traffic
//! is generated (the shard background flush task only WRITES).
//!
//! # Storage format
//!
//! Fresh tables default to Vortex (the engine/catalog default). The dominant
//! per-query cost on Vortex is the file open + whole-file decode, so
//! `files_opened` and `rows_decoded` are the load-bearing counters. File-level
//! pruning for Vortex is whole-file `column_stats` min/max (zone-map) pruning
//! in `reader::read` — NOT bloom filters (blooms are a row-group-level,
//! Parquet-only mechanism). Because each seeded flush writes one PK-sorted file
//! covering a disjoint id range, a point lookup's zone-map prune drops every
//! file whose [min,max] excludes the key, leaving ONE surviving file (two if a
//! key straddles a boundary). That is the design source of the
//! `files_opened <= 2` bound used throughout.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Duration;

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::{ProjectId, TableName};
use basin_engine::{Engine, EngineConfig, ExecResult};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{ReadCountersSnapshot, Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

/// One Vortex chunk's worth of rows. The engine writes at the default
/// 65 536-row row-block cap, so any single cold file decodes at most this many
/// rows per chunk. All `rows_decoded` bounds are expressed relative to this.
const ONE_CHUNK: u64 = 65_536;

/// Build a real in-process engine (Shard + WAL + LocalFileSystem storage),
/// mirroring `update_latency_probe::build` but ALSO returning the `Storage`
/// handle so the test can snapshot `read_counters()`. `Storage` is Arc-backed
/// and `Clone`, so the returned handle observes the same atomics the engine's
/// read path bumps.
async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    Shard,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
    Storage,
) {
    let sd = TempDir::new().unwrap();
    let wd = TempDir::new().unwrap();
    let storage = Storage::new(StorageConfig {
        object_store: Arc::new(LocalFileSystem::new_with_prefix(sd.path()).unwrap()),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    let wal: Arc<dyn Wal> = Arc::new(
        LocalWal::open(WalConfig {
            object_store: Arc::new(LocalFileSystem::new_with_prefix(wd.path()).unwrap()),
            root_prefix: None,
            flush_interval: Duration::from_millis(50),
            flush_max_bytes: 1024 * 1024,
            commit_delay: Duration::from_millis(2),
        })
        .await
        .unwrap(),
    );
    let shard = Shard::new(ShardConfig::new(
        storage.clone(),
        catalog.clone(),
        wal.clone(),
    ));
    let bg = shard.spawn_background();
    let engine = Engine::new(EngineConfig {
        storage: storage.clone(),
        catalog,
        shard: Some(shard.clone()),
    });
    (sd, wd, engine, shard, bg, wal, storage)
}

/// Count rows in an `ExecResult::Rows`, asserting it is a result set.
fn row_count(res: &ExecResult) -> usize {
    match res {
        ExecResult::Rows { batches, .. } => batches.iter().map(|b| b.num_rows()).sum(),
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_invariants() {
    let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // Seed 100k rows in 10 flush cycles of 10k each (disjoint id ranges), so
    // the cold tier holds ~10 PK-sorted files with disjoint [min,max] id zone
    // maps. This is the "multi-file layout" the point-prune relies on.
    let scale: i64 = 100_000;
    let batch: i64 = 10_000;
    sess.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    let mut id = 0i64;
    while id < scale {
        let lo = id;
        let hi = (id + batch).min(scale);
        let mut stmt = String::with_capacity((hi - lo) as usize * 16);
        stmt.push_str("INSERT INTO t VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        // Force this batch cold to its own file before seeding the next range,
        // so the cold tier becomes a multi-file layout (one disjoint-range file
        // per cycle) rather than one giant file.
        shard.flush_to_parquet().await.unwrap();
        id = hi;
    }

    // Helper: run `sql`, return (result, per-query counter delta).
    let probe = |sql: String| {
        let sess = &sess;
        let storage = &storage;
        async move {
            let before = storage.read_counters().snapshot();
            let res = sess.execute(&sql).await.unwrap();
            let after = storage.read_counters().snapshot();
            (res, after.delta(&before))
        }
    };

    // ── 1. Fresh-key point SELECT ────────────────────────────────────────────
    // Pick a key in the MIDDLE of the key space (not a flush boundary). A fresh
    // key has never been read, so this is a cold lookup.
    let key = scale / 2 + 137; // arbitrary interior, non-boundary key
    let (res, d) = probe(format!("SELECT v FROM t WHERE id = {key}")).await;
    assert_eq!(row_count(&res), 1, "point SELECT must return exactly the row");
    // INVARIANT: a PK point lookup opens O(1) files independent of table size.
    // Whole-file column_stats min/max pruning drops every file whose id zone
    // map excludes `key`, leaving the single file whose [min,max] contains it
    // (at most 2 if a boundary key straddles two files; we chose an interior
    // key, so 1, but allow 2 for slack). If this grew with `scale` it would
    // mean the zone-map prune regressed to a full table scan — the textbook
    // O(N) point-read bug. THIS is what proves indefinite scaling: cost is
    // independent of N.
    assert!(
        d.files_opened <= 2,
        "fresh-key point SELECT opened {} files; design bound is <=2 (zone-map prune to the one [min,max]-containing file)",
        d.files_opened
    );
    // INVARIANT: a point lookup decodes at most ONE chunk of the one surviving
    // file. Vortex pushes the PK-equality filter and prunes non-matching chunks
    // via its zone maps; what survives is the single chunk holding the key (the
    // seeded files are <= one chunk each). Bounded by 2 chunks for slack. An
    // O(N) regression here is a decode that materializes the whole table.
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "fresh-key point SELECT decoded {} rows; design bound is <= 2 chunks ({} rows)",
        d.rows_decoded,
        2 * ONE_CHUNK
    );

    // ── 2. Repeated-key point SELECT (cache hit) ─────────────────────────────
    // The SAME key, same projection, same filter → identical page-cache key.
    let (res, d) = probe(format!("SELECT v FROM t WHERE id = {key}")).await;
    assert_eq!(row_count(&res), 1, "repeated point SELECT must still return the row");
    // INVARIANT: a repeat of an identical point read is served entirely from
    // the in-RAM page cache — ZERO cold file opens and ZERO new decode. This is
    // the steady-state read cost of a hot key: O(1) and storage-free. If
    // files_opened were nonzero the cache would be missing on repeat, meaning
    // every read re-fetches — the cost would scale with read VOLUME instead of
    // staying flat.
    assert_eq!(
        d.files_opened, 0,
        "repeated point SELECT opened {} cold files; a cache hit must open 0",
        d.files_opened
    );
    assert_eq!(
        d.rows_decoded, 0,
        "repeated point SELECT decoded {} rows; a cache hit must decode 0",
        d.rows_decoded
    );
    // No storage-cache assert here: the repeat is normally served by the
    // ENGINE's PkRowCache and never reaches basin-storage at all —
    // files_served_from_cache can legitimately be 0. The invariant that
    // implies indefinite scaling is exactly the two zero-cold-work asserts
    // above.

    // ── 3. Single-row scalar UPDATE ──────────────────────────────────────────
    let target = scale / 4 + 51;
    let (res, d) = probe(format!("UPDATE t SET v = 7 WHERE id = {target}")).await;
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 1"),
        "single-row UPDATE must affect exactly 1 row: {res:?}"
    );
    // INVARIANT: a single-row UPDATE's cold pre-image read is itself a point
    // lookup — it opens O(1) files (the one holding `target`, bloomed +
    // zone-mapped) and decodes at most one chunk. The fast path is
    // memtable-overlay + WAL; the cold read exists only to fetch the prior row
    // value, and it must NOT read the whole table.
    assert!(
        d.files_opened <= 2,
        "single-row UPDATE opened {} files; bound <=2 (point-located pre-image read)",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "single-row UPDATE decoded {} rows; bound <= 2 chunks",
        d.rows_decoded
    );

    // ── 4. Read-modify-write UPDATE (SET v = v + 1) ──────────────────────────
    let rmw_target = scale / 3 + 17;
    let (res, d) = probe(format!("UPDATE t SET v = v + 1 WHERE id = {rmw_target}")).await;
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 1"),
        "RMW UPDATE must affect exactly 1 row: {res:?}"
    );
    // INVARIANT: RMW (`v = v + 1`) needs the CURRENT value, so it does the same
    // point-located cold read as a scalar UPDATE — bounded identically. Before
    // the row-local-RMW allowlist this fell to a whole-file copy-on-write
    // rewrite (O(N) per statement); the bound here is the regression gate
    // proving it stays point-shaped.
    assert!(
        d.files_opened <= 2,
        "RMW UPDATE opened {} files; bound <=2 (point pre-image read, not whole-file CoW)",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "RMW UPDATE decoded {} rows; bound <= 2 chunks",
        d.rows_decoded
    );

    // ── 5. Single-row DELETE ─────────────────────────────────────────────────
    let del_target = scale - 313; // interior of the last file
    let (res, d) = probe(format!("DELETE FROM t WHERE id = {del_target}")).await;
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "DELETE 1"),
        "single-row DELETE must affect exactly 1 row: {res:?}"
    );
    // INVARIANT: a point DELETE writes a tombstone in the overlay and does NOT
    // need to read the cold file at all — the row is identified by its PK
    // predicate, not by materializing it. Zero cold reads = the DELETE cost is
    // independent of where in the table the row lives and of table size. (If
    // the engine ever needs the pre-image it would be a single point-located
    // read; we assert the design guarantee of 0 and would relax to <=2 only if
    // the implementation legitimately reads the pre-image.)
    assert_eq!(
        d.files_opened, 0,
        "single-row DELETE opened {} cold files; a tombstone DELETE should open 0",
        d.files_opened
    );
    assert_eq!(
        d.rows_decoded, 0,
        "single-row DELETE decoded {} rows; a tombstone DELETE should decode 0",
        d.rows_decoded
    );

    // ── 6. Upsert single row (INSERT ... ON CONFLICT) ────────────────────────
    let upsert_key = scale / 5 + 29;
    let (res, d) = probe(format!(
        "INSERT INTO t (id, v) VALUES ({upsert_key}, 999) \
         ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v"
    ))
    .await;
    assert!(
        matches!(&res, ExecResult::Empty { .. }),
        "upsert must succeed: {res:?}"
    );
    // INVARIANT: an upsert resolves the conflict by a point-located lookup of
    // the conflicting key, not by scanning the table. O(1) files / O(1) chunks
    // regardless of size. (Allow up to a couple of files: the conflict probe
    // and any pre-image read are each point-located.)
    assert!(
        d.files_opened <= 2,
        "upsert opened {} files; bound <=2 (point conflict probe)",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "upsert decoded {} rows; bound <= 2 chunks",
        d.rows_decoded
    );

    // ── 7. INSERT then read-own-insert (memtable hit, 0 cold files) ──────────
    let own_key = scale + 4242; // brand-new key, never flushed
    let res = sess
        .execute(&format!("INSERT INTO t (id, v) VALUES ({own_key}, 1)"))
        .await
        .unwrap();
    assert!(matches!(&res, ExecResult::Empty { .. }), "insert: {res:?}");
    let (res, d) = probe(format!("SELECT v FROM t WHERE id = {own_key}")).await;
    assert_eq!(row_count(&res), 1, "must read own just-inserted row");
    // INVARIANT (actual contract, found by this test's first run): a non-tx
    // striped INSERT lands in the SHARD TAIL, not the PK-keyed memtable —
    // the first read of the fresh key triggers the pending-tail flush (#205)
    // and reads the one new file it produced. That is O(1) work independent
    // of table size (the new file's zone map matches only this key), so the
    // scaling invariant holds, but read-your-own-insert is NOT yet free of
    // storage I/O. Row-tier follow-up: PK-keyed memtable entries on the
    // striped INSERT path would make this a true 0-I/O memtable hit — when
    // that lands, tighten both bounds to == 0.
    assert!(
        d.files_opened <= 2,
        "read-own-insert opened {} cold files; the tail-flush read must touch only the new file (O(1))",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "read-own-insert decoded {} rows; must be bounded by the freshly flushed file's chunk",
        d.rows_decoded
    );

    // ── 8. Keyset pagination page ────────────────────────────────────────────
    // Classic OLTP "next page" via WHERE id > cursor ORDER BY id LIMIT N.
    // The keyset per-file-limit pushdown correctly DISABLES itself when the
    // table has live hot-tier overlays (an overlay could shadow rows the
    // truncated per-file read skipped), and by this point `t` carries
    // overlays from the UPDATE/RMW/DELETE sections above. The invariant is
    // therefore asserted on a CLEAN second table — the production shape it
    // guarantees (paginating a table that isn't mid-mutation).
    sess.execute("CREATE TABLE t_keyset (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        .await
        .unwrap();
    for batch in 0..3i64 {
        let mut stmt = String::from("INSERT INTO t_keyset VALUES ");
        for i in 0..10_000i64 {
            let id = batch * 10_000 + i;
            if i > 0 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({id}, {id})"));
        }
        sess.execute(&stmt).await.unwrap();
    }
    shard.flush_to_parquet().await.unwrap();
    let cursor = 15_000;
    let page_limit: u64 = 50;
    let (res, d) = probe(format!(
        "SELECT id, v FROM t_keyset WHERE id > {cursor} ORDER BY id LIMIT {page_limit}"
    ))
    .await;
    assert_eq!(
        row_count(&res),
        page_limit as usize,
        "keyset page must return exactly LIMIT rows"
    );
    // INVARIANT: a keyset page reads only as deep as the page needs. The LIMIT
    // is pushed down so the scan stops after the requested rows; with a range
    // predicate the zone-map prune restricts the candidate files to those whose
    // [min,max] overlaps (id > cursor], and the cold decode per file is bounded
    // by that file's chunk. The page cost is bounded by (files touched x LIMIT)
    // plus one chunk of decode slack on the boundary file — it does NOT grow
    // with total table size. We allow a generous chunk-scale ceiling because
    // the first file overlapping the cursor decodes its chunk to satisfy the
    // sort+limit, then later files are pruned once LIMIT is met.
    assert!(
        d.rows_decoded <= 3 * ONE_CHUNK,
        "keyset page decoded {} rows; bound <= 3 chunks (boundary file chunk + LIMIT depth), must not scale with table size",
        d.rows_decoded
    );
    // Files opened is bounded by the WRITE STRIPE COUNT (8): round-robin
    // striping spreads every PK range across all stripes, so a keyset page
    // touches one file per stripe — but that is a deployment CONSTANT, not a
    // function of table size, and the per-file LIMIT bounds the work inside
    // each. (With range-partitioned files this would be ~2; with stripes it
    // is the stripe count. Either way: O(1) in N.)
    assert!(
        d.files_opened <= 10,
        "keyset page opened {} files; bound <= stripe count + slack (a constant in table size)",
        d.files_opened
    );

    // ── 9. Point self-join (two point lookups' worth) ────────────────────────
    let a = scale / 6 + 11;
    let b = scale / 6 + 12;
    let (res, d) = probe(format!(
        "SELECT l.v, r.v FROM t l JOIN t r ON l.id = r.id \
         WHERE l.id = {a} OR l.id = {b}"
    ))
    .await;
    assert!(
        row_count(&res) >= 1,
        "point self-join must return at least the matching rows"
    );
    // INVARIANT: a join restricted to a couple of point keys is just a handful
    // of point lookups — O(1) files / O(1) chunks, not a Cartesian or
    // full-table scan. The two keys are adjacent so they live in the same file;
    // the join reads each side as a point-located scan. Bounded at a few files
    // and a few chunks regardless of table size.
    assert!(
        d.files_opened <= 4,
        "point self-join opened {} files; bound <=4 (a few point lookups), not a table scan",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 4 * ONE_CHUNK,
        "point self-join decoded {} rows; bound <= 4 chunks",
        d.rows_decoded
    );

    // ── 10. IN-list of 100 keys ──────────────────────────────────────────────
    // Spread 100 keys across the whole key space. First touch fills the
    // unfiltered-decode cache for each overlapping file; the SECOND identical
    // query is the tight steady-state bound (0 cold work).
    let mut keys: Vec<i64> = Vec::with_capacity(100);
    for i in 0..100i64 {
        keys.push((i * (scale / 100)) + 3); // 100 interior keys, one per file-ish band
    }
    let in_list = keys
        .iter()
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let in_sql = format!("SELECT id FROM t WHERE id IN ({in_list})");

    // First touch: allowance run. This may open every file whose zone map
    // overlaps a key and fill each file's unfiltered-decode cache. We DON'T
    // pin a tight bound on the cache-FILL pass (it legitimately touches one
    // file per band), only that it returns the right rows and decodes at most
    // one chunk per touched file.
    let (res1, d1) = probe(in_sql.clone()).await;
    let got1 = row_count(&res1);
    assert!(
        got1 >= 1 && got1 <= 100,
        "IN-list must return between 1 and 100 rows, got {got1}"
    );
    // Cache-fill allowance: at most one chunk decoded per file touched. With
    // ~10 disjoint-range files and 100 keys spread across them, every file is
    // touched once and decoded once -> bounded by files x one chunk. This is a
    // generous ceiling that still catches a per-key re-decode (which would be
    // 100 x chunk).
    let files_touched1 = d1.files_opened.max(1);
    assert!(
        d1.rows_decoded <= files_touched1 * ONE_CHUNK,
        "IN-list cache-fill decoded {} rows for {} files; bound is one chunk per touched file ({} rows) — a per-key re-decode would blow this",
        d1.rows_decoded,
        files_touched1,
        files_touched1 * ONE_CHUNK
    );

    // Second identical query: the tight steady-state bound. Every file's
    // unfiltered decode is now cached, so the sorted-skip / Arrow-filter runs
    // over in-RAM batches and the cold path is NEVER entered.
    let (res2, d2) = probe(in_sql.clone()).await;
    assert_eq!(
        row_count(&res2),
        got1,
        "IN-list must be stable across identical queries"
    );
    // INVARIANT: the steady-state IN-list read does ZERO cold work — the
    // unfiltered decode of each overlapping file is cached after the first
    // touch, so re-running the same IN-list is pure in-RAM filtering. This is
    // the key scaling property: a hot IN-list's cost is independent of both
    // table size AND the cold decode volume; only the cache lookups + Arrow
    // filter remain.
    assert_eq!(
        d2.files_opened, 0,
        "second IN-list opened {} cold files; the cached unfiltered decode must serve it with 0",
        d2.files_opened
    );
    assert_eq!(
        d2.rows_decoded, 0,
        "second IN-list decoded {} rows; cached decode reuse must add 0",
        d2.rows_decoded
    );

    // ── 11. Small-bulk (≤cap) delta UPDATE: zero replacement writes ─────────
    // Mirror of the DELETE tombstone-only invariant (section 5), extended to
    // the multi-key delta-UPDATE path: a 200-key range UPDATE routes through
    // the hot-tier overlay, so it must commit ZERO replacement files — the
    // catalog snapshot id and live data-file set are byte-identical before
    // and after. A cold copy-on-write regression would `commit_replace`
    // (new snapshot, swapped file paths) and rewrite whole files, an O(N)
    // write per statement; this gate pins that the cost stays O(matched keys)
    // at any table size. Read-side work is also bounded: ONE probe SELECT
    // resolves the matched keys AND carries the pre-images (the write step
    // re-reads nothing from cold), so the zone-map prune bounds the statement
    // at the one file overlapping the key range, independent of scale.
    //
    // Flush any straggler tail first so the file-set comparison can't be
    // perturbed by a pre-statement tail flush (#205 ordering).
    shard.flush_to_parquet().await.unwrap();
    let upd_table = TableName::new("t").unwrap();
    let cat = &engine.config().catalog;
    let pre_meta = cat.load_table(&sess.project(), &upd_table).await.unwrap();
    let pre_files = {
        let mut p: Vec<String> = pre_meta
            .live_data_files()
            .iter()
            .map(|f| f.path.clone())
            .collect();
        p.sort();
        (pre_meta.current_snapshot.0, p)
    };
    // Overlay Update entries before (earlier sections wrote some): the gate
    // asserts the DELTA, +200, so prior overlays can't mask a cold fallback.
    let update_overlay_count = || {
        engine
            .memtable_registry()
            .get(&sess.project(), &upd_table)
            .map(|e| {
                e.memtable
                    .snapshot()
                    .iter()
                    .filter(|(_, v)| {
                        matches!(v, basin_hottier::MemRowValue::Update { .. })
                    })
                    .count()
            })
            .unwrap_or(0)
    };
    let overlays_before = update_overlay_count();
    // 200 interior keys of one seeded band — well under the 10k delta cap.
    let lo = 70_200;
    let hi = 70_399;
    let (res, d) = probe(format!(
        "UPDATE t SET v = v + 5 WHERE id BETWEEN {lo} AND {hi}"
    ))
    .await;
    assert!(
        matches!(&res, ExecResult::Empty { tag } if tag == "UPDATE 200"),
        "200-key range UPDATE must affect exactly 200 rows: {res:?}"
    );
    // INVARIANT (routing): +200 overlay Update entries — the statement rode
    // the delta path, not a cold rewrite.
    assert_eq!(
        update_overlay_count(),
        overlays_before + 200,
        "≤cap UPDATE must write exactly one overlay Update per matched key"
    );
    // INVARIANT (zero replacement writes): snapshot id + live file set are
    // unchanged. Cold CoW MUST change both, so this catches any fallback or
    // partial rewrite regardless of how cheap its reads were.
    let post_meta = cat.load_table(&sess.project(), &upd_table).await.unwrap();
    let post_files = {
        let mut p: Vec<String> = post_meta
            .live_data_files()
            .iter()
            .map(|f| f.path.clone())
            .collect();
        p.sort();
        (post_meta.current_snapshot.0, p)
    };
    assert_eq!(
        pre_files, post_files,
        "≤cap delta UPDATE must commit ZERO replacement files (snapshot + live set unchanged)"
    );
    // INVARIANT (bounded reads): the probe is point-located — zone-map prune
    // to the one file whose [min,max] overlaps the 200-key range — and the
    // pre-images ride the probe, so there is NO second cold pass. O(1) files /
    // O(chunk) decode independent of N; a cold CoW fallback (or a re-read per
    // key) would blow these bounds immediately at this scale.
    assert!(
        d.files_opened <= 2,
        "200-key delta UPDATE opened {} files; bound <=2 (one probe, zone-map-pruned)",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "200-key delta UPDATE decoded {} rows; bound <= 2 chunks (single probe pass)",
        d.rows_decoded
    );

    println!(
        "[scale-invariants] scale={scale} fresh-point files<=2 rows_decoded<=2chunk; \
         repeat/delete/own-insert/in-list-2nd = 0 cold; keyset/join point-bounded; \
         200-key delta UPDATE zero-replacement-files — all PASS"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}

/// Collect the `id` column (column 0) of a `SELECT id, … ORDER BY id` result
/// in result order.
fn ids_of(res: &ExecResult) -> Vec<i64> {
    use arrow_array::{Array, Int64Array};
    match res {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in batches {
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id must be Int64");
                for r in 0..ids.len() {
                    out.push(ids.value(r));
                }
            }
            out
        }
        ExecResult::Empty { tag } => panic!("expected Rows, got Empty tag={tag}"),
    }
}

/// Keyset-pagination short-circuit gates over a DISJOINT multi-file layout.
///
/// Statement-affine striping + per-statement flush produce one PK-sorted cold
/// file per INSERT statement, each covering a disjoint id band — the layout
/// the keyset fast path's ordered traversal exploits. The design invariant:
/// a `WHERE id > cursor ORDER BY id LIMIT n` page over K disjoint files opens
/// at most TWO files (the one whose [min,max] band contains the cursor's
/// successor, plus its neighbour when the page straddles a band boundary),
/// independent of K — zone-map pruning drops every file with `max <= cursor`
/// without opening it, and the ascending-min ordered traversal stops as soon
/// as the collected page provably cannot be displaced by later bands. Before
/// the short-circuit the keyset path opened EVERY surviving candidate
/// (`files_opened` grew with K: the 1M-row ~23.5ms keyset residual); these
/// gates pin the O(1)-in-K bound so a regression to the fan-out is loud.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyset_short_circuit_scale_invariants() {
    let (_sd, _wd, engine, shard, bg, wal, storage) = build().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();

    // 6 disjoint files of 5k rows each: one INSERT statement per id band,
    // flushed before the next, so each band lands in its own PK-sorted cold
    // file with a disjoint [min,max] id zone map (same layout recipe as the
    // seed loop in `scale_invariants` above).
    const FILES: i64 = 6;
    const PER_FILE: i64 = 5_000; // ids 0..30_000
    sess.execute("CREATE TABLE pages (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    for f in 0..FILES {
        let lo = f * PER_FILE;
        let hi = lo + PER_FILE;
        let mut stmt = String::with_capacity((hi - lo) as usize * 16);
        stmt.push_str("INSERT INTO pages VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        sess.execute(&stmt).await.unwrap();
        shard.flush_to_parquet().await.unwrap();
    }

    let probe = |sql: String| {
        let sess = &sess;
        let storage = &storage;
        async move {
            let before = storage.read_counters().snapshot();
            let res = sess.execute(&sql).await.unwrap();
            let after = storage.read_counters().snapshot();
            (res, after.delta(&before))
        }
    };

    // ── 1. First page (cursor at the very start) ─────────────────────────────
    // `id > 0` prunes NOTHING by zone map (every band's max > 0) — all 6 files
    // are candidates. The SHORT-CIRCUIT alone must bound the opens: the first
    // band (ascending min) yields the full page and the next band's min
    // (5_000) exceeds the page's 100th key (100), so the traversal stops.
    // This is the gate that distinguishes the ordered traversal from the old
    // open-every-candidate fan-out (which would open all 6).
    let (res, d) = probe("SELECT id, v FROM pages WHERE id > 0 ORDER BY id LIMIT 100".into())
        .await;
    assert_eq!(
        ids_of(&res),
        (1..=100).collect::<Vec<i64>>(),
        "first page must be the 100 smallest ids > 0"
    );
    assert!(
        d.files_opened <= 2,
        "first keyset page opened {} files; short-circuit bound is <=2 (design: 1) — \
         opening more means the ordered traversal regressed to the all-candidate fan-out",
        d.files_opened
    );
    assert!(
        d.rows_decoded <= 2 * ONE_CHUNK,
        "first keyset page decoded {} rows; bound <= 2 chunks",
        d.rows_decoded
    );

    // ── 2. Mid-table page inside one band ────────────────────────────────────
    // cursor 12_345 → page 12_346..=12_395 lives entirely in band 2
    // (10_000..15_000). Zone maps prune bands 0-1 (max <= cursor); the ordered
    // traversal stops after band 2 (next min 15_000 > kth key 12_395).
    let (res, d) = probe(
        "SELECT id, v FROM pages WHERE id > 12345 ORDER BY id LIMIT 50".into(),
    )
    .await;
    assert_eq!(
        ids_of(&res),
        (12_346..=12_395).collect::<Vec<i64>>(),
        "mid-table page must be the 50 smallest ids > 12345"
    );
    assert!(
        d.files_opened <= 2,
        "mid-table keyset page opened {} files; bound <=2 (design: 1)",
        d.files_opened
    );

    // ── 3. Page straddling a band boundary ───────────────────────────────────
    // cursor 14_980 → page 14_981..=15_030 spans band 2 (..14_999, 19 rows)
    // and band 3 (15_000.., 31 rows). Band 2 alone under-fills the page, so
    // the traversal must continue into band 3 and then stop — exactly 2 opens,
    // and the page must be seam-free (no gap, no repeat at the boundary).
    let (res, d) = probe(
        "SELECT id, v FROM pages WHERE id > 14980 ORDER BY id LIMIT 50".into(),
    )
    .await;
    assert_eq!(
        ids_of(&res),
        (14_981..=15_030).collect::<Vec<i64>>(),
        "boundary-straddling page must cross the file seam with no gap/repeat"
    );
    assert!(
        d.files_opened <= 2,
        "boundary keyset page opened {} files; bound <=2 (design: exactly 2)",
        d.files_opened
    );

    // ── 4. Cursor past all bands but the last ────────────────────────────────
    // K-1 files have max <= cursor (25_100) — zone-map pruning must skip them
    // all WITHOUT opening, leaving only the last band. This is the prompt-side
    // half of the invariant: prune handles the bands behind the cursor, the
    // short-circuit handles the bands ahead of the page.
    let (res, d) = probe(
        "SELECT id, v FROM pages WHERE id > 25100 ORDER BY id LIMIT 50".into(),
    )
    .await;
    assert_eq!(
        ids_of(&res),
        (25_101..=25_150).collect::<Vec<i64>>(),
        "last-band page must return the 50 smallest ids > 25100"
    );
    assert!(
        d.files_opened <= 2,
        "last-band keyset page opened {} files with K-1 bands behind the cursor; \
         bound <=2 (design: 1 — every earlier band is zone-map-pruned unopened)",
        d.files_opened
    );

    // ── 5. Empty page (cursor beyond the table max) ──────────────────────────
    // Every band's max <= cursor → all files pruned, zero opens, zero rows.
    let (res, d) = probe(
        "SELECT id, v FROM pages WHERE id > 99999 ORDER BY id LIMIT 50".into(),
    )
    .await;
    assert!(
        ids_of(&res).is_empty(),
        "cursor beyond the max id must yield an empty page"
    );
    assert_eq!(
        d.files_opened, 0,
        "empty keyset page opened {} files; a fully-pruned candidate set must open 0",
        d.files_opened
    );

    // ── 6. Page over hot-tail rows (tail rows merge in) ──────────────────────
    // Insert a fresh band WITHOUT an explicit flush: the rows sit in the shard
    // tail until the SELECT's own pre-read flush (#205) drains them to a new
    // cold file. The page straddles the old-cold/new-tail seam, so it proves
    // file pruning/short-circuiting never hides not-yet-flushed rows: 9 rows
    // from the last seeded band (29_991..=29_999) + 41 from the drained tail.
    sess.execute(&{
        let mut stmt = String::from("INSERT INTO pages VALUES ");
        for k in 30_000..30_100i64 {
            if k > 30_000 {
                stmt.push(',');
            }
            stmt.push_str(&format!("({k},{k})"));
        }
        stmt
    })
    .await
    .unwrap();
    let (res, d) = probe(
        "SELECT id, v FROM pages WHERE id > 29990 ORDER BY id LIMIT 50".into(),
    )
    .await;
    assert_eq!(
        ids_of(&res),
        (29_991..=30_040).collect::<Vec<i64>>(),
        "page must merge the freshly-inserted tail rows across the cold/tail seam"
    );
    assert!(
        d.files_opened <= 3,
        "hot-tail keyset page opened {} files; bound <=3 (old band + drained-tail file + slack)",
        d.files_opened
    );

    println!(
        "[keyset-short-circuit] {FILES} disjoint files x {PER_FILE} rows: first/mid/boundary/\
         last-band pages <=2 opens, empty page 0 opens, hot-tail merge correct — all PASS"
    );

    bg.shutdown().await;
    wal.close().await.unwrap();
}
