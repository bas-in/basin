//! Read-residual coverage for the three pagination shapes the 1M LocalFS card
//! flagged as slow, plus the two-phase top-k OFFSET fast path that fixes the
//! worst of them.
//!
//! Residuals under test:
//!
//!   1. OFFSET pagination — `SELECT … ORDER BY k DESC LIMIT m OFFSET n`. The
//!      recogniser now CAPTURES OFFSET (previously every OFFSET query fell to
//!      DataFusion), and `apply_order_by_limit` resolves the window with a
//!      two-phase global top-k: concat only the sort-key column, partial-sort
//!      to `limit + offset`, then `interleave` the surviving ≤ `limit` rows out
//!      of the per-batch columns. These tests pin the CORRECTNESS of that path
//!      against a ground-truth in-memory sort: multiple pages, DESC, ties, an
//!      OFFSET past the end, and an UPDATE overlay landing inside the window.
//!
//!   2. LIMIT-no-ORDER — `SELECT … WHERE <non-pk> = … LIMIT n`. The reader
//!      cuts each file to `n` rows early and the cross-file stream limit closes
//!      the pipeline once `n` post-filter rows exist; the test pins that the
//!      result is EXACTLY `n` matching rows (never an over-/under-return).
//!
//!   3. Keyset pagination — `SELECT … WHERE k > $1 ORDER BY k ASC LIMIT n`.
//!      The per-file head-LIMIT pushdown must keep producing the exact global
//!      top-`n`; the test pins that correctness is unchanged after the per-file
//!      reads were switched from a sequential loop to bounded-concurrency
//!      `buffered` reads.
//!
//! The `#[ignore]`d `timing_*` tests print wall-clock numbers at 200k rows for
//! all three shapes — run them explicitly on an idle box
//! (`cargo test -p basin-integration-tests --test pagination_residuals -- \
//!  --ignored --nocapture`). They are NOT correctness gates and never assert a
//! latency bound (the box is shared; see CLAUDE.md timing-fragility note).
//!
//! Fairness: no bench-keyed table / column / literal names — generic `paging`
//! table with `id` / `bucket` / `label` columns and ordinary literals.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::InMemoryCatalog;
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ── Engine + session helpers ──────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: None,
        page_cache: None,
    });
    let catalog: Arc<dyn basin_catalog::Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

async fn open(eng: &Engine) -> ProjectSession {
    eng.open_session(ProjectId::new()).await.unwrap()
}

async fn exec(sess: &ProjectSession, sql: &str) {
    sess.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("exec failed for {sql:?}: {e:?}"));
}

/// Collect `(id, bucket)` pairs from a `SELECT id, bucket …` result, in result
/// order (the order the engine produced — used as the verifiable page order).
async fn select_id_bucket(sess: &ProjectSession, sql: &str) -> Vec<(i64, i64)> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => {
            let mut out = Vec::new();
            for b in &batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let ids = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id must be Int64");
                let buckets = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("bucket must be Int64");
                for r in 0..b.num_rows() {
                    out.push((ids.value(r), buckets.value(r)));
                }
            }
            out
        }
        ExecResult::Empty { .. } => Vec::new(),
    }
}

/// Total rows from any `SELECT *`-shaped result.
fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn rows(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        ExecResult::Empty { .. } => Vec::new(),
    }
}

// ── Seeding ────────────────────────────────────────────────────────────────────

/// Create `paging(id BIGINT PK, bucket BIGINT, label TEXT)` and insert `n`
/// rows. `id` is the unique total-order key; `bucket = id % tie_groups` makes
/// the `bucket` column heavily TIED so a DESC-by-bucket page exercises the
/// tie-handling of the partial sort. `label` is a wide-ish payload column so
/// the two-phase top-k actually has a non-key column to interleave.
async fn seed(sess: &ProjectSession, n: i64, tie_groups: i64) {
    exec(
        sess,
        "CREATE TABLE paging (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL, label TEXT NOT NULL)",
    )
    .await;
    let chunk = 1_000_i64;
    let mut i = 1_i64;
    while i <= n {
        let lo = i;
        let hi = (i + chunk - 1).min(n);
        let mut stmt = String::from("INSERT INTO paging (id, bucket, label) VALUES ");
        let mut first = true;
        for k in lo..=hi {
            if !first {
                stmt.push(',');
            }
            first = false;
            let bucket = k % tie_groups;
            stmt.push_str(&format!("({k}, {bucket}, 'row-{k}')"));
        }
        exec(sess, &stmt).await;
        i = hi + 1;
    }
}

/// Ground-truth page: sort the synthetic `(id, bucket)` universe the same way
/// the SQL does (`ORDER BY <key> DESC, id DESC` to break bucket ties on the
/// unique id — matching the recogniser's single-key sort with a deterministic
/// secondary by construction), then slice `[offset, offset+limit)`.
///
/// `by_bucket=false` orders by id (the unique key) DESC; `by_bucket=true`
/// orders by bucket DESC then id DESC so the tie order is well-defined.
fn ground_truth_page(
    n: i64,
    tie_groups: i64,
    by_bucket: bool,
    offset: usize,
    limit: usize,
) -> Vec<(i64, i64)> {
    let mut all: Vec<(i64, i64)> = (1..=n).map(|k| (k, k % tie_groups)).collect();
    if by_bucket {
        // bucket DESC, then id DESC.
        all.sort_by(|a, b| b.1.cmp(&a.1).then(b.0.cmp(&a.0)));
    } else {
        all.sort_by(|a, b| b.0.cmp(&a.0));
    }
    all.into_iter().skip(offset).take(limit).collect()
}

// ── 1. OFFSET pagination correctness ───────────────────────────────────────────

/// Multiple DESC pages over the UNIQUE key reproduce the ground-truth global
/// order exactly — proves the two-phase top-k window is correct page after
/// page (the OFFSET slide lands on the right rows every time).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_pages_desc_unique_key_match_ground_truth() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, /*tie_groups*/ N).await; // tie_groups=N ⇒ every bucket unique

    let limit = 50usize;
    for page in 0..5usize {
        let offset = page * limit;
        let sql = format!(
            "SELECT id, bucket FROM paging ORDER BY id DESC LIMIT {limit} OFFSET {offset}"
        );
        let got = select_id_bucket(&sess, &sql).await;
        let want = ground_truth_page(N, N, /*by_bucket*/ false, offset, limit);
        assert_eq!(
            got, want,
            "DESC page {page} (offset {offset}) must equal the ground-truth slice"
        );
    }
}

/// DESC pages over the heavily-TIED `bucket` column. The fast path sorts on a
/// single key with an UNSTABLE partial sort, so the row order WITHIN a tie
/// group is engine-defined and is NOT guaranteed identical across independent
/// per-page sorts of different `limit+offset` bounds. We therefore assert only
/// properties that the sort KEY guarantees regardless of tie order:
///
///   * each page returns exactly `limit` rows,
///   * the bucket sort key is non-increasing within each page, and
///   * every bucket value on a page lies inside the ground-truth bucket-value
///     band `[min, max]` for that `[offset, offset+limit)` rank window.
///
/// These hold for any correct single-key DESC top-k window; they do not depend
/// on a stable tie-break the fast path does not promise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_pages_desc_ties_cover_correct_buckets() {
    const N: i64 = 50_000;
    const TIE_GROUPS: i64 = 100; // 500 rows per bucket value
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, TIE_GROUPS).await;

    let limit = 137usize; // deliberately not a divisor of the bucket size
    let pages = 6usize;
    for page in 0..pages {
        let offset = page * limit;
        let sql = format!(
            "SELECT id, bucket FROM paging ORDER BY bucket DESC LIMIT {limit} OFFSET {offset}"
        );
        let got = select_id_bucket(&sess, &sql).await;
        assert_eq!(got.len(), limit, "page {page} must return exactly {limit} rows");

        // Non-increasing bucket key within the page.
        for w in got.windows(2) {
            assert!(
                w[0].1 >= w[1].1,
                "page {page}: bucket key must be non-increasing: {:?} then {:?}",
                w[0],
                w[1]
            );
        }

        // Ground-truth bucket band for this rank window.
        let want = ground_truth_page(N, TIE_GROUPS, /*by_bucket*/ true, offset, limit);
        let want_min = want.iter().map(|(_, b)| *b).min().unwrap();
        let want_max = want.iter().map(|(_, b)| *b).max().unwrap();
        for (id, bucket) in &got {
            assert!(
                *bucket >= want_min && *bucket <= want_max,
                "page {page}: row (id={id}, bucket={bucket}) outside ground-truth \
                 bucket band [{want_min}, {want_max}]"
            );
        }
        // ids within a single page must be distinct (no row served twice in one page).
        let mut ids: Vec<i64> = got.iter().map(|(id, _)| *id).collect();
        let total = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(total, ids.len(), "page {page}: no id may repeat within a page");
    }
}

/// OFFSET past the end of the result returns zero rows (no panic, no wrap).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_beyond_end_returns_empty() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, N).await;

    let sql = "SELECT id, bucket FROM paging ORDER BY id DESC LIMIT 50 OFFSET 1000000";
    let got = select_id_bucket(&sess, sql).await;
    assert!(got.is_empty(), "OFFSET past the end must yield zero rows, got {}", got.len());

    // Boundary: OFFSET exactly at N returns empty; OFFSET = N-10 returns 10.
    let at_end = format!("SELECT id, bucket FROM paging ORDER BY id DESC LIMIT 50 OFFSET {N}");
    assert!(select_id_bucket(&sess, &at_end).await.is_empty());
    let near_end = format!(
        "SELECT id, bucket FROM paging ORDER BY id DESC LIMIT 50 OFFSET {}",
        N - 10
    );
    let tail = select_id_bucket(&sess, &near_end).await;
    assert_eq!(tail.len(), 10, "OFFSET N-10 with LIMIT 50 must return the last 10 rows");
    let want = ground_truth_page(N, N, false, (N - 10) as usize, 50);
    assert_eq!(tail, want, "the final partial page must equal the ground-truth tail");
}

/// An UPDATE that lands a row INSIDE the paged window must be reflected: the
/// overlay merge runs before the two-phase top-k, so the page shows the new
/// value. Here we bump a row's `bucket` so it sorts into the top window of a
/// `bucket DESC` page that it was not previously in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_window_reflects_update_overlay() {
    const N: i64 = 50_000;
    const TIE_GROUPS: i64 = 100;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, TIE_GROUPS).await;

    // Promote id=42 to a bucket strictly larger than any existing bucket value
    // (max existing bucket is TIE_GROUPS-1 = 99), so it must appear on the very
    // first `bucket DESC` page regardless of tie order.
    let new_bucket = TIE_GROUPS + 5; // 105
    exec(&sess, &format!("UPDATE paging SET bucket = {new_bucket} WHERE id = 42")).await;

    let sql = "SELECT id, bucket FROM paging ORDER BY bucket DESC LIMIT 50 OFFSET 0";
    let got = select_id_bucket(&sess, sql).await;
    assert_eq!(got.len(), 50);
    // The very first row must be the promoted one (unique max bucket).
    assert_eq!(
        got[0],
        (42, new_bucket),
        "UPDATE overlay must surface id=42 at the top of the bucket-DESC window"
    );
    // It must appear exactly once.
    let appearances = got.iter().filter(|(id, _)| *id == 42).count();
    assert_eq!(appearances, 1, "updated row must not be duplicated (cold + overlay)");

    // A second page (OFFSET 50) must NOT contain id=42 again.
    let page2 = select_id_bucket(
        &sess,
        "SELECT id, bucket FROM paging ORDER BY bucket DESC LIMIT 50 OFFSET 50",
    )
    .await;
    assert!(
        !page2.iter().any(|(id, _)| *id == 42),
        "the promoted row must appear only on the first page"
    );
}

// ── 2. LIMIT-no-ORDER returns exactly LIMIT matching rows ───────────────────────

/// `WHERE bucket = v LIMIT n` returns exactly `n` rows, every one matching the
/// predicate — the early per-file + cross-file limit cut never over- or
/// under-returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_no_order_returns_exactly_limit_matching_rows() {
    const N: i64 = 50_000;
    const TIE_GROUPS: i64 = 10; // 5_000 rows per bucket — plenty > LIMIT
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, TIE_GROUPS).await;

    let limit = 100usize;
    let sql = format!("SELECT id, bucket FROM paging WHERE bucket = 3 LIMIT {limit}");
    let got = select_id_bucket(&sess, &sql).await;
    assert_eq!(got.len(), limit, "must return exactly {limit} rows");
    assert!(
        got.iter().all(|(_, b)| *b == 3),
        "every returned row must satisfy bucket = 3"
    );
    // ids must be distinct (no row served twice across file boundaries).
    let mut ids: Vec<i64> = got.iter().map(|(id, _)| *id).collect();
    let total = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(total, ids.len(), "no duplicate rows under LIMIT");
}

/// `LIMIT n` where the predicate matches FEWER than `n` rows returns all the
/// matches and no more (no over-reach into non-matching rows).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_no_order_under_limit_returns_all_matches() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, N).await; // every bucket unique ⇒ exactly one match

    let got = select_id_bucket(
        &sess,
        "SELECT id, bucket FROM paging WHERE bucket = 7 LIMIT 100",
    )
    .await;
    assert_eq!(got.len(), 1, "only one row has the unique bucket value");
    assert_eq!(got[0].1, 7);
}

// ── 3. Keyset pagination correctness (must stay green) ──────────────────────────

/// `WHERE id > $1 ORDER BY id ASC LIMIT n` returns the exact global next page —
/// the contiguous `n` smallest ids strictly greater than the cursor. Walking
/// the table page-by-page must reconstruct the full ascending sequence with no
/// gaps and no repeats. This pins the keyset path's correctness after the
/// per-file reads were switched to bounded-concurrency `buffered` reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_pages_reconstruct_full_ascending_sequence() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, N).await;

    let page = 200usize;
    let mut cursor = 0i64;
    let mut seen: Vec<i64> = Vec::new();
    loop {
        let sql = format!(
            "SELECT id, bucket FROM paging WHERE id > {cursor} ORDER BY id ASC LIMIT {page}"
        );
        let got = select_id_bucket(&sess, &sql).await;
        if got.is_empty() {
            break;
        }
        // Each page is strictly ascending and starts just past the cursor.
        for w in got.windows(2) {
            assert!(w[0].0 < w[1].0, "keyset page must be strictly ascending");
        }
        assert!(got[0].0 > cursor, "first row must be > cursor");
        cursor = got.last().unwrap().0;
        seen.extend(got.iter().map(|(id, _)| *id));
        if seen.len() as i64 >= N {
            break;
        }
    }
    let expected: Vec<i64> = (1..=N).collect();
    assert_eq!(seen, expected, "keyset walk must reproduce 1..=N with no gaps/repeats");
}

/// Keyset with an OFFSET on top: `WHERE id > $1 ORDER BY id ASC LIMIT m OFFSET n`
/// must skip the first `n` of the keyset window — the per-file head limit was
/// widened to `limit + offset` to keep the superset reasoning valid.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_with_offset_skips_window_prefix() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, N, N).await;

    // After id > 1000, the ascending window is 1001, 1002, …; OFFSET 25 LIMIT 10
    // must be ids 1026..=1035.
    let got = select_id_bucket(
        &sess,
        "SELECT id, bucket FROM paging WHERE id > 1000 ORDER BY id ASC LIMIT 10 OFFSET 25",
    )
    .await;
    let want: Vec<(i64, i64)> = (1026..=1035).map(|k| (k, k)).collect();
    assert_eq!(got, want, "keyset+offset must skip the first 25 of the window");
}

// ── 4. Small live overlay no longer disables the LIMIT pushdowns ────────────────
//
// A handful of hot-tier UPDATE overrides / DELETE tombstones used to flip
// `post_read_shrinking` and silently demote keyset + unordered-LIMIT to the
// unbounded full-file read until the overlay drained. The per-file limits now
// carry `overlay_slack = |tombstones| + |updates|` extra rows instead, so the
// fast paths stay engaged AND exact. These tests pin both halves: the result
// is byte-correct under live overlay activity, and the engine counters prove
// the pushdown branch actually served the query.

/// Build an engine whose background overlay reconciler is DISABLED, so the
/// overlay planted by the test deterministically survives until the asserts
/// run (the default age trigger could otherwise drain it mid-test on a slow
/// box). Env is read once at `Engine::new`, so set/restore tightly around it.
fn engine_in_no_reconcile(dir: &TempDir) -> Engine {
    let prev = std::env::var("BASIN_OVERLAY_RECONCILE_SECS").ok();
    std::env::set_var("BASIN_OVERLAY_RECONCILE_SECS", "0");
    let eng = engine_in(dir);
    match prev {
        Some(v) => std::env::set_var("BASIN_OVERLAY_RECONCILE_SECS", v),
        None => std::env::remove_var("BASIN_OVERLAY_RECONCILE_SECS"),
    }
    eng
}

/// Overlay entry counts for `paging` — proves the fast-path DELETE/UPDATE
/// actually parked overlay entries (vs. silently taking the cold path, which
/// would make these tests vacuous).
fn paging_overlay_counts(eng: &Engine, project: basin_common::ProjectId) -> (u64, u64) {
    let table = basin_common::TableName::new("paging").unwrap();
    match eng.memtable_registry().get(&project, &table) {
        Some(e) => (e.memtable.update_count(), e.memtable.tombstone_count()),
        None => (0, 0),
    }
}

/// Keyset pagination with LIVE overlay activity (tombstones + UPDATE
/// overrides inside the window) must stay exact AND keep the per-file-LIMIT
/// branch engaged (counter proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyset_engages_and_stays_exact_with_live_overlay() {
    const N: i64 = 50_000;
    let dir = TempDir::new().unwrap();
    let eng = engine_in_no_reconcile(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    seed(&sess, N, N).await;

    // Plant the overlay INSIDE the keyset window: 3 fast-path DELETE
    // tombstones + 2 fast-path UPDATE overrides right past the cursor.
    exec(&sess, "DELETE FROM paging WHERE id IN (25001, 25003, 25005)").await;
    exec(&sess, "UPDATE paging SET bucket = -1 WHERE id = 25002").await;
    exec(&sess, "UPDATE paging SET label = 'patched' WHERE id = 25004").await;
    let (upd, tomb) = paging_overlay_counts(&eng, project);
    assert_eq!(
        (upd, tomb),
        (2, 3),
        "the DELETE/UPDATE statements must ride the hot-tier overlay fast \
         path (otherwise this test no longer exercises the overlay-tolerant \
         keyset read)"
    );

    let before = eng.keyset_fast_select_count();
    let got = select_id_bucket(
        &sess,
        "SELECT id, bucket FROM paging WHERE id > 25000 ORDER BY id ASC LIMIT 10",
    )
    .await;
    // Ground truth: ascending ids > 25000, minus the three deleted, with
    // id=25002 carrying its overridden bucket (-1); every other row keeps
    // bucket = id (tie_groups = N).
    let want: Vec<(i64, i64)> = [25002, 25004, 25006, 25007, 25008, 25009, 25010, 25011, 25012, 25013]
        .into_iter()
        .map(|id| (id, if id == 25002 { -1 } else { id }))
        .collect();
    assert_eq!(
        got, want,
        "keyset page under live overlay must suppress tombstoned rows, \
         surface the override value, and never under-fill"
    );
    assert_eq!(
        eng.keyset_fast_select_count(),
        before + 1,
        "the keyset per-file-LIMIT branch must stay ENGAGED while a small \
         overlay is live (overlay slack, not a fallback to the full read)"
    );

    // Page-walk across the overlay zone reconstructs the exact surviving
    // sequence (no gaps, no repeats, no resurrected tombstones).
    let mut cursor = 24_990i64;
    let mut seen: Vec<i64> = Vec::new();
    for _ in 0..5 {
        let sql = format!(
            "SELECT id, bucket FROM paging WHERE id > {cursor} ORDER BY id ASC LIMIT 7"
        );
        let page = select_id_bucket(&sess, &sql).await;
        assert_eq!(page.len(), 7, "every page in this range is full");
        for w in page.windows(2) {
            assert!(w[0].0 < w[1].0, "page must be strictly ascending");
        }
        cursor = page.last().unwrap().0;
        seen.extend(page.iter().map(|(id, _)| *id));
    }
    let want_walk: Vec<i64> = (24_991..=25_028)
        .filter(|id| ![25001, 25003, 25005].contains(id))
        .collect();
    assert_eq!(
        seen, want_walk,
        "page walk across the overlay zone must match the surviving id set"
    );
}

/// Unordered `LIMIT k` with LIVE overlay activity that bites inside the
/// scanned prefix (tombstones on matching rows, an override that LEAVES the
/// match set, an override that JOINS it) must still return exactly `k`
/// matching rows — and keep the early-exit branch engaged (counter proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_no_order_engages_and_stays_exact_with_live_overlay() {
    const N: i64 = 50_000;
    const TIE_GROUPS: i64 = 10; // bucket = id % 10; 5_000 matches for bucket=3
    let dir = TempDir::new().unwrap();
    let eng = engine_in_no_reconcile(&dir);
    let sess = open(&eng).await;
    let project = sess.project();
    seed(&sess, N, TIE_GROUPS).await;

    // Overlay biting the earliest-id matches (the first file's head):
    //   * ids 3, 13, 23  (bucket 3)  → tombstoned;
    //   * id 33          (bucket 3)  → override moves it OUT of the match set;
    //   * id 41          (bucket 1)  → override moves it INTO the match set;
    //   * id 43          (bucket 3)  → override keeps it matching (label only).
    exec(&sess, "DELETE FROM paging WHERE id IN (3, 13, 23)").await;
    exec(&sess, "UPDATE paging SET bucket = 4 WHERE id = 33").await;
    exec(&sess, "UPDATE paging SET bucket = 3 WHERE id = 41").await;
    exec(&sess, "UPDATE paging SET label = 'zz' WHERE id = 43").await;
    let (upd, tomb) = paging_overlay_counts(&eng, project);
    assert_eq!(
        (upd, tomb),
        (3, 3),
        "the DELETE/UPDATE statements must ride the hot-tier overlay fast path"
    );

    let limit = 100usize;
    let before = eng.unordered_limit_fast_select_count();
    let got = select_id_bucket(
        &sess,
        &format!("SELECT id, bucket FROM paging WHERE bucket = 3 LIMIT {limit}"),
    )
    .await;
    assert_eq!(
        got.len(),
        limit,
        "must return exactly {limit} rows — overlay suppression must never \
         leave a shortfall while more matches exist"
    );
    assert!(
        got.iter().all(|(_, b)| *b == 3),
        "every returned row must satisfy bucket = 3 (override re-check included)"
    );
    for banned in [3i64, 13, 23, 33] {
        assert!(
            !got.iter().any(|(id, _)| *id == banned),
            "id {banned} was tombstoned / moved out of the match set and must \
             not appear"
        );
    }
    let mut ids: Vec<i64> = got.iter().map(|(id, _)| *id).collect();
    let total = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(total, ids.len(), "no duplicate rows under LIMIT with overlay");
    assert_eq!(
        eng.unordered_limit_fast_select_count(),
        before + 1,
        "the unordered-LIMIT early-exit branch must stay ENGAGED while a \
         small overlay is live"
    );
}

// ── Timing prints (ignored; run explicitly on an idle box) ──────────────────────

const TIMING_N: i64 = 200_000;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing print; run with --ignored --nocapture on an idle box"]
async fn timing_offset_pagination_200k() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, TIMING_N, TIMING_N).await;

    // Warm any caches, then time a deep OFFSET page.
    let sql = "SELECT id, bucket, label FROM paging ORDER BY id DESC LIMIT 50 OFFSET 100";
    let _ = rows(&sess, sql).await;
    let t = Instant::now();
    let out = rows(&sess, sql).await;
    let dt = t.elapsed();
    println!(
        "[timing] OFFSET pagination 200k: {:?} ({} rows)",
        dt,
        count_rows(&out)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing print; run with --ignored --nocapture on an idle box"]
async fn timing_limit_no_order_200k() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, TIMING_N, /*tie_groups*/ 10).await;

    let sql = "SELECT id, bucket FROM paging WHERE bucket = 3 LIMIT 100";
    let _ = rows(&sess, sql).await;
    let t = Instant::now();
    let out = rows(&sess, sql).await;
    let dt = t.elapsed();
    println!(
        "[timing] LIMIT-no-ORDER 200k: {:?} ({} rows)",
        dt,
        count_rows(&out)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing print; run with --ignored --nocapture on an idle box"]
async fn timing_keyset_200k() {
    let dir = TempDir::new().unwrap();
    let eng = engine_in(&dir);
    let sess = open(&eng).await;
    seed(&sess, TIMING_N, TIMING_N).await;

    let sql = "SELECT id, bucket FROM paging WHERE id > 1000 ORDER BY id ASC LIMIT 50";
    let _ = rows(&sess, sql).await;
    let t = Instant::now();
    let out = rows(&sess, sql).await;
    let dt = t.elapsed();
    println!("[timing] keyset 200k: {:?} ({} rows)", dt, count_rows(&out));
}
