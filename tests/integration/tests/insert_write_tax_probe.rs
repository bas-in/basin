//! Diagnostic: where does Basin's bulk-INSERT "write tax" go?
//!
//! The bench shows Bulk INSERT ~5.7x slower than PG at 1M. This test
//! separates the two cost centres so we can attack the right one:
//!
//!   1. INSERT critical path — the shard auto-commit path returns after
//!      `write_batch` = WAL IPC-encode + append + in-memory tail push. This
//!      is what the bench's INSERT-loop timer actually measures. Vortex
//!      encoding is NOT on this path (it happens on the background flush).
//!
//!   2. Vortex flush encode — `flush_to_parquet` drains the tail and encodes
//!      to Vortex. EncodingMode::Fast (the always-on mode for non-tx direct
//!      INSERT and hot-tier flush) skips the encoding search;
//!      EncodingMode::Best runs the full BtrBlocks cascade (smallest files,
//!      slowest) and is reserved for in-tx INSERT + compaction.
//!
//! By timing (1) and (2) separately we learn:
//!   - If the INSERT-loop time dominates, the tax is in SQL parse + WAL, NOT
//!     the cascade → COPY path / WAL work is the lever.
//!   - If the flush time dominates, the Vortex encode is the lever.
//!
//! Run: cargo test --release -p basin-integration-tests \
//!        --test insert_write_tax_probe -- --ignored --nocapture
//!
//! ─────────────────────────────────────────────────────────────────────────
//!
//! ABLATION PROBE (`insert_write_tax_ablation_10k`): decompose the
//! `bulk_insert_10000_ms` benchmark shape (~2248ms Basin vs ~123ms PG) using
//! only test-level schema/statement variants — no engine instrumentation.
//! The same 10k deterministically-generated rows are inserted under variants
//! that each remove (or add) exactly one suspected cost source; the delta vs
//! the faithful baseline attributes the cost.
//!
//! Benchmark-fidelity notes (verified against
//! `tests/integration/tests/compare_postgres_common.rs`, the source of the
//! `bulk_insert_{rows}_ms` metric):
//!
//!   * FRESH, not accumulated: the timed bulk insert is the one-shot seeding
//!     of a freshly created `events` table on a freshly built engine — a
//!     single sample (n=1), never repeated, so rows do NOT accumulate across
//!     samples. We mirror that with a fresh shard-backed engine per repeat
//!     (sharing one engine across variants would let earlier variants' WAL /
//!     hot-tier state contaminate later timings; the benchmark's own
//!     isolation unit is a whole `build_basin_engine()` instance).
//!
//!   * NO FK in the benchmark: the benchmark `events` DDL
//!     (compare_postgres_common.rs ~L4546) is `id BIGINT NOT NULL PRIMARY
//!     KEY, user_id BIGINT NOT NULL, amount DOUBLE PRECISION NOT NULL,
//!     status TEXT NOT NULL, created_at BIGINT NOT NULL, payload JSONB` —
//!     `user_id` has NO `REFERENCES users(id)` constraint. The baseline here
//!     therefore has NO FK; instead of a "no_fk" ablation we run a `with_fk`
//!     ADDITION variant so the hypothetical FK tax is still measured.
//!
//!   * Single statement at 10k scale: `insert_batch_for(10_000)` returns
//!     10_000, so the benchmark issues exactly ONE multi-row INSERT. Each
//!     tuple carries a `'<json>'::jsonb` cast on the payload literal
//!     (optional on Basin, kept for byte-parity with PG); baseline keeps it.
//!
//!   * Pre-timing engine state: before the timed insert the benchmark has
//!     already created and seeded `users` (rows/10 = 1000 rows, NULL
//!     last_login every 10th) and `categories` (5 rows). We replicate that
//!     seeding before every timed variant, which also serves as the warm-up
//!     (first-execute / catalog-init costs land there, as in the benchmark).
//!
//!   * Engine shape: the benchmark engine is shard-backed with a real WAL
//!     (flush_interval 200ms, flush_max_bytes 1MiB) and the background loop
//!     running — `build_bench()` below matches those parameters exactly. A
//!     `shard: None` engine (the lighter harness some probe files use) would
//!     remove the WAL/stripe IPC component this probe is trying to measure.
//!
//! Variants (same generated rows everywhere, K=3 reps, min reported):
//!   baseline      — exact benchmark DDL + one 10k-row INSERT with ::jsonb.
//!   with_fk       — baseline + `REFERENCES users(id)` on user_id
//!                   (fk_tax_ms = with_fk - baseline; NOT in the benchmark).
//!   no_pk         — id is plain BIGINT NOT NULL (pk_tax = baseline - no_pk).
//!   no_jsonb      — payload column TEXT; same literal bytes minus the
//!                   `::jsonb` cast (jsonb_tax = baseline - no_jsonb).
//!   floor         — no PK + TEXT payload (and no FK, like the benchmark):
//!                   parse/scan + batch build + WAL/hot-tier write floor.
//!   second_stmt   — untimed baseline 10k fill, then time a SECOND 10k
//!                   INSERT (ids 10000..20000) into the same table — exposes
//!                   O(N_existing) PK-uniqueness scans if present.
//!   half_rows     — baseline shape with 5k rows — linearity check.
//!
//! No timing assertions — correctness asserts only (row counts).

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Int64Array, RecordBatch};
use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::ProjectId;
use basin_engine::{Engine, EngineConfig, ExecResult, ProjectSession};
use basin_shard::{Shard, ShardConfig};
use basin_storage::{Storage, StorageConfig};
use basin_wal::{LocalWal, Wal, WalConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

async fn build() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
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
        storage,
        catalog,
        shard: Some(shard),
    });
    (sd, wd, engine, bg, wal)
}

/// Seed `rows` into `events` in 10k batches (mirrors the bench shape: 5
/// scalar columns + a ~200B JSONB payload), timing the INSERT loop. Returns
/// (insert_loop_ms, flush_ms). The flush is timed separately via a SELECT
/// that forces `shard.flush_to_parquet()`.
async fn measure(engine: &Engine, rows: i64, label: &str) {
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    sess.execute(
        "CREATE TABLE events (id BIGINT NOT NULL PRIMARY KEY, user_id BIGINT, amount DOUBLE PRECISION, status TEXT, created_at BIGINT, payload JSONB)",
    )
    .await
    .unwrap();

    let batch = 10_000i64;
    let insert_started = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let lo = id;
        let hi = (id + batch).min(rows);
        let mut stmt = String::with_capacity((hi - lo) as usize * 240);
        stmt.push_str("INSERT INTO events VALUES ");
        for k in lo..hi {
            if k > lo {
                stmt.push(',');
            }
            let payload = format!(
                r#"{{"category":"cat{}","device":{{"os":"ios","version":"1.{}"}},"tags":[{},{},{}],"metadata":{{"score":{}}}}}"#,
                k % 20,
                k % 9,
                k % 3,
                k % 5,
                k % 7,
                k % 100
            );
            stmt.push_str(&format!(
                "({k}, {}, {}, 'pending', {k}, '{payload}'::jsonb)",
                k % 1000,
                k as f64 * 0.5
            ));
        }
        sess.execute(&stmt).await.unwrap();
        id = hi;
    }
    let insert_loop_ms = insert_started.elapsed().as_secs_f64() * 1000.0;

    // Force the tail → Vortex flush and time it. A SELECT triggers
    // shard.flush_to_parquet() in the fast_select path.
    let flush_started = Instant::now();
    let _ = sess
        .execute("SELECT id FROM events WHERE id = 1")
        .await
        .unwrap();
    let flush_ms = flush_started.elapsed().as_secs_f64() * 1000.0;

    // Non-tx direct INSERT is always-on `Fast` (#203 removed the opt-in env
    // gate); the in-tx path keeps `Best`. This probe issues non-tx INSERTs,
    // so the encode is `Fast`.
    let mode = "Fast";
    let rps = rows as f64 / (insert_loop_ms / 1000.0);
    println!(
        "[write-tax] {label:>6} rows={rows:>7} mode={mode:<4} \
         insert-loop={insert_loop_ms:8.1}ms ({:.1}us/row, {rps:.0} rows/s via pre-parse path)  \
         first-SELECT(incl flush)={flush_ms:8.1}ms",
        insert_loop_ms * 1000.0 / rows as f64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn write_tax_200k() {
    // Non-tx direct INSERT is always-on `Fast` (#203); this measures that
    // path at 200k rows.
    let (_sd, _wd, engine, bg, wal) = build().await;
    measure(&engine, 200_000, "200k").await;
    bg.shutdown().await;
    wal.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "diagnostic — run explicitly with --ignored"]
async fn write_tax_1m() {
    // Same always-on `Fast` non-tx path, at 1M rows (the bench scale).
    let (_sd, _wd, engine, bg, wal) = build().await;
    measure(&engine, 1_000_000, "1m").await;
    bg.shutdown().await;
    wal.close().await.unwrap();
}

// ═════════════════════════════════════════════════════════════════════════
// Ablation probe — decompose bulk_insert_10000_ms (see module docs).
// ═════════════════════════════════════════════════════════════════════════

/// Benchmark-faithful engine: identical to `build()` except the WAL flush
/// parameters match `build_basin_engine()` in compare_postgres_common.rs
/// (flush_interval 200ms — `build()` above uses 50ms; flush_max_bytes 1MiB).
async fn build_bench() -> (
    TempDir,
    TempDir,
    Engine,
    basin_shard::ShardBackgroundHandle,
    Arc<dyn Wal>,
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
            flush_interval: Duration::from_millis(200),
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
        storage,
        catalog,
        shard: Some(shard),
    });
    (sd, wd, engine, bg, wal)
}

// ─── select/count helpers (copied from values_fast_ingest.rs preamble) ──────

async fn select(sess: &ProjectSession, sql: &str) -> Vec<RecordBatch> {
    match sess.execute(sql).await.unwrap() {
        ExecResult::Rows { batches, .. } => batches,
        other => panic!("expected rows from {sql:?}, got {other:?}"),
    }
}

fn col_i64(batches: &[RecordBatch], name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            });
        }
    }
    out
}

// ─── deterministic row generators (copied from compare_postgres_common.rs) ──

const EPOCH: i64 = 1_700_000_000;

/// Benchmark scale: 10k events; users = rows/10 = 1000, so user_id =
/// id % 1000 always hits a seeded user (matters for the with_fk variant).
const ABLATION_ROWS: usize = 10_000;
const ABLATION_USERS: i64 = (ABLATION_ROWS / 10) as i64;
const ABLATION_REPS: usize = 3;

fn email_for(i: i64) -> String {
    let domain = match i % 10 {
        0 => "gmail.com",
        1 => "outlook.com",
        2 => "yahoo.com",
        3 => "proton.me",
        4 => "icloud.com",
        5 => "company.io",
        6 => "example.org",
        7 => "test.dev",
        8 => "fastmail.com",
        _ => "tutanota.com",
    };
    format!("user{:08}@{}", i, domain)
}

fn status_for(i: i64) -> &'static str {
    match i % 4 {
        0 => "pending",
        1 => "active",
        2 => "completed",
        _ => "archived",
    }
}

/// Deterministic ~240-byte JSON payload — byte-identical to the benchmark's
/// `payload_for`, so canonicalization cost is measured on realistic shapes
/// (nested objects, varying-length array, float).
fn payload_for(i: i64) -> String {
    let category = match i % 3 {
        0 => "purchase",
        1 => "signup",
        _ => "click",
    };
    let device_os = if i % 2 == 0 { "ios" } else { "android" };
    let major = 1 + (i % 3);
    let minor = (i / 3) % 5;
    let patch = i % 10;
    let version = format!("{major}.{minor}.{patch}");
    let tags = match i % 3 {
        0 => r#"["red","green"]"#,
        1 => r#"["blue"]"#,
        _ => r#"["red","green","blue"]"#,
    };
    let campaign = match i % 4 {
        0 => "promo_2024",
        1 => "spring_sale",
        2 => "newsletter",
        _ => "referral",
    };
    let score = 0.5 + ((i % 199) as f64) * 0.5;
    format!(
        r#"{{"category":"{category}","tags":{tags},"device":{{"os":"{device_os}","version":"{version}"}},"metadata":{{"campaign":"{campaign}","score":{score}}}}}"#
    )
}

// ─── variant plumbing ────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct AblationShape {
    /// `id BIGINT NOT NULL PRIMARY KEY` vs plain `id BIGINT NOT NULL`.
    pk: bool,
    /// Add `REFERENCES users(id)` on user_id. NOT in the benchmark (see
    /// module docs) — this is an addition variant, not an ablation.
    fk: bool,
    /// `payload JSONB` + `'…'::jsonb` literal vs `payload TEXT` + plain
    /// string literal (same JSON bytes either way, only the cast differs).
    jsonb: bool,
}

const ABLATION_BASELINE: AblationShape = AblationShape {
    pk: true,
    fk: false,
    jsonb: true,
};

fn ablation_events_ddl(shape: AblationShape) -> String {
    let id_col = if shape.pk {
        "id BIGINT NOT NULL PRIMARY KEY"
    } else {
        "id BIGINT NOT NULL"
    };
    let user_col = if shape.fk {
        "user_id BIGINT NOT NULL REFERENCES users(id)"
    } else {
        "user_id BIGINT NOT NULL"
    };
    let payload_col = if shape.jsonb {
        "payload JSONB"
    } else {
        "payload TEXT"
    };
    format!(
        "CREATE TABLE events ({id_col}, {user_col}, \
         amount DOUBLE PRECISION NOT NULL, status TEXT NOT NULL, \
         created_at BIGINT NOT NULL, {payload_col})"
    )
}

/// One multi-row INSERT covering ids `[start, start+count)` with the exact
/// per-row shape of the benchmark: user_id = id % 1000, amount = id * 0.5,
/// status_for, created_at = EPOCH + id, ~240-byte payload. The benchmark
/// appends `::jsonb` to the payload literal; TEXT variants drop only the
/// cast, keeping the literal bytes identical.
fn ablation_insert_stmt(start: i64, count: usize, jsonb: bool) -> String {
    let mut stmt = String::with_capacity(count * 240);
    stmt.push_str("INSERT INTO events VALUES ");
    for j in 0..count {
        if j > 0 {
            stmt.push(',');
        }
        let id = start + j as i64;
        let user_id = id % ABLATION_USERS;
        let amount = (id as f64) * 0.5;
        let status = status_for(id);
        let payload = payload_for(id);
        let cast = if jsonb { "::jsonb" } else { "" };
        stmt.push_str(&format!(
            "({id}, {user_id}, {amount}, '{status}', {}, '{payload}'{cast})",
            EPOCH + id
        ));
    }
    stmt
}

/// Replicate the benchmark's pre-insert engine state: `users` (1000 rows,
/// NULL last_login every 10th) and `categories` (5 buckets) created and
/// seeded BEFORE the timed events insert. Besides fidelity, this seeding is
/// the warm-up the benchmark gets for free — first-execute / catalog-init
/// costs are paid here, outside the timed window.
async fn ablation_seed_env(sess: &ProjectSession) {
    sess.execute(
        "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email TEXT NOT NULL, \
         created_at BIGINT NOT NULL, last_login BIGINT)",
    )
    .await
    .unwrap();
    sess.execute(
        "CREATE TABLE categories (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL, \
         min_amt DOUBLE PRECISION NOT NULL, max_amt DOUBLE PRECISION NOT NULL)",
    )
    .await
    .unwrap();

    let mut stmt = String::with_capacity(ABLATION_USERS as usize * 80);
    stmt.push_str("INSERT INTO users VALUES ");
    for i in 0..ABLATION_USERS {
        if i > 0 {
            stmt.push(',');
        }
        let last_login = if i % 10 == 0 {
            "NULL".to_string()
        } else {
            (EPOCH + i + 100_000).to_string()
        };
        stmt.push_str(&format!(
            "({i}, '{}', {}, {last_login})",
            email_for(i),
            EPOCH + i
        ));
    }
    sess.execute(&stmt).await.expect("seed users");

    // Same 5 buckets as the benchmark's cat_rows; max_amt of the event
    // distribution = (ROWS-1) * 0.5.
    let max_amt = (ABLATION_ROWS as f64 - 1.0) * 0.5;
    let cat_rows: [(i64, &str, f64, f64); 5] = [
        (1, "micro", 0.0, max_amt * 0.20),
        (2, "small", max_amt * 0.20 + 0.001, max_amt * 0.40),
        (3, "medium", max_amt * 0.40 + 0.001, max_amt * 0.60),
        (4, "large", max_amt * 0.60 + 0.001, max_amt * 0.80),
        (5, "xlarge", max_amt * 0.80 + 0.001, max_amt + 1.0),
    ];
    let mut stmt = String::from("INSERT INTO categories VALUES ");
    for (i, (id, name, lo, hi)) in cat_rows.iter().enumerate() {
        if i > 0 {
            stmt.push(',');
        }
        stmt.push_str(&format!("({id}, '{name}', {lo}, {hi})"));
    }
    sess.execute(&stmt).await.expect("seed categories");
}

/// One rep of one variant on a FRESH benchmark-shaped engine (fresh dirs,
/// fresh WAL, background loop running — the benchmark's isolation unit).
/// Returns the timed milliseconds for the statement under measurement only:
/// the `Instant` brackets exactly `sess.execute(&stmt)` and nothing else
/// (statement string is built before the timer starts).
async fn ablation_run_one(shape: AblationShape, rows: usize, second_statement: bool) -> f64 {
    let (_sd, _wd, engine, bg, wal) = build_bench().await;
    let sess = engine.open_session(ProjectId::new()).await.unwrap();
    ablation_seed_env(&sess).await;
    sess.execute(&ablation_events_ddl(shape)).await.unwrap();

    if second_statement {
        // Untimed baseline-shaped fill (ids 0..rows), then time the SECOND
        // statement (ids rows..2*rows) against the now-populated table.
        let warm = ablation_insert_stmt(0, rows, shape.jsonb);
        sess.execute(&warm).await.expect("first (untimed) insert");
    }

    let start_id = if second_statement { rows as i64 } else { 0 };
    let stmt = ablation_insert_stmt(start_id, rows, shape.jsonb);

    let started = Instant::now();
    sess.execute(&stmt).await.expect("timed insert");
    let ms = started.elapsed().as_secs_f64() * 1000.0;

    // Correctness only — no timing assertions.
    let expect = if second_statement { 2 * rows } else { rows } as i64;
    let r = select(&sess, "SELECT count(*) AS c FROM events").await;
    assert_eq!(col_i64(&r, "c"), vec![Some(expect)]);

    bg.shutdown().await;
    wal.close().await.unwrap();
    ms
}

async fn ablation_run_variant(shape: AblationShape, rows: usize, second_statement: bool) -> f64 {
    let mut best = f64::INFINITY;
    for _ in 0..ABLATION_REPS {
        let ms = ablation_run_one(shape, rows, second_statement).await;
        if ms < best {
            best = ms;
        }
    }
    best
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing probe; run explicitly with --ignored"]
async fn insert_write_tax_ablation_10k() {
    println!(
        "[insert-tax] bulk INSERT ablation: {ABLATION_ROWS} rows/stmt, \
         {ABLATION_REPS} reps/variant (fresh engine each), min reported"
    );
    println!(
        "[insert-tax] baseline mirrors compare_postgres bulk_insert_10000_ms: fresh shard-backed \
         engine, users({ABLATION_USERS})+categories pre-seeded, ONE multi-row INSERT, PK on id, \
         JSONB payload with ::jsonb cast, NO FK (the benchmark DDL has none)"
    );

    let baseline = ablation_run_variant(ABLATION_BASELINE, ABLATION_ROWS, false).await;
    let with_fk = ablation_run_variant(
        AblationShape {
            fk: true,
            ..ABLATION_BASELINE
        },
        ABLATION_ROWS,
        false,
    )
    .await;
    let no_pk = ablation_run_variant(
        AblationShape {
            pk: false,
            ..ABLATION_BASELINE
        },
        ABLATION_ROWS,
        false,
    )
    .await;
    let no_jsonb = ablation_run_variant(
        AblationShape {
            jsonb: false,
            ..ABLATION_BASELINE
        },
        ABLATION_ROWS,
        false,
    )
    .await;
    let floor = ablation_run_variant(
        AblationShape {
            pk: false,
            fk: false,
            jsonb: false,
        },
        ABLATION_ROWS,
        false,
    )
    .await;
    let second_stmt = ablation_run_variant(ABLATION_BASELINE, ABLATION_ROWS, true).await;
    let half_rows = ablation_run_variant(ABLATION_BASELINE, ABLATION_ROWS / 2, false).await;

    println!(
        "\n{:<24} {:>10} {:>22}",
        "variant", "min_ms", "delta_vs_baseline_ms"
    );
    let row = |name: &str, ms: f64| {
        println!("{:<24} {:>10.1} {:>+22.1}", name, ms, ms - baseline);
    };
    row("baseline", baseline);
    row("with_fk (+REFERENCES)", with_fk);
    row("no_pk", no_pk);
    row("no_jsonb (TEXT)", no_jsonb);
    row("floor (no_pk+TEXT)", floor);
    row("second_stmt (10k more)", second_stmt);
    row("half_rows (5k)", half_rows);

    println!(
        "\n[insert-tax] interpretation: pk_tax_ms = baseline - no_pk = {:.1}; \
         jsonb_tax_ms = baseline - no_jsonb = {:.1}; \
         fk_tax_ms (hypothetical, not in benchmark) = with_fk - baseline = {:.1}; \
         floor_ms (parse + batch build + WAL/hot-tier) = {:.1}; \
         existing-data growth = second_stmt - baseline = {:.1} (>0 suggests O(N_cold) PK scan); \
         linearity: half_rows {:.1} vs baseline/2 = {:.1}",
        baseline - no_pk,
        baseline - no_jsonb,
        with_fk - baseline,
        floor,
        second_stmt - baseline,
        half_rows,
        baseline / 2.0
    );
}
