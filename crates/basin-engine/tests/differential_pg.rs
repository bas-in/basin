//! Differential PG-parity harness.
//!
//! Each case replays an identical SQL script against (a) basin's embedded
//! engine and (b) a real PostgreSQL, then asserts the two backends agree on
//! the SQLSTATE outcome of a designated "probe" statement.
//!
//! The real-PG leg only runs when `PG_DIFF_TEST_DSN` is set to a libpq DSN
//! (e.g. `postgres://user:pw@localhost/postgres`). When the env var is
//! absent the cases SKIP CLEANLY (return early, test passes) — there is no
//! `#[ignore]`, so CI without a PG instance still exercises the basin leg
//! and the harness fails honestly if basin's own behaviour regresses.
//!
//! `DifferentialRunner` is the shared driver: it owns a fresh basin
//! `ProjectSession` and (when a DSN is present) a `tokio_postgres::Client`
//! against a throwaway schema, runs the setup statements on both, then
//! compares the probe statement's success/SQLSTATE.

use std::sync::Arc;

use basin_catalog::InMemoryCatalog;
use basin_common::{BasinError, ProjectId};
use basin_engine::{Engine, EngineConfig, ProjectSession};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// --------------------------------------------------------------------------
// basin embedded engine plumbing (same shape as the other test files)
// --------------------------------------------------------------------------

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

/// Map a basin error to the PG SQLSTATE basin's router would surface, so the
/// two backends can be compared on equal footing. Only the codes the
/// differential cases below probe for are modelled.
fn basin_sqlstate(err: &BasinError) -> &'static str {
    match err {
        BasinError::UniqueViolation(_) => "23505",
        BasinError::CheckViolation(_) => "23514",
        // insufficient_privilege — RLS WITH CHECK / USING write violation.
        BasinError::RlsViolation(_) => "42501",
        // string_data_right_truncation — VARCHAR(n)/CHAR(n) over-length.
        BasinError::StringTooLong(_) => "22001",
        _ => "XXOTHER",
    }
}

/// The outcome of running one statement: either it succeeded, or it failed
/// with a specific SQLSTATE. Used to compare basin vs PG.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Err(String),
}

/// Shared differential driver. Holds the basin session and an optional real
/// PG client (present only when `PG_DIFF_TEST_DSN` is set).
struct DifferentialRunner {
    sess: ProjectSession,
    pg: Option<tokio_postgres::Client>,
    /// Unique throwaway schema name so concurrent test runs don't collide.
    pg_schema: String,
    _dir: TempDir,
    _eng: Engine,
}

impl DifferentialRunner {
    /// Build a runner. If `PG_DIFF_TEST_DSN` is unset, `pg` is `None` and the
    /// case will run basin-only (still a real assertion against basin).
    async fn new(case_tag: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let sess = open(&eng).await;

        let (pg, pg_schema) = match std::env::var("PG_DIFF_TEST_DSN") {
            Ok(dsn) if !dsn.trim().is_empty() => {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .expect("PG_DIFF_TEST_DSN connect");
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let schema = format!("basin_diff_{}_{}", case_tag, std::process::id());
                client
                    .batch_execute(&format!(
                        "DROP SCHEMA IF EXISTS {schema} CASCADE; \
                         CREATE SCHEMA {schema}; SET search_path TO {schema};"
                    ))
                    .await
                    .expect("PG schema bootstrap");
                (Some(client), schema)
            }
            _ => (None, String::new()),
        };

        Self {
            sess,
            pg,
            pg_schema,
            _dir: dir,
            _eng: eng,
        }
    }

    /// Run a setup statement on basin and (if configured) PG. Both are
    /// expected to succeed; a mismatch here is itself a parity failure.
    async fn setup(&self, sql: &str) {
        self.sess
            .execute(sql)
            .await
            .unwrap_or_else(|e| panic!("basin setup failed for {sql:?}: {e:?}"));
        if let Some(pg) = &self.pg {
            pg.batch_execute(sql)
                .await
                .unwrap_or_else(|e| panic!("PG setup failed for {sql:?}: {e}"));
        }
    }

    /// Run the probe statement on basin and PG and return both outcomes.
    async fn probe(&self, sql: &str) -> (Outcome, Option<Outcome>) {
        let basin = match self.sess.execute(sql).await {
            Ok(_) => Outcome::Ok,
            Err(e) => Outcome::Err(basin_sqlstate(&e).to_string()),
        };
        let pg = match &self.pg {
            None => None,
            Some(pg) => Some(match pg.batch_execute(sql).await {
                Ok(_) => Outcome::Ok,
                Err(e) => {
                    let code = e
                        .as_db_error()
                        .map(|db| db.code().code().to_string())
                        .unwrap_or_else(|| "XXOTHER".to_string());
                    Outcome::Err(code)
                }
            }),
        };
        (basin, pg)
    }

    /// Run a single-cell `SELECT count(*) ...` probe on basin and (when a
    /// DSN is set) PG, returning the two `count(*)` values. Used by the
    /// TABLESAMPLE case, where the two engines' RNGs can't produce
    /// bit-identical samples — so we compare the *derived property* (the
    /// row count lands in a tolerance band) on each side independently.
    async fn count_both(&self, sql: &str) -> (i64, Option<i64>) {
        use basin_engine::ExecResult;
        let basin = match self.sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                let b = &batches[0];
                let col = b.column(0);
                if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    a.value(0)
                } else {
                    let a = col
                        .as_any()
                        .downcast_ref::<arrow_array::Int32Array>()
                        .expect("count(*) is an integer column");
                    a.value(0) as i64
                }
            }
            other => panic!("expected a count row from {sql:?}, got {other:?}"),
        };
        let pg = match &self.pg {
            None => None,
            Some(pg) => {
                let row = pg
                    .query_one(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("PG count probe failed for {sql:?}: {e}"));
                Some(row.get::<_, i64>(0))
            }
        };
        (basin, pg)
    }
}

impl Drop for DifferentialRunner {
    fn drop(&mut self) {
        // Best-effort schema cleanup; the PG client's connection task is
        // detached so this is fire-and-forget if it can't complete in time.
        if self.pg.is_some() && !self.pg_schema.is_empty() {
            let schema = self.pg_schema.clone();
            // We can't easily await in Drop; rely on DROP SCHEMA IF EXISTS in
            // the next run with the same pid+tag to clean up. Document intent.
            let _ = schema;
        }
    }
}

// --------------------------------------------------------------------------
// Cases
// --------------------------------------------------------------------------

/// BUG #136: a plain-column `CREATE UNIQUE INDEX` must reject a duplicate
/// INSERT with SQLSTATE 23505 on BOTH basin and real PG.
#[tokio::test]
async fn diff_create_unique_index_duplicate_insert() {
    let r = DifferentialRunner::new("uidx_dup").await;

    r.setup("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await;
    r.setup("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await;
    r.setup("INSERT INTO users (id, email) VALUES (1, 'ada@example.com')")
        .await;

    let (basin, pg) = r
        .probe("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
        .await;

    // basin leg (always runs): the duplicate must be rejected with 23505.
    assert_eq!(
        basin,
        Outcome::Err("23505".to_string()),
        "basin must reject duplicate under CREATE UNIQUE INDEX with 23505"
    );

    // PG leg (only when a DSN is configured): the two must agree.
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on CREATE UNIQUE INDEX duplicate-insert outcome"
        );
    }
}

/// Distinct values under a `CREATE UNIQUE INDEX` succeed on both backends.
#[tokio::test]
async fn diff_create_unique_index_distinct_values_ok() {
    let r = DifferentialRunner::new("uidx_ok").await;

    r.setup("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .await;
    r.setup("CREATE UNIQUE INDEX users_email_uidx ON users (email)")
        .await;
    r.setup("INSERT INTO users (id, email) VALUES (1, 'a@x.com')")
        .await;

    let (basin, pg) = r
        .probe("INSERT INTO users (id, email) VALUES (2, 'b@x.com')")
        .await;

    assert_eq!(basin, Outcome::Ok, "distinct values must succeed in basin");
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin and PG disagree on distinct-value insert");
    }
}

/// Multi-column `CREATE UNIQUE INDEX` enforces on the tuple on both backends.
#[tokio::test]
async fn diff_create_unique_index_multicolumn_duplicate() {
    let r = DifferentialRunner::new("uidx_multi").await;

    r.setup(
        "CREATE TABLE memberships (\
             id BIGINT PRIMARY KEY, org_id BIGINT NOT NULL, user_id BIGINT NOT NULL)",
    )
    .await;
    r.setup(
        "CREATE UNIQUE INDEX memberships_org_user_uidx \
         ON memberships (org_id, user_id)",
    )
    .await;
    r.setup("INSERT INTO memberships VALUES (1, 10, 100)").await;
    r.setup("INSERT INTO memberships VALUES (2, 10, 200)").await;

    let (basin, pg) = r.probe("INSERT INTO memberships VALUES (3, 10, 100)").await;

    assert_eq!(
        basin,
        Outcome::Err("23505".to_string()),
        "basin must reject duplicate tuple under multi-col CREATE UNIQUE INDEX"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on multi-column CREATE UNIQUE INDEX duplicate"
        );
    }
}

/// BUG #140: an INSERT of a value longer than a declared `VARCHAR(n)`
/// must be rejected with SQLSTATE 22001 (`string_data_right_truncation`)
/// — the same error PG raises (`value too long for type character
/// varying(n)`) — on BOTH basin and real PG.
#[tokio::test]
async fn diff_varchar_overlength_insert_rejected() {
    let r = DifferentialRunner::new("vchar_over").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY, code VARCHAR(5))")
        .await;
    // Exactly-n is accepted on both backends.
    r.setup("INSERT INTO t (id, code) VALUES (1, 'abcde')")
        .await;

    let (basin, pg) = r
        .probe("INSERT INTO t (id, code) VALUES (2, 'toolong')")
        .await;

    assert_eq!(
        basin,
        Outcome::Err("22001".to_string()),
        "basin must reject over-length VARCHAR(n) INSERT with 22001"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on over-length VARCHAR(n) INSERT outcome"
        );
    }
}

/// BUG #140 (CHAR variant): over-length `CHAR(n)` INSERT must also raise
/// SQLSTATE 22001 on both backends; an at-or-under-length value is
/// accepted (PG blank-pads it to n, as does basin).
#[tokio::test]
async fn diff_char_overlength_insert_rejected() {
    let r = DifferentialRunner::new("char_over").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY, code CHAR(3))")
        .await;
    r.setup("INSERT INTO t (id, code) VALUES (1, 'ab')").await;

    let (basin, pg) = r.probe("INSERT INTO t (id, code) VALUES (2, 'abcd')").await;

    assert_eq!(
        basin,
        Outcome::Err("22001".to_string()),
        "basin must reject over-length CHAR(n) INSERT with 22001"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG disagree on over-length CHAR(n) INSERT outcome"
        );
    }
}

/// BUG #134: `TABLESAMPLE` must actually sample. PG's and basin's RNGs are
/// independent, so a `REPEATABLE(seed)` sample cannot be bit-identical
/// across the two engines — instead we compare the *derived property*:
/// over a 10_000-row table, `BERNOULLI(10)` must land in a generous
/// tolerance band on **both** backends (and the hard 0% / 100% edges must
/// match exactly). This catches the historical bug (basin returned all
/// 10_000) on the basin leg even when no PG DSN is configured.
#[tokio::test]
async fn diff_tablesample_bernoulli_count_in_band() {
    let r = DifferentialRunner::new("tsample").await;

    r.setup("CREATE TABLE samp (id BIGINT)").await;
    // 10_000 rows, 1000 per INSERT to keep the VALUES list reasonable.
    for base in (0..10_000).step_by(1000) {
        let mut sql = String::from("INSERT INTO samp (id) VALUES ");
        for j in base..base + 1000 {
            if j > base {
                sql.push(',');
            }
            sql.push_str(&format!("({j})"));
        }
        r.setup(&sql).await;
    }

    // ~10% of 10_000 ≈ 1000. Band [600, 1400] is ≈ ±4.5σ for a
    // Binomial(10000, 0.1) (σ ≈ 30) — it will not flake, yet rejects the
    // all-rows bug (10000) and the empty-result failure mode.
    let (b, p) = r
        .count_both("SELECT count(*) FROM samp TABLESAMPLE BERNOULLI(10) REPEATABLE(12345)")
        .await;
    assert!(
        (600..=1400).contains(&b),
        "basin BERNOULLI(10) count {b} outside [600,1400] (sample not taken?)"
    );
    if let Some(p) = p {
        assert!(
            (600..=1400).contains(&p),
            "PG BERNOULLI(10) count {p} outside [600,1400]"
        );
    }

    // Hard edges must agree exactly on both engines.
    let (b0, p0) = r
        .count_both("SELECT count(*) FROM samp TABLESAMPLE BERNOULLI(0)")
        .await;
    assert_eq!(b0, 0, "basin BERNOULLI(0) must be 0");
    if let Some(p0) = p0 {
        assert_eq!(p0, 0, "PG BERNOULLI(0) must be 0");
    }

    let (b100, p100) = r
        .count_both("SELECT count(*) FROM samp TABLESAMPLE BERNOULLI(100)")
        .await;
    assert_eq!(b100, 10_000, "basin BERNOULLI(100) must be all rows");
    if let Some(p100) = p100 {
        assert_eq!(p100, 10_000, "PG BERNOULLI(100) must be all rows");
    }
}

/// BUG #132 — INTENTIONAL, DOCUMENTED DIVERGENCE.
///
/// PostgreSQL *succeeds* at `CREATE TRIGGER` (it has a PL/pgSQL trigger
/// runtime). Basin has no trigger runtime (ADR 0012), so silently
/// "succeeding" would be a correctness lie — apps would believe their
/// audit/derived/validation triggers fire when nothing does. Basin
/// therefore deliberately diverges and rejects loudly.
///
/// This case asserts basin's honest-reject explicitly and does NOT call
/// `assert_eq!(basin, pg)` for the probe — forcing basin to match PG's
/// success here would re-introduce the bug. Instead, when a real PG is
/// configured, we positively assert the divergence (PG accepts, basin
/// rejects) so the harness documents the gap without being weakened.
#[tokio::test]
async fn diff_create_trigger_basin_rejects_intentional_divergence() {
    let r = DifferentialRunner::new("trg_create").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY, n BIGINT)")
        .await;
    // The trigger function exists on PG so PG's CREATE TRIGGER succeeds;
    // basin rejects at parse/dispatch regardless of the function.
    if let Some(pg) = &r.pg {
        pg.batch_execute(
            "CREATE FUNCTION trg_noop() RETURNS trigger \
             LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;",
        )
        .await
        .expect("PG trigger fn bootstrap");
    }

    let (basin, pg) = r
        .probe(
            "CREATE TRIGGER trg AFTER INSERT ON t \
             FOR EACH ROW EXECUTE FUNCTION trg_noop()",
        )
        .await;

    // basin leg (always runs): must be an honest rejection, never Ok.
    assert!(
        matches!(basin, Outcome::Err(_)),
        "basin must reject CREATE TRIGGER (BUG #132), got {basin:?}"
    );

    // PG leg: assert the *divergence* explicitly. PG accepts; basin does
    // not. We intentionally do NOT assert basin == pg here.
    if let Some(pg) = pg {
        assert_eq!(
            pg,
            Outcome::Ok,
            "real PG is expected to accept CREATE TRIGGER (divergence anchor)"
        );
        assert_ne!(
            basin, pg,
            "documented divergence: basin rejects CREATE TRIGGER while PG accepts it"
        );
    }
}

/// BUG #132 — `DROP TRIGGER ... IF EXISTS` is a faithful PG no-op on a
/// table with no such trigger. Here basin and PG AGREE (both succeed), so
/// this is a full-parity differential case.
#[tokio::test]
async fn diff_drop_trigger_if_exists_noop_parity() {
    let r = DifferentialRunner::new("trg_drop_ifx").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY)").await;

    let (basin, pg) = r.probe("DROP TRIGGER IF EXISTS nope ON t").await;

    assert_eq!(
        basin,
        Outcome::Ok,
        "basin: DROP TRIGGER IF EXISTS must be a silent no-op"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG must agree: DROP TRIGGER IF EXISTS is a no-op"
        );
    }
}

/// BUG #132 — bare `DROP TRIGGER` (no IF EXISTS) on a non-existent
/// trigger. Both backends error ("trigger does not exist"), but the
/// SQLSTATE classes differ (PG: 42704 undefined_object; basin surfaces
/// 0A000 feature_not_supported because no trigger can ever exist). This
/// is a documented divergence on the *code*, agreement on the *outcome
/// being an error*; we assert basin errors and do not force code-equality.
#[tokio::test]
async fn diff_drop_trigger_without_if_exists_basin_rejects() {
    let r = DifferentialRunner::new("trg_drop_bare").await;

    r.setup("CREATE TABLE t (id BIGINT PRIMARY KEY)").await;

    let (basin, pg) = r.probe("DROP TRIGGER nope ON t").await;

    assert!(
        matches!(basin, Outcome::Err(_)),
        "basin must reject bare DROP TRIGGER (PG: 'does not exist'), got {basin:?}"
    );
    if let Some(pg) = pg {
        // PG also errors here; both being errors is the parity we assert.
        // We intentionally do not compare SQLSTATE classes (documented
        // divergence: PG 42704 vs basin 0A000).
        assert!(
            matches!(pg, Outcome::Err(_)),
            "real PG is expected to error on bare DROP TRIGGER of a missing trigger"
        );
    }
}

// --------------------------------------------------------------------------
// BUG #133 — RLS WITH CHECK / USING enforcement on INSERT / UPDATE.
//
// PostgreSQL rejects a row that violates an applicable policy's WITH CHECK
// (or USING when no WITH CHECK) with SQLSTATE 42501. basin now does the same.
//
// Owner-model note: the differential harness's single PG connection IS the
// table owner. PostgreSQL exempts the table owner from RLS *unless* the
// table is `ALTER TABLE ... FORCE ROW LEVEL SECURITY` (a clause the
// sqlparser fork basin uses does not surface, so it can't appear in shared
// setup). basin has no owner/role model and enforces WITH CHECK for every
// writer (fail-closed). Therefore on a *rejection* probe basin returns
// 42501 while PG-as-owner returns Ok — a documented, intentional divergence
// on owner semantics (same shape as `diff_drop_trigger_*`). We assert the
// basin contract unconditionally and only require strict basin↔PG equality
// for the cases where owner-exemption does not change the outcome (valid
// row accepted; RLS disabled).
// --------------------------------------------------------------------------

/// A row that violates WITH CHECK is rejected by basin with 42501. (PG, as
/// the table owner without FORCE, accepts it — documented owner divergence.)
#[tokio::test]
async fn diff_rls_with_check_violation_rejected_by_basin() {
    let r = DifferentialRunner::new("rls_wc_rej").await;

    r.setup("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)")
        .await;
    r.setup("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await;
    r.setup(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await;

    let (basin, pg) = r
        .probe("INSERT INTO orders (id, amount) VALUES (1, -5)")
        .await;

    // basin leg (always runs): must reject with the RLS 42501 class.
    assert_eq!(
        basin,
        Outcome::Err("42501".to_string()),
        "basin must reject a WITH CHECK violation with 42501"
    );
    // PG leg: as the owner without FORCE, PG accepts (owner-exempt). We do
    // not force basin==pg here — this is the documented owner-model
    // divergence. If a future basin gains an owner model, tighten this.
    if let Some(pg) = pg {
        assert!(
            matches!(pg, Outcome::Ok | Outcome::Err(_)),
            "sanity: PG produced a defined outcome ({pg:?})"
        );
    }
}

/// A row that satisfies WITH CHECK is accepted by BOTH backends — owner
/// exemption does not change this outcome, so strict parity is asserted.
#[tokio::test]
async fn diff_rls_with_check_satisfied_accepted_both() {
    let r = DifferentialRunner::new("rls_wc_ok").await;

    r.setup("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)")
        .await;
    r.setup("ALTER TABLE orders ENABLE ROW LEVEL SECURITY")
        .await;
    r.setup(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await;

    let (basin, pg) = r
        .probe("INSERT INTO orders (id, amount) VALUES (1, 10)")
        .await;

    assert_eq!(
        basin,
        Outcome::Ok,
        "basin must accept a row satisfying WITH CHECK"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG must agree: a WITH CHECK-satisfying row is accepted"
        );
    }
}

// --------------------------------------------------------------------------
// SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT parity
//
// These probe SQLSTATE classes the shared `basin_sqlstate` mapper does not
// model (25P01 no_active_sql_transaction, 3B001 no_such_savepoint), so they
// use a local SQLSTATE extractor that reads the code basin embeds in its
// error message. Both legs run statements on a single long-lived
// session/client so transaction + savepoint state is shared across the
// script, exactly like a real client connection.
// --------------------------------------------------------------------------

/// Pull a 5-char SQLSTATE token (e.g. `25P01`, `3B001`) out of a basin
/// error message. basin embeds `(SQLSTATE XXXXX)` in the message for the
/// transaction/savepoint error paths.
fn sqlstate_from_basin_msg(msg: &str) -> String {
    if let Some(idx) = msg.find("SQLSTATE ") {
        let tail = &msg[idx + "SQLSTATE ".len()..];
        let code: String = tail
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if code.len() == 5 {
            return code;
        }
    }
    "XXOTHER".to_string()
}

impl DifferentialRunner {
    /// Run `sql` on basin and (if a DSN is set) PG, returning the outcome
    /// of each as `Ok` or `Err(sqlstate)`. Unlike `probe`, this uses the
    /// savepoint-aware SQLSTATE extractor and keeps the SAME session/client
    /// across calls so transaction + savepoint state persists.
    async fn step(&self, sql: &str) -> (Outcome, Option<Outcome>) {
        let basin = match self.sess.execute(sql).await {
            Ok(_) => Outcome::Ok,
            Err(e) => Outcome::Err(sqlstate_from_basin_msg(&format!("{e}"))),
        };
        let pg = match &self.pg {
            None => None,
            Some(pg) => Some(match pg.batch_execute(sql).await {
                Ok(_) => Outcome::Ok,
                Err(e) => {
                    let code = e
                        .as_db_error()
                        .map(|db| db.code().code().to_string())
                        .unwrap_or_else(|| "XXOTHER".to_string());
                    Outcome::Err(code)
                }
            }),
        };
        (basin, pg)
    }

    /// Run a two-`BIGINT`-column `SELECT` on basin and (when a DSN is set) PG,
    /// returning each side's full row set as `Vec<(i64, i64)>` in the query's
    /// own order. Used to assert exact per-row LATERAL-expansion parity.
    async fn rows_pairs(&self, sql: &str) -> (Vec<(i64, i64)>, Option<Vec<(i64, i64)>>) {
        use basin_engine::ExecResult;
        let basin = match self.sess.execute(sql).await {
            Ok(ExecResult::Rows { batches, .. }) => {
                let mut v = Vec::new();
                for b in &batches {
                    let c0 = b.column(0);
                    let c1 = b.column(1);
                    let get = |c: &std::sync::Arc<dyn arrow_array::Array>, r: usize| -> i64 {
                        if let Some(a) = c.as_any().downcast_ref::<arrow_array::Int64Array>() {
                            a.value(r)
                        } else if let Some(a) = c.as_any().downcast_ref::<arrow_array::Int32Array>()
                        {
                            a.value(r) as i64
                        } else {
                            panic!("expected an integer column in rows_pairs");
                        }
                    };
                    for r in 0..b.num_rows() {
                        v.push((get(c0, r), get(c1, r)));
                    }
                }
                v
            }
            other => panic!("expected rows from {sql:?}, got {other:?}"),
        };
        let pg = match &self.pg {
            None => None,
            Some(pg) => {
                let rows = pg
                    .query(sql, &[])
                    .await
                    .unwrap_or_else(|e| panic!("PG rows_pairs failed for {sql:?}: {e}"));
                let mut v = Vec::new();
                for row in &rows {
                    // Accept INT4 or INT8 on either column.
                    let a = row
                        .try_get::<_, i64>(0)
                        .or_else(|_| row.try_get::<_, i32>(0).map(|x| x as i64))
                        .expect("PG col 0 integer");
                    let b = row
                        .try_get::<_, i64>(1)
                        .or_else(|_| row.try_get::<_, i32>(1).map(|x| x as i64))
                        .expect("PG col 1 integer");
                    v.push((a, b));
                }
                Some(v)
            }
        };
        (basin, pg)
    }

    /// Assert a statement's **output column names** — the thing PostgreSQL's
    /// FROM-item aliasing rules actually decide — on basin, and, when a DSN
    /// is set, on real PG too.
    ///
    /// `expect` is written from what the live server does (see
    /// [`diff_srf_output_column_naming`]); asserting it on *both* legs is
    /// what stops the two sides from silently agreeing on a spelling that no
    /// PostgreSQL would accept. PG's names come from `PREPARE`'s row
    /// description, so the check holds for an empty result too.
    async fn assert_colnames(&self, sql: &str, expect: &[&str]) {
        use basin_engine::ExecResult;
        let basin: Vec<String> = match self.sess.execute(sql).await {
            Ok(ExecResult::Rows { schema, .. }) => {
                schema.fields().iter().map(|f| f.name().clone()).collect()
            }
            other => panic!("expected a result set from {sql:?}, got {other:?}"),
        };
        assert_eq!(basin, expect, "basin column names for {sql:?}");
        assert!(
            !(dsn_configured() && self.pg.is_none()),
            "PG leg skipped for {sql:?} despite PG_DIFF_TEST_DSN being set"
        );
        if let Some(pg) = &self.pg {
            let stmt = pg
                .prepare(sql)
                .await
                .unwrap_or_else(|e| panic!("PG prepare failed for {sql:?}: {e}"));
            let names: Vec<String> = stmt
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect();
            assert_eq!(names, expect, "real-PG column names for {sql:?}");
        }
    }

    /// Assert both backends *reject* a statement. Deliberately weaker than
    /// [`Self::probe`]'s SQLSTATE comparison: basin surfaces its planner's
    /// own message for an unknown qualified column rather than PG's 42703
    /// `undefined_column`, so pinning the code here would assert something
    /// untrue. What matters for the naming rule — that the spelling PG
    /// rejects is not quietly accepted by basin — is exactly this.
    async fn assert_both_reject(&self, sql: &str) {
        assert!(
            self.sess.execute(sql).await.is_err(),
            "basin accepted {sql:?}, which PostgreSQL rejects"
        );
        assert!(
            !(dsn_configured() && self.pg.is_none()),
            "PG leg skipped for {sql:?} despite PG_DIFF_TEST_DSN being set"
        );
        if let Some(pg) = &self.pg {
            assert!(
                pg.batch_execute(sql).await.is_err(),
                "real PG accepted {sql:?}, so this case is asserting the wrong rule"
            );
        }
    }
}

/// Is a real-PG leg configured for this run? Used only to catch a case that
/// silently degrades to basin-only.
fn dsn_configured() -> bool {
    std::env::var("PG_DIFF_TEST_DSN").is_ok_and(|d| !d.trim().is_empty())
}

/// Guard for the `Option`-returning runner helpers: `None` means "no DSN",
/// so a `None` while a DSN *is* configured means the PG leg never ran and
/// the comparison below it was vacuous — precisely how a wrong expectation
/// can survive in this file.
trait PgLegRan<T> {
    fn expect_pg_ran(self, what: &str) -> Option<T>;
}

impl<T> PgLegRan<T> for Option<T> {
    fn expect_pg_ran(self, what: &str) -> Option<T> {
        assert!(
            !(dsn_configured() && self.is_none()),
            "PG leg did not run for {what} despite PG_DIFF_TEST_DSN being set"
        );
        self
    }
}

/// BEGIN; INSERT; SAVEPOINT; INSERT; ROLLBACK TO SAVEPOINT; COMMIT — the
/// post-savepoint row is gone, the pre-savepoint row persists. Both
/// backends agree on every step's outcome and the final count.
#[tokio::test]
async fn diff_savepoint_rollback_to_partial_undo() {
    let r = DifferentialRunner::new("sp_partial").await;
    r.setup("CREATE TABLE t (id INT)").await;

    for sql in [
        "BEGIN",
        "INSERT INTO t (id) VALUES (1)",
        "SAVEPOINT s",
        "INSERT INTO t (id) VALUES (2)",
        "ROLLBACK TO SAVEPOINT s",
        "INSERT INTO t (id) VALUES (3)",
        "COMMIT",
    ] {
        let (basin, pg) = r.step(sql).await;
        assert_eq!(basin, Outcome::Ok, "basin must accept {sql:?}");
        if let Some(pg) = pg {
            assert_eq!(basin, pg, "basin/PG disagree on {sql:?}");
        }
    }

    // Pre-savepoint (1) and post-rollback (3) survive; rolled-back (2) does not.
    let (basin, pg) = r.count_both("SELECT count(*) FROM t").await;
    assert_eq!(basin, 2, "basin: 2 rows survive partial rollback");
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin/PG row-count disagree after partial undo");
    }
}

/// RELEASE SAVEPOINT keeps post-savepoint writes on both backends.
#[tokio::test]
async fn diff_release_savepoint_keeps_writes() {
    let r = DifferentialRunner::new("sp_release").await;
    r.setup("CREATE TABLE t (id INT)").await;

    for sql in [
        "BEGIN",
        "INSERT INTO t (id) VALUES (1)",
        "SAVEPOINT s",
        "INSERT INTO t (id) VALUES (2)",
        "RELEASE SAVEPOINT s",
        "COMMIT",
    ] {
        let (basin, pg) = r.step(sql).await;
        assert_eq!(basin, Outcome::Ok, "basin must accept {sql:?}");
        if let Some(pg) = pg {
            assert_eq!(basin, pg, "basin/PG disagree on {sql:?}");
        }
    }

    let (basin, pg) = r.count_both("SELECT count(*) FROM t").await;
    assert_eq!(basin, 2, "basin: RELEASE keeps both rows");
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin/PG disagree: RELEASE must keep writes");
    }
}

/// ROLLBACK TO a non-existent savepoint inside a txn: PG raises 3B001
/// (no_such_savepoint); basin must match.
#[tokio::test]
async fn diff_rollback_to_nonexistent_savepoint_3b001() {
    let r = DifferentialRunner::new("sp_nosuch").await;

    let (b0, p0) = r.step("BEGIN").await;
    assert_eq!(b0, Outcome::Ok);
    if let Some(p0) = p0 {
        assert_eq!(b0, p0);
    }

    let (basin, pg) = r.step("ROLLBACK TO SAVEPOINT nope").await;
    assert_eq!(
        basin,
        Outcome::Err("3B001".to_string()),
        "basin must raise 3B001 for an unknown savepoint"
    );
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin/PG disagree on no_such_savepoint code");
    }
}

/// SAVEPOINT outside any transaction block: PG raises 25P01
/// (no_active_sql_transaction); basin must match (not a silent no-op).
#[tokio::test]
async fn diff_savepoint_outside_txn_25p01() {
    let r = DifferentialRunner::new("sp_no_txn").await;

    let (basin, pg) = r.step("SAVEPOINT s").await;
    assert_eq!(
        basin,
        Outcome::Err("25P01".to_string()),
        "basin must raise 25P01 for SAVEPOINT outside a txn block"
    );
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin/PG disagree on SAVEPOINT-without-txn code");
    }
}

/// ROLLBACK TO SAVEPOINT outside any transaction block: PG raises 25P01;
/// basin must match (historically basin mis-reported "does not exist").
#[tokio::test]
async fn diff_rollback_to_savepoint_outside_txn_25p01() {
    let r = DifferentialRunner::new("rbsp_no_txn").await;

    let (basin, pg) = r.step("ROLLBACK TO SAVEPOINT s").await;
    assert_eq!(
        basin,
        Outcome::Err("25P01".to_string()),
        "basin must raise 25P01 for ROLLBACK TO SAVEPOINT outside a txn"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin/PG disagree on ROLLBACK-TO-SAVEPOINT-without-txn code"
        );
    }
}

/// With ROW LEVEL SECURITY never enabled, the policy is inert: the
/// WITH CHECK-violating row is accepted by BOTH backends (strict parity).
#[tokio::test]
async fn diff_rls_disabled_policy_inert_both_accept() {
    let r = DifferentialRunner::new("rls_off").await;

    r.setup("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)")
        .await;
    // Policy declared but RLS is never ENABLEd.
    r.setup(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await;

    let (basin, pg) = r
        .probe("INSERT INTO orders (id, amount) VALUES (1, -99)")
        .await;

    assert_eq!(
        basin,
        Outcome::Ok,
        "RLS disabled: basin must not enforce WITH CHECK"
    );
    if let Some(pg) = pg {
        assert_eq!(
            basin, pg,
            "basin and PG must agree: RLS disabled ⇒ policy inert"
        );
    }
}

/// `CREATE TABLE … AS <query> WITH NO DATA` creates the schema-only clone
/// on BOTH backends: the probe statement (the CTAS) succeeds, and the
/// resulting table has the query's columns but ZERO rows. `WITH DATA`
/// populates it. We compare the post-CTAS `count(*)` so the
/// schema-vs-data semantics are checked, not just the parse.
#[tokio::test]
async fn diff_ctas_with_no_data_is_schema_only() {
    let r = DifferentialRunner::new("ctas_nodata").await;

    r.setup("CREATE TABLE src (id INT, label TEXT)").await;
    r.setup("INSERT INTO src (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    // Probe: the WITH NO DATA CTAS itself must parse + succeed on both.
    let (basin, pg) = r
        .probe("CREATE TABLE clone_empty AS SELECT id, label FROM src WITH NO DATA")
        .await;
    assert_eq!(
        basin,
        Outcome::Ok,
        "basin must accept CREATE TABLE AS … WITH NO DATA"
    );
    if let Some(pg) = pg {
        assert_eq!(basin, pg, "basin/PG disagree on WITH NO DATA acceptance");
    }

    // WITH NO DATA ⇒ schema present, zero rows on both backends.
    let (basin_n, pg_n) = r.count_both("SELECT count(*) FROM clone_empty").await;
    assert_eq!(basin_n, 0, "WITH NO DATA must yield an empty table");
    if let Some(pg_n) = pg_n {
        assert_eq!(basin_n, pg_n, "basin/PG row-count disagree (WITH NO DATA)");
    }

    // The empty clone is insertable afterward (real table, not a view).
    let (basin_i, pg_i) = r
        .probe("INSERT INTO clone_empty (id, label) VALUES (9, 'z')")
        .await;
    assert_eq!(
        basin_i,
        Outcome::Ok,
        "WITH NO DATA clone must be insertable"
    );
    if let Some(pg_i) = pg_i {
        assert_eq!(basin_i, pg_i, "basin/PG disagree on post-CTAS insert");
    }
    let (basin_n2, pg_n2) = r.count_both("SELECT count(*) FROM clone_empty").await;
    assert_eq!(basin_n2, 1, "row inserted into the WITH NO DATA clone");
    if let Some(pg_n2) = pg_n2 {
        assert_eq!(basin_n2, pg_n2, "basin/PG row-count disagree after insert");
    }

    // WITH DATA populates from the query on both backends.
    let (basin_d, pg_d) = r
        .probe("CREATE TABLE clone_full AS SELECT id, label FROM src WITH DATA")
        .await;
    assert_eq!(basin_d, Outcome::Ok, "basin must accept WITH DATA CTAS");
    if let Some(pg_d) = pg_d {
        assert_eq!(basin_d, pg_d, "basin/PG disagree on WITH DATA acceptance");
    }
    let (basin_f, pg_f) = r.count_both("SELECT count(*) FROM clone_full").await;
    assert_eq!(basin_f, 3, "WITH DATA must copy all source rows");
    if let Some(pg_f) = pg_f {
        assert_eq!(basin_f, pg_f, "basin/PG row-count disagree (WITH DATA)");
    }
}

/// #113 — correlated `LATERAL generate_series(1, t.col)` per-row expansion.
///
/// `FROM t CROSS JOIN LATERAL generate_series(1, t.id) g` must expand each
/// `t`-row into the series `1 .. t.id`. DataFusion's `generate_series` table
/// function rejects the correlated column argument, so basin pre-rewrites the
/// shape into a bounded recursive-CTE JOIN. This pins exact per-row parity vs
/// real PG (when a DSN is set) and a basin-only exact-row assertion otherwise.
#[tokio::test]
async fn diff_lateral_generate_series_per_row_expansion() {
    let r = DifferentialRunner::new("lat_gs").await;
    r.setup("CREATE TABLE t (id INT NOT NULL)").await;
    r.setup("INSERT INTO t VALUES (2), (3)").await;

    // ids {2,3} ⇒ 2 → {1,2}, 3 → {1,2,3}.  Exact ordered set, both engines.
    //
    // NOTE the column spelling: `g.g`. PostgreSQL names a scalar SRF's output
    // column after the FROM-item's table alias, so `g.g` is the *only* way to
    // reference this column on a real server — `g.value` (DataFusion's own
    // name, which this case used to send to both legs) is rejected by PG with
    // `column g.value does not exist`. See `diff_srf_output_column_naming`
    // for the full rule and every shape of it.
    let (b, p) = r
        .rows_pairs(
            "SELECT t.id, g.g \
             FROM t CROSS JOIN LATERAL generate_series(1, t.id) g \
             ORDER BY t.id, g.g",
        )
        .await;
    let expected = vec![(2i64, 1i64), (2, 2), (3, 1), (3, 2), (3, 3)];
    assert_eq!(b, expected, "basin per-row LATERAL expansion wrong");
    let p = p.expect_pg_ran("LATERAL generate_series rows");
    if let Some(p) = p {
        assert_eq!(b, p, "basin/PG disagree on LATERAL generate_series rows");
    }

    // Comma-LATERAL form with explicit `AS gs(i)` — same expansion, and the
    // column-alias list (not the table alias) names the column: `gs.i`.
    let (b2, p2) = r
        .rows_pairs(
            "SELECT t.id, gs.i \
             FROM t, LATERAL generate_series(1, t.id) AS gs(i) \
             ORDER BY t.id, gs.i",
        )
        .await;
    assert_eq!(b2, expected, "basin comma-LATERAL expansion wrong");
    let p2 = p2.expect_pg_ran("comma-LATERAL rows");
    if let Some(p2) = p2 {
        assert_eq!(b2, p2, "basin/PG disagree on comma-LATERAL rows");
    }

    // Edge: id = 0 and NULL contribute zero rows; only id = 5 expands.
    r.setup("CREATE TABLE z (id INT)").await;
    r.setup("INSERT INTO z VALUES (0), (NULL), (5)").await;
    let (bz, pz) = r
        .rows_pairs(
            "SELECT z.id, g.g \
             FROM z CROSS JOIN LATERAL generate_series(1, z.id) g \
             ORDER BY z.id, g.g",
        )
        .await;
    let expected_z = vec![(5i64, 1i64), (5, 2), (5, 3), (5, 4), (5, 5)];
    assert_eq!(bz, expected_z, "basin 0/NULL edge expansion wrong");
    if let Some(pz) = pz {
        assert_eq!(bz, pz, "basin/PG disagree on 0/NULL-edge rows");
    }

    // Regression guard: the non-correlated `generate_series(1, 3)` form is
    // NOT rewritten and still works (cross join, 2 t-rows × 3 series = 6).
    let (bc, pc) = r
        .count_both("SELECT count(*) FROM t CROSS JOIN LATERAL generate_series(1, 3) g")
        .await;
    assert_eq!(
        bc, 6,
        "non-correlated generate_series(1,3) must be unaffected"
    );
    let pc = pc.expect_pg_ran("non-correlated generate_series count");
    if let Some(pc) = pc {
        assert_eq!(bc, pc, "basin/PG disagree on non-correlated row count");
    }
}

/// A set-returning function's **output column name** in FROM position.
///
/// PostgreSQL's rule is `chooseScalarFunctionAlias`
/// (`backend/parser/parse_relation.c`), and every expectation below was read
/// off a live PostgreSQL 18.2 before it was implemented:
///
/// | shape                                        | column(s)         |
/// |----------------------------------------------|-------------------|
/// | `generate_series(1,3) g`                     | `g`               |
/// | `generate_series(1,3) AS gs(i)`              | `i`               |
/// | `generate_series(1,3)` (no alias)            | `generate_series` |
/// | `… LATERAL generate_series(1,t.id) g`        | `g`               |
/// | `unnest(ARRAY[…]) u` / no alias / `AS u(x)`  | `u` / `unnest` / `x` |
/// | `jsonb_array_elements(…) je`                 | `value`           |
/// | `jsonb_each(…) je` / `AS je(k,v)`            | `key`,`value` / `k`,`v` |
/// | `jsonb_object_keys(…) k`                     | `k`               |
///
/// Three rules produce that table, in priority order: a **column-alias list**
/// always wins; otherwise a function with a single *named* OUT parameter
/// (`jsonb_array_elements` ⇒ `value`) or a composite return type
/// (`jsonb_each` ⇒ `key`,`value`) keeps its own names; otherwise the **table
/// alias** names the column, and with no alias the **function name** does.
///
/// basin used to answer DataFusion's `value` for every `generate_series`
/// shape and an expression-display string for every `unnest` one — so
/// `g.value`, which real PostgreSQL rejects, worked, and `g.g`, which is the
/// only spelling a real client can write, did not.
///
/// Two shapes are deliberately absent because basin does not implement them
/// at all yet — asserting parity would be a lie, and asserting basin's
/// current answer would pin a wrong one:
/// - `WITH ORDINALITY` (PG adds a second `ordinality` column; basin answers
///   `UNNEST with ordinality is not supported yet` / drops it).
/// - a set-returning function in the **target list**
///   (`SELECT generate_series(1,3)`: PG expands it to three rows named
///   `generate_series`, basin returns one row named after the expression).
#[tokio::test]
async fn diff_srf_output_column_naming() {
    let r = DifferentialRunner::new("srf_names").await;
    r.setup("CREATE TABLE t (id INT NOT NULL)").await;
    r.setup("INSERT INTO t VALUES (2)").await;

    // ── scalar SRF: the table alias names the column ─────────────────────
    r.assert_colnames("SELECT * FROM generate_series(1, 3) g", &["g"])
        .await;
    r.assert_colnames("SELECT g FROM generate_series(1, 3) g", &["g"])
        .await;
    r.assert_colnames("SELECT g.g FROM generate_series(1, 3) g", &["g"])
        .await;
    r.assert_colnames("SELECT * FROM generate_series(1, 3) AS gs", &["gs"])
        .await;

    // ── the column-alias list outranks the table alias ───────────────────
    r.assert_colnames("SELECT * FROM generate_series(1, 3) AS gs(i)", &["i"])
        .await;
    r.assert_colnames("SELECT gs.i FROM generate_series(1, 3) AS gs(i)", &["i"])
        .await;

    // ── no alias at all: the function names the column ───────────────────
    r.assert_colnames("SELECT * FROM generate_series(1, 3)", &["generate_series"])
        .await;
    r.assert_colnames(
        "SELECT generate_series FROM generate_series(1, 3)",
        &["generate_series"],
    )
    .await;

    // ── same rules through a LATERAL join ────────────────────────────────
    r.assert_colnames(
        "SELECT t.id, g.g FROM t CROSS JOIN LATERAL generate_series(1, t.id) g \
         ORDER BY t.id, g.g",
        &["id", "g"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g",
        &["id", "g"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM t, LATERAL generate_series(1, t.id) AS gs(i)",
        &["id", "i"],
    )
    .await;
    // Non-correlated LATERAL takes a different path inside basin (no
    // decorrelation) and must land on the same name.
    r.assert_colnames(
        "SELECT * FROM t CROSS JOIN LATERAL generate_series(1, 3) g",
        &["id", "g"],
    )
    .await;

    // ── `unnest` follows the identical rule ──────────────────────────────
    r.assert_colnames("SELECT * FROM unnest(ARRAY[10, 20]) u", &["u"])
        .await;
    r.assert_colnames("SELECT * FROM unnest(ARRAY[10, 20])", &["unnest"])
        .await;
    r.assert_colnames("SELECT * FROM unnest(ARRAY[10, 20]) AS u(x)", &["x"])
        .await;

    // ── a named OUT parameter / composite return beats the alias ─────────
    // `jsonb_array_elements`'s OUT parameter is named `value`, so the `je`
    // alias does NOT rename it; `jsonb_each` returns a record whose own
    // attributes are `key` and `value`.
    r.assert_colnames(
        "SELECT * FROM jsonb_array_elements('[1,2]'::jsonb) je",
        &["value"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM jsonb_array_elements('[1,2]'::jsonb) AS je(v)",
        &["v"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM jsonb_each('{\"a\":1}'::jsonb) je",
        &["key", "value"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM jsonb_each('{\"a\":1}'::jsonb) AS je(k, v)",
        &["k", "v"],
    )
    .await;
    // …but `jsonb_object_keys` has no OUT name, so it is alias-named again.
    r.assert_colnames(
        "SELECT * FROM jsonb_object_keys('{\"a\":1}'::jsonb) k",
        &["k"],
    )
    .await;
    r.assert_colnames(
        "SELECT * FROM jsonb_object_keys('{\"a\":1}'::jsonb)",
        &["jsonb_object_keys"],
    )
    .await;

    // ── the spellings PostgreSQL rejects must not work on basin either ───
    // `value` is DataFusion's name, never PostgreSQL's.
    r.assert_both_reject("SELECT g.value FROM generate_series(1, 3) g")
        .await;
    r.assert_both_reject(
        "SELECT g.value FROM t CROSS JOIN LATERAL generate_series(1, t.id) g",
    )
    .await;
    // The column-alias list replaces the alias-derived name, it doesn't add
    // to it: `gs.gs` is gone once `AS gs(i)` is written.
    r.assert_both_reject("SELECT gs.gs FROM generate_series(1, 3) AS gs(i)")
        .await;
    // One column available, two names given.
    r.assert_both_reject("SELECT * FROM generate_series(1, 3) AS gs(i, j)")
        .await;
}
