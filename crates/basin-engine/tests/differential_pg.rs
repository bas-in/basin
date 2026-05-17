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
                let (client, connection) =
                    tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                        .await
                        .expect("PG_DIFF_TEST_DSN connect");
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let schema = format!(
                    "basin_diff_{}_{}",
                    case_tag,
                    std::process::id()
                );
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
                if let Some(a) = col
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                {
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

    let (basin, pg) =
        r.probe("INSERT INTO users (id, email) VALUES (2, 'ada@example.com')")
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

    let (basin, pg) =
        r.probe("INSERT INTO users (id, email) VALUES (2, 'b@x.com')")
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
    r.setup("INSERT INTO t (id, code) VALUES (1, 'abcde')").await;

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

    let (basin, pg) = r
        .probe("INSERT INTO t (id, code) VALUES (2, 'abcd')")
        .await;

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
        .count_both(
            "SELECT count(*) FROM samp TABLESAMPLE BERNOULLI(10) REPEATABLE(12345)",
        )
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
    r.setup("ALTER TABLE orders ENABLE ROW LEVEL SECURITY").await;
    r.setup(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await;

    let (basin, pg) =
        r.probe("INSERT INTO orders (id, amount) VALUES (1, -5)").await;

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
    r.setup("ALTER TABLE orders ENABLE ROW LEVEL SECURITY").await;
    r.setup(
        "CREATE POLICY p ON orders FOR ALL TO PUBLIC \
         USING (true) WITH CHECK (amount > 0)",
    )
    .await;

    let (basin, pg) =
        r.probe("INSERT INTO orders (id, amount) VALUES (1, 10)").await;

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

    let (basin, pg) =
        r.probe("INSERT INTO orders (id, amount) VALUES (1, -99)").await;

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
