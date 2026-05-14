//! SQL-syntax support matrix — ADR 0014 / Phase 1 substrate.
//!
//! Runs every SQL fragment through the Basin engine and classifies each
//! outcome. The results are written to `docs/sql-support.md` so that the
//! per-syntax support table is always derived from real test execution, never
//! hand-maintained.
//!
//! Run with:
//!   cargo test -p basin-integration-tests --test sql_support_matrix
//!
//! The test sets `BASIN_PG_QUERY` / `BASIN_PG_QUERY_PLAN` env vars before
//! constructing each engine pass so that every configuration is exercised in
//! a single test binary. Because the env vars are process-wide, the three
//! passes run serially inside one `#[tokio::test]`.

#![allow(clippy::print_stdout)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use basin_catalog::{Catalog, InMemoryCatalog};
use basin_common::TenantId;
use basin_engine::{Engine, EngineConfig};
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Outcome {
    /// Executed end-to-end and produced an expected result (Ok or a
    /// well-understood empty result for DML that has no return value).
    Ok,
    /// Parsed and planned, but the executor returned a runtime error that
    /// is not a parse/plan rejection (e.g. table not found after DROP).
    ExecFailed,
    /// The parser or planner refused the statement (sqlparser error,
    /// "unsupported in PoC", "FeatureNotSupported").
    PlannerRejected,
    /// The SQL could not even be parsed by sqlparser.
    ParserRejected,
    /// Explicitly out-of-scope / gated behind env flags that are not
    /// set in this pass (LISTEN/NOTIFY/VACUUM/etc.).
    OutOfScope,
}

impl Outcome {
    fn emoji(&self) -> &'static str {
        match self {
            Outcome::Ok => "✅",
            Outcome::ExecFailed => "🛠",
            Outcome::PlannerRejected => "📜",
            Outcome::ParserRejected => "❌",
            Outcome::OutOfScope => "🚫",
        }
    }
}

struct MatrixRow {
    category: &'static str,
    sql: &'static str,
    /// Outcomes for [Default, +PG_QUERY, +PG_QUERY+PG_PLAN]
    outcomes: [Outcome; 3],
    notes: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Engine bootstrap
// ─────────────────────────────────────────────────────────────────────────────

fn engine_in(dir: &TempDir) -> Engine {
    let fs = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    let storage = basin_storage::Storage::new(basin_storage::StorageConfig {
        object_store: Arc::new(fs),
        root_prefix: None,
        disk_cache: basin_integration_tests::cache_defaults::default_test_disk_cache(),
        page_cache: basin_integration_tests::cache_defaults::default_test_page_cache(),
    });
    let catalog: Arc<dyn Catalog> = Arc::new(InMemoryCatalog::new());
    Engine::new(EngineConfig {
        storage,
        catalog,
        shard: None,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL matrix definition
// ─────────────────────────────────────────────────────────────────────────────

/// (category, sql, setup_sql, teardown_sql)
/// setup_sql runs before the test statement (may be empty).
/// teardown_sql runs after (may be empty).
type Entry = (
    &'static str,
    &'static str,
    &'static [&'static str], // setup
    &'static [&'static str], // teardown
);

static MATRIX: &[Entry] = &[
    // ── DDL — Tables ─────────────────────────────────────────────────────────
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, name TEXT)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE IF NOT EXISTS t (id INT)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT PRIMARY KEY)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT NOT NULL)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT DEFAULT 0)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, name TEXT UNIQUE)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT REFERENCES u(id))",
        &["CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, CHECK (id > 0))",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT GENERATED ALWAYS AS (1+1) STORED)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (LIKE u INCLUDING ALL)",
        &["CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t () INHERITS (u)",
        &["CREATE TABLE u (id INT)"],
        &["DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT) PARTITION BY RANGE (id)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TEMPORARY TABLE t (id INT)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE UNLOGGED TABLE t (id INT)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ADD COLUMN c TEXT",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t DROP COLUMN c",
        &["CREATE TABLE t (id INT NOT NULL, c TEXT)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t RENAME COLUMN c TO d",
        &["CREATE TABLE t (id INT NOT NULL, c TEXT)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t RENAME TO u",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ALTER COLUMN c TYPE BIGINT",
        &["CREATE TABLE t (id INT NOT NULL, c INT)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t DROP CONSTRAINT ck",
        &["CREATE TABLE t (id INT NOT NULL, CONSTRAINT ck CHECK (id > 0))"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t DISABLE ROW LEVEL SECURITY",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t SET cold_after = '7d'",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t CLUSTER BY (id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "DROP TABLE t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &[],
    ),
    (
        "DDL/Tables",
        "DROP TABLE IF EXISTS t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &[],
    ),
    (
        "DDL/Tables",
        "DROP TABLE t CASCADE",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &[],
    ),
    (
        "DDL/Tables",
        "TRUNCATE TABLE t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    // ── DDL — Other objects ──────────────────────────────────────────────────
    (
        "DDL/Other",
        "CREATE INDEX idx ON t(id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE UNIQUE INDEX idx ON t(id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE INDEX idx ON t(id) WHERE id > 0",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE INDEX idx ON t(LOWER(name))",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE INDEX idx ON t USING gin (name)",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP INDEX idx",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE INDEX idx ON t(id)",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE SCHEMA s",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "DROP SCHEMA s",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)",
        &[],
        &["DROP DOMAIN positive_int"],
    ),
    (
        "DDL/Other",
        "DROP DOMAIN positive_int",
        &["CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)"],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE TYPE color AS ENUM ('red', 'green', 'blue')",
        &[],
        &["DROP TYPE color"],
    ),
    (
        "DDL/Other",
        "ALTER TYPE color ADD VALUE 'purple'",
        &["CREATE TYPE color AS ENUM ('red', 'green', 'blue')"],
        &["DROP TYPE color"],
    ),
    (
        "DDL/Other",
        "DROP TYPE color",
        &["CREATE TYPE color AS ENUM ('red', 'green', 'blue')"],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE SEQUENCE s START 100 INCREMENT 2",
        &[],
        &["DROP SEQUENCE s"],
    ),
    (
        "DDL/Other",
        "DROP SEQUENCE s",
        &["CREATE SEQUENCE s START 100"],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x*2 $$",
        &[],
        &["DROP FUNCTION f(INT)"],
    ),
    (
        "DDL/Other",
        "CREATE PROCEDURE p(x INT) LANGUAGE sql AS $$ INSERT INTO t VALUES (x) $$",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP PROCEDURE p(INT)", "DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "ALTER FUNCTION f(INT) RENAME TO g",
        &["CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x*2 $$"],
        &["DROP FUNCTION g(INT)"],
    ),
    (
        "DDL/Other",
        "DROP FUNCTION f(INT)",
        &["CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x*2 $$"],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE VIEW v AS SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP VIEW v",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "REFRESH MATERIALIZED VIEW mv",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE MATERIALIZED VIEW mv WITH (basin.continuous, refresh_interval = '1h') AS SELECT * FROM t",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP MATERIALIZED VIEW mv",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE MATERIALIZED VIEW mv WITH (basin.continuous, refresh_interval = '1h') AS SELECT * FROM t",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn()",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE POLICY p ON t USING (id = 1)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP POLICY p ON t",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
            "CREATE POLICY p ON t USING (id = 1)",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "COMMENT ON TABLE t IS 'x'",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE EXTENSION pgcrypto",
        &[],
        &[],
    ),
    // ── DML ──────────────────────────────────────────────────────────────────
    (
        "DML",
        "INSERT INTO t VALUES (1)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t (id) VALUES (1)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1), (2), (3)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t SELECT id FROM u",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1) RETURNING id",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = excluded.id",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t DEFAULT VALUES",
        &["CREATE TABLE t (id INT DEFAULT 42)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (99)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = 1 WHERE id = 99",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (99)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = id + 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = (SELECT MAX(id) FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO u VALUES (5)",
            "INSERT INTO t VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "UPDATE t SET id = 1 FROM u WHERE t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "UPDATE t SET id = 1 RETURNING id",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (99)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = 1 WHERE id IN (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "DELETE FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "DELETE FROM t WHERE id = 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "DELETE FROM t USING u WHERE t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "DELETE FROM t RETURNING id",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET id = u.id WHEN NOT MATCHED THEN INSERT VALUES (u.id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "COPY t FROM STDIN",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "COPY t TO STDOUT",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    // ── SELECT — projection / filtering ──────────────────────────────────────
    ("SELECT/Projection", "SELECT 1", &[], &[]),
    (
        "SELECT/Projection",
        "SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id, name FROM t",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'a')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT t.id FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id AS x FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT DISTINCT id FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT DISTINCT ON (id) id, name FROM t ORDER BY id, name",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'a')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id = 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id IN (1,2,3)",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id IS NULL",
        &["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id IS NOT NULL",
        &["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id IS DISTINCT FROM 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (2)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE id BETWEEN 1 AND 10",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (5)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE name LIKE 'a%'",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'abc')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE name ILIKE 'A%'",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'abc')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE name SIMILAR TO 'a%'",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'abc')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE name ~ '^a'",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'abc')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT * FROM t WHERE name ~* '^A'",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'abc')"],
        &["DROP TABLE t"],
    ),
    // ── SELECT — joins & subqueries ───────────────────────────────────────────
    (
        "SELECT/Joins",
        "SELECT * FROM t INNER JOIN u ON t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t LEFT JOIN u ON t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t RIGHT JOIN u ON t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t FULL JOIN u ON t.id = u.id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t CROSS JOIN u",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (2)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t NATURAL JOIN u",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t JOIN u USING (id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t, LATERAL (SELECT id FROM u WHERE u.id = t.id) sub",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM (SELECT 1 AS x) sub",
        &[],
        &[],
    ),
    (
        "SELECT/Joins",
        "SELECT (SELECT MAX(id) FROM u) FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (5)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t WHERE id = ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t WHERE id > ALL (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (10)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    // ── SELECT — aggregate / window ───────────────────────────────────────────
    (
        "SELECT/Aggregate",
        "SELECT COUNT(*) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT SUM(id), AVG(id), MIN(id), MAX(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT COUNT(DISTINCT id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT COUNT(*) FILTER (WHERE id > 0) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT STRING_AGG(name, ',') FROM t",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'a')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT ARRAY_AGG(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT JSON_AGG(t) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, SUM(id) FROM t GROUP BY id",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, SUM(id) FROM t GROUP BY id HAVING SUM(id) > 0",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, GROUPING(id) FROM t GROUP BY ROLLUP (id)",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, name FROM t GROUP BY CUBE (id, name)",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'a')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, name FROM t GROUP BY GROUPING SETS ((id), (name))",
        &["CREATE TABLE t (id INT NOT NULL, name TEXT)", "INSERT INTO t VALUES (1, 'a')"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER () FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, RANK() OVER (PARTITION BY id ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, LAG(id) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    // ── SELECT — set ops ──────────────────────────────────────────────────────
    ("SELECT/SetOps", "SELECT 1 UNION SELECT 2", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 UNION ALL SELECT 2", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 INTERSECT SELECT 1", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 EXCEPT SELECT 2", &[], &[]),
    // ── SELECT — CTE ──────────────────────────────────────────────────────────
    (
        "SELECT/CTE",
        "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH cte AS (SELECT * FROM t) SELECT * FROM cte",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH ins AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM ins",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    // ── Expressions / casts ───────────────────────────────────────────────────
    (
        "Expressions",
        "SELECT CASE WHEN 1=1 THEN 'a' ELSE 'b' END",
        &[],
        &[],
    ),
    ("Expressions", "SELECT COALESCE(NULL, 'x')", &[], &[]),
    ("Expressions", "SELECT NULLIF(1, 1)", &[], &[]),
    ("Expressions", "SELECT GREATEST(1,2,3)", &[], &[]),
    ("Expressions", "SELECT LEAST(1,2,3)", &[], &[]),
    ("Expressions", "SELECT 1::TEXT", &[], &[]),
    ("Expressions", "SELECT CAST(1 AS TEXT)", &[], &[]),
    ("Expressions", "SELECT 'a' || 'b'", &[], &[]),
    ("Expressions", "SELECT 'abc' LIKE 'a%'", &[], &[]),
    // ── Types ─────────────────────────────────────────────────────────────────
    (
        "Types",
        "CREATE TABLE __t (c SMALLINT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BIGINT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c REAL); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c DOUBLE PRECISION); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c NUMERIC); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c NUMERIC(10,2)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TEXT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c VARCHAR); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c VARCHAR(255)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c CHAR(10)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c CITEXT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BOOLEAN); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c DATE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TIME); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TIMESTAMP); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TIMESTAMPTZ); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INTERVAL); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c UUID); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c JSON); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c JSONB); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BYTEA); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT[]); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TEXT[]); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INET); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c CIDR); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c MACADDR); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c MONEY); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c XML); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSVECTOR); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c POINT); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT4RANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c VECTOR(3)); DROP TABLE __t",
        &[],
        &[],
    ),
    // ── Functions / operators ─────────────────────────────────────────────────
    ("Functions/String", "SELECT LOWER('A')", &[], &[]),
    ("Functions/String", "SELECT UPPER('a')", &[], &[]),
    (
        "Functions/String",
        "SELECT SUBSTRING('abc' FROM 1 FOR 2)",
        &[],
        &[],
    ),
    ("Functions/String", "SELECT LENGTH('abc')", &[], &[]),
    (
        "Functions/String",
        "SELECT REPLACE('abc','a','z')",
        &[],
        &[],
    ),
    ("Functions/String", "SELECT TRIM(' a ')", &[], &[]),
    ("Functions/String", "SELECT LPAD('x',3,'0')", &[], &[]),
    ("Functions/String", "SELECT RPAD('x',3,'0')", &[], &[]),
    (
        "Functions/String",
        "SELECT REGEXP_REPLACE('a1','[0-9]','')",
        &[],
        &[],
    ),
    ("Functions/DateTime", "SELECT NOW()", &[], &[]),
    ("Functions/DateTime", "SELECT CURRENT_TIMESTAMP", &[], &[]),
    ("Functions/DateTime", "SELECT CURRENT_DATE", &[], &[]),
    (
        "Functions/DateTime",
        "SELECT DATE_TRUNC('hour', NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT AGE(NOW(), NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(YEAR FROM NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT TO_CHAR(NOW(),'YYYY')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT TO_TIMESTAMP('2024-01-01','YYYY-MM-DD')",
        &[],
        &[],
    ),
    ("Functions/Math", "SELECT ABS(-1)", &[], &[]),
    ("Functions/Math", "SELECT CEIL(1.5)", &[], &[]),
    ("Functions/Math", "SELECT FLOOR(1.5)", &[], &[]),
    ("Functions/Math", "SELECT ROUND(1.5)", &[], &[]),
    ("Functions/Math", "SELECT POWER(2,10)", &[], &[]),
    ("Functions/Math", "SELECT SQRT(4)", &[], &[]),
    ("Functions/Math", "SELECT MOD(10,3)", &[], &[]),
    ("Functions/Crypto", "SELECT GEN_RANDOM_UUID()", &[], &[]),
    (
        "Functions/Crypto",
        "SELECT DIGEST('a','sha256')",
        &[],
        &[],
    ),
    ("Functions/Crypto", "SELECT ENCODE('a','hex')", &[], &[]),
    ("Functions/Crypto", "SELECT DECODE('61','hex')", &[], &[]),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb -> 'a'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb ->> 'a'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb #> '{a}'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb @> '{\"a\":1}'",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT '{1,2}'::int[] && '{2,3}'::int[]",
        &[],
        &[],
    ),
    // ── Full-text search ──────────────────────────────────────────────────────
    (
        "FullTextSearch",
        "SELECT to_tsvector('english', 'a quick brown fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT to_tsquery('english', 'quick & fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT 'a quick brown fox'::tsvector @@ to_tsquery('english', 'fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT plainto_tsquery('english', 'quick fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT phraseto_tsquery('english', 'quick fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT websearch_to_tsquery('english', 'quick OR fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_rank(to_tsvector('a quick'), to_tsquery('quick'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_headline('a quick fox', to_tsquery('quick'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "CREATE TABLE doc (body TEXT, ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED)",
        &[],
        &["DROP TABLE doc"],
    ),
    (
        "FullTextSearch",
        "CREATE INDEX ON doc USING gin (ts)",
        &["CREATE TABLE doc (body TEXT, ts TSVECTOR)"],
        &["DROP TABLE doc"],
    ),
    // ── Range types ──────────────────────────────────────────────────────────
    ("Ranges", "SELECT int4range(1, 10)", &[], &[]),
    ("Ranges", "SELECT '[1,10)'::int4range", &[], &[]),
    ("Ranges", "SELECT int4range(1,10) @> 5", &[], &[]),
    (
        "Ranges",
        "SELECT int4range(1,10) && int4range(5,15)",
        &[],
        &[],
    ),
    ("Ranges", "SELECT lower(int4range(1,10))", &[], &[]),
    ("Ranges", "SELECT upper(int4range(1,10))", &[], &[]),
    ("Ranges", "SELECT isempty(int4range(1,1))", &[], &[]),
    (
        "Ranges",
        "SELECT '[2020-01-01,2020-12-31]'::daterange",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT tsrange(NOW() - interval '1 hour', NOW())",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4multirange(int4range(1,5), int4range(10,15))",
        &[],
        &[],
    ),
    // ── Array advanced ───────────────────────────────────────────────────────
    ("Functions/Array", "SELECT ARRAY[1,2,3]", &[], &[]),
    ("Functions/Array", "SELECT '{1,2,3}'::int[]", &[], &[]),
    (
        "Functions/Array",
        "SELECT '{{1,2},{3,4}}'::int[][]",
        &[],
        &[],
    ),
    ("Functions/Array", "SELECT (ARRAY[1,2,3])[2]", &[], &[]),
    (
        "Functions/Array",
        "SELECT (ARRAY[1,2,3,4,5])[2:4]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_length(ARRAY[1,2,3], 1)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_ndims(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_lower(ARRAY[1,2,3], 1)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_upper(ARRAY[1,2,3], 1)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_position(ARRAY[1,2,3], 2)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_remove(ARRAY[1,2,3,2], 2)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_replace(ARRAY[1,2,3], 2, 99)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_append(ARRAY[1,2], 3)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_prepend(0, ARRAY[1,2])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_cat(ARRAY[1,2], ARRAY[3,4])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_to_string(ARRAY['a','b','c'], ',', '*')",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT string_to_array('a,b,c', ',')",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT unnest(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT * FROM unnest(ARRAY[1,2,3]) WITH ORDINALITY",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT generate_subscripts(ARRAY[10,20,30], 1)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2] @> ARRAY[1]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2] <@ ARRAY[1,2,3]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2] || ARRAY[3,4]",
        &[],
        &[],
    ),
    // ── JSONB advanced ───────────────────────────────────────────────────────
    (
        "Functions/JSONB",
        "SELECT jsonb_set('{\"a\":1}'::jsonb, '{a}', '2'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_insert('{\"a\":[1,2]}'::jsonb, '{a,1}', '99'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_strip_nulls('{\"a\":1,\"b\":null}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_query('{\"a\":1}'::jsonb, '$.a')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_exists('{\"a\":1}'::jsonb, '$.a')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_match('{\"a\":1}'::jsonb, '$.a == 1')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":{\"b\":1}}'::jsonb @? '$.a.b'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb @@ '$.a == 1'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_typeof('1'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_pretty('{\"a\":1}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_array_length('[1,2,3]'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_object_keys('{\"a\":1,\"b\":2}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM jsonb_each('{\"a\":1}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM jsonb_each_text('{\"a\":1}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM jsonb_array_elements_text('[\"a\",\"b\"]'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_build_object('a', 1, 'b', 2)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_build_array(1, 'a', true)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT to_jsonb(ROW(1, 'a'))",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT to_json(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT row_to_json(t) FROM (SELECT 1 AS a) t",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT array_to_json(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_agg(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_object_agg(name, id) FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &["DROP TABLE t"],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1,\"b\":2}'::jsonb - 'a'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1,\"b\":2}'::jsonb - ARRAY['a','b']",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '[1,2,3]'::jsonb - 1",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb",
        &[],
        &[],
    ),
    // ── String advanced ──────────────────────────────────────────────────────
    ("Functions/String", "SELECT initcap('hello world')", &[], &[]),
    (
        "Functions/String",
        "SELECT split_part('a,b,c', ',', 2)",
        &[],
        &[],
    ),
    ("Functions/String", "SELECT reverse('abc')", &[], &[]),
    (
        "Functions/String",
        "SELECT format('Hello, %s', 'world')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT format('%I.%s', 'schema', 'tab')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT quote_ident('table name')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT quote_literal('abc')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT quote_nullable(NULL)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_match('abc123', '([a-z]+)([0-9]+)')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_matches('abc123 def456', '[a-z]+\\d+', 'g')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_split_to_array('a,b,c', ',')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_split_to_table('a,b,c', ',')",
        &[],
        &[],
    ),
    ("Functions/String", "SELECT chr(65)", &[], &[]),
    ("Functions/String", "SELECT ascii('A')", &[], &[]),
    (
        "Functions/String",
        "SELECT char_length('hello')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT bit_length('hello')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT octet_length('hello')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT encode(E'\\x12'::bytea, 'base64')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT decode('EgA=', 'base64')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT translate('12abc', 'abc', 'xyz')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT btrim('xxabcxx', 'x')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT ltrim('xxabc', 'x')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT rtrim('abcxx', 'x')",
        &[],
        &[],
    ),
    // ── Date/time advanced ───────────────────────────────────────────────────
    (
        "Functions/DateTime",
        "SELECT make_date(2024, 1, 15)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_time(12, 30, 45.5)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_timestamp(2024, 1, 15, 12, 30, 45.5)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_timestamptz(2024, 1, 15, 12, 30, 45.5, 'UTC')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_interval(years => 1, days => 30)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date_part('year', NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(EPOCH FROM NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date '2024-01-01' + interval '1 day'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date '2024-12-31' - date '2024-01-01'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT NOW() AT TIME ZONE 'America/New_York'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT (NOW(), NOW() + interval '1h') OVERLAPS (NOW() + interval '30m', NOW() + interval '90m')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_hours(interval '36 hours')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_days(interval '40 days')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_interval(interval '1 mon -1 hour')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT isfinite(NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT isfinite(date '2024-01-01')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT 'infinity'::timestamp",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT '-infinity'::timestamp",
        &[],
        &[],
    ),
    // ── Window advanced ──────────────────────────────────────────────────────
    (
        "SELECT/Window",
        "SELECT ROW_NUMBER() OVER () FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT DENSE_RANK() OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT PERCENT_RANK() OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT CUME_DIST() OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT NTILE(4) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT LAG(id, 1, 0) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT LEAD(id) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT FIRST_VALUE(id) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT LAST_VALUE(id) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT NTH_VALUE(id, 3) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER w FROM t WINDOW w AS (PARTITION BY id)",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    // ── Aggregates advanced ──────────────────────────────────────────────────
    (
        "SELECT/Aggregate",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT percentile_disc(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT mode() WITHIN GROUP (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT stddev(id), stddev_pop(id), stddev_samp(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT variance(id), var_pop(id), var_samp(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT corr(id, id), covar_pop(id, id), covar_samp(id, id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT regr_slope(id, id), regr_intercept(id, id), regr_r2(id, id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT regr_avgx(id, id), regr_avgy(id, id), regr_count(id, id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT regr_sxx(id, id), regr_syy(id, id), regr_sxy(id, id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT bool_and(id > 0), bool_or(id > 0), every(id > 0) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT bit_and(id), bit_or(id), bit_xor(id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT array_agg(id ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT array_agg(DISTINCT id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT string_agg(name, ',' ORDER BY id) FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &["DROP TABLE t"],
    ),
    // ── IS predicates ────────────────────────────────────────────────────────
    ("Expressions", "SELECT true IS TRUE", &[], &[]),
    ("Expressions", "SELECT true IS NOT TRUE", &[], &[]),
    ("Expressions", "SELECT false IS FALSE", &[], &[]),
    ("Expressions", "SELECT false IS NOT FALSE", &[], &[]),
    ("Expressions", "SELECT NULL::bool IS UNKNOWN", &[], &[]),
    (
        "Expressions",
        "SELECT NULL::bool IS NOT UNKNOWN",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT 1 IS DISTINCT FROM 2",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT 1 IS NOT DISTINCT FROM 1",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT ROW(1, NULL) IS NULL",
        &[],
        &[],
    ),
    // ── PG-specific operators ─────────────────────────────────────────────────
    // Comparison / null-handling
    ("PG/Operators", "SELECT 1 IS DISTINCT FROM 2", &[], &[]),
    ("PG/Operators", "SELECT 1 IS NOT DISTINCT FROM 1", &[], &[]),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id IS NULL",
        &["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id IS NOT NULL",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id BETWEEN 1 AND 10",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (5)"],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id NOT BETWEEN 1 AND 10",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (20)"],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id BETWEEN SYMMETRIC 10 AND 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (5)"],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id NOT BETWEEN SYMMETRIC 10 AND 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (50)"],
        &["DROP TABLE t"],
    ),
    // POSIX regex operators
    (
        "PG/Operators",
        "SELECT * FROM t WHERE name ~ '^a'",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'abc')",
        ],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE name !~ '^z'",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'abc')",
        ],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE name ~* '^A'",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'abc')",
        ],
        &["DROP TABLE t"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE name !~* '^Z'",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'abc')",
        ],
        &["DROP TABLE t"],
    ),
    // Array operators
    ("PG/Operators", "SELECT ARRAY[1,2] || ARRAY[3,4]", &[], &[]),
    ("PG/Operators", "SELECT ARRAY[1,2,3] @> ARRAY[1,2]", &[], &[]),
    ("PG/Operators", "SELECT ARRAY[1,2] <@ ARRAY[1,2,3]", &[], &[]),
    ("PG/Operators", "SELECT ARRAY[1,2] && ARRAY[2,3]", &[], &[]),
    // Quantified subquery (ANY / ALL / SOME)
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id = ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id > ALL (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (10)",
            "INSERT INTO u VALUES (1), (2)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id = SOME (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    // Bitwise operators — DataFusion supports these natively in arithmetic
    ("PG/Operators", "SELECT 5 & 3", &[], &[]),
    ("PG/Operators", "SELECT 5 | 3", &[], &[]),
    ("PG/Operators", "SELECT 5 # 3", &[], &[]),
    ("PG/Operators", "SELECT ~5", &[], &[]),
    ("PG/Operators", "SELECT 1 << 3", &[], &[]),
    ("PG/Operators", "SELECT 8 >> 2", &[], &[]),
    // OVERLAPS — shipped by datetime-extras agent; verify present
    (
        "PG/Operators",
        "SELECT (NOW(), NOW() + INTERVAL '1 hour') OVERLAPS (NOW() + INTERVAL '30 minutes', NOW() + INTERVAL '90 minutes')",
        &[],
        &[],
    ),
    // ── Row-level locking ────────────────────────────────────────────────────
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR UPDATE",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR UPDATE OF t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR NO KEY UPDATE",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR SHARE",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR KEY SHARE",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR UPDATE NOWAIT",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Locking",
        "SELECT * FROM t FOR UPDATE SKIP LOCKED",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    // ── CTE variants ─────────────────────────────────────────────────────────
    (
        "SELECT/CTE",
        "WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r",
        &[],
        &[],
    ),
    // ── LATERAL extras ───────────────────────────────────────────────────────
    (
        "SELECT/Joins",
        "SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t LEFT JOIN LATERAL (SELECT id FROM u WHERE u.id = t.id) sub ON true",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t, LATERAL unnest(ARRAY[1,2,3]) tag",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    // ── Generated columns + IDENTITY ─────────────────────────────────────────
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id SERIAL)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id BIGSERIAL)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id SMALLSERIAL)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) STORED)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) VIRTUAL)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ALTER COLUMN id SET GENERATED ALWAYS",
        &["CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ALTER COLUMN id SET GENERATED BY DEFAULT",
        &["CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ALTER COLUMN id DROP IDENTITY",
        &["CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (1)",
        &["CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t OVERRIDING USER VALUE VALUES (1)",
        &["CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)"],
        &["DROP TABLE t"],
    ),
    // ── Constraints advanced ─────────────────────────────────────────────────
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT, EXCLUDE USING gist (id WITH =))",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT REFERENCES u DEFERRABLE INITIALLY DEFERRED)",
        &["CREATE TABLE u (id INT PRIMARY KEY)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES u(id) MATCH FULL)",
        &["CREATE TABLE u (id INT PRIMARY KEY)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (a INT REFERENCES u ON UPDATE CASCADE ON DELETE SET NULL)",
        &["CREATE TABLE u (id INT PRIMARY KEY)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT NOT NULL, name TEXT, UNIQUE (id, name) INCLUDE (name))",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t VALIDATE CONSTRAINT ck",
        &["CREATE TABLE t (id INT NOT NULL, CONSTRAINT ck CHECK (id > 0))"],
        &["DROP TABLE t"],
    ),
    // ── Inheritance and partitioning ─────────────────────────────────────────
    (
        "DDL/Tables",
        "SELECT * FROM ONLY t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t_2024 PARTITION OF t FOR VALUES FROM (2024) TO (2025)",
        &["CREATE TABLE t (id INT) PARTITION BY RANGE (id)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (region TEXT) PARTITION BY LIST (region)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (id INT) PARTITION BY HASH (id)",
        &[],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t ATTACH PARTITION p FOR VALUES IN ('us')",
        &[
            "CREATE TABLE t (region TEXT) PARTITION BY LIST (region)",
            "CREATE TABLE p (region TEXT)",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Tables",
        "ALTER TABLE t DETACH PARTITION p",
        &[
            "CREATE TABLE t (region TEXT) PARTITION BY LIST (region)",
            "CREATE TABLE p PARTITION OF t FOR VALUES IN ('us')",
        ],
        &["DROP TABLE t"],
    ),
    // ── Roles/permissions/security ───────────────────────────────────────────
    ("Roles", "CREATE ROLE alice", &[], &["DROP ROLE alice"]),
    (
        "Roles",
        "CREATE ROLE alice WITH LOGIN PASSWORD 'pw'",
        &[],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "ALTER ROLE alice WITH SUPERUSER",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "DROP ROLE alice",
        &["CREATE ROLE alice"],
        &[],
    ),
    (
        "Roles",
        "GRANT SELECT ON t TO alice",
        &["CREATE TABLE t (id INT)", "CREATE ROLE alice"],
        &["DROP TABLE t", "DROP ROLE alice"],
    ),
    (
        "Roles",
        "GRANT ALL PRIVILEGES ON t TO alice",
        &["CREATE TABLE t (id INT)", "CREATE ROLE alice"],
        &["DROP TABLE t", "DROP ROLE alice"],
    ),
    (
        "Roles",
        "REVOKE INSERT ON t FROM alice",
        &["CREATE TABLE t (id INT)", "CREATE ROLE alice"],
        &["DROP TABLE t", "DROP ROLE alice"],
    ),
    ("Roles", "SET ROLE alice", &[], &[]),
    ("Roles", "RESET ROLE", &[], &[]),
    ("Roles", "SELECT current_user", &[], &[]),
    ("Roles", "SELECT session_user", &[], &[]),
    // ── Schemas ──────────────────────────────────────────────────────────────
    (
        "Schemas",
        "CREATE SCHEMA myschema",
        &[],
        &["DROP SCHEMA myschema"],
    ),
    (
        "Schemas",
        "CREATE SCHEMA AUTHORIZATION alice",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Schemas",
        "SET search_path = myschema, public",
        &[],
        &[],
    ),
    (
        "Schemas",
        "CREATE TABLE myschema.t (id INT)",
        &["CREATE SCHEMA myschema"],
        &["DROP SCHEMA myschema CASCADE"],
    ),
    (
        "Schemas",
        "DROP SCHEMA myschema CASCADE",
        &["CREATE SCHEMA myschema"],
        &[],
    ),
    // ── Triggers ─────────────────────────────────────────────────────────────
    (
        "DDL/Other",
        "CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW WHEN (NEW.id <> OLD.id) EXECUTE FUNCTION fn()",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE TRIGGER trg INSTEAD OF DELETE ON vv FOR EACH ROW EXECUTE FUNCTION fn()",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE VIEW vv AS SELECT * FROM t",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE TRIGGER trg AFTER INSERT ON t REFERENCING NEW TABLE AS new_t FOR EACH STATEMENT EXECUTE FUNCTION fn()",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE CONSTRAINT TRIGGER trg AFTER INSERT ON t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fn()",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    // ── Misc ─────────────────────────────────────────────────────────────────
    (
        "Misc",
        "TABLE t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "Misc",
        "VALUES (1,2), (3,4)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT * FROM (VALUES (1,'a'), (2,'b')) AS v(id, name)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT * INTO new_t FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "Misc",
        "COPY t FROM '/tmp/x' WITH (FORMAT csv, HEADER, DELIMITER ',')",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Misc",
        "COPY (SELECT * FROM t) TO '/tmp/x' WITH (FORMAT csv)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Misc",
        "DO $$ BEGIN RAISE NOTICE 'hi'; END; $$ LANGUAGE plpgsql",
        &[],
        &[],
    ),
    (
        "Misc",
        "COMMENT ON COLUMN t.id IS 'pk'",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Misc",
        "COMMENT ON FUNCTION f(int) IS 'x'",
        &["CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x*2 $$"],
        &["DROP FUNCTION f(INT)"],
    ),
    (
        "Misc",
        "SELECT pg_advisory_lock(1)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_advisory_unlock(1)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_try_advisory_lock(1)",
        &[],
        &[],
    ),
    (
        "Misc",
        "UNLISTEN *",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_typeof(1)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_size_pretty(1024::bigint)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_column_size('hello')",
        &[],
        &[],
    ),
    // ── Transactions ──────────────────────────────────────────────────────────
    ("Transactions", "BEGIN", &[], &[]),
    ("Transactions", "COMMIT", &[], &[]),
    ("Transactions", "ROLLBACK", &[], &[]),
    (
        "Transactions",
        "SAVEPOINT s",
        &[],
        &[],
    ),
    (
        "Transactions",
        "RELEASE SAVEPOINT s",
        &[],
        &[],
    ),
    (
        "Transactions",
        "ROLLBACK TO s",
        &[],
        &[],
    ),
    (
        "Transactions",
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        &[],
        &[],
    ),
    ("Transactions", "BEGIN READ ONLY", &[], &[]),
    // ── Sessions / admin ──────────────────────────────────────────────────────
    (
        "Admin/Sessions",
        "LISTEN ch",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "NOTIFY ch, 'msg'",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "UNLISTEN ch",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "PREPARE stmt AS SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "EXECUTE stmt",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DEALLOCATE stmt",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DECLARE c CURSOR FOR SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "FETCH 1 FROM c",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "CLOSE c",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "LOCK TABLE t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "VACUUM",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "ANALYZE",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "CLUSTER t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "EXPLAIN SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "EXPLAIN ANALYZE SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SET search_path = public",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SHOW search_path",
        &[],
        &[],
    ),
];

// ─────────────────────────────────────────────────────────────────────────────
// Classification helpers
// ─────────────────────────────────────────────────────────────────────────────

fn classify_error(err: &basin_common::BasinError) -> (Outcome, String) {
    let msg = format!("{err}");
    let lower = msg.to_lowercase();

    // Out-of-scope: LISTEN/NOTIFY/VACUUM/etc. explicitly rejected by engine
    if lower.contains("0a000")
        || lower.contains("not supported")
        || lower.contains("feature not supported")
        || lower.contains("unsupported in poc")
        || lower.contains("out of scope")
        || lower.contains("no pub/sub")
    {
        return (Outcome::OutOfScope, short_note(&msg));
    }

    // Parser-level rejection (sqlparser or internal parse error)
    if lower.contains("parse error")
        || lower.contains("syntax error")
        || lower.contains("sql parser")
        || lower.contains("expected")
        || lower.contains("unrecognized")
        || lower.contains("42601")
    {
        return (Outcome::ParserRejected, short_note(&msg));
    }

    // Planner rejection (plan-time, schema check, "not representable", etc.)
    if lower.contains("not representable")
        || lower.contains("invalid schema")
        || lower.contains("not a supported")
        || lower.contains("unsupported")
        || lower.contains("not yet")
        || lower.contains("deferred")
        || lower.contains("requires")
        || lower.contains("rejected")
    {
        return (Outcome::PlannerRejected, short_note(&msg));
    }

    // Runtime exec failure (table not found, type mismatch, etc.)
    (Outcome::ExecFailed, short_note(&msg))
}

fn short_note(msg: &str) -> String {
    // Trim to 80 chars max for the markdown table
    let s = msg.replace('\n', " ").replace('|', "/");
    if s.len() > 80 {
        format!("{}…", &s[..77])
    } else {
        s
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Single-pass executor
// ─────────────────────────────────────────────────────────────────────────────

/// Run one pass of the matrix against a fresh engine+tenant for each row.
/// Returns `(outcomes, notes)` parallel to MATRIX indices.
async fn run_pass() -> Vec<(Outcome, String)> {
    let mut results = Vec::with_capacity(MATRIX.len());

    for (_, sql, setup, _teardown) in MATRIX.iter() {
        // Each row gets its own fresh tenant so prior failures don't bleed.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let tenant = TenantId::new();

        let sess = match eng.open_session(tenant).await {
            Ok(s) => s,
            Err(e) => {
                results.push((Outcome::ExecFailed, short_note(&format!("{e}"))));
                continue;
            }
        };

        // Run setup statements — failures abort this row (mark ExecFailed).
        let mut setup_ok = true;
        for setup_sql in setup.iter() {
            if let Err(e) = sess.execute(setup_sql).await {
                results.push((
                    Outcome::ExecFailed,
                    short_note(&format!("setup failed: {e}")),
                ));
                setup_ok = false;
                break;
            }
        }
        if !setup_ok {
            continue;
        }

        // Run the test SQL.
        // For "TYPE" rows the SQL contains semicolons (CREATE + DROP in one string).
        // The engine's execute() handles exactly one statement; we need to split
        // multi-statement type-test entries here.
        let stmts: Vec<&str> = if sql.contains("; DROP") || sql.contains(";DROP") {
            sql.split(';').filter(|s| !s.trim().is_empty()).collect()
        } else {
            vec![sql]
        };

        let mut final_outcome = Outcome::Ok;
        let mut final_note = String::new();

        'stmts: for stmt in &stmts {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            match sess.execute(stmt).await {
                Ok(_) => {}
                Err(e) => {
                    let (outcome, note) = classify_error(&e);
                    final_outcome = outcome;
                    final_note = note;
                    break 'stmts;
                }
            }
        }

        results.push((final_outcome, final_note));
    }

    results
}

// ─────────────────────────────────────────────────────────────────────────────
// Markdown generation
// ─────────────────────────────────────────────────────────────────────────────

fn generate_markdown(rows: &[MatrixRow], timestamp: &str, total: usize) -> String {
    let green: usize = rows
        .iter()
        .flat_map(|r| r.outcomes.iter())
        .filter(|o| **o == Outcome::Ok)
        .count();

    let mut out = format!(
        r#"# Basin SQL support — auto-generated matrix

Run `cargo test -p basin-integration-tests --test sql_support_matrix` to refresh.

Last run: {timestamp}
SQL fragments tested: {total} total / {green} green (across all three configurations).

## Configurations

- **Default**: sqlparser-rs front-end (what ships in v0.1).
- **`BASIN_PG_QUERY=1`**: libpg_query parses every statement; unsupported kinds
  are rejected early with SQLSTATE 0A000.
- **`BASIN_PG_QUERY=1 BASIN_PG_QUERY_PLAN=1`**: also Phase 2 PgNode →
  DataFusion LogicalPlan translator for single-table SELECT.

## Legend

| Symbol | Meaning |
|---|---|
| ✅ | Ran end-to-end, produced expected result |
| 🛠 | Parsed + planned, runtime exec error |
| 📜 | Planner/executor rejected (plan-time error) |
| ❌ | Parser refused (sqlparser / pg_query syntax error) |
| 🚫 | Explicitly out-of-scope (LISTEN/NOTIFY/VACUUM etc.) |

"#,
    );

    // Collect categories in sorted order
    let mut categories: Vec<&str> = rows.iter().map(|r| r.category).collect();
    categories.dedup();
    categories.sort_unstable();
    categories.dedup();

    for cat in &categories {
        let cat_rows: Vec<&MatrixRow> = rows.iter().filter(|r| r.category == *cat).collect();
        out.push_str(&format!("## {cat}\n\n"));
        out.push_str("| SQL | Default | +PG\\_QUERY | +PG\\_PLAN | Notes |\n");
        out.push_str("|---|---|---|---|---|\n");
        for row in &cat_rows {
            let sql_escaped = row.sql.replace('|', "\\|").replace('`', "'");
            let o0 = row.outcomes[0].emoji();
            let o1 = row.outcomes[1].emoji();
            let o2 = row.outcomes[2].emoji();
            let notes = if row.notes.is_empty() {
                String::new()
            } else {
                row.notes.replace('|', "/")
            };
            out.push_str(&format!(
                "| `{sql_escaped}` | {o0} | {o1} | {o2} | {notes} |\n"
            ));
        }
        out.push('\n');
    }

    // Footer — surfaces a way for users to flag missing PG syntax and points
    // at the source file so contributors can add coverage. This block is
    // mirrored on the marketing /compatibility page so the two surfaces stay
    // in sync.
    out.push_str("---\n\n");
    out.push_str("## Missing something?\n\n");
    out.push_str(
        "If you tried PG syntax that's not in this matrix, \
[open an issue](https://github.com/bas-in/basin/issues/new?template=sql_compatibility.yml&title=Missing+SQL+syntax%3A+) \
— we triage compatibility gaps within 48 hours.\n\n",
    );
    out.push_str(
        "This page is regenerated by `cargo test -p basin-integration-tests --test sql_support_matrix`. \
To suggest an addition to the matrix, edit `tests/integration/tests/sql_support_matrix.rs` and rerun.\n",
    );

    out
}

// ─────────────────────────────────────────────────────────────────────────────
// Test entry point
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_support_matrix() {
    basin_common::telemetry::try_init_for_tests();

    // ── Pass 0: Default (no env vars) ────────────────────────────────────────
    // Unset both gates to ensure a clean state
    std::env::remove_var("BASIN_PG_QUERY");
    std::env::remove_var("BASIN_PG_QUERY_PLAN");
    let pass0 = run_pass().await;

    // ── Pass 1: BASIN_PG_QUERY=1 ─────────────────────────────────────────────
    std::env::set_var("BASIN_PG_QUERY", "1");
    std::env::remove_var("BASIN_PG_QUERY_PLAN");
    let pass1 = run_pass().await;

    // ── Pass 2: BASIN_PG_QUERY=1 + BASIN_PG_QUERY_PLAN=1 ────────────────────
    std::env::set_var("BASIN_PG_QUERY", "1");
    std::env::set_var("BASIN_PG_QUERY_PLAN", "1");
    let pass2 = run_pass().await;

    // Clean up env for other tests running in process
    std::env::remove_var("BASIN_PG_QUERY");
    std::env::remove_var("BASIN_PG_QUERY_PLAN");

    // ── Assemble MatrixRow vec ────────────────────────────────────────────────
    assert_eq!(pass0.len(), MATRIX.len());
    assert_eq!(pass1.len(), MATRIX.len());
    assert_eq!(pass2.len(), MATRIX.len());

    let matrix_rows: Vec<MatrixRow> = MATRIX
        .iter()
        .zip(pass0.iter())
        .zip(pass1.iter())
        .zip(pass2.iter())
        .map(|(((entry, p0), p1), p2)| {
            let notes = if p0.0 != Outcome::Ok {
                p0.1.clone()
            } else if p1.0 != Outcome::Ok {
                p1.1.clone()
            } else {
                p2.1.clone()
            };
            MatrixRow {
                category: entry.0,
                sql: entry.1,
                outcomes: [p0.0.clone(), p1.0.clone(), p2.0.clone()],
                notes,
            }
        })
        .collect();

    // ── Print summary ─────────────────────────────────────────────────────────
    let total = matrix_rows.len();
    for (pass_idx, pass_label) in [(0usize, "Default"), (1usize, "+PG_QUERY"), (2usize, "+PG_QUERY+PG_PLAN")] {
        let ok = matrix_rows.iter().filter(|r| r.outcomes[pass_idx] == Outcome::Ok).count();
        let exec_fail = matrix_rows.iter().filter(|r| r.outcomes[pass_idx] == Outcome::ExecFailed).count();
        let plan_rej = matrix_rows.iter().filter(|r| r.outcomes[pass_idx] == Outcome::PlannerRejected).count();
        let parse_rej = matrix_rows.iter().filter(|r| r.outcomes[pass_idx] == Outcome::ParserRejected).count();
        let oos = matrix_rows.iter().filter(|r| r.outcomes[pass_idx] == Outcome::OutOfScope).count();
        println!(
            "Pass {pass_label}: ✅ {ok}  🛠 {exec_fail}  📜 {plan_rej}  ❌ {parse_rej}  🚫 {oos}  (total {total})"
        );
    }

    // ── Write docs/sql-support.md ─────────────────────────────────────────────
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    // Format as a human-readable UTC string
    let timestamp = format!("{ts} (Unix epoch)");

    let md = generate_markdown(&matrix_rows, &timestamp, total);

    // The docs/ dir is at workspace root; find it relative to CARGO_MANIFEST_DIR.
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| "tests/integration".to_string());
    // Walk up from tests/integration to workspace root
    let workspace_root = std::path::PathBuf::from(&manifest_dir)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    let docs_dir = workspace_root.join("docs");
    let out_path = docs_dir.join("sql-support.md");

    std::fs::create_dir_all(&docs_dir).expect("create docs/");
    std::fs::write(&out_path, &md).unwrap_or_else(|e| {
        eprintln!("Warning: could not write {}: {e}", out_path.display());
    });

    println!("Wrote {}", out_path.display());

    // ── Sanity assertions ─────────────────────────────────────────────────────
    // The matrix must have at least 120 rows.
    assert!(
        total >= 120,
        "expected at least 120 SQL fragments, got {total}"
    );
    // At least some rows must succeed in the default config.
    let default_ok = matrix_rows.iter().filter(|r| r.outcomes[0] == Outcome::Ok).count();
    assert!(
        default_ok >= 20,
        "expected at least 20 OK rows in default config, got {default_ok}"
    );
}
