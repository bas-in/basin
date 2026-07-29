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
use basin_common::ProjectId;
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
        &["CREATE TABLE u (id INT PRIMARY KEY)"],
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
        &["CREATE SCHEMA s"],
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
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE VIEW v AS SELECT * FROM t"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "REFRESH MATERIALIZED VIEW mv",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "CREATE MATERIALIZED VIEW mv WITH (basin.continuous, refresh_interval = '1h') AS SELECT * FROM t",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP MATERIALIZED VIEW mv",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
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
    (
        "DDL/Other",
        "CREATE EXTENSION IF NOT EXISTS pgcrypto",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "DROP EXTENSION IF EXISTS pgcrypto",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE AGGREGATE myagg(INT) (SFUNC = int4pl, STYPE = INT)",
        &[],
        &["DROP AGGREGATE IF EXISTS myagg(INT)"],
    ),
    (
        "DDL/Other",
        "CREATE OPERATOR + (LEFTARG = INT, RIGHTARG = INT, FUNCTION = int4pl)",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE TYPE mycomposite AS (x INT, y INT)",
        &[],
        &["DROP TYPE IF EXISTS mycomposite"],
    ),
    (
        "DDL/Other",
        "CREATE TYPE myenum AS ENUM ('a', 'b', 'c')",
        &[],
        &["DROP TYPE IF EXISTS myenum"],
    ),
    (
        "DDL/Other",
        "CREATE RULE myrule AS ON INSERT TO t DO ALSO NOTHING",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE EVENT TRIGGER myevt ON ddl_command_start EXECUTE FUNCTION fn()",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE PUBLICATION mypub FOR ALL TABLES",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "CREATE FOREIGN TABLE ft (id INT) SERVER myserver",
        &[],
        &[],
    ),
    (
        "DDL/Other",
        "ALTER VIEW v AS SELECT id FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE VIEW v AS SELECT * FROM t",
        ],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "CREATE OR REPLACE VIEW v AS SELECT id FROM t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DDL/Other",
        "DROP TRIGGER trg ON t",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn()",
        ],
        &["DROP TABLE t"],
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
            "CREATE TABLE t (id INT PRIMARY KEY)",
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
            "CREATE TABLE t (id INT PRIMARY KEY)",
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
    // ON CONFLICT extra variants
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT (id) DO NOTHING",
        &["CREATE TABLE t (id INT PRIMARY KEY)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE t.id > 0",
        &["CREATE TABLE t (id INT PRIMARY KEY, name TEXT)"],
        &["DROP TABLE t"],
    ),
    // MERGE extra variants
    (
        "DML",
        "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED AND u.id > 0 THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (u.id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    // Correlated / NOT-style subqueries in DML
    (
        "DML",
        "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "UPDATE t SET id = 99 WHERE id NOT IN (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
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
    // Correlated subqueries
    (
        "SELECT/Joins",
        "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t WHERE id NOT IN (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT t.id, (SELECT COUNT(*) FROM u WHERE u.id = t.id) AS cnt FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1), (1)",
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
    // ── SELECT — ORDER BY / LIMIT / OFFSET / FETCH ───────────────────────────
    (
        "SELECT/Projection",
        "SELECT id FROM t ORDER BY id ASC NULLS FIRST",
        &["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1), (NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t ORDER BY id DESC NULLS LAST",
        &["CREATE TABLE t (id INT)", "INSERT INTO t VALUES (1), (NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t FETCH FIRST 3 ROWS ONLY",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t FETCH FIRST 3 ROWS WITH TIES",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t ORDER BY 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    // ── SELECT — set ops ──────────────────────────────────────────────────────
    ("SELECT/SetOps", "SELECT 1 UNION SELECT 2", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 UNION ALL SELECT 2", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 INTERSECT SELECT 1", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 INTERSECT ALL SELECT 1", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 EXCEPT SELECT 2", &[], &[]),
    ("SELECT/SetOps", "SELECT 1 EXCEPT ALL SELECT 2", &[], &[]),
    (
        "SELECT/SetOps",
        "SELECT id FROM t UNION SELECT id FROM u ORDER BY id",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (2), (3)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/SetOps",
        "SELECT id FROM t INTERSECT SELECT id FROM u",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (2), (3)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/SetOps",
        "SELECT id FROM t EXCEPT SELECT id FROM u",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1), (2)",
            "INSERT INTO u VALUES (2), (3)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
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
    (
        "SELECT/CTE",
        "WITH upd AS (UPDATE t SET id = 99 WHERE id = 1 RETURNING id) SELECT * FROM upd",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH del AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT * FROM del",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE fib(a, b) AS (SELECT 1, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT a FROM fib",
        &[],
        &[],
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
    (
        "Types",
        "CREATE TABLE __t (c BIT(8)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BIT VARYING(8)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c OID); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c REGCLASS); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c REGTYPE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSQUERY); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c PG_LSN); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "Expressions",
        "SELECT id, CASE WHEN id < 0 THEN 'neg' WHEN id = 0 THEN 'zero' ELSE 'pos' END FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (5)"],
        &["DROP TABLE t"],
    ),
    ("Expressions", "SELECT 1::BIGINT + 2::BIGINT", &[], &[]),
    ("Expressions", "SELECT '2024-01-01'::DATE", &[], &[]),
    ("Expressions", "SELECT '12:00:00'::TIME", &[], &[]),
    ("Expressions", "SELECT '2024-01-01 12:00:00'::TIMESTAMP", &[], &[]),
    ("Expressions", "SELECT '00:01:00'::INTERVAL", &[], &[]),
    ("Expressions", "SELECT 'a6c5e8f0-1234-5678-abcd-000000000000'::UUID", &[], &[]),
    ("Expressions", "SELECT 'true'::BOOLEAN", &[], &[]),
    ("Expressions", "SELECT B'1010'", &[], &[]),
    ("Expressions", "SELECT X'FF'", &[], &[]),
    (
        "Expressions",
        "SELECT $1",
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
    ("Functions/Math", "SELECT ROUND(3.14159, 2)", &[], &[]),
    ("Functions/Math", "SELECT TRUNC(3.7)", &[], &[]),
    ("Functions/Math", "SELECT TRUNC(3.7, 1)", &[], &[]),
    ("Functions/Math", "SELECT POWER(2,10)", &[], &[]),
    ("Functions/Math", "SELECT SQRT(4)", &[], &[]),
    ("Functions/Math", "SELECT MOD(10,3)", &[], &[]),
    ("Functions/Math", "SELECT LOG(100)", &[], &[]),
    ("Functions/Math", "SELECT LOG(10, 100)", &[], &[]),
    ("Functions/Math", "SELECT LN(2.718281828::float8)", &[], &[]),
    ("Functions/Math", "SELECT EXP(1.0::float8)", &[], &[]),
    ("Functions/Math", "SELECT PI()", &[], &[]),
    ("Functions/Math", "SELECT SIGN(-5)", &[], &[]),
    ("Functions/Math", "SELECT RANDOM()", &[], &[]),
    ("Functions/Math", "SELECT DIV(10, 3)", &[], &[]),
    ("Functions/Math", "SELECT FACTORIAL(5)", &[], &[]),
    ("Functions/Math", "SELECT GCD(12, 8)", &[], &[]),
    ("Functions/Math", "SELECT LCM(4, 6)", &[], &[]),
    ("Functions/Math", "SELECT DEGREES(3.14159)", &[], &[]),
    ("Functions/Math", "SELECT RADIANS(180.0)", &[], &[]),
    ("Functions/Math", "SELECT SIN(0.0)", &[], &[]),
    ("Functions/Math", "SELECT COS(0.0)", &[], &[]),
    ("Functions/Math", "SELECT TAN(0.0)", &[], &[]),
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
    (
        "FullTextSearch",
        "SELECT tsvector_to_array(to_tsvector('a quick fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT tsquery_phrase(to_tsquery('quick'), to_tsquery('fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT to_tsvector('a') @@ to_tsquery('b')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_rank_cd(to_tsvector('a quick fox'), to_tsquery('quick'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT numnode(to_tsquery('quick & fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT querytree(to_tsquery('quick & fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT strip(to_tsvector('a quick fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT * FROM t WHERE ts @@ to_tsquery('english', 'quick')",
        &[
            "CREATE TABLE t (id INT NOT NULL, ts TSVECTOR)",
            "INSERT INTO t VALUES (1, to_tsvector('a quick fox'))",
        ],
        &["DROP TABLE t"],
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
    ("Ranges", "SELECT numrange(1.5, 2.5)", &[], &[]),
    ("Ranges", "SELECT numrange(1.0, 10.0) @> 5.5", &[], &[]),
    ("Ranges", "SELECT int8range(1, 100)", &[], &[]),
    ("Ranges", "SELECT int8range(1, 100) && int8range(50, 200)", &[], &[]),
    (
        "Ranges",
        "SELECT tstzrange(NOW() - interval '1 hour', NOW())",
        &[],
        &[],
    ),
    ("Ranges", "SELECT lower_inc(int4range(1, 10))", &[], &[]),
    ("Ranges", "SELECT upper_inc(int4range(1, 10))", &[], &[]),
    ("Ranges", "SELECT lower_inf(int4range(NULL, 10))", &[], &[]),
    ("Ranges", "SELECT upper_inf(int4range(1, NULL))", &[], &[]),
    (
        "Ranges",
        "SELECT int4range(1,5) + int4range(3,8)",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4range(1,10) * int4range(5,15)",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4range(1,10) - int4range(5,15)",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4range(1,5) << int4range(7,10)",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4range(1,5) -|- int4range(5,10)",
        &[],
        &[],
    ),
    (
        "Ranges",
        "SELECT int4multirange(int4range(1,5)) @> 3",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT8RANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c NUMRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSTZRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c DATERANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT4MULTIRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c INT8MULTIRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c NUMMULTIRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSMULTIRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSTZMULTIRANGE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c DATEMULTIRANGE); DROP TABLE __t",
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
    (
        "Functions/Array",
        "SELECT cardinality(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_fill(0, ARRAY[3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_fill(0, ARRAY[2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT 2 = ANY(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT 5 > ALL(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_positions(ARRAY[1,2,1,3], 1)",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_dims(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT * FROM unnest(ARRAY['a','b'], ARRAY[1,2]) AS t(letter, num)",
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
    (
        "Functions/JSONB",
        "SELECT json_to_record('{\"a\":1,\"b\":\"foo\"}'::json) AS t(a int, b text)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_to_record('{\"a\":1,\"b\":\"foo\"}'::jsonb) AS t(a int, b text)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM json_to_recordset('[{\"a\":1},{\"a\":2}]'::json) AS t(a int)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT * FROM jsonb_to_recordset('[{\"a\":1},{\"a\":2}]'::jsonb) AS t(a int)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_query_array('{\"a\":[1,2,3]}'::jsonb, '$.a[*]')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":1}'::jsonb #- '{a}'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_extract_path('{\"a\":{\"b\":1}}'::jsonb, 'a', 'b')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_extract_path_text('{\"a\":{\"b\":\"x\"}}'::jsonb, 'a', 'b')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_populate_record(NULL::t, '{\"id\":1}'::jsonb) FROM t LIMIT 1",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
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
    ("Functions/String", "SELECT left('abcdef', 3)", &[], &[]),
    ("Functions/String", "SELECT right('abcdef', 3)", &[], &[]),
    ("Functions/String", "SELECT repeat('ab', 3)", &[], &[]),
    ("Functions/String", "SELECT position('c' IN 'abcdef')", &[], &[]),
    ("Functions/String", "SELECT strpos('abcdef', 'cd')", &[], &[]),
    ("Functions/String", "SELECT overlay('abcdef' PLACING 'xyz' FROM 2 FOR 3)", &[], &[]),
    ("Functions/String", "SELECT md5('abc')", &[], &[]),
    ("Functions/String", "SELECT sha256('abc'::bytea)", &[], &[]),
    ("Functions/String", "SELECT starts_with('abcdef', 'abc')", &[], &[]),
    ("Functions/String", "SELECT ends_with('abcdef', 'def')", &[], &[]),
    ("Functions/String", "SELECT SUBSTRING('abcdef' FROM '[a-c]+')", &[], &[]),
    ("Functions/String", "SELECT SUBSTRING('abc' FROM 2)", &[], &[]),
    ("Functions/String", "SELECT concat('a', 'b', 'c')", &[], &[]),
    ("Functions/String", "SELECT concat_ws(',', 'a', 'b', 'c')", &[], &[]),
    ("Functions/String", "SELECT to_hex(255)", &[], &[]),
    ("Functions/String", "SELECT lpad('x', 5)", &[], &[]),
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
    ("Functions/DateTime", "SELECT clock_timestamp()", &[], &[]),
    ("Functions/DateTime", "SELECT statement_timestamp()", &[], &[]),
    ("Functions/DateTime", "SELECT transaction_timestamp()", &[], &[]),
    ("Functions/DateTime", "SELECT localtime", &[], &[]),
    ("Functions/DateTime", "SELECT localtimestamp", &[], &[]),
    ("Functions/DateTime", "SELECT timeofday()", &[], &[]),
    (
        "Functions/DateTime",
        "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT TO_NUMBER('12345.67', '99999.99')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date_bin('1 hour'::interval, NOW(), '2000-01-01'::timestamptz)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT interval '1 year 2 months 3 days'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(DOW FROM NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(QUARTER FROM NOW())",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(WEEK FROM NOW())",
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
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER w1, AVG(id) OVER w2 FROM t WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY id ORDER BY id)",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT COUNT(*) OVER (PARTITION BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT SUM(id) FILTER (WHERE id > 0) OVER (ORDER BY id) FROM t",
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
    (
        "SELECT/Aggregate",
        "SELECT SUM(id) FILTER (WHERE id > 0), COUNT(*) FILTER (WHERE id < 0) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1), (-1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT json_object_agg(name, id) FROM t",
        &[
            "CREATE TABLE t (id INT NOT NULL, name TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT XMLAGG(XMLELEMENT(NAME foo, id)) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
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
    // Comparison-form ANY subqueries
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id > ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (5)",
            "INSERT INTO u VALUES (1), (2)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id < ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (5), (10)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id >= ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (3)",
            "INSERT INTO u VALUES (1), (3)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "PG/Operators",
        "SELECT * FROM t WHERE id <= ANY (SELECT id FROM u)",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (3)",
            "INSERT INTO u VALUES (3), (10)",
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
    (
        "SELECT/Joins",
        "SELECT t.id, sub.* FROM t, LATERAL jsonb_each('{\"a\":1}'::jsonb) sub",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t JOIN LATERAL (SELECT id * 2 AS dbl FROM u WHERE u.id = t.id) sub ON true",
        &[
            "CREATE TABLE t (id INT NOT NULL)",
            "CREATE TABLE u (id INT NOT NULL)",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO u VALUES (1)",
        ],
        &["DROP TABLE t", "DROP TABLE u"],
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
    (
        "Roles",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "GRANT USAGE ON SCHEMA public TO alice",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM alice",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO alice",
        &["CREATE ROLE alice"],
        &["DROP ROLE alice"],
    ),
    (
        "Roles",
        "CREATE ROLE mygrp NOLOGIN",
        &[],
        &["DROP ROLE mygrp"],
    ),
    (
        "Roles",
        "GRANT mygrp TO alice",
        &["CREATE ROLE mygrp NOLOGIN", "CREATE ROLE alice"],
        &["DROP ROLE alice", "DROP ROLE mygrp"],
    ),
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
    (
        "Schemas",
        "DROP SCHEMA IF EXISTS myschema",
        &[],
        &[],
    ),
    (
        "Schemas",
        "SELECT myschema.t.id FROM myschema.t",
        &[
            "CREATE SCHEMA myschema",
            "CREATE TABLE myschema.t (id INT NOT NULL)",
            "INSERT INTO myschema.t VALUES (1)",
        ],
        &["DROP SCHEMA myschema CASCADE"],
    ),
    (
        "Schemas",
        "ALTER TABLE myschema.t ADD COLUMN name TEXT",
        &[
            "CREATE SCHEMA myschema",
            "CREATE TABLE myschema.t (id INT NOT NULL)",
        ],
        &["DROP SCHEMA myschema CASCADE"],
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
    // System information functions
    ("Misc", "SELECT version()", &[], &[]),
    ("Misc", "SELECT current_schema()", &[], &[]),
    ("Misc", "SELECT current_database()", &[], &[]),
    ("Misc", "SELECT current_schemas(false)", &[], &[]),
    ("Misc", "SELECT current_setting('search_path')", &[], &[]),
    (
        "Misc",
        "SELECT set_config('search_path', 'public', false)",
        &[],
        &[],
    ),
    ("Misc", "SELECT pg_postmaster_start_time()", &[], &[]),
    ("Misc", "SELECT pg_backend_pid()", &[], &[]),
    (
        "Misc",
        "SELECT pg_is_in_recovery()",
        &[],
        &[],
    ),
    // Collation
    (
        "DDL/Other",
        "CREATE COLLATION my_collation (LOCALE = 'en-US')",
        &[],
        &["DROP COLLATION IF EXISTS my_collation"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t (name TEXT COLLATE \"C\")",
        &[],
        &["DROP TABLE t"],
    ),
    // CREATE TABLE AS (CTAS)
    (
        "DDL/Tables",
        "CREATE TABLE t2 AS SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t", "DROP TABLE t2"],
    ),
    (
        "DDL/Tables",
        "CREATE TABLE t2 AS SELECT * FROM t WITH NO DATA",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE t2"],
    ),
    // Default partition
    (
        "DDL/Tables",
        "CREATE TABLE t_default PARTITION OF t DEFAULT",
        &["CREATE TABLE t (id INT) PARTITION BY RANGE (id)"],
        &["DROP TABLE t"],
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
    ("Transactions", "BEGIN READ WRITE", &[], &[]),
    ("Transactions", "START TRANSACTION", &[], &[]),
    (
        "Transactions",
        "START TRANSACTION ISOLATION LEVEL READ COMMITTED",
        &[],
        &[],
    ),
    (
        "Transactions",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        &[],
        &[],
    ),
    ("Transactions", "ROLLBACK TO SAVEPOINT s", &[], &[]),
    ("Transactions", "RELEASE SAVEPOINT s", &[], &[]),
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
        "PREPARE stmt(INT) AS SELECT $1",
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
        "EXECUTE stmt(42)",
        &["PREPARE stmt(INT) AS SELECT $1"],
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
        "DEALLOCATE ALL",
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
        "DECLARE c SCROLL CURSOR FOR SELECT id FROM t ORDER BY id",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1), (2), (3)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "DECLARE c NO SCROLL CURSOR FOR SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "FETCH 1 FROM c",
        &["DECLARE c CURSOR FOR SELECT 1"],
        &[],
    ),
    (
        "Admin/Sessions",
        "FETCH ALL FROM c",
        &["DECLARE c CURSOR FOR SELECT 1"],
        &[],
    ),
    (
        "Admin/Sessions",
        "FETCH FORWARD 5 FROM c",
        &["DECLARE c CURSOR FOR SELECT 1"],
        &[],
    ),
    (
        "Admin/Sessions",
        "MOVE FORWARD 2 IN c",
        &["DECLARE c CURSOR FOR SELECT 1"],
        &[],
    ),
    (
        "Admin/Sessions",
        "CLOSE c",
        &["DECLARE c CURSOR FOR SELECT 1"],
        &[],
    ),
    (
        "Admin/Sessions",
        "CLOSE ALL",
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
    // ─────────────────────────────────────────────────────────────────────────
    // Phase-5.x expansion (2026-05-21): +200 fragments targeting gap surfaces
    // identified by auditing docs/sql-support.md and the limitations columns of
    // the CAPABILITIES.md, basin-cli-design.md, and HTAP guide.
    //
    // Coverage philosophy (ADR 0014): one fragment per syntactic shape, not
    // per data point. Setup teardown stays minimal — the engine uses a fresh
    // tempdir per row, so cleanup is for visual hygiene and to support the
    // markdown table rendering, not for crash safety.
    // ─────────────────────────────────────────────────────────────────────────

    // ── CTE — recursive / mutually-referential / data-modifying ──────────────
    (
        "SELECT/CTE",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<10) SELECT MAX(n) FROM r",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n+2 FROM r WHERE n<20) SELECT * FROM r",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE tree(id, parent) AS (SELECT 1, NULL::INT UNION ALL SELECT id+1, id FROM tree WHERE id<5) SELECT COUNT(*) FROM tree",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH a AS (SELECT 1 AS x), b AS (SELECT x+1 AS x FROM a), c AS (SELECT x*10 AS x FROM b) SELECT * FROM c",
        &[],
        &[],
    ),
    (
        "SELECT/CTE",
        "WITH ins AS (INSERT INTO t VALUES (99) RETURNING id) SELECT * FROM ins",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH del AS (DELETE FROM t WHERE id=1 RETURNING id) SELECT COUNT(*) FROM del",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH upd AS (UPDATE t SET id=id+100 WHERE id<5 RETURNING id) SELECT MAX(id) FROM upd",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH a AS MATERIALIZED (SELECT id FROM t) SELECT COUNT(*) FROM a",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH a AS NOT MATERIALIZED (SELECT id FROM t) SELECT COUNT(*) FROM a",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/CTE",
        "WITH RECURSIVE r(n,acc) AS (SELECT 1, 1::BIGINT UNION ALL SELECT n+1, acc*(n+1) FROM r WHERE n<10) SELECT acc FROM r ORDER BY n DESC LIMIT 1",
        &[],
        &[],
    ),

    // ── Window — frames, named windows, more aggregates ──────────────────────
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER w FROM t WINDOW w AS (ORDER BY id)",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, FIRST_VALUE(id) OVER (ORDER BY id), LAST_VALUE(id) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (10),(20),(30)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, CUME_DIST() OVER (ORDER BY id), PERCENT_RANK() OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, NTILE(4) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4),(5),(6),(7),(8)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, LAG(id, 2, -1) OVER (ORDER BY id), LEAD(id, 2, -1) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY id%2 ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) FILTER (WHERE id>1) OVER (ORDER BY id) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, SUM(id) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Window",
        "SELECT id, MIN(id) OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2),(3),(4),(5)"],
        &["DROP TABLE t"],
    ),

    // ── Joins — USING, NATURAL, FULL OUTER, complex multi-join ───────────────
    (
        "SELECT/Joins",
        "SELECT * FROM t FULL OUTER JOIN u ON t.id = u.id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t JOIN u USING (id)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t NATURAL JOIN u",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t NATURAL LEFT JOIN u",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t a JOIN t b ON a.id = b.id JOIN t c ON c.id = a.id",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t a JOIN u b ON a.id<b.id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t a LEFT JOIN u b ON a.id=b.id AND b.id<>5",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM (VALUES (1),(2)) AS s(x) JOIN t ON t.id=s.x",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t a JOIN t b ON a.id=b.id AND a.id IS NOT DISTINCT FROM b.id",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t CROSS JOIN u",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t, u WHERE t.id=u.id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),

    // ── Subqueries — scalar, ANY/ALL/SOME, EXISTS variants ───────────────────
    (
        "SELECT/Projection",
        "SELECT (SELECT MAX(id) FROM t) AS m",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE id = ANY (SELECT id FROM u)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE id = SOME (SELECT id FROM u)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE id < ALL (SELECT id FROM u)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id=t.id)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Projection",
        "SELECT (SELECT id FROM t LIMIT 1) AS first_id",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE (id, id+1) IN (SELECT id, id+1 FROM u)",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Projection",
        "SELECT id FROM t WHERE (id, id) = (1, 1)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Projection",
        "SELECT ROW(1, 2) = ROW(1, 2)",
        &[],
        &[],
    ),

    // ── DML — ON CONFLICT extras, RETURNING shapes, MERGE ────────────────────
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id + 1000",
        &["CREATE TABLE t (id INT PRIMARY KEY)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id WHERE t.id < 5",
        &["CREATE TABLE t (id INT PRIMARY KEY)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING",
        &["CREATE TABLE t (id INT PRIMARY KEY)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "INSERT INTO t (id) SELECT id FROM u",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)", "INSERT INTO u VALUES (1)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "INSERT INTO t (id) SELECT generate_series(1, 5)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "UPDATE t SET id = id*2 RETURNING id, id-1 AS prev",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1),(2)"],
        &["DROP TABLE t"],
    ),
    (
        "DML",
        "DELETE FROM t WHERE id IN (SELECT id FROM u) RETURNING id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)", "INSERT INTO u VALUES (1)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "UPDATE t SET id = u.id FROM u WHERE t.id = u.id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "DELETE FROM t USING u WHERE t.id = u.id",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "DML",
        "MERGE INTO t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
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
        "INSERT INTO t SELECT * FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "INSERT INTO t VALUES (1)"],
        &["DROP TABLE t"],
    ),

    // ── DateTime — more functions ────────────────────────────────────────────
    (
        "Functions/DateTime",
        "SELECT make_date(2026, 5, 21)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_time(12, 34, 56.789)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_timestamp(2026, 5, 21, 12, 34, 56.0)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_timestamptz(2026, 5, 21, 12, 34, 56.0, 'UTC')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT make_interval(years => 1, months => 2, days => 3)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT age(TIMESTAMP '2020-01-01', TIMESTAMP '2010-01-01')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date_bin(INTERVAL '15 minutes', TIMESTAMP '2026-05-21 12:37:00', TIMESTAMP '2026-05-21 12:00:00')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT to_date('2026-05-21', 'YYYY-MM-DD')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT to_timestamp('2026-05-21 12:34:56', 'YYYY-MM-DD HH24:MI:SS')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT to_timestamp(1716000000)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_days(INTERVAL '35 days')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_hours(INTERVAL '50 hours')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT justify_interval(INTERVAL '1 year 13 months 35 days')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT date_part('hour', TIMESTAMP '2026-05-21 12:34:56')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT TIMESTAMP '2026-05-21 12:00:00' + INTERVAL '2 hours 30 minutes'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT TIMESTAMP '2026-05-22' - TIMESTAMP '2026-05-21'",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT INTERVAL '1 day' * 7",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(ISODOW FROM TIMESTAMP '2026-05-21')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP '2026-05-21')",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT pg_sleep(0)",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT statement_timestamp()",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT transaction_timestamp()",
        &[],
        &[],
    ),
    (
        "Functions/DateTime",
        "SELECT clock_timestamp()",
        &[],
        &[],
    ),

    // ── String — regex, padding, formatting, encoding ────────────────────────
    (
        "Functions/String",
        "SELECT regexp_match('abc123', '([a-z]+)(\\d+)')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_matches('abc 123 def 456', '\\d+', 'g')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_replace('abc123', '\\d+', 'X', 'g')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_split_to_array('a,b,,c', ',')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT regexp_split_to_table('a b c', ' ')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT format('Hello %s, you are %s years old', 'world', 42)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT format('%I = %L', 'id', 'value')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT quote_ident('weird name')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT quote_literal('it''s')",
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
        "SELECT lpad('7', 4, '0')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT rpad('hi', 6, '.')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT split_part('a,b,c,d', ',', 3)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT string_to_array('a,b,c', ',')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT string_agg(c, ',') FROM (VALUES ('a'),('b'),('c')) s(c)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT string_agg(c, ',' ORDER BY c DESC) FROM (VALUES ('a'),('b'),('c')) s(c)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT btrim('  abc  ')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT translate('hello', 'el', 'ip')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT initcap('hello world')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT chr(65)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT ascii('A')",
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
        "SELECT octet_length('héllo')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT encode('\\xdeadbeef'::BYTEA, 'hex')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT decode('deadbeef', 'hex')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT convert_from('hello'::BYTEA, 'UTF8')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT POSITION('lo' IN 'hello world')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT OVERLAY('hello world' PLACING 'X' FROM 7 FOR 5)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT TRIM(BOTH 'x' FROM 'xxhelloxx')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT TRIM(LEADING '0' FROM '000123')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT SUBSTRING('hello' FROM 2 FOR 3)",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT SUBSTRING('hello world' FROM '\\w+')",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT 'abc' SIMILAR TO 'a.c'",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT 'abc' ~ '^a'",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT 'ABC' ~* '^a'",
        &[],
        &[],
    ),
    (
        "Functions/String",
        "SELECT 'abc' !~ '^z'",
        &[],
        &[],
    ),

    // ── JSONB — path operators, jsonpath, mutators ───────────────────────────
    (
        "Functions/JSONB",
        "SELECT '{\"a\":[1,2,3]}'::jsonb @? '$.a[*] ? (@ > 1)'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT '{\"a\":[1,2,3]}'::jsonb @@ '$.a[*] > 0'",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_query('{\"a\":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 1)')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_query_array('{\"a\":[1,2,3]}'::jsonb, '$.a[*]')",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_path_query_first('{\"a\":[1,2,3]}'::jsonb, '$.a[0]')",
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
        "SELECT jsonb_set('{\"a\":1}'::jsonb, '{a}', '99'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_set_lax('{\"a\":1}'::jsonb, '{a}', NULL, true, 'use_json_null')",
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
        "SELECT jsonb_strip_nulls('{\"a\":null,\"b\":1}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_pretty('{\"a\":1,\"b\":[2,3]}'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_object_agg(k, v) FROM (VALUES ('a',1),('b',2)) s(k,v)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_agg(x) FROM (VALUES (1),(2),(3)) s(x)",
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
        "SELECT '{\"a\":1}'::jsonb || '{\"b\":2}'::jsonb",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_array_length('[1,2,3,4]'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_typeof('null'::jsonb)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_to_record('{\"a\":1,\"b\":\"x\"}'::jsonb) AS r(a INT, b TEXT)",
        &[],
        &[],
    ),
    (
        "Functions/JSONB",
        "SELECT jsonb_populate_record(NULL::record, '{\"a\":1}'::jsonb)",
        &[],
        &[],
    ),

    // ── Array — slicing, unnest, set returning ───────────────────────────────
    (
        "Functions/Array",
        "SELECT ARRAY[[1,2],[3,4]]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT (ARRAY[[1,2],[3,4]])[2][1]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT unnest(ARRAY[1,2,3]) WITH ORDINALITY",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT unnest(ARRAY[1,2], ARRAY['a','b'])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_dims(ARRAY[[1,2],[3,4]])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT array_fill(0, ARRAY[3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2,3] && ARRAY[3,4,5]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2,3] @> ARRAY[2,3]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[2,3] <@ ARRAY[1,2,3]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT ARRAY[1,2] || ARRAY[3,4]",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT 5 = ANY(ARRAY[1,2,3,5])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT 5 > ALL(ARRAY[1,2,3])",
        &[],
        &[],
    ),
    (
        "Functions/Array",
        "SELECT cardinality(ARRAY[1,2,3,4])",
        &[],
        &[],
    ),

    // ── PG operators — bitwise, JSONB, string concat ─────────────────────────
    (
        "PG/Operators",
        "SELECT 5 & 3",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 5 | 3",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 5 # 3",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT ~5",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 1 << 4",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 16 >> 2",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 7 % 3",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 2 ^ 10",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT |/ 25",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT ||/ 27",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT @ -7",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 'foo' || 'bar'",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 'foo' || NULL",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 1 BETWEEN SYMMETRIC 3 AND 0",
        &[],
        &[],
    ),
    (
        "PG/Operators",
        "SELECT 1 IS NOT DISTINCT FROM NULL",
        &[],
        &[],
    ),

    // ── Types — temporal, network, monetary ──────────────────────────────────
    (
        "Types",
        "CREATE TABLE __t (c MONEY); DROP TABLE __t",
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
        "CREATE TABLE __t (c INET); DROP TABLE __t",
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
        "CREATE TABLE __t (c MACADDR8); DROP TABLE __t",
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
        "CREATE TABLE __t (c LINE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c LSEG); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BOX); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c CIRCLE); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c PATH); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c POLYGON); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BIT(8)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BIT VARYING(16)); DROP TABLE __t",
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
        "CREATE TABLE __t (c PG_LSN); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TSQUERY); DROP TABLE __t",
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
        "CREATE TABLE __t (c VARCHAR(255)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c DECIMAL(10,2)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TIME(3)); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c TIMETZ); DROP TABLE __t",
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
        "CREATE TABLE __t (c SERIAL); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c BIGSERIAL); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c SMALLSERIAL); DROP TABLE __t",
        &[],
        &[],
    ),
    (
        "Types",
        "CREATE TABLE __t (c OID); DROP TABLE __t",
        &[],
        &[],
    ),

    // ── Expressions — CASE, NULLIF, COALESCE, GREATEST/LEAST, casts ──────────
    (
        "Expressions",
        "SELECT CASE WHEN 1=1 THEN 'a' WHEN 2=2 THEN 'b' ELSE 'c' END",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE '?' END",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT GREATEST(1, 5, 3, NULL, 7)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT LEAST(1, 5, 3, NULL, 7)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT NULLIF(0, 0)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT COALESCE(NULL, NULL, 'x', 'y')",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT CAST(123 AS TEXT)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT 123::TEXT",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT '2026-05-21'::DATE",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT '12.34'::NUMERIC(10,2)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT '{\"a\":1}'::JSON",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT 'abc'::CHAR(5)",
        &[],
        &[],
    ),
    (
        "Expressions",
        "SELECT 1::BOOLEAN",
        &[],
        &[],
    ),

    // ── Aggregates — ordered-set, hypothetical-set, FILTER, DISTINCT ─────────
    (
        "SELECT/Aggregate",
        "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(4)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT percentile_disc(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(4)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT mode() WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(2),(3)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT rank(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT dense_rank(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT cume_dist(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT COUNT(*) FILTER (WHERE x > 1), SUM(x) FILTER (WHERE x > 2) FROM (VALUES (1),(2),(3)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT bool_and(b), bool_or(b) FROM (VALUES (true),(false),(true)) s(b)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT every(b) FROM (VALUES (true),(true)) s(b)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT bit_and(x), bit_or(x), bit_xor(x) FROM (VALUES (1),(2),(3)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT corr(x, y), covar_pop(x,y), covar_samp(x,y) FROM (VALUES (1,2),(3,4),(5,6)) s(x,y)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT stddev(x), stddev_pop(x), stddev_samp(x), variance(x), var_pop(x), var_samp(x) FROM (VALUES (1),(2),(3),(4)) s(x)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT regr_slope(y,x), regr_intercept(y,x), regr_r2(y,x) FROM (VALUES (1,2),(3,4),(5,6)) s(x,y)",
        &[],
        &[],
    ),
    (
        "SELECT/Aggregate",
        "SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((id),())",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, SUM(id) FROM t GROUP BY ROLLUP (id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT id, SUM(id) FROM t GROUP BY CUBE (id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "SELECT/Aggregate",
        "SELECT GROUPING(id), id, COUNT(*) FROM t GROUP BY ROLLUP (id)",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),

    // ── Set ops — INTERSECT/EXCEPT/ALL variants, parenthesised, ORDER BY ─────
    (
        "SELECT/SetOps",
        "(SELECT 1) UNION (SELECT 2) ORDER BY 1",
        &[],
        &[],
    ),
    (
        "SELECT/SetOps",
        "SELECT 1 UNION ALL SELECT 1",
        &[],
        &[],
    ),
    (
        "SELECT/SetOps",
        "SELECT 1 INTERSECT ALL SELECT 1",
        &[],
        &[],
    ),
    (
        "SELECT/SetOps",
        "SELECT 1 EXCEPT ALL SELECT 2",
        &[],
        &[],
    ),
    (
        "SELECT/SetOps",
        "(SELECT id FROM t UNION SELECT id FROM u) INTERSECT SELECT id FROM t",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/SetOps",
        "TABLE t UNION TABLE u",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/SetOps",
        "VALUES (1,'a'),(2,'b') ORDER BY 1",
        &[],
        &[],
    ),
    (
        "SELECT/SetOps",
        "SELECT * FROM (VALUES (1),(2),(3)) AS s(x)",
        &[],
        &[],
    ),

    // ── FullTextSearch — tsquery / tsvector ops, ranking ─────────────────────
    (
        "FullTextSearch",
        "SELECT to_tsvector('the quick brown fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT to_tsquery('quick & fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT plainto_tsquery('quick fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT phraseto_tsquery('quick brown fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT websearch_to_tsquery('\"quick fox\"')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT to_tsvector('a quick fox') @@ to_tsquery('quick & fox')",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_rank(to_tsvector('a quick fox'), to_tsquery('fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_rank_cd(to_tsvector('a quick fox'), to_tsquery('fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT ts_headline('a quick brown fox', to_tsquery('fox'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT length(to_tsvector('one two three'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT numnode(to_tsquery('a & b'))",
        &[],
        &[],
    ),
    (
        "FullTextSearch",
        "SELECT strip(to_tsvector('a quick brown fox'))",
        &[],
        &[],
    ),

    // ── Admin / Sessions — RESET, SHOW variants, SAVEPOINT ───────────────────
    (
        "Admin/Sessions",
        "RESET search_path",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "RESET ALL",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SHOW ALL",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SHOW TIMEZONE",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SET TIME ZONE 'UTC'",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SET LOCAL search_path = public",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DISCARD ALL",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DISCARD TEMP",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DISCARD PLANS",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DISCARD SEQUENCES",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "EXPLAIN (FORMAT JSON) SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "EXPLAIN (VERBOSE, FORMAT TEXT) SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "EXPLAIN (ANALYZE, BUFFERS) SELECT 1",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "VACUUM FULL t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "VACUUM (ANALYZE) t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "ANALYZE t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "REINDEX TABLE t",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),
    (
        "Admin/Sessions",
        "CHECKPOINT",
        &[],
        &[],
    ),
    (
        "Admin/Sessions",
        "DO $$ BEGIN PERFORM 1; END $$",
        &[],
        &[],
    ),

    // ── LATERAL extras — multiple LATERAL refs, generate_series, ROWS ────────
    (
        "SELECT/Joins",
        "SELECT * FROM generate_series(1, 5) g, LATERAL (SELECT g*2 AS dbl) sub",
        &[],
        &[],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t, LATERAL (SELECT max(u.id) FROM u WHERE u.id <= t.id) sub",
        &["CREATE TABLE t (id INT NOT NULL)", "CREATE TABLE u (id INT NOT NULL)"],
        &["DROP TABLE t", "DROP TABLE u"],
    ),
    (
        "SELECT/Joins",
        "SELECT * FROM t a, LATERAL (SELECT a.id+1 AS np) b, LATERAL (SELECT b.np*2 AS dd) c",
        &["CREATE TABLE t (id INT NOT NULL)"],
        &["DROP TABLE t"],
    ),

    // ── Misc — VALUES, generated SRFs, ROW constructors ──────────────────────
    (
        "Misc",
        "SELECT generate_series(1, 10, 2)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT generate_series('2026-01-01'::DATE, '2026-01-05'::DATE, INTERVAL '1 day')",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT generate_subscripts(ARRAY[10,20,30], 1)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT random()",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT setseed(0.5)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT version()",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT current_database()",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT current_schema()",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT current_schemas(true)",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_backend_pid()",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT pg_typeof('abc')",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT current_user",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT session_user",
        &[],
        &[],
    ),
    (
        "Misc",
        "SELECT user",
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

/// Run one pass of the matrix against a fresh engine+project for each row.
/// Returns `(outcomes, notes)` parallel to MATRIX indices.
async fn run_pass() -> Vec<(Outcome, String)> {
    let mut results = Vec::with_capacity(MATRIX.len());

    for (_, sql, setup, _teardown) in MATRIX.iter() {
        // Each row gets its own fresh project so prior failures don't bleed.
        let dir = TempDir::new().unwrap();
        let eng = engine_in(&dir);
        let project = ProjectId::new();

        let sess = match eng.open_session(project).await {
            Ok(s) => s,
            Err(e) => {
                results.push((Outcome::ExecFailed, short_note(&format!("{e}"))));
                continue;
            }
        };

        // Run setup statements — failures abort this row.
        // If the setup statement itself is rejected as out-of-scope (0A000 /
        // FeatureNotSupported), the whole row is a design exclusion (🚫),
        // not a runtime failure (🛠).
        let mut setup_ok = true;
        for setup_sql in setup.iter() {
            if let Err(e) = sess.execute(setup_sql).await {
                let note = short_note(&format!("setup failed: {e}"));
                let (outcome, _) = classify_error(&e);
                results.push((outcome, note));
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
[open an issue](https://github.com/vul-os/basin/issues/new?template=sql_compatibility.yml&title=Missing+SQL+syntax%3A+) \
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

#[test]
fn sql_support_matrix() {
    basin_integration_tests::big_stack::run(async {
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
    for (pass_idx, pass_label) in [
        (0usize, "Default"),
        (1usize, "+PG_QUERY"),
        (2usize, "+PG_QUERY+PG_PLAN"),
    ] {
        let ok = matrix_rows
            .iter()
            .filter(|r| r.outcomes[pass_idx] == Outcome::Ok)
            .count();
        let exec_fail = matrix_rows
            .iter()
            .filter(|r| r.outcomes[pass_idx] == Outcome::ExecFailed)
            .count();
        let plan_rej = matrix_rows
            .iter()
            .filter(|r| r.outcomes[pass_idx] == Outcome::PlannerRejected)
            .count();
        let parse_rej = matrix_rows
            .iter()
            .filter(|r| r.outcomes[pass_idx] == Outcome::ParserRejected)
            .count();
        let oos = matrix_rows
            .iter()
            .filter(|r| r.outcomes[pass_idx] == Outcome::OutOfScope)
            .count();
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
    let manifest_dir =
        std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| "tests/integration".to_string());
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
    // The matrix must have at least 900 rows (post 2026-05-21 expansion;
    // floor was 550 before +200 fragments landed). Guards against an
    // accidental fragment deletion regressing coverage.
    assert!(
        total >= 900,
        "expected at least 900 SQL fragments, got {total}"
    );
    // At least some rows must succeed in the default config.
    let default_ok = matrix_rows
        .iter()
        .filter(|r| r.outcomes[0] == Outcome::Ok)
        .count();
    assert!(
        default_ok >= 20,
        "expected at least 20 OK rows in default config, got {default_ok}"
    );
    });
}
