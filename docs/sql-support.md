# Basin SQL support — auto-generated matrix

Run `cargo test -p basin-integration-tests --test sql_support_matrix` to refresh.

Last run: 1781128594 (Unix epoch)
SQL fragments tested: 975 total / 2616 green (across all three configurations).

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

## Admin/Sessions

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `LISTEN ch` | ✅ | ✅ | ✅ |  |
| `NOTIFY ch, 'msg'` | ✅ | ✅ | ✅ |  |
| `UNLISTEN ch` | ✅ | ✅ | ✅ |  |
| `PREPARE stmt AS SELECT 1` | ✅ | ✅ | ✅ |  |
| `PREPARE stmt(INT) AS SELECT $1` | ✅ | ✅ | ✅ |  |
| `EXECUTE stmt` | ✅ | ✅ | ✅ |  |
| `EXECUTE stmt(42)` | ✅ | ✅ | ✅ |  |
| `DEALLOCATE stmt` | ✅ | ✅ | ✅ |  |
| `DEALLOCATE ALL` | ✅ | ✅ | ✅ |  |
| `DECLARE c CURSOR FOR SELECT 1` | ✅ | ✅ | ✅ |  |
| `DECLARE c SCROLL CURSOR FOR SELECT id FROM t ORDER BY id` | ✅ | ✅ | ✅ |  |
| `DECLARE c NO SCROLL CURSOR FOR SELECT 1` | ✅ | ✅ | ✅ |  |
| `FETCH 1 FROM c` | ✅ | ✅ | ✅ |  |
| `FETCH ALL FROM c` | ✅ | ✅ | ✅ |  |
| `FETCH FORWARD 5 FROM c` | ✅ | ✅ | ✅ |  |
| `MOVE FORWARD 2 IN c` | ✅ | ✅ | ✅ |  |
| `CLOSE c` | ✅ | ✅ | ✅ |  |
| `CLOSE ALL` | 🚫 | 🚫 | 🚫 | internal: CLOSE ALL is not supported in v0.1; close cursors individually |
| `LOCK TABLE t` | ✅ | ✅ | ✅ |  |
| `VACUUM` | ✅ | ✅ | ✅ |  |
| `ANALYZE` | ✅ | ✅ | ✅ |  |
| `CLUSTER t` | ✅ | ✅ | ✅ |  |
| `EXPLAIN SELECT 1` | ✅ | ✅ | ✅ |  |
| `EXPLAIN ANALYZE SELECT 1` | ✅ | ✅ | ✅ |  |
| `SET search_path = public` | ✅ | ✅ | ✅ |  |
| `SHOW search_path` | ✅ | ✅ | ✅ |  |
| `RESET search_path` | ✅ | ✅ | ✅ |  |
| `RESET ALL` | ✅ | ✅ | ✅ |  |
| `SHOW ALL` | ✅ | ✅ | ✅ |  |
| `SHOW TIMEZONE` | ✅ | ✅ | ✅ |  |
| `SET TIME ZONE 'UTC'` | ✅ | ✅ | ✅ |  |
| `SET LOCAL search_path = public` | ✅ | ✅ | ✅ |  |
| `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED` | ✅ | ✅ | ✅ |  |
| `DISCARD ALL` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DISCARD ALL |
| `DISCARD TEMP` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DISCARD TEMP |
| `DISCARD PLANS` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DISCARD PLANS |
| `DISCARD SEQUENCES` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DISCARD SEQUENCES |
| `EXPLAIN (FORMAT JSON) SELECT 1` | ✅ | ✅ | ✅ |  |
| `EXPLAIN (VERBOSE, FORMAT TEXT) SELECT 1` | ✅ | ✅ | ✅ |  |
| `EXPLAIN (ANALYZE, BUFFERS) SELECT 1` | ✅ | ✅ | ✅ |  |
| `VACUUM FULL t` | ✅ | ✅ | ✅ |  |
| `VACUUM (ANALYZE) t` | ✅ | ✅ | ✅ |  |
| `ANALYZE t` | ✅ | ✅ | ✅ |  |
| `REINDEX TABLE t` | ✅ | ✅ | ✅ |  |
| `CHECKPOINT` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an SQL statement, found: C… |
| `DO $$ BEGIN PERFORM 1; END $$` | 🚫 | 🚫 | 🚫 | feature not supported: DO is not supported (SQLSTATE 0A000) |

## DDL/Other

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE INDEX idx ON t(id)` | ✅ | ✅ | ✅ |  |
| `CREATE UNIQUE INDEX idx ON t(id)` | ✅ | ✅ | ✅ |  |
| `CREATE INDEX idx ON t(id) WHERE id > 0` | ✅ | ✅ | ✅ |  |
| `CREATE INDEX idx ON t(LOWER(name))` | ✅ | ✅ | ✅ |  |
| `CREATE INDEX idx ON t USING gin (name)` | ✅ | ✅ | ✅ |  |
| `DROP INDEX idx` | ✅ | ✅ | ✅ |  |
| `CREATE SCHEMA s` | ✅ | ✅ | ✅ |  |
| `DROP SCHEMA s` | ✅ | ✅ | ✅ |  |
| `CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0)` | ✅ | ✅ | ✅ |  |
| `DROP DOMAIN positive_int` | ✅ | ✅ | ✅ |  |
| `CREATE TYPE color AS ENUM ('red', 'green', 'blue')` | ✅ | ✅ | ✅ |  |
| `ALTER TYPE color ADD VALUE 'purple'` | ✅ | ✅ | ✅ |  |
| `DROP TYPE color` | ✅ | ✅ | ✅ |  |
| `CREATE SEQUENCE s START 100 INCREMENT 2` | ✅ | ✅ | ✅ |  |
| `DROP SEQUENCE s` | ✅ | ✅ | ✅ |  |
| `CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE sql AS $$ SELECT x*2 $$` | ✅ | ✅ | ✅ |  |
| `CREATE PROCEDURE p(x INT) LANGUAGE sql AS $$ INSERT INTO t VALUES (x) $$` | ✅ | ✅ | ✅ |  |
| `ALTER FUNCTION f(INT) RENAME TO g` | ✅ | ✅ | ✅ |  |
| `DROP FUNCTION f(INT)` | ✅ | ✅ | ✅ |  |
| `CREATE VIEW v AS SELECT * FROM t` | ✅ | ✅ | ✅ |  |
| `DROP VIEW v` | ✅ | ✅ | ✅ |  |
| `CREATE MATERIALIZED VIEW mv AS SELECT * FROM t` | ✅ | ✅ | ✅ |  |
| `REFRESH MATERIALIZED VIEW mv` | ✅ | ✅ | ✅ |  |
| `DROP MATERIALIZED VIEW mv` | ✅ | ✅ | ✅ |  |
| `CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn()` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE TRIGGER is not supported (SQLSTATE 0A000): Basi… |
| `CREATE POLICY p ON t USING (id = 1)` | ✅ | ✅ | ✅ |  |
| `DROP POLICY p ON t` | ✅ | ✅ | ✅ |  |
| `COMMENT ON TABLE t IS 'x'` | ✅ | ✅ | ✅ |  |
| `CREATE EXTENSION pgcrypto` | ✅ | ✅ | ✅ |  |
| `CREATE EXTENSION IF NOT EXISTS pgcrypto` | ✅ | ✅ | ✅ |  |
| `DROP EXTENSION IF EXISTS pgcrypto` | ✅ | ✅ | ✅ |  |
| `CREATE AGGREGATE myagg(INT) (SFUNC = int4pl, STYPE = INT)` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an object type after CREAT… |
| `CREATE OPERATOR + (LEFTARG = INT, RIGHTARG = INT, FUNCTION = int4pl)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: CREATE OPERATOR + (FUNCTION = int4pl, LEFTARG =… |
| `CREATE TYPE mycomposite AS (x INT, y INT)` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE TYPE … AS (composite) is out of scope for v0.… |
| `CREATE TYPE myenum AS ENUM ('a', 'b', 'c')` | ✅ | ✅ | ✅ |  |
| `CREATE RULE myrule AS ON INSERT TO t DO ALSO NOTHING` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an object type after CREAT… |
| `CREATE EVENT TRIGGER myevt ON ddl_command_start EXECUTE FUNCTION fn()` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an object type after CREAT… |
| `CREATE PUBLICATION mypub FOR ALL TABLES` | ✅ | ✅ | ✅ |  |
| `CREATE SUBSCRIPTION mysub CONNECTION 'host=localhost' PUBLICATION mypub` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an object type after CREAT… |
| `CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw` | ✅ | ✅ | ✅ |  |
| `CREATE FOREIGN TABLE ft (id INT) SERVER myserver` | ✅ | ✅ | ✅ |  |
| `ALTER VIEW v AS SELECT id FROM t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: ALTER VIEW v AS SELECT id FROM t |
| `CREATE OR REPLACE VIEW v AS SELECT id FROM t` | ✅ | ✅ | ✅ |  |
| `DROP TRIGGER trg ON t` | 🚫 | 🚫 | 🚫 | setup failed: feature not supported: CREATE TRIGGER is not supported (SQLSTAT… |
| `CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW WHEN (NEW.id <> OLD.id) EXECUTE FUNCTION fn()` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE TRIGGER is not supported (SQLSTATE 0A000): Basi… |
| `CREATE TRIGGER trg INSTEAD OF DELETE ON vv FOR EACH ROW EXECUTE FUNCTION fn()` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE TRIGGER is not supported (SQLSTATE 0A000): Basi… |
| `CREATE TRIGGER trg AFTER INSERT ON t REFERENCING NEW TABLE AS new_t FOR EACH STATEMENT EXECUTE FUNCTION fn()` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE TRIGGER is not supported (SQLSTATE 0A000): Basi… |
| `CREATE CONSTRAINT TRIGGER trg AFTER INSERT ON t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fn()` | 🚫 | 🚫 | 🚫 | feature not supported: CREATE CONSTRAINT TRIGGER is not supported (SQLSTATE 0… |
| `CREATE COLLATION my_collation (LOCALE = 'en-US')` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an object type after CREAT… |

## DDL/Tables

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE TABLE t (id INT, name TEXT)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE IF NOT EXISTS t (id INT)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT PRIMARY KEY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT NOT NULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT DEFAULT 0)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, name TEXT UNIQUE)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT REFERENCES u(id))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, CHECK (id > 0))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED ALWAYS AS (1+1) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (LIKE u INCLUDING ALL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t () INHERITS (u)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT) PARTITION BY RANGE (id)` | 📜 | 📜 | 📜 | invalid schema: PARTITION BY RANGE column id must be TIMESTAMPTZ or BIGINT-as… |
| `CREATE TEMPORARY TABLE t (id INT)` | ✅ | ✅ | ✅ |  |
| `CREATE UNLOGGED TABLE t (id INT)` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ADD COLUMN c TEXT` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t DROP COLUMN c` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t RENAME COLUMN c TO d` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t RENAME TO u` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ALTER COLUMN c TYPE BIGINT` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0)` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t DROP CONSTRAINT ck` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ENABLE ROW LEVEL SECURITY` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t DISABLE ROW LEVEL SECURITY` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t SET cold_after = '7d'` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t CLUSTER BY (id)` | ✅ | ✅ | ✅ |  |
| `DROP TABLE t` | ✅ | ✅ | ✅ |  |
| `DROP TABLE IF EXISTS t` | ✅ | ✅ | ✅ |  |
| `DROP TABLE t CASCADE` | ✅ | ✅ | ✅ |  |
| `TRUNCATE TABLE t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id SERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id BIGSERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id SMALLSERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) VIRTUAL)` | 🚫 | 🚫 | 🚫 | feature not supported: VIRTUAL generated columns deferred to v0.2; use STORED |
| `ALTER TABLE t ALTER COLUMN id SET GENERATED ALWAYS` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ALTER COLUMN id SET GENERATED BY DEFAULT` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ALTER COLUMN id DROP IDENTITY` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, EXCLUDE USING gist (id WITH =))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT REFERENCES u DEFERRABLE INITIALLY DEFERRED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES u(id) MATCH FULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (a INT REFERENCES u ON UPDATE CASCADE ON DELETE SET NULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT NOT NULL, name TEXT, UNIQUE (id, name) INCLUDE (name))` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t VALIDATE CONSTRAINT ck` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM ONLY t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t_2024 PARTITION OF t FOR VALUES FROM (2024) TO (2025)` | 📜 | 📜 | 📜 | setup failed: invalid schema: PARTITION BY RANGE column id must be TIMESTAMPT… |
| `CREATE TABLE t (region TEXT) PARTITION BY LIST (region)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT) PARTITION BY HASH (id)` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ATTACH PARTITION p FOR VALUES IN ('us')` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t DETACH PARTITION p` | 🚫 | 🚫 | 🚫 | setup failed: feature not supported: CREATE TABLE … PARTITION OF is not sup… |
| `CREATE TABLE t (name TEXT COLLATE "C")` | 📜 | 📜 | 📜 | invalid schema: unsupported column option in PoC: COLLATE "C" |
| `CREATE TABLE t2 AS SELECT * FROM t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t2 AS SELECT * FROM t WITH NO DATA` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t_default PARTITION OF t DEFAULT` | 📜 | 📜 | 📜 | setup failed: invalid schema: PARTITION BY RANGE column id must be TIMESTAMPT… |

## DML

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `INSERT INTO t VALUES (1)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t (id) VALUES (1)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1), (2), (3)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t SELECT id FROM u` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) RETURNING id` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = excluded.id` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t DEFAULT VALUES` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 1` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 1 WHERE id = 99` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = id + 1` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = (SELECT MAX(id) FROM u)` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 1 FROM u WHERE t.id = u.id` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 1 RETURNING id` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 1 WHERE id IN (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t WHERE id = 1` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t USING u WHERE t.id = u.id` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t RETURNING id` | ✅ | ✅ | ✅ |  |
| `MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET id = u.id WHEN NOT MATCHED THEN INSERT VALUES (u.id)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: MERGE INTO t USING u ON t.id = u.id WHEN MATCHE… |
| `COPY t FROM STDIN` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ;, found: EOF |
| `COPY t TO STDOUT` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY t TO STDOUT |
| `INSERT INTO t VALUES (1) ON CONFLICT (id) DO NOTHING` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE t.id > 0` | ✅ | ✅ | ✅ |  |
| `MERGE INTO t USING u ON t.id = u.id WHEN MATCHED AND u.id > 0 THEN DELETE WHEN NOT MATCHED THEN INSERT VALUES (u.id)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: MERGE INTO t USING u ON t.id = u.id WHEN MATCHE… |
| `DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = 99 WHERE id NOT IN (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (1)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t OVERRIDING USER VALUE VALUES (1)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id + 1000` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id WHERE t.id < 5` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING` | 🚫 | 🚫 | 🚫 | feature not supported: ON CONFLICT ON CONSTRAINT DO NOTHING is not yet suppor… |
| `INSERT INTO t (id) SELECT id FROM u` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t (id) SELECT generate_series(1, 5)` | 📜 | 📜 | 📜 | invalid schema: INSERT INTO t column "id": source type List(Field { data_type… |
| `UPDATE t SET id = id*2 RETURNING id, id-1 AS prev` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t WHERE id IN (SELECT id FROM u) RETURNING id` | ✅ | ✅ | ✅ |  |
| `UPDATE t SET id = u.id FROM u WHERE t.id = u.id` | 📜 | 📜 | 📜 | invalid schema: UPDATE … FROM requires the target table "t" to have a PRIMA… |
| `DELETE FROM t USING u WHERE t.id = u.id` | 📜 | 📜 | 📜 | invalid schema: DELETE … USING requires the target table "t" to have a PRIM… |
| `MERGE INTO t USING (SELECT 1 AS id) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: MERGE INTO t USING (SELECT 1 AS id) s ON t.id =… |
| `INSERT INTO t DEFAULT VALUES` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t SELECT * FROM t` | ✅ | ✅ | ✅ |  |

## Expressions

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT CASE WHEN 1=1 THEN 'a' ELSE 'b' END` | ✅ | ✅ | ✅ |  |
| `SELECT COALESCE(NULL, 'x')` | ✅ | ✅ | ✅ |  |
| `SELECT NULLIF(1, 1)` | ✅ | ✅ | ✅ |  |
| `SELECT GREATEST(1,2,3)` | ✅ | ✅ | ✅ |  |
| `SELECT LEAST(1,2,3)` | ✅ | ✅ | ✅ |  |
| `SELECT 1::TEXT` | ✅ | ✅ | ✅ |  |
| `SELECT CAST(1 AS TEXT)` | ✅ | ✅ | ✅ |  |
| `SELECT 'a' \|\| 'b'` | ✅ | ✅ | ✅ |  |
| `SELECT 'abc' LIKE 'a%'` | ✅ | ✅ | ✅ |  |
| `SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, CASE WHEN id < 0 THEN 'neg' WHEN id = 0 THEN 'zero' ELSE 'pos' END FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT 1::BIGINT + 2::BIGINT` | ✅ | ✅ | ✅ |  |
| `SELECT '2024-01-01'::DATE` | ✅ | ✅ | ✅ |  |
| `SELECT '12:00:00'::TIME` | ✅ | ✅ | ✅ |  |
| `SELECT '2024-01-01 12:00:00'::TIMESTAMP` | ✅ | ✅ | ✅ |  |
| `SELECT '00:01:00'::INTERVAL` | ✅ | ✅ | ✅ |  |
| `SELECT 'a6c5e8f0-1234-5678-abcd-000000000000'::UUID` | ✅ | ✅ | ✅ |  |
| `SELECT 'true'::BOOLEAN` | ✅ | ✅ | ✅ |  |
| `SELECT B'1010'` | ✅ | ✅ | ✅ |  |
| `SELECT X'FF'` | ✅ | ✅ | ✅ |  |
| `SELECT $1` | 🛠 | 🛠 | 🛠 | internal: execute: Execution error: Placeholder '$1' was not provided a value… |
| `SELECT true IS TRUE` | ✅ | ✅ | ✅ |  |
| `SELECT true IS NOT TRUE` | ✅ | ✅ | ✅ |  |
| `SELECT false IS FALSE` | ✅ | ✅ | ✅ |  |
| `SELECT false IS NOT FALSE` | ✅ | ✅ | ✅ |  |
| `SELECT NULL::bool IS UNKNOWN` | ✅ | ✅ | ✅ |  |
| `SELECT NULL::bool IS NOT UNKNOWN` | ✅ | ✅ | ✅ |  |
| `SELECT 1 IS DISTINCT FROM 2` | 🛠 | 🛠 | 🛠 | internal: execute: Optimizer rule 'optimize_projections' failed caused by Che… |
| `SELECT 1 IS NOT DISTINCT FROM 1` | 🛠 | 🛠 | 🛠 | internal: execute: Optimizer rule 'optimize_projections' failed caused by Che… |
| `SELECT ROW(1, NULL) IS NULL` | ✅ | ✅ | ✅ |  |
| `SELECT CASE WHEN 1=1 THEN 'a' WHEN 2=2 THEN 'b' ELSE 'c' END` | ✅ | ✅ | ✅ |  |
| `SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE '?' END` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: )… |
| `SELECT GREATEST(1, 5, 3, NULL, 7)` | ✅ | ✅ | ✅ |  |
| `SELECT LEAST(1, 5, 3, NULL, 7)` | ✅ | ✅ | ✅ |  |
| `SELECT NULLIF(0, 0)` | ✅ | ✅ | ✅ |  |
| `SELECT COALESCE(NULL, NULL, 'x', 'y')` | ✅ | ✅ | ✅ |  |
| `SELECT CAST(123 AS TEXT)` | ✅ | ✅ | ✅ |  |
| `SELECT 123::TEXT` | ✅ | ✅ | ✅ |  |
| `SELECT '2026-05-21'::DATE` | ✅ | ✅ | ✅ |  |
| `SELECT '12.34'::NUMERIC(10,2)` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::JSON` | ✅ | ✅ | ✅ |  |
| `SELECT 'abc'::CHAR(5)` | ✅ | ✅ | ✅ |  |
| `SELECT 1::BOOLEAN` | ✅ | ✅ | ✅ |  |

## FullTextSearch

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT to_tsvector('english', 'a quick brown fox')` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsquery('english', 'quick & fox')` | ✅ | ✅ | ✅ |  |
| `SELECT 'a quick brown fox'::tsvector @@ to_tsquery('english', 'fox')` | ✅ | ✅ | ✅ |  |
| `SELECT plainto_tsquery('english', 'quick fox')` | ✅ | ✅ | ✅ |  |
| `SELECT phraseto_tsquery('english', 'quick fox')` | ✅ | ✅ | ✅ |  |
| `SELECT websearch_to_tsquery('english', 'quick OR fox')` | ✅ | ✅ | ✅ |  |
| `SELECT ts_rank(to_tsvector('a quick'), to_tsquery('quick'))` | ✅ | ✅ | ✅ |  |
| `SELECT ts_headline('a quick fox', to_tsquery('quick'))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE doc (body TEXT, ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE INDEX ON doc USING gin (ts)` | ✅ | ✅ | ✅ |  |
| `SELECT tsvector_to_array(to_tsvector('a quick fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT tsquery_phrase(to_tsquery('quick'), to_tsquery('fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsvector('a') @@ to_tsquery('b')` | ✅ | ✅ | ✅ |  |
| `SELECT ts_rank_cd(to_tsvector('a quick fox'), to_tsquery('quick'))` | ✅ | ✅ | ✅ |  |
| `SELECT numnode(to_tsquery('quick & fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT querytree(to_tsquery('quick & fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT strip(to_tsvector('a quick fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE ts @@ to_tsquery('english', 'quick')` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsvector('the quick brown fox')` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsquery('quick & fox')` | ✅ | ✅ | ✅ |  |
| `SELECT plainto_tsquery('quick fox')` | ✅ | ✅ | ✅ |  |
| `SELECT phraseto_tsquery('quick brown fox')` | ✅ | ✅ | ✅ |  |
| `SELECT websearch_to_tsquery('"quick fox"')` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsvector('a quick fox') @@ to_tsquery('quick & fox')` | ✅ | ✅ | ✅ |  |
| `SELECT ts_rank(to_tsvector('a quick fox'), to_tsquery('fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT ts_rank_cd(to_tsvector('a quick fox'), to_tsquery('fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT ts_headline('a quick brown fox', to_tsquery('fox'))` | ✅ | ✅ | ✅ |  |
| `SELECT length(to_tsvector('one two three'))` | ✅ | ✅ | ✅ |  |
| `SELECT numnode(to_tsquery('a & b'))` | ✅ | ✅ | ✅ |  |
| `SELECT strip(to_tsvector('a quick brown fox'))` | ✅ | ✅ | ✅ |  |

## Functions/Array

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT '{1,2}'::int[] && '{2,3}'::int[]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT '{1,2,3}'::int[]` | ✅ | ✅ | ✅ |  |
| `SELECT '{{1,2},{3,4}}'::int[][]` | ✅ | ✅ | ✅ |  |
| `SELECT (ARRAY[1,2,3])[2]` | ✅ | ✅ | ✅ |  |
| `SELECT (ARRAY[1,2,3,4,5])[2:4]` | ✅ | ✅ | ✅ |  |
| `SELECT array_length(ARRAY[1,2,3], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT array_ndims(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT array_lower(ARRAY[1,2,3], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT array_upper(ARRAY[1,2,3], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT array_position(ARRAY[1,2,3], 2)` | ✅ | ✅ | ✅ |  |
| `SELECT array_remove(ARRAY[1,2,3,2], 2)` | ✅ | ✅ | ✅ |  |
| `SELECT array_replace(ARRAY[1,2,3], 2, 99)` | ✅ | ✅ | ✅ |  |
| `SELECT array_append(ARRAY[1,2], 3)` | ✅ | ✅ | ✅ |  |
| `SELECT array_prepend(0, ARRAY[1,2])` | ✅ | ✅ | ✅ |  |
| `SELECT array_cat(ARRAY[1,2], ARRAY[3,4])` | ✅ | ✅ | ✅ |  |
| `SELECT array_to_string(ARRAY['a','b','c'], ',', '*')` | ✅ | ✅ | ✅ |  |
| `SELECT string_to_array('a,b,c', ',')` | ✅ | ✅ | ✅ |  |
| `SELECT unnest(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM unnest(ARRAY[1,2,3]) WITH ORDINALITY` | 🚫 | 🚫 | 🚫 | internal: plan: This feature is not implemented: UNNEST with ordinality is no… |
| `SELECT generate_subscripts(ARRAY[10,20,30], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] @> ARRAY[1]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] \|\| ARRAY[3,4]` | ✅ | ✅ | ✅ |  |
| `SELECT cardinality(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT array_fill(0, ARRAY[3])` | ✅ | ✅ | ✅ |  |
| `SELECT array_fill(0, ARRAY[2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT 2 = ANY(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT 5 > ALL(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT array_positions(ARRAY[1,2,1,3], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT array_dims(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM unnest(ARRAY['a','b'], ARRAY[1,2]) AS t(letter, num)` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[[1,2],[3,4]]` | ✅ | ✅ | ✅ |  |
| `SELECT (ARRAY[[1,2],[3,4]])[2][1]` | ✅ | ✅ | ✅ |  |
| `SELECT unnest(ARRAY[1,2,3]) WITH ORDINALITY` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: W… |
| `SELECT unnest(ARRAY[1,2], ARRAY['a','b'])` | 📜 | 📜 | 📜 | internal: plan: Error during planning: unnest() requires exactly one argument |
| `SELECT array_dims(ARRAY[[1,2],[3,4]])` | ✅ | ✅ | ✅ |  |
| `SELECT array_fill(0, ARRAY[3])` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2,3] && ARRAY[3,4,5]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2,3] @> ARRAY[2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[2,3] <@ ARRAY[1,2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] \|\| ARRAY[3,4]` | ✅ | ✅ | ✅ |  |
| `SELECT 5 = ANY(ARRAY[1,2,3,5])` | ✅ | ✅ | ✅ |  |
| `SELECT 5 > ALL(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT cardinality(ARRAY[1,2,3,4])` | ✅ | ✅ | ✅ |  |

## Functions/Crypto

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT GEN_RANDOM_UUID()` | ✅ | ✅ | ✅ |  |
| `SELECT DIGEST('a','sha256')` | ✅ | ✅ | ✅ |  |
| `SELECT ENCODE('a','hex')` | ✅ | ✅ | ✅ |  |
| `SELECT DECODE('61','hex')` | ✅ | ✅ | ✅ |  |

## Functions/DateTime

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT NOW()` | ✅ | ✅ | ✅ |  |
| `SELECT CURRENT_TIMESTAMP` | ✅ | ✅ | ✅ |  |
| `SELECT CURRENT_DATE` | ✅ | ✅ | ✅ |  |
| `SELECT DATE_TRUNC('hour', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT AGE(NOW(), NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(YEAR FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT TO_CHAR(NOW(),'YYYY')` | ✅ | ✅ | ✅ |  |
| `SELECT TO_TIMESTAMP('2024-01-01','YYYY-MM-DD')` | ✅ | ✅ | ✅ |  |
| `SELECT make_date(2024, 1, 15)` | ✅ | ✅ | ✅ |  |
| `SELECT make_time(12, 30, 45.5)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamp(2024, 1, 15, 12, 30, 45.5)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamptz(2024, 1, 15, 12, 30, 45.5, 'UTC')` | ✅ | ✅ | ✅ |  |
| `SELECT make_interval(years => 1, days => 30)` | ✅ | ✅ | ✅ |  |
| `SELECT date_part('year', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(EPOCH FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT date '2024-01-01' + interval '1 day'` | ✅ | ✅ | ✅ |  |
| `SELECT date '2024-12-31' - date '2024-01-01'` | ✅ | ✅ | ✅ |  |
| `SELECT NOW() AT TIME ZONE 'America/New_York'` | ✅ | ✅ | ✅ |  |
| `SELECT (NOW(), NOW() + interval '1h') OVERLAPS (NOW() + interval '30m', NOW() + interval '90m')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_hours(interval '36 hours')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_days(interval '40 days')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_interval(interval '1 mon -1 hour')` | ✅ | ✅ | ✅ |  |
| `SELECT isfinite(NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT isfinite(date '2024-01-01')` | ✅ | ✅ | ✅ |  |
| `SELECT 'infinity'::timestamp` | ✅ | ✅ | ✅ |  |
| `SELECT '-infinity'::timestamp` | ✅ | ✅ | ✅ |  |
| `SELECT clock_timestamp()` | ✅ | ✅ | ✅ |  |
| `SELECT statement_timestamp()` | ✅ | ✅ | ✅ |  |
| `SELECT transaction_timestamp()` | ✅ | ✅ | ✅ |  |
| `SELECT localtime` | ✅ | ✅ | ✅ |  |
| `SELECT localtimestamp` | ✅ | ✅ | ✅ |  |
| `SELECT timeofday()` | ✅ | ✅ | ✅ |  |
| `SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD')` | ✅ | ✅ | ✅ |  |
| `SELECT TO_NUMBER('12345.67', '99999.99')` | ✅ | ✅ | ✅ |  |
| `SELECT date_bin('1 hour'::interval, NOW(), '2000-01-01'::timestamptz)` | ✅ | ✅ | ✅ |  |
| `SELECT interval '1 year 2 months 3 days'` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(DOW FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(QUARTER FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(WEEK FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT make_date(2026, 5, 21)` | ✅ | ✅ | ✅ |  |
| `SELECT make_time(12, 34, 56.789)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamp(2026, 5, 21, 12, 34, 56.0)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamptz(2026, 5, 21, 12, 34, 56.0, 'UTC')` | ✅ | ✅ | ✅ |  |
| `SELECT make_interval(years => 1, months => 2, days => 3)` | ✅ | ✅ | ✅ |  |
| `SELECT age(TIMESTAMP '2020-01-01', TIMESTAMP '2010-01-01')` | ✅ | ✅ | ✅ |  |
| `SELECT date_bin(INTERVAL '15 minutes', TIMESTAMP '2026-05-21 12:37:00', TIMESTAMP '2026-05-21 12:00:00')` | ✅ | ✅ | ✅ |  |
| `SELECT to_char(NOW(), 'YYYY-MM-DD HH24:MI:SS')` | ✅ | ✅ | ✅ |  |
| `SELECT to_date('2026-05-21', 'YYYY-MM-DD')` | ✅ | ✅ | ✅ |  |
| `SELECT to_timestamp('2026-05-21 12:34:56', 'YYYY-MM-DD HH24:MI:SS')` | ✅ | ✅ | ✅ |  |
| `SELECT to_timestamp(1716000000)` | ❌ | ❌ | ❌ | internal: plan: Error during planning: The function 'to_timestamp' expected 2… |
| `SELECT justify_days(INTERVAL '35 days')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_hours(INTERVAL '50 hours')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_interval(INTERVAL '1 year 13 months 35 days')` | ✅ | ✅ | ✅ |  |
| `SELECT date_part('hour', TIMESTAMP '2026-05-21 12:34:56')` | ✅ | ✅ | ✅ |  |
| `SELECT TIMESTAMP '2026-05-21 12:00:00' + INTERVAL '2 hours 30 minutes'` | ✅ | ✅ | ✅ |  |
| `SELECT TIMESTAMP '2026-05-22' - TIMESTAMP '2026-05-21'` | ✅ | ✅ | ✅ |  |
| `SELECT INTERVAL '1 day' * 7` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Cannot get result type for temporal op… |
| `SELECT EXTRACT(ISODOW FROM TIMESTAMP '2026-05-21')` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP '2026-05-21')` | 🚫 | 🚫 | 🚫 | internal: execute: Execution error: Date part 'MILLENNIUM' not supported |
| `SELECT pg_sleep(0)` | ✅ | ✅ | ✅ |  |
| `SELECT statement_timestamp()` | ✅ | ✅ | ✅ |  |
| `SELECT transaction_timestamp()` | ✅ | ✅ | ✅ |  |
| `SELECT clock_timestamp()` | ✅ | ✅ | ✅ |  |

## Functions/JSONB

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT '{"a":1}'::jsonb -> 'a'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb ->> 'a'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb #> '{a}'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb @> '{"a":1}'` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '2'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,1}', '99'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_strip_nulls('{"a":1,"b":null}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_query('{"a":1}'::jsonb, '$.a')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_exists('{"a":1}'::jsonb, '$.a')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_match('{"a":1}'::jsonb, '$.a == 1')` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":{"b":1}}'::jsonb @? '$.a.b'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb @@ '$.a == 1'` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_typeof('1'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_pretty('{"a":1}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_array_length('[1,2,3]'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_object_keys('{"a":1,"b":2}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM jsonb_each('{"a":1}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM jsonb_each_text('{"a":1}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM jsonb_array_elements_text('["a","b"]'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_build_object('a', 1, 'b', 2)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_build_array(1, 'a', true)` | ✅ | ✅ | ✅ |  |
| `SELECT to_jsonb(ROW(1, 'a'))` | ✅ | ✅ | ✅ |  |
| `SELECT to_json(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT row_to_json(t) FROM (SELECT 1 AS a) t` | ✅ | ✅ | ✅ |  |
| `SELECT array_to_json(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_agg(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_object_agg(name, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1,"b":2}'::jsonb - 'a'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1,"b":2}'::jsonb - ARRAY['a','b']` | ✅ | ✅ | ✅ |  |
| `SELECT '[1,2,3]'::jsonb - 1` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb \|\| '{"b":2}'::jsonb` | ✅ | ✅ | ✅ |  |
| `SELECT json_to_record('{"a":1,"b":"foo"}'::json) AS t(a int, b text)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_to_record('{"a":1,"b":"foo"}'::jsonb) AS t(a int, b text)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM json_to_recordset('[{"a":1},{"a":2}]'::json) AS t(a int)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM jsonb_to_recordset('[{"a":1},{"a":2}]'::jsonb) AS t(a int)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_query_array('{"a":[1,2,3]}'::jsonb, '$.a[*]')` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb #- '{a}'` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_extract_path('{"a":{"b":1}}'::jsonb, 'a', 'b')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_extract_path_text('{"a":{"b":"x"}}'::jsonb, 'a', 'b')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_populate_record(NULL::t, '{"id":1}'::jsonb) FROM t LIMIT 1` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type t |
| `SELECT '{"a":[1,2,3]}'::jsonb @? '$.a[*] ? (@ > 1)'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":[1,2,3]}'::jsonb @@ '$.a[*] > 0'` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_query('{"a":[1,2,3]}'::jsonb, '$.a[*] ? (@ > 1)')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_query_array('{"a":[1,2,3]}'::jsonb, '$.a[*]')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_query_first('{"a":[1,2,3]}'::jsonb, '$.a[0]')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_exists('{"a":1}'::jsonb, '$.a')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_path_match('{"a":1}'::jsonb, '$.a == 1')` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '99'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_set_lax('{"a":1}'::jsonb, '{a}', NULL, true, 'use_json_null')` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'jsonb_set_lax'. Did … |
| `SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,1}', '99'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_strip_nulls('{"a":null,"b":1}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_pretty('{"a":1,"b":[2,3]}'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_object_agg(k, v) FROM (VALUES ('a',1),('b',2)) s(k,v)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_agg(x) FROM (VALUES (1),(2),(3)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT '[1,2,3]'::jsonb - 1` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1,"b":2}'::jsonb - 'a'` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1,"b":2}'::jsonb - ARRAY['a','b']` | ✅ | ✅ | ✅ |  |
| `SELECT '{"a":1}'::jsonb \|\| '{"b":2}'::jsonb` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_array_length('[1,2,3,4]'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_typeof('null'::jsonb)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_to_record('{"a":1,"b":"x"}'::jsonb) AS r(a INT, b TEXT)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_populate_record(NULL::record, '{"a":1}'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type record |

## Functions/Math

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT ABS(-1)` | ✅ | ✅ | ✅ |  |
| `SELECT CEIL(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT FLOOR(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT ROUND(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT ROUND(3.14159, 2)` | ✅ | ✅ | ✅ |  |
| `SELECT TRUNC(3.7)` | ✅ | ✅ | ✅ |  |
| `SELECT TRUNC(3.7, 1)` | ✅ | ✅ | ✅ |  |
| `SELECT POWER(2,10)` | ✅ | ✅ | ✅ |  |
| `SELECT SQRT(4)` | ✅ | ✅ | ✅ |  |
| `SELECT MOD(10,3)` | ✅ | ✅ | ✅ |  |
| `SELECT LOG(100)` | ✅ | ✅ | ✅ |  |
| `SELECT LOG(10, 100)` | ✅ | ✅ | ✅ |  |
| `SELECT LN(2.718281828::float8)` | ✅ | ✅ | ✅ |  |
| `SELECT EXP(1.0::float8)` | ✅ | ✅ | ✅ |  |
| `SELECT PI()` | ✅ | ✅ | ✅ |  |
| `SELECT SIGN(-5)` | ✅ | ✅ | ✅ |  |
| `SELECT RANDOM()` | ✅ | ✅ | ✅ |  |
| `SELECT DIV(10, 3)` | ✅ | ✅ | ✅ |  |
| `SELECT FACTORIAL(5)` | ✅ | ✅ | ✅ |  |
| `SELECT GCD(12, 8)` | ✅ | ✅ | ✅ |  |
| `SELECT LCM(4, 6)` | ✅ | ✅ | ✅ |  |
| `SELECT DEGREES(3.14159)` | ✅ | ✅ | ✅ |  |
| `SELECT RADIANS(180.0)` | ✅ | ✅ | ✅ |  |
| `SELECT SIN(0.0)` | ✅ | ✅ | ✅ |  |
| `SELECT COS(0.0)` | ✅ | ✅ | ✅ |  |
| `SELECT TAN(0.0)` | ✅ | ✅ | ✅ |  |

## Functions/String

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT LOWER('A')` | ✅ | ✅ | ✅ |  |
| `SELECT UPPER('a')` | ✅ | ✅ | ✅ |  |
| `SELECT SUBSTRING('abc' FROM 1 FOR 2)` | ✅ | ✅ | ✅ |  |
| `SELECT LENGTH('abc')` | ✅ | ✅ | ✅ |  |
| `SELECT REPLACE('abc','a','z')` | ✅ | ✅ | ✅ |  |
| `SELECT TRIM(' a ')` | ✅ | ✅ | ✅ |  |
| `SELECT LPAD('x',3,'0')` | ✅ | ✅ | ✅ |  |
| `SELECT RPAD('x',3,'0')` | ✅ | ✅ | ✅ |  |
| `SELECT REGEXP_REPLACE('a1','[0-9]','')` | ✅ | ✅ | ✅ |  |
| `SELECT initcap('hello world')` | ✅ | ✅ | ✅ |  |
| `SELECT split_part('a,b,c', ',', 2)` | ✅ | ✅ | ✅ |  |
| `SELECT reverse('abc')` | ✅ | ✅ | ✅ |  |
| `SELECT left('abcdef', 3)` | ✅ | ✅ | ✅ |  |
| `SELECT right('abcdef', 3)` | ✅ | ✅ | ✅ |  |
| `SELECT repeat('ab', 3)` | ✅ | ✅ | ✅ |  |
| `SELECT position('c' IN 'abcdef')` | ✅ | ✅ | ✅ |  |
| `SELECT strpos('abcdef', 'cd')` | ✅ | ✅ | ✅ |  |
| `SELECT overlay('abcdef' PLACING 'xyz' FROM 2 FOR 3)` | ✅ | ✅ | ✅ |  |
| `SELECT md5('abc')` | ✅ | ✅ | ✅ |  |
| `SELECT sha256('abc'::bytea)` | ✅ | ✅ | ✅ |  |
| `SELECT starts_with('abcdef', 'abc')` | ✅ | ✅ | ✅ |  |
| `SELECT ends_with('abcdef', 'def')` | ✅ | ✅ | ✅ |  |
| `SELECT SUBSTRING('abcdef' FROM '[a-c]+')` | ✅ | ✅ | ✅ |  |
| `SELECT SUBSTRING('abc' FROM 2)` | ✅ | ✅ | ✅ |  |
| `SELECT concat('a', 'b', 'c')` | ✅ | ✅ | ✅ |  |
| `SELECT concat_ws(',', 'a', 'b', 'c')` | ✅ | ✅ | ✅ |  |
| `SELECT to_hex(255)` | ✅ | ✅ | ✅ |  |
| `SELECT lpad('x', 5)` | ✅ | ✅ | ✅ |  |
| `SELECT format('Hello, %s', 'world')` | ✅ | ✅ | ✅ |  |
| `SELECT format('%I.%s', 'schema', 'tab')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_ident('table name')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_literal('abc')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_nullable(NULL)` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_match('abc123', '([a-z]+)([0-9]+)')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_matches('abc123 def456', '[a-z]+\d+', 'g')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_split_to_array('a,b,c', ',')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_split_to_table('a,b,c', ',')` | ✅ | ✅ | ✅ |  |
| `SELECT chr(65)` | ✅ | ✅ | ✅ |  |
| `SELECT ascii('A')` | ✅ | ✅ | ✅ |  |
| `SELECT char_length('hello')` | ✅ | ✅ | ✅ |  |
| `SELECT bit_length('hello')` | ✅ | ✅ | ✅ |  |
| `SELECT octet_length('hello')` | ✅ | ✅ | ✅ |  |
| `SELECT encode(E'\x12'::bytea, 'base64')` | ✅ | ✅ | ✅ |  |
| `SELECT decode('EgA=', 'base64')` | ✅ | ✅ | ✅ |  |
| `SELECT translate('12abc', 'abc', 'xyz')` | ✅ | ✅ | ✅ |  |
| `SELECT btrim('xxabcxx', 'x')` | ✅ | ✅ | ✅ |  |
| `SELECT ltrim('xxabc', 'x')` | ✅ | ✅ | ✅ |  |
| `SELECT rtrim('abcxx', 'x')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_match('abc123', '([a-z]+)(\d+)')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_matches('abc 123 def 456', '\d+', 'g')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_replace('abc123', '\d+', 'X', 'g')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_split_to_array('a,b,,c', ',')` | ✅ | ✅ | ✅ |  |
| `SELECT regexp_split_to_table('a b c', ' ')` | ✅ | ✅ | ✅ |  |
| `SELECT format('Hello %s, you are %s years old', 'world', 42)` | ✅ | ✅ | ✅ |  |
| `SELECT format('%I = %L', 'id', 'value')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_ident('weird name')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_literal('it''s')` | ✅ | ✅ | ✅ |  |
| `SELECT quote_nullable(NULL)` | ✅ | ✅ | ✅ |  |
| `SELECT lpad('7', 4, '0')` | ✅ | ✅ | ✅ |  |
| `SELECT rpad('hi', 6, '.')` | ✅ | ✅ | ✅ |  |
| `SELECT split_part('a,b,c,d', ',', 3)` | ✅ | ✅ | ✅ |  |
| `SELECT string_to_array('a,b,c', ',')` | ✅ | ✅ | ✅ |  |
| `SELECT string_agg(c, ',') FROM (VALUES ('a'),('b'),('c')) s(c)` | ✅ | ✅ | ✅ |  |
| `SELECT string_agg(c, ',' ORDER BY c DESC) FROM (VALUES ('a'),('b'),('c')) s(c)` | ✅ | ✅ | ✅ |  |
| `SELECT btrim('  abc  ')` | ✅ | ✅ | ✅ |  |
| `SELECT translate('hello', 'el', 'ip')` | ✅ | ✅ | ✅ |  |
| `SELECT initcap('hello world')` | ✅ | ✅ | ✅ |  |
| `SELECT chr(65)` | ✅ | ✅ | ✅ |  |
| `SELECT ascii('A')` | ✅ | ✅ | ✅ |  |
| `SELECT bit_length('hello')` | ✅ | ✅ | ✅ |  |
| `SELECT octet_length('héllo')` | ✅ | ✅ | ✅ |  |
| `SELECT encode('\xdeadbeef'::BYTEA, 'hex')` | ✅ | ✅ | ✅ |  |
| `SELECT decode('deadbeef', 'hex')` | ✅ | ✅ | ✅ |  |
| `SELECT convert_from('hello'::BYTEA, 'UTF8')` | ✅ | ✅ | ✅ |  |
| `SELECT POSITION('lo' IN 'hello world')` | ✅ | ✅ | ✅ |  |
| `SELECT OVERLAY('hello world' PLACING 'X' FROM 7 FOR 5)` | ✅ | ✅ | ✅ |  |
| `SELECT TRIM(BOTH 'x' FROM 'xxhelloxx')` | ✅ | ✅ | ✅ |  |
| `SELECT TRIM(LEADING '0' FROM '000123')` | ✅ | ✅ | ✅ |  |
| `SELECT SUBSTRING('hello' FROM 2 FOR 3)` | ✅ | ✅ | ✅ |  |
| `SELECT SUBSTRING('hello world' FROM '\w+')` | ✅ | ✅ | ✅ |  |
| `SELECT 'abc' SIMILAR TO 'a.c'` | ✅ | ✅ | ✅ |  |
| `SELECT 'abc' ~ '^a'` | ✅ | ✅ | ✅ |  |
| `SELECT 'ABC' ~* '^a'` | ✅ | ✅ | ✅ |  |
| `SELECT 'abc' !~ '^z'` | ✅ | ✅ | ✅ |  |

## Misc

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `TABLE t` | ✅ | ✅ | ✅ |  |
| `VALUES (1,2), (3,4)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM (VALUES (1,'a'), (2,'b')) AS v(id, name)` | ✅ | ✅ | ✅ |  |
| `SELECT * INTO new_t FROM t` | ✅ | ✅ | ✅ |  |
| `COPY t FROM '/tmp/x' WITH (FORMAT csv, HEADER, DELIMITER ',')` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY t FROM '/tmp/x' (FORMAT csv, HEADER, DELIM… |
| `COPY (SELECT * FROM t) TO '/tmp/x' WITH (FORMAT csv)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY (SELECT * FROM t) TO '/tmp/x' (FORMAT csv) |
| `DO $$ BEGIN RAISE NOTICE 'hi'; END; $$ LANGUAGE plpgsql` | 🚫 | 🚫 | 🚫 | feature not supported: DO is not supported (SQLSTATE 0A000) |
| `COMMENT ON COLUMN t.id IS 'pk'` | ✅ | ✅ | ✅ |  |
| `COMMENT ON FUNCTION f(int) IS 'x'` | ✅ | ✅ | ✅ |  |
| `SELECT pg_advisory_lock(1)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_advisory_unlock(1)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_try_advisory_lock(1)` | ✅ | ✅ | ✅ |  |
| `UNLISTEN *` | ✅ | ✅ | ✅ |  |
| `SELECT pg_typeof(1)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_size_pretty(1024::bigint)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_column_size('hello')` | ✅ | ✅ | ✅ |  |
| `SELECT version()` | ✅ | ✅ | ✅ |  |
| `SELECT current_schema()` | ✅ | ✅ | ✅ |  |
| `SELECT current_database()` | ✅ | ✅ | ✅ |  |
| `SELECT current_schemas(false)` | ✅ | ✅ | ✅ |  |
| `SELECT current_setting('search_path')` | ✅ | ✅ | ✅ |  |
| `SELECT set_config('search_path', 'public', false)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_postmaster_start_time()` | ✅ | ✅ | ✅ |  |
| `SELECT pg_backend_pid()` | ✅ | ✅ | ✅ |  |
| `SELECT pg_is_in_recovery()` | ✅ | ✅ | ✅ |  |
| `SELECT generate_series(1, 10, 2)` | ✅ | ✅ | ✅ |  |
| `SELECT generate_series('2026-01-01'::DATE, '2026-01-05'::DATE, INTERVAL '1 day')` | ✅ | ✅ | ✅ |  |
| `SELECT generate_subscripts(ARRAY[10,20,30], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT random()` | ✅ | ✅ | ✅ |  |
| `SELECT setseed(0.5)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'setseed'. Did you me… |
| `SELECT version()` | ✅ | ✅ | ✅ |  |
| `SELECT current_database()` | ✅ | ✅ | ✅ |  |
| `SELECT current_schema()` | ✅ | ✅ | ✅ |  |
| `SELECT current_schemas(true)` | ✅ | ✅ | ✅ |  |
| `SELECT pg_backend_pid()` | ✅ | ✅ | ✅ |  |
| `SELECT pg_typeof('abc')` | ✅ | ✅ | ✅ |  |
| `SELECT current_user` | ✅ | ✅ | ✅ |  |
| `SELECT session_user` | ✅ | ✅ | ✅ |  |
| `SELECT user` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named user. |

## PG/Operators

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT 1 IS DISTINCT FROM 2` | 🛠 | 🛠 | 🛠 | internal: execute: Optimizer rule 'optimize_projections' failed caused by Che… |
| `SELECT 1 IS NOT DISTINCT FROM 1` | 🛠 | 🛠 | 🛠 | internal: execute: Optimizer rule 'optimize_projections' failed caused by Che… |
| `SELECT * FROM t WHERE id IS NULL` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id IS NOT NULL` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id BETWEEN 1 AND 10` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id NOT BETWEEN 1 AND 10` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id BETWEEN SYMMETRIC 10 AND 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id NOT BETWEEN SYMMETRIC 10 AND 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name ~ '^a'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name !~ '^z'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name ~* '^A'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name !~* '^Z'` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] \|\| ARRAY[3,4]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2,3] @> ARRAY[1,2]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY[1,2] && ARRAY[2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id = ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id > ALL (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id = SOME (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id > ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id < ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id >= ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id <= ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT 5 & 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 \| 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 # 3` | ✅ | ✅ | ✅ |  |
| `SELECT ~5` | ✅ | ✅ | ✅ |  |
| `SELECT 1 << 3` | ✅ | ✅ | ✅ |  |
| `SELECT 8 >> 2` | ✅ | ✅ | ✅ |  |
| `SELECT (NOW(), NOW() + INTERVAL '1 hour') OVERLAPS (NOW() + INTERVAL '30 minutes', NOW() + INTERVAL '90 minutes')` | ✅ | ✅ | ✅ |  |
| `SELECT 5 & 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 \| 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 # 3` | ✅ | ✅ | ✅ |  |
| `SELECT ~5` | ✅ | ✅ | ✅ |  |
| `SELECT 1 << 4` | ✅ | ✅ | ✅ |  |
| `SELECT 16 >> 2` | ✅ | ✅ | ✅ |  |
| `SELECT 7 % 3` | ✅ | ✅ | ✅ |  |
| `SELECT 2 ^ 10` | ✅ | ✅ | ✅ |  |
| `SELECT \|/ 25` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected: an expression, found: // at… |
| `SELECT \|\|/ 27` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected: an expression, found: /// a… |
| `SELECT @ -7` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected: placeholder, found:   at Li… |
| `SELECT 'foo' \|\| 'bar'` | ✅ | ✅ | ✅ |  |
| `SELECT 'foo' \|\| NULL` | ✅ | ✅ | ✅ |  |
| `SELECT 1 BETWEEN SYMMETRIC 3 AND 0` | ✅ | ✅ | ✅ |  |
| `SELECT 1 IS NOT DISTINCT FROM NULL` | 🛠 | 🛠 | 🛠 | internal: execute: Optimizer rule 'optimize_projections' failed caused by Che… |

## Ranges

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT int4range(1, 10)` | ✅ | ✅ | ✅ |  |
| `SELECT '[1,10)'::int4range` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,10) @> 5` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,10) && int4range(5,15)` | ✅ | ✅ | ✅ |  |
| `SELECT lower(int4range(1,10))` | ✅ | ✅ | ✅ |  |
| `SELECT upper(int4range(1,10))` | ✅ | ✅ | ✅ |  |
| `SELECT isempty(int4range(1,1))` | ✅ | ✅ | ✅ |  |
| `SELECT '[2020-01-01,2020-12-31]'::daterange` | ✅ | ✅ | ✅ |  |
| `SELECT tsrange(NOW() - interval '1 hour', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT int4multirange(int4range(1,5), int4range(10,15))` | ✅ | ✅ | ✅ |  |
| `SELECT numrange(1.5, 2.5)` | ✅ | ✅ | ✅ |  |
| `SELECT numrange(1.0, 10.0) @> 5.5` | ✅ | ✅ | ✅ |  |
| `SELECT int8range(1, 100)` | ✅ | ✅ | ✅ |  |
| `SELECT int8range(1, 100) && int8range(50, 200)` | ✅ | ✅ | ✅ |  |
| `SELECT tstzrange(NOW() - interval '1 hour', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT lower_inc(int4range(1, 10))` | ✅ | ✅ | ✅ |  |
| `SELECT upper_inc(int4range(1, 10))` | ✅ | ✅ | ✅ |  |
| `SELECT lower_inf(int4range(NULL, 10))` | ✅ | ✅ | ✅ |  |
| `SELECT upper_inf(int4range(1, NULL))` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,5) + int4range(3,8)` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,10) * int4range(5,15)` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,10) - int4range(5,15)` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,5) << int4range(7,10)` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,5) -\|- int4range(5,10)` | ✅ | ✅ | ✅ |  |
| `SELECT int4multirange(int4range(1,5)) @> 3` | ✅ | ✅ | ✅ |  |

## Roles

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE ROLE alice` | ✅ | ✅ | ✅ |  |
| `CREATE ROLE alice WITH LOGIN PASSWORD 'pw'` | ✅ | ✅ | ✅ |  |
| `ALTER ROLE alice WITH SUPERUSER` | ✅ | ✅ | ✅ |  |
| `DROP ROLE alice` | ✅ | ✅ | ✅ |  |
| `GRANT SELECT ON t TO alice` | ✅ | ✅ | ✅ |  |
| `GRANT ALL PRIVILEGES ON t TO alice` | ✅ | ✅ | ✅ |  |
| `REVOKE INSERT ON t FROM alice` | ✅ | ✅ | ✅ |  |
| `SET ROLE alice` | ✅ | ✅ | ✅ |  |
| `RESET ROLE` | ✅ | ✅ | ✅ |  |
| `SELECT current_user` | ✅ | ✅ | ✅ |  |
| `SELECT session_user` | ✅ | ✅ | ✅ |  |
| `GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice` | ✅ | ✅ | ✅ |  |
| `GRANT USAGE ON SCHEMA public TO alice` | ✅ | ✅ | ✅ |  |
| `REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM alice` | ✅ | ✅ | ✅ |  |
| `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO alice` | ✅ | ✅ | ✅ |  |
| `CREATE ROLE mygrp NOLOGIN` | ✅ | ✅ | ✅ |  |
| `GRANT mygrp TO alice` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: a privilege keyword, found… |

## SELECT/Aggregate

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT COUNT(*) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id), AVG(id), MIN(id), MAX(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(DISTINCT id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(*) FILTER (WHERE id > 0) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT STRING_AGG(name, ',') FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT ARRAY_AGG(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT JSON_AGG(t) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FROM t GROUP BY id` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FROM t GROUP BY id HAVING SUM(id) > 0` | ✅ | ✅ | ✅ |  |
| `SELECT id, GROUPING(id) FROM t GROUP BY ROLLUP (id)` | ✅ | ✅ | ✅ |  |
| `SELECT id, name FROM t GROUP BY CUBE (id, name)` | ✅ | ✅ | ✅ |  |
| `SELECT id, name FROM t GROUP BY GROUPING SETS ((id), (name))` | ✅ | ✅ | ✅ |  |
| `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT percentile_disc(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT mode() WITHIN GROUP (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT stddev(id), stddev_pop(id), stddev_samp(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT variance(id), var_pop(id), var_samp(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT corr(id, id), covar_pop(id, id), covar_samp(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_slope(id, id), regr_intercept(id, id), regr_r2(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_avgx(id, id), regr_avgy(id, id), regr_count(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_sxx(id, id), regr_syy(id, id), regr_sxy(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT bool_and(id > 0), bool_or(id > 0), every(id > 0) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT bit_and(id), bit_or(id), bit_xor(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT array_agg(id ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT array_agg(DISTINCT id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT string_agg(name, ',' ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) FILTER (WHERE id > 0), COUNT(*) FILTER (WHERE id < 0) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT json_object_agg(name, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT XMLAGG(XMLELEMENT(NAME foo, id)) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: foo at Line: 1, … |
| `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(4)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT percentile_disc(ARRAY[0.25,0.5,0.75]) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(4)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT mode() WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(2),(3)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT rank(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'rank'. Did you mean … |
| `SELECT dense_rank(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'dense_rank'. Did you… |
| `SELECT cume_dist(5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1),(2),(3),(7)) s(x)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'cume_dist'. Did you … |
| `SELECT COUNT(*) FILTER (WHERE x > 1), SUM(x) FILTER (WHERE x > 2) FROM (VALUES (1),(2),(3)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT bool_and(b), bool_or(b) FROM (VALUES (true),(false),(true)) s(b)` | ✅ | ✅ | ✅ |  |
| `SELECT every(b) FROM (VALUES (true),(true)) s(b)` | ✅ | ✅ | ✅ |  |
| `SELECT bit_and(x), bit_or(x), bit_xor(x) FROM (VALUES (1),(2),(3)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT corr(x, y), covar_pop(x,y), covar_samp(x,y) FROM (VALUES (1,2),(3,4),(5,6)) s(x,y)` | ✅ | ✅ | ✅ |  |
| `SELECT stddev(x), stddev_pop(x), stddev_samp(x), variance(x), var_pop(x), var_samp(x) FROM (VALUES (1),(2),(3),(4)) s(x)` | ✅ | ✅ | ✅ |  |
| `SELECT regr_slope(y,x), regr_intercept(y,x), regr_r2(y,x) FROM (VALUES (1,2),(3,4),(5,6)) s(x,y)` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((id),())` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FROM t GROUP BY ROLLUP (id)` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FROM t GROUP BY CUBE (id)` | ✅ | ✅ | ✅ |  |
| `SELECT GROUPING(id), id, COUNT(*) FROM t GROUP BY ROLLUP (id)` | ✅ | ✅ | ✅ |  |

## SELECT/CTE

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `WITH cte AS (SELECT 1 AS x) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH cte AS (SELECT * FROM t) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r` | ✅ | ✅ | ✅ |  |
| `WITH ins AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM ins` | ✅ | ✅ | ✅ |  |
| `WITH upd AS (UPDATE t SET id = 99 WHERE id = 1 RETURNING id) SELECT * FROM upd` | ✅ | ✅ | ✅ |  |
| `WITH del AS (DELETE FROM t WHERE id = 1 RETURNING id) SELECT * FROM del` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE fib(a, b) AS (SELECT 1, 1 UNION ALL SELECT b, a+b FROM fib WHERE b < 100) SELECT a FROM fib` | ✅ | ✅ | ✅ |  |
| `WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<10) SELECT MAX(n) FROM r` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r(n) AS (VALUES (1) UNION ALL SELECT n+2 FROM r WHERE n<20) SELECT * FROM r` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named n. Valid fields are r.column1. |
| `WITH RECURSIVE tree(id, parent) AS (SELECT 1, NULL::INT UNION ALL SELECT id+1, id FROM tree WHERE id<5) SELECT COUNT(*) FROM tree` | ✅ | ✅ | ✅ |  |
| `WITH a AS (SELECT 1 AS x), b AS (SELECT x+1 AS x FROM a), c AS (SELECT x*10 AS x FROM b) SELECT * FROM c` | ✅ | ✅ | ✅ |  |
| `WITH ins AS (INSERT INTO t VALUES (99) RETURNING id) SELECT * FROM ins` | ✅ | ✅ | ✅ |  |
| `WITH del AS (DELETE FROM t WHERE id=1 RETURNING id) SELECT COUNT(*) FROM del` | ✅ | ✅ | ✅ |  |
| `WITH upd AS (UPDATE t SET id=id+100 WHERE id<5 RETURNING id) SELECT MAX(id) FROM upd` | ✅ | ✅ | ✅ |  |
| `WITH a AS MATERIALIZED (SELECT id FROM t) SELECT COUNT(*) FROM a` | ✅ | ✅ | ✅ |  |
| `WITH a AS NOT MATERIALIZED (SELECT id FROM t) SELECT COUNT(*) FROM a` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r(n,acc) AS (SELECT 1, 1::BIGINT UNION ALL SELECT n+1, acc*(n+1) FROM r WHERE n<10) SELECT acc FROM r ORDER BY n DESC LIMIT 1` | ✅ | ✅ | ✅ |  |

## SELECT/Joins

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT * FROM t INNER JOIN u ON t.id = u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t LEFT JOIN u ON t.id = u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t RIGHT JOIN u ON t.id = u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FULL JOIN u ON t.id = u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t CROSS JOIN u` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t NATURAL JOIN u` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t JOIN u USING (id)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t, LATERAL (SELECT id FROM u WHERE u.id = t.id) sub` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM (SELECT 1 AS x) sub` | ✅ | ✅ | ✅ |  |
| `SELECT (SELECT MAX(id) FROM u) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id = ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id > ALL (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id = t.id)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id NOT IN (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT t.id, (SELECT COUNT(*) FROM u WHERE u.id = t.id) AS cnt FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t LEFT JOIN LATERAL (SELECT id FROM u WHERE u.id = t.id) sub ON true` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t, LATERAL unnest(ARRAY[1,2,3]) tag` | ✅ | ✅ | ✅ |  |
| `SELECT t.id, sub.* FROM t, LATERAL jsonb_each('{"a":1}'::jsonb) sub` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t JOIN LATERAL (SELECT id * 2 AS dbl FROM u WHERE u.id = t.id) sub ON true` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FULL OUTER JOIN u ON t.id = u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t JOIN u USING (id)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t NATURAL JOIN u` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t NATURAL LEFT JOIN u` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t a JOIN t b ON a.id = b.id JOIN t c ON c.id = a.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t a JOIN u b ON a.id<b.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t a LEFT JOIN u b ON a.id=b.id AND b.id<>5` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM (VALUES (1),(2)) AS s(x) JOIN t ON t.id=s.x` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t a JOIN t b ON a.id=b.id AND a.id IS NOT DISTINCT FROM b.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t CROSS JOIN u` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t, u WHERE t.id=u.id` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM generate_series(1, 5) g, LATERAL (SELECT g*2 AS dbl) sub` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named g. |
| `SELECT * FROM t, LATERAL (SELECT max(u.id) FROM u WHERE u.id <= t.id) sub` | 🛠 | 🛠 | 🛠 | internal: execute: This feature is not implemented: Physical plan does not su… |
| `SELECT * FROM t a, LATERAL (SELECT a.id+1 AS np) b, LATERAL (SELECT b.np*2 AS dd) c` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named b.np. |

## SELECT/Locking

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT * FROM t FOR UPDATE` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR UPDATE OF t` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR NO KEY UPDATE` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR SHARE` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR KEY SHARE` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR UPDATE NOWAIT` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t FOR UPDATE SKIP LOCKED` | ✅ | ✅ | ✅ |  |

## SELECT/Projection

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, name FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT t.id FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id AS x FROM t` | 🛠 | 🛠 | 🛠 | internal: fast_select: computed expr on non-numeric column id (Int32); use Da… |
| `SELECT DISTINCT id FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT DISTINCT ON (id) id, name FROM t ORDER BY id, name` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id = 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id IN (1,2,3)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id IS NULL` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id IS NOT NULL` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id IS DISTINCT FROM 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id BETWEEN 1 AND 10` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name LIKE 'a%'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name ILIKE 'A%'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name SIMILAR TO 'a%'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name ~ '^a'` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE name ~* '^A'` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t ORDER BY id ASC NULLS FIRST` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t ORDER BY id DESC NULLS LAST` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t FETCH FIRST 3 ROWS ONLY` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t FETCH FIRST 3 ROWS WITH TIES` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t ORDER BY 1` | ✅ | ✅ | ✅ |  |
| `SELECT (SELECT MAX(id) FROM t) AS m` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t WHERE id = ANY (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t WHERE id = SOME (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t WHERE id < ALL (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.id=t.id)` | ✅ | ✅ | ✅ |  |
| `SELECT (SELECT id FROM t LIMIT 1) AS first_id` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t WHERE (id, id+1) IN (SELECT id, id+1 FROM u)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Too many columns! The subquery should … |
| `SELECT id FROM t WHERE (id, id) = (1, 1)` | ✅ | ✅ | ✅ |  |
| `SELECT ROW(1, 2) = ROW(1, 2)` | ✅ | ✅ | ✅ |  |

## SELECT/SetOps

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT 1 UNION SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 UNION ALL SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 INTERSECT SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 INTERSECT ALL SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 EXCEPT SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 EXCEPT ALL SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t UNION SELECT id FROM u ORDER BY id` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t INTERSECT SELECT id FROM u` | ✅ | ✅ | ✅ |  |
| `SELECT id FROM t EXCEPT SELECT id FROM u` | ✅ | ✅ | ✅ |  |
| `(SELECT 1) UNION (SELECT 2) ORDER BY 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 UNION ALL SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 INTERSECT ALL SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 EXCEPT ALL SELECT 2` | ✅ | ✅ | ✅ |  |
| `(SELECT id FROM t UNION SELECT id FROM u) INTERSECT SELECT id FROM t` | ✅ | ✅ | ✅ |  |
| `TABLE t UNION TABLE u` | 🛠 | 🛠 | 🛠 | internal: plan: This feature is not implemented: Query TABLE u not implemente… |
| `VALUES (1,'a'),(2,'b') ORDER BY 1` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM (VALUES (1),(2),(3)) AS s(x)` | ✅ | ✅ | ✅ |  |

## SELECT/Window

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT id, SUM(id) OVER () FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, RANK() OVER (PARTITION BY id ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, LAG(id) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT ROW_NUMBER() OVER () FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT DENSE_RANK() OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT PERCENT_RANK() OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT CUME_DIST() OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT NTILE(4) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT LAG(id, 1, 0) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT LEAD(id) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT FIRST_VALUE(id) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT LAST_VALUE(id) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT NTH_VALUE(id, 3) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (RANGE BETWEEN INTERVAL '1 day' PRECEDING AND CURRENT ROW) FROM t` | 📜 | 📜 | 📜 | internal: plan: Error during planning: RANGE requires exactly one ORDER BY co… |
| `SELECT SUM(id) OVER (GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t` | 📜 | 📜 | 📜 | internal: plan: Error during planning: GROUPS requires an ORDER BY clause |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: EXCLUDE at Line:… |
| `SELECT id, SUM(id) OVER w FROM t WINDOW w AS (PARTITION BY id)` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: EXCLUDE at Line:… |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: EXCLUDE at Line:… |
| `SELECT SUM(id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: EXCLUDE at Line:… |
| `SELECT id, SUM(id) OVER w1, AVG(id) OVER w2 FROM t WINDOW w1 AS (ORDER BY id), w2 AS (PARTITION BY id ORDER BY id)` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(*) OVER (PARTITION BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id) FILTER (WHERE id > 0) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, AVG(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) OVER w FROM t WINDOW w AS (ORDER BY id)` | ✅ | ✅ | ✅ |  |
| `SELECT id, FIRST_VALUE(id) OVER (ORDER BY id), LAST_VALUE(id) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, NTH_VALUE(id, 2) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, CUME_DIST() OVER (ORDER BY id), PERCENT_RANK() OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, NTILE(4) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, LAG(id, 2, -1) OVER (ORDER BY id), LEAD(id, 2, -1) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, ROW_NUMBER() OVER (PARTITION BY id%2 ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FILTER (WHERE id>1) OVER (ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT id, MIN(id) OVER (ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) FROM t` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ), found: EXCLUDE at Line:… |

## Schemas

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE SCHEMA myschema` | ✅ | ✅ | ✅ |  |
| `CREATE SCHEMA AUTHORIZATION alice` | ✅ | ✅ | ✅ |  |
| `SET search_path = myschema, public` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE myschema.t (id INT)` | ✅ | ✅ | ✅ |  |
| `DROP SCHEMA myschema CASCADE` | ✅ | ✅ | ✅ |  |
| `DROP SCHEMA IF EXISTS myschema` | ✅ | ✅ | ✅ |  |
| `SELECT myschema.t.id FROM myschema.t` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE myschema.t ADD COLUMN name TEXT` | ✅ | ✅ | ✅ |  |

## Transactions

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `BEGIN` | ✅ | ✅ | ✅ |  |
| `COMMIT` | ✅ | ✅ | ✅ |  |
| `ROLLBACK` | ✅ | ✅ | ✅ |  |
| `SAVEPOINT s` | 📜 | 📜 | 📜 | invalid schema: SAVEPOINT can only be used in transaction blocks (SQLSTATE 25… |
| `RELEASE SAVEPOINT s` | 📜 | 📜 | 📜 | invalid schema: RELEASE SAVEPOINT can only be used in transaction blocks (SQL… |
| `ROLLBACK TO s` | 📜 | 📜 | 📜 | invalid schema: ROLLBACK TO SAVEPOINT can only be used in transaction blocks … |
| `BEGIN ISOLATION LEVEL SERIALIZABLE` | ✅ | ✅ | ✅ |  |
| `BEGIN READ ONLY` | ✅ | ✅ | ✅ |  |
| `BEGIN READ WRITE` | ✅ | ✅ | ✅ |  |
| `START TRANSACTION` | ✅ | ✅ | ✅ |  |
| `START TRANSACTION ISOLATION LEVEL READ COMMITTED` | ✅ | ✅ | ✅ |  |
| `START TRANSACTION ISOLATION LEVEL REPEATABLE READ` | ✅ | ✅ | ✅ |  |
| `ROLLBACK TO SAVEPOINT s` | 📜 | 📜 | 📜 | invalid schema: ROLLBACK TO SAVEPOINT can only be used in transaction blocks … |
| `RELEASE SAVEPOINT s` | 📜 | 📜 | 📜 | invalid schema: RELEASE SAVEPOINT can only be used in transaction blocks (SQL… |

## Types

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE TABLE __t (c SMALLINT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BIGINT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c REAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c DOUBLE PRECISION); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c NUMERIC); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c NUMERIC(10,2)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TEXT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c VARCHAR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c VARCHAR(255)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c CHAR(10)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c CITEXT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BOOLEAN); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c DATE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TIME); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TIMESTAMP); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TIMESTAMPTZ); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INTERVAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c UUID); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c JSON); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c JSONB); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BYTEA); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INT[]); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TEXT[]); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INET); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c CIDR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c MACADDR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c MONEY); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c XML); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TSVECTOR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c POINT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INT4RANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c VECTOR(3)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BIT(8)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BIT VARYING(8)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c OID); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: OID |
| `CREATE TABLE __t (c REGCLASS); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: REGCLASS |
| `CREATE TABLE __t (c REGTYPE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: REGTYPE |
| `CREATE TABLE __t (c TSQUERY); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c PG_LSN); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: PG_LSN |
| `CREATE TABLE __t (c INT8RANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c NUMRANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TSRANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TSTZRANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c DATERANGE); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INT4MULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: INT4MULTIRANGE |
| `CREATE TABLE __t (c INT8MULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: INT8MULTIRANGE |
| `CREATE TABLE __t (c NUMMULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: NUMMULTIRANGE |
| `CREATE TABLE __t (c TSMULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: TSMULTIRANGE |
| `CREATE TABLE __t (c TSTZMULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: TSTZMULTIRANGE |
| `CREATE TABLE __t (c DATEMULTIRANGE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: DATEMULTIRANGE |
| `CREATE TABLE __t (c MONEY); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c CIDR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c INET); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c MACADDR); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c MACADDR8); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c POINT); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c LINE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: LINE |
| `CREATE TABLE __t (c LSEG); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: LSEG |
| `CREATE TABLE __t (c BOX); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: BOX |
| `CREATE TABLE __t (c CIRCLE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: CIRCLE |
| `CREATE TABLE __t (c PATH); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: PATH |
| `CREATE TABLE __t (c POLYGON); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: POLYGON |
| `CREATE TABLE __t (c BIT(8)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BIT VARYING(16)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c XML); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c PG_LSN); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: PG_LSN |
| `CREATE TABLE __t (c TSQUERY); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c CHAR(10)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c VARCHAR(255)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c DECIMAL(10,2)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TIME(3)); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c TIMETZ); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: TIMETZ |
| `CREATE TABLE __t (c REAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c SERIAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c BIGSERIAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c SMALLSERIAL); DROP TABLE __t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE __t (c OID); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: OID |

---

## Missing something?

If you tried PG syntax that's not in this matrix, [open an issue](https://github.com/bas-in/basin/issues/new?template=sql_compatibility.yml&title=Missing+SQL+syntax%3A+) — we triage compatibility gaps within 48 hours.

This page is regenerated by `cargo test -p basin-integration-tests --test sql_support_matrix`. To suggest an addition to the matrix, edit `tests/integration/tests/sql_support_matrix.rs` and rerun.
