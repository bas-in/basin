# Basin SQL support — auto-generated matrix

Run `cargo test -p basin-integration-tests --test sql_support_matrix` to refresh.

Last run: 1778807244 (Unix epoch)
SQL fragments tested: 486 total / 996 green (across all three configurations).

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
| `LISTEN ch` | 🚫 | 🚫 | 🚫 | feature not supported: LISTEN is not supported (SQLSTATE 0A000) |
| `NOTIFY ch, 'msg'` | 🚫 | 🚫 | 🚫 | feature not supported: NOTIFY is not supported (SQLSTATE 0A000) |
| `UNLISTEN ch` | 🚫 | 🚫 | 🚫 | feature not supported: UNLISTEN is not supported (SQLSTATE 0A000) |
| `PREPARE stmt AS SELECT 1` | ✅ | ✅ | ✅ |  |
| `EXECUTE stmt` | ✅ | ✅ | ✅ |  |
| `DEALLOCATE stmt` | ✅ | ✅ | ✅ |  |
| `DECLARE c CURSOR FOR SELECT 1` | ✅ | ✅ | ✅ |  |
| `FETCH 1 FROM c` | 🛠 | 🛠 | 🛠 | internal: cursor "c" does not exist |
| `CLOSE c` | 🛠 | 🛠 | 🛠 | internal: cursor "c" does not exist |
| `LOCK TABLE t` | ✅ | ✅ | ✅ |  |
| `VACUUM` | ✅ | ✅ | ✅ |  |
| `ANALYZE` | ✅ | ✅ | ✅ |  |
| `CLUSTER t` | ✅ | ✅ | ✅ |  |
| `EXPLAIN SELECT 1` | ✅ | ✅ | ✅ |  |
| `EXPLAIN ANALYZE SELECT 1` | ✅ | ✅ | ✅ |  |
| `SET search_path = public` | ✅ | ✅ | ✅ |  |
| `SHOW search_path` | ✅ | ✅ | ✅ |  |

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
| `DROP SCHEMA s` | 🛠 | 🛠 | 🛠 | not found: schema "s" does not exist |
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
| `DROP VIEW v` | 🛠 | 🛠 | 🛠 | not found: view "v" does not exist |
| `CREATE MATERIALIZED VIEW mv AS SELECT * FROM t` | 📜 | 📜 | 📜 | invalid schema: CREATE MATERIALIZED VIEW requires WITH (basin.continuous, ref… |
| `REFRESH MATERIALIZED VIEW mv` | 🛠 | 🛠 | 🛠 | setup failed: invalid schema: CREATE MATERIALIZED VIEW: source query returned… |
| `DROP MATERIALIZED VIEW mv` | 🛠 | 🛠 | 🛠 | setup failed: invalid schema: CREATE MATERIALIZED VIEW: source query returned… |
| `CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION fn()` | ✅ | ✅ | ✅ |  |
| `CREATE POLICY p ON t USING (id = 1)` | ✅ | ✅ | ✅ |  |
| `DROP POLICY p ON t` | ✅ | ✅ | ✅ |  |
| `COMMENT ON TABLE t IS 'x'` | ✅ | ✅ | ✅ |  |
| `CREATE EXTENSION pgcrypto` | ✅ | ✅ | ✅ |  |
| `CREATE TRIGGER trg AFTER UPDATE ON t FOR EACH ROW WHEN (NEW.id <> OLD.id) EXECUTE FUNCTION fn()` | ✅ | ✅ | ✅ |  |
| `CREATE TRIGGER trg INSTEAD OF DELETE ON vv FOR EACH ROW EXECUTE FUNCTION fn()` | ✅ | ✅ | ✅ |  |
| `CREATE TRIGGER trg AFTER INSERT ON t REFERENCING NEW TABLE AS new_t FOR EACH STATEMENT EXECUTE FUNCTION fn()` | ✅ | ✅ | ✅ |  |
| `CREATE CONSTRAINT TRIGGER trg AFTER INSERT ON t DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fn()` | ✅ | ✅ | ✅ |  |

## DDL/Tables

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE TABLE t (id INT, name TEXT)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE IF NOT EXISTS t (id INT)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT PRIMARY KEY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT NOT NULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT DEFAULT 0)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, name TEXT UNIQUE)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT REFERENCES u(id))` | 📜 | 📜 | 📜 | invalid schema: FOREIGN KEY "t_id_fkey": referenced table "u" has no PRIMARY … |
| `CREATE TABLE t (id INT, CHECK (id > 0))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED ALWAYS AS (1+1) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (LIKE u INCLUDING ALL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t () INHERITS (u)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT) PARTITION BY RANGE (id)` | ✅ | ✅ | ✅ |  |
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
| `DROP TABLE t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE t |
| `DROP TABLE IF EXISTS t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE IF EXISTS t |
| `DROP TABLE t CASCADE` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE t CASCADE |
| `TRUNCATE TABLE t` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED ALWAYS AS IDENTITY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT GENERATED BY DEFAULT AS IDENTITY)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id SERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id BIGSERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id SMALLSERIAL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT, total INT GENERATED ALWAYS AS (id * 2) VIRTUAL)` | 🚫 | 🚫 | 🚫 | feature not supported: VIRTUAL generated columns deferred to v0.2; use STORED |
| `ALTER TABLE t ALTER COLUMN id SET GENERATED ALWAYS` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: SET/DROP NOT NULL, SET DEF… |
| `ALTER TABLE t ALTER COLUMN id SET GENERATED BY DEFAULT` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: SET/DROP NOT NULL, SET DEF… |
| `ALTER TABLE t ALTER COLUMN id DROP IDENTITY` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: SET/DROP NOT NULL, SET DEF… |
| `CREATE TABLE t (id INT, EXCLUDE USING gist (id WITH =))` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ',' or ')' after column de… |
| `CREATE TABLE t (id INT REFERENCES u DEFERRABLE INITIALLY DEFERRED)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (a INT, b INT, FOREIGN KEY (a) REFERENCES u(id) MATCH FULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (a INT REFERENCES u ON UPDATE CASCADE ON DELETE SET NULL)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT NOT NULL, name TEXT, UNIQUE (id, name) INCLUDE (name))` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t VALIDATE CONSTRAINT ck` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM ONLY t` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: table 'datafusion.public.only' not found |
| `CREATE TABLE t_2024 PARTITION OF t FOR VALUES FROM (2024) TO (2025)` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: P… |
| `CREATE TABLE t (region TEXT) PARTITION BY LIST (region)` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE t (id INT) PARTITION BY HASH (id)` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t ATTACH PARTITION p FOR VALUES IN ('us')` | ✅ | ✅ | ✅ |  |
| `ALTER TABLE t DETACH PARTITION p` | 🛠 | 🛠 | 🛠 | setup failed: internal: parse error: sql parser error: Expected: end of state… |

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
| `UPDATE t SET id = (SELECT MAX(id) FROM u)` | 🚫 | 🚫 | 🚫 | invalid schema: UPDATE SET id: scalar subquery on RHS not supported in v0.1 |
| `UPDATE t SET id = 1 FROM u WHERE t.id = u.id` | 📜 | 📜 | 📜 | invalid schema: UPDATE … FROM requires the target table "t" to have a PRIMA… |
| `UPDATE t SET id = 1 RETURNING id` | 🚫 | 🚫 | 🚫 | invalid schema: UPDATE ... RETURNING not supported |
| `UPDATE t SET id = 1 WHERE id IN (SELECT id FROM u)` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t WHERE id = 1` | ✅ | ✅ | ✅ |  |
| `DELETE FROM t USING u WHERE t.id = u.id` | 📜 | 📜 | 📜 | invalid schema: DELETE … USING requires the target table "t" to have a PRIM… |
| `DELETE FROM t RETURNING id` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: i… |
| `MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN UPDATE SET id = u.id WHEN NOT MATCHED THEN INSERT VALUES (u.id)` | ✅ | ✅ | ✅ |  |
| `COPY t FROM STDIN` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: ;, found: EOF |
| `COPY t TO STDOUT` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY t TO STDOUT |
| `INSERT INTO t OVERRIDING SYSTEM VALUE VALUES (1)` | ✅ | ✅ | ✅ |  |
| `INSERT INTO t OVERRIDING USER VALUE VALUES (1)` | ✅ | ✅ | ✅ |  |

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
| `SELECT true IS TRUE` | ✅ | ✅ | ✅ |  |
| `SELECT true IS NOT TRUE` | ✅ | ✅ | ✅ |  |
| `SELECT false IS FALSE` | ✅ | ✅ | ✅ |  |
| `SELECT false IS NOT FALSE` | ✅ | ✅ | ✅ |  |
| `SELECT NULL::bool IS UNKNOWN` | ✅ | ✅ | ✅ |  |
| `SELECT NULL::bool IS NOT UNKNOWN` | ✅ | ✅ | ✅ |  |
| `SELECT 1 IS DISTINCT FROM 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 IS NOT DISTINCT FROM 1` | ✅ | ✅ | ✅ |  |
| `SELECT ROW(1, NULL) IS NULL` | ✅ | ✅ | ✅ |  |

## FullTextSearch

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT to_tsvector('english', 'a quick brown fox')` | ✅ | ✅ | ✅ |  |
| `SELECT to_tsquery('english', 'quick & fox')` | ✅ | ✅ | ✅ |  |
| `SELECT 'a quick brown fox'::tsvector @@ to_tsquery('english', 'fox')` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT plainto_tsquery('english', 'quick fox')` | ✅ | ✅ | ✅ |  |
| `SELECT phraseto_tsquery('english', 'quick fox')` | ✅ | ✅ | ✅ |  |
| `SELECT websearch_to_tsquery('english', 'quick OR fox')` | ✅ | ✅ | ✅ |  |
| `SELECT ts_rank(to_tsvector('a quick'), to_tsquery('quick'))` | ✅ | ✅ | ✅ |  |
| `SELECT ts_headline('a quick fox', to_tsquery('quick'))` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE doc (body TEXT, ts TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', body)) STORED)` | ✅ | ✅ | ✅ |  |
| `CREATE INDEX ON doc USING gin (ts)` | ✅ | ✅ | ✅ |  |

## Functions/Array

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT '{1,2}'::int[] && '{2,3}'::int[]` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: type modifiers, found: [ a… |
| `SELECT ARRAY[1,2,3]` | ✅ | ✅ | ✅ |  |
| `SELECT '{1,2,3}'::int[]` | 🛠 | 🛠 | 🛠 | internal: execute: Arrow error: Cast error: Cannot cast string '{1,2,3}' to v… |
| `SELECT '{{1,2},{3,4}}'::int[][]` | 🛠 | 🛠 | 🛠 | internal: execute: Arrow error: Cast error: Cannot cast string '{{1,2},{3,4}}… |
| `SELECT (ARRAY[1,2,3])[2]` | ✅ | ✅ | ✅ |  |
| `SELECT (ARRAY[1,2,3,4,5])[2:4]` | ✅ | ✅ | ✅ |  |
| `SELECT array_length(ARRAY[1,2,3], 1)` | ✅ | ✅ | ✅ |  |
| `SELECT array_ndims(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT array_lower(ARRAY[1,2,3], 1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'array_lower'. Did yo… |
| `SELECT array_upper(ARRAY[1,2,3], 1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'array_upper'. Did yo… |
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
| `SELECT generate_subscripts(ARRAY[10,20,30], 1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'generate_subscripts'… |
| `SELECT ARRAY[1,2] @> ARRAY[1]` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]` | ❌ | ❌ | ❌ | internal: execute: Execution error: jsonb_contained_by: expected LargeBinary … |
| `SELECT ARRAY[1,2] \|\| ARRAY[3,4]` | ✅ | ✅ | ✅ |  |

## Functions/Crypto

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT GEN_RANDOM_UUID()` | ✅ | ✅ | ✅ |  |
| `SELECT DIGEST('a','sha256')` | ✅ | ✅ | ✅ |  |
| `SELECT ENCODE('a','hex')` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT DECODE('61','hex')` | ✅ | ✅ | ✅ |  |

## Functions/DateTime

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT NOW()` | ✅ | ✅ | ✅ |  |
| `SELECT CURRENT_TIMESTAMP` | ✅ | ✅ | ✅ |  |
| `SELECT CURRENT_DATE` | ✅ | ✅ | ✅ |  |
| `SELECT DATE_TRUNC('hour', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT AGE(NOW(), NOW())` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT EXTRACT(YEAR FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT TO_CHAR(NOW(),'YYYY')` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT TO_TIMESTAMP('2024-01-01','YYYY-MM-DD')` | ✅ | ✅ | ✅ |  |
| `SELECT make_date(2024, 1, 15)` | ✅ | ✅ | ✅ |  |
| `SELECT make_time(12, 30, 45.5)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamp(2024, 1, 15, 12, 30, 45.5)` | ✅ | ✅ | ✅ |  |
| `SELECT make_timestamptz(2024, 1, 15, 12, 30, 45.5, 'UTC')` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'make_timestamptz'. D… |
| `SELECT make_interval(years => 1, days => 30)` | ✅ | ✅ | ✅ |  |
| `SELECT date_part('year', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT EXTRACT(EPOCH FROM NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT date '2024-01-01' + interval '1 day'` | ✅ | ✅ | ✅ |  |
| `SELECT date '2024-12-31' - date '2024-01-01'` | ✅ | ✅ | ✅ |  |
| `SELECT NOW() AT TIME ZONE 'America/New_York'` | ✅ | ✅ | ✅ |  |
| `SELECT (NOW(), NOW() + interval '1h') OVERLAPS (NOW() + interval '30m', NOW() + interval '90m')` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: (… |
| `SELECT justify_hours(interval '36 hours')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_days(interval '40 days')` | ✅ | ✅ | ✅ |  |
| `SELECT justify_interval(interval '1 mon -1 hour')` | ✅ | ✅ | ✅ |  |
| `SELECT isfinite(NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT isfinite(date '2024-01-01')` | ✅ | ✅ | ✅ |  |
| `SELECT 'infinity'::timestamp` | ✅ | ✅ | ✅ |  |
| `SELECT '-infinity'::timestamp` | ✅ | ✅ | ✅ |  |

## Functions/JSONB

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT '{"a":1}'::jsonb -> 'a'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT '{"a":1}'::jsonb ->> 'a'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT '{"a":1}'::jsonb #> '{a}'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT '{"a":1}'::jsonb @> '{"a":1}'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT jsonb_set('{"a":1}'::jsonb, '{a}', '2'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_insert('{"a":[1,2]}'::jsonb, '{a,1}', '99'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_strip_nulls('{"a":1,"b":null}'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_path_query('{"a":1}'::jsonb, '$.a')` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_path_exists('{"a":1}'::jsonb, '$.a')` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_path_match('{"a":1}'::jsonb, '$.a == 1')` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT '{"a":{"b":1}}'::jsonb @? '$.a.b'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT '{"a":1}'::jsonb @@ '$.a == 1'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_typeof('1'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_pretty('{"a":1}'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_array_length('[1,2,3]'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT jsonb_object_keys('{"a":1,"b":2}'::jsonb)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT * FROM jsonb_each('{"a":1}'::jsonb)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: table function 'jsonb_each' not found |
| `SELECT * FROM jsonb_each_text('{"a":1}'::jsonb)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: table function 'jsonb_each_text' not f… |
| `SELECT * FROM jsonb_array_elements('[1,2,3]'::jsonb)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: table function 'jsonb_array_elements' … |
| `SELECT * FROM jsonb_array_elements_text('["a","b"]'::jsonb)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: table function 'jsonb_array_elements_t… |
| `SELECT jsonb_build_object('a', 1, 'b', 2)` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_build_array(1, 'a', true)` | ✅ | ✅ | ✅ |  |
| `SELECT to_jsonb(ROW(1, 'a'))` | ✅ | ✅ | ✅ |  |
| `SELECT to_json(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT row_to_json(t) FROM (SELECT 1 AS a) t` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named t. Valid fields are t.a. |
| `SELECT array_to_json(ARRAY[1,2,3])` | ✅ | ✅ | ✅ |  |
| `SELECT jsonb_agg(id) FROM t` | 📜 | 📜 | 📜 | internal: execute: Execution error: jsonb_agg requires AggregateUDFImpl; defe… |
| `SELECT jsonb_object_agg(name, id) FROM t` | 📜 | 📜 | 📜 | internal: execute: Execution error: jsonb_object_agg requires AggregateUDFImp… |
| `SELECT '{"a":1,"b":2}'::jsonb - 'a'` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT '{"a":1,"b":2}'::jsonb - ARRAY['a','b']` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT '[1,2,3]'::jsonb - 1` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |
| `SELECT '{"a":1}'::jsonb \|\| '{"b":2}'::jsonb` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type JSONB |

## Functions/Math

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT ABS(-1)` | ✅ | ✅ | ✅ |  |
| `SELECT CEIL(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT FLOOR(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT ROUND(1.5)` | ✅ | ✅ | ✅ |  |
| `SELECT POWER(2,10)` | ✅ | ✅ | ✅ |  |
| `SELECT SQRT(4)` | ✅ | ✅ | ✅ |  |
| `SELECT MOD(10,3)` | ✅ | ✅ | ✅ |  |

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
| `SELECT split_part('a,b,c', ',', 2)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT reverse('abc')` | ✅ | ✅ | ✅ |  |
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

## Misc

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `TABLE t` | ✅ | ✅ | ✅ |  |
| `VALUES (1,2), (3,4)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM (VALUES (1,'a'), (2,'b')) AS v(id, name)` | ✅ | ✅ | ✅ |  |
| `SELECT * INTO new_t FROM t` | ✅ | ✅ | ✅ |  |
| `COPY t FROM '/tmp/x' WITH (FORMAT csv, HEADER, DELIMITER ',')` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY t FROM '/tmp/x' (FORMAT csv, HEADER, DELIM… |
| `COPY (SELECT * FROM t) TO '/tmp/x' WITH (FORMAT csv)` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: COPY (SELECT * FROM t) TO '/tmp/x' (FORMAT csv) |
| `DO $$ BEGIN RAISE NOTICE 'hi'; END; $$ LANGUAGE plpgsql` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an SQL statement, found: D… |
| `COMMENT ON COLUMN t.id IS 'pk'` | ✅ | ✅ | ✅ |  |
| `COMMENT ON FUNCTION f(int) IS 'x'` | ✅ | ✅ | ✅ |  |
| `SELECT pg_advisory_lock(1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_advisory_lock'. D… |
| `SELECT pg_advisory_unlock(1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_advisory_unlock'.… |
| `SELECT pg_try_advisory_lock(1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_try_advisory_lock… |
| `UNLISTEN *` | 🚫 | 🚫 | 🚫 | feature not supported: UNLISTEN is not supported (SQLSTATE 0A000) |
| `SELECT pg_typeof(1)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_typeof'. Did you … |
| `SELECT pg_size_pretty(1024::bigint)` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_size_pretty'. Did… |
| `SELECT pg_column_size('hello')` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'pg_column_size'. Did… |

## PG/Operators

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT 1 IS DISTINCT FROM 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 IS NOT DISTINCT FROM 1` | ✅ | ✅ | ✅ |  |
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
| `SELECT ARRAY[1,2,3] @> ARRAY[1,2]` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Error during planning: Failed to coerc… |
| `SELECT ARRAY[1,2] <@ ARRAY[1,2,3]` | ❌ | ❌ | ❌ | internal: execute: Execution error: jsonb_contained_by: expected LargeBinary … |
| `SELECT ARRAY[1,2] && ARRAY[2,3]` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'arrays_overlap'. Did… |
| `SELECT * FROM t WHERE id = ANY (SELECT id FROM u)` | 🛠 | 🛠 | 🛠 | internal: execute: type_coercion caused by Error during planning: Error durin… |
| `SELECT * FROM t WHERE id > ALL (SELECT id FROM u)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported ast node in sqlt… |
| `SELECT * FROM t WHERE id = SOME (SELECT id FROM u)` | 🛠 | 🛠 | 🛠 | internal: execute: type_coercion caused by Error during planning: Error durin… |
| `SELECT 5 & 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 \| 3` | ✅ | ✅ | ✅ |  |
| `SELECT 5 # 3` | 🛠 | 🛠 | 🛠 | internal: plan: SQL error: ParserError("No infix parser for token Sharp") |
| `SELECT ~5` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: an SQL statement, found: r… |
| `SELECT 1 << 3` | ✅ | ✅ | ✅ |  |
| `SELECT 8 >> 2` | ✅ | ✅ | ✅ |  |
| `SELECT (NOW(), NOW() + INTERVAL '1 hour') OVERLAPS (NOW() + INTERVAL '30 minutes', NOW() + INTERVAL '90 minutes')` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: (… |

## Ranges

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT int4range(1, 10)` | ✅ | ✅ | ✅ |  |
| `SELECT '[1,10)'::int4range` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT int4range(1,10) @> 5` | ✅ | ✅ | ✅ |  |
| `SELECT int4range(1,10) && int4range(5,15)` | ❌ | ❌ | ❌ | internal: parse error: sql parser error: Expected: end of statement, found: (… |
| `SELECT lower(int4range(1,10))` | ✅ | ✅ | ✅ |  |
| `SELECT upper(int4range(1,10))` | ✅ | ✅ | ✅ |  |
| `SELECT isempty(int4range(1,1))` | ✅ | ✅ | ✅ |  |
| `SELECT '[2020-01-01,2020-12-31]'::daterange` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported SQL type Custom(… |
| `SELECT tsrange(NOW() - interval '1 hour', NOW())` | ✅ | ✅ | ✅ |  |
| `SELECT int4multirange(int4range(1,5), int4range(10,15))` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'int4multirange'. Did… |

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
| `SELECT current_user` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'current_user'. Did y… |
| `SELECT session_user` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Invalid function 'session_user'. Did y… |

## SELECT/Aggregate

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT COUNT(*) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT SUM(id), AVG(id), MIN(id), MAX(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(DISTINCT id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT COUNT(*) FILTER (WHERE id > 0) FROM t` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected end of statement, found: (") |
| `SELECT STRING_AGG(name, ',') FROM t` | 📜 | 📜 | 📜 | invalid schema: cannot convert df-arrow type to workspace-arrow: LargeUtf8 |
| `SELECT ARRAY_AGG(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT JSON_AGG(t) FROM t` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named t. Valid fields are t.id. |
| `SELECT id, SUM(id) FROM t GROUP BY id` | ✅ | ✅ | ✅ |  |
| `SELECT id, SUM(id) FROM t GROUP BY id HAVING SUM(id) > 0` | ✅ | ✅ | ✅ |  |
| `SELECT id, GROUPING(id) FROM t GROUP BY ROLLUP (id)` | ✅ | ✅ | ✅ |  |
| `SELECT id, name FROM t GROUP BY CUBE (id, name)` | ✅ | ✅ | ✅ |  |
| `SELECT id, name FROM t GROUP BY GROUPING SETS ((id), (name))` | ✅ | ✅ | ✅ |  |
| `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY id) FROM t` | 🚫 | 🚫 | 🚫 | internal: plan: This feature is not implemented: WITHIN GROUP is not supporte… |
| `SELECT percentile_disc(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY id) FROM t` | 🚫 | 🚫 | 🚫 | internal: plan: This feature is not implemented: WITHIN GROUP is not supporte… |
| `SELECT mode() WITHIN GROUP (ORDER BY id) FROM t` | 🚫 | 🚫 | 🚫 | internal: plan: This feature is not implemented: WITHIN GROUP is not supporte… |
| `SELECT stddev(id), stddev_pop(id), stddev_samp(id) FROM t` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Projections require unique expression … |
| `SELECT variance(id), var_pop(id), var_samp(id) FROM t` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Projections require unique expression … |
| `SELECT corr(id, id), covar_pop(id, id), covar_samp(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_slope(id, id), regr_intercept(id, id), regr_r2(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_avgx(id, id), regr_avgy(id, id), regr_count(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT regr_sxx(id, id), regr_syy(id, id), regr_sxy(id, id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT bool_and(id > 0), bool_or(id > 0), every(id > 0) FROM t` | 🛠 | 🛠 | 🛠 | internal: plan: Error during planning: Projections require unique expression … |
| `SELECT bit_and(id), bit_or(id), bit_xor(id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT array_agg(id ORDER BY id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT array_agg(DISTINCT id) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT string_agg(name, ',' ORDER BY id) FROM t` | 📜 | 📜 | 📜 | invalid schema: cannot convert df-arrow type to workspace-arrow: LargeUtf8 |

## SELECT/CTE

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `WITH cte AS (SELECT 1 AS x) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH cte AS (SELECT * FROM t) SELECT * FROM cte` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r` | 🛠 | 🛠 | 🛠 | internal: plan: Schema error: No field named n. Valid fields are r."Int64(1)". |
| `WITH ins AS (INSERT INTO t VALUES (1) RETURNING id) SELECT * FROM ins` | 🛠 | 🛠 | 🛠 | internal: plan: This feature is not implemented: Query INSERT INTO t VALUES (… |
| `WITH cte AS MATERIALIZED (SELECT 1) SELECT * FROM cte` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected: (, found: MATERIALIZED") |
| `WITH cte AS NOT MATERIALIZED (SELECT 1) SELECT * FROM cte` | ❌ | ❌ | ❌ | internal: plan: SQL error: ParserError("Expected: (, found: NOT") |
| `WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a, b` | ✅ | ✅ | ✅ |  |
| `WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n+1 FROM r WHERE n < 5) SELECT * FROM r` | 🚫 | 🚫 | 🚫 | internal: plan: This feature is not implemented: Recursive queries with a dis… |

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
| `SELECT * FROM t, LATERAL (SELECT id FROM u WHERE u.id = t.id) sub` | 🛠 | 🛠 | 🛠 | internal: execute: This feature is not implemented: Physical plan does not su… |
| `SELECT * FROM (SELECT 1 AS x) sub` | ✅ | ✅ | ✅ |  |
| `SELECT (SELECT MAX(id) FROM u) FROM t` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.id = t.id)` | ✅ | ✅ | ✅ |  |
| `SELECT * FROM t WHERE id = ANY (SELECT id FROM u)` | 🛠 | 🛠 | 🛠 | internal: execute: type_coercion caused by Error during planning: Error durin… |
| `SELECT * FROM t WHERE id > ALL (SELECT id FROM u)` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported ast node in sqlt… |
| `SELECT * FROM t CROSS JOIN LATERAL generate_series(1, t.id) g` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported ast node Functio… |
| `SELECT * FROM t LEFT JOIN LATERAL (SELECT id FROM u WHERE u.id = t.id) sub ON true` | 🛠 | 🛠 | 🛠 | internal: execute: This feature is not implemented: Physical plan does not su… |
| `SELECT * FROM t, LATERAL unnest(ARRAY[1,2,3]) tag` | 📜 | 📜 | 📜 | internal: plan: This feature is not implemented: Unsupported ast node Functio… |

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
| `SELECT id AS x FROM t` | ✅ | ✅ | ✅ |  |
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

## SELECT/SetOps

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `SELECT 1 UNION SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 UNION ALL SELECT 2` | ✅ | ✅ | ✅ |  |
| `SELECT 1 INTERSECT SELECT 1` | ✅ | ✅ | ✅ |  |
| `SELECT 1 EXCEPT SELECT 2` | ✅ | ✅ | ✅ |  |

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

## Schemas

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE SCHEMA myschema` | ✅ | ✅ | ✅ |  |
| `CREATE SCHEMA AUTHORIZATION alice` | ✅ | ✅ | ✅ |  |
| `SET search_path = myschema, public` | ✅ | ✅ | ✅ |  |
| `CREATE TABLE myschema.t (id INT)` | ✅ | ✅ | ✅ |  |
| `DROP SCHEMA myschema CASCADE` | ✅ | ✅ | ✅ |  |

## Transactions

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `BEGIN` | ✅ | ✅ | ✅ |  |
| `COMMIT` | ✅ | ✅ | ✅ |  |
| `ROLLBACK` | ✅ | ✅ | ✅ |  |
| `SAVEPOINT s` | ✅ | ✅ | ✅ |  |
| `RELEASE SAVEPOINT s` | ✅ | ✅ | ✅ |  |
| `ROLLBACK TO s` | ✅ | ✅ | ✅ |  |
| `BEGIN ISOLATION LEVEL SERIALIZABLE` | ✅ | ✅ | ✅ |  |
| `BEGIN READ ONLY` | ✅ | ✅ | ✅ |  |

## Types

| SQL | Default | +PG\_QUERY | +PG\_PLAN | Notes |
|---|---|---|---|---|
| `CREATE TABLE __t (c SMALLINT); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c INT); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c BIGINT); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c REAL); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: REAL |
| `CREATE TABLE __t (c DOUBLE PRECISION); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c NUMERIC); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c NUMERIC(10,2)); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c TEXT); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c VARCHAR); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c VARCHAR(255)); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c CHAR(10)); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c CITEXT); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: CITEXT |
| `CREATE TABLE __t (c BOOLEAN); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c DATE); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: DATE |
| `CREATE TABLE __t (c TIME); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: TIME |
| `CREATE TABLE __t (c TIMESTAMP); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c TIMESTAMPTZ); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c INTERVAL); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: INTERVAL |
| `CREATE TABLE __t (c UUID); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c JSON); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c JSONB); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c BYTEA); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c INT[]); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: INT[] |
| `CREATE TABLE __t (c TEXT[]); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported column type in PoC: TEXT[] |
| `CREATE TABLE __t (c INET); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c CIDR); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c MACADDR); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c MONEY); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c XML); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c TSVECTOR); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c POINT); DROP TABLE __t` | 📜 | 📜 | 📜 | invalid schema: unsupported custom type: POINT |
| `CREATE TABLE __t (c INT4RANGE); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |
| `CREATE TABLE __t (c VECTOR(3)); DROP TABLE __t` | 🚫 | 🚫 | 🚫 | internal: unsupported in PoC: DROP TABLE __t |

---

## Missing something?

If you tried PG syntax that's not in this matrix, [open an issue](https://github.com/bas-in/basin/issues/new?template=sql_compatibility.yml&title=Missing+SQL+syntax%3A+) — we triage compatibility gaps within 48 hours.

This page is regenerated by `cargo test -p basin-integration-tests --test sql_support_matrix`. To suggest an addition to the matrix, edit `tests/integration/tests/sql_support_matrix.rs` and rerun.
