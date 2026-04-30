# Basin

Bucket-native, multi-tenant, Postgres-compatible database.

Status: pre-alpha. Phase 1 substrate (Parquet + Iceberg-style catalog) ships,
plus a single-process **proof-of-concept** that speaks pgwire end-to-end.
See [`TASK.md`](./TASK.md) for the build plan and
[`docs/architecture.md`](./docs/architecture.md) for the four-layer architecture.

## Layout

```
crates/
  basin-common      shared types, errors, telemetry
  basin-storage     Parquet + object_store under tenant prefixes
  basin-catalog     Iceberg-style catalog client (in-memory + REST stub)
  basin-engine      DataFusion-backed SQL execution, per-tenant sessions
  basin-router      pgwire v3 front-end (simple query only, in PoC)
  basin-wal         (Phase 2) Raft-backed regional WAL
  basin-shard       (Phase 3) stateful shard owner
  basin-placement   (Phase 3) (tenant, partition) → owner mapping
  basin-analytical  (Phase 5) DuckDB / DataFusion against Iceberg directly
services/
  basin-server      single-process PoC binary
  control-plane     tenant CRUD, billing, admin API (not yet built)
  dashboard         customer-facing UI (not yet built)
deploy/             helm + terraform (not yet built)
tests/integration/  cross-crate tests
docs/               architecture + ops + sql-compat
```

## Build and test

```sh
cargo build --workspace
cargo test  --workspace
```

## Try the PoC

Start the server:

```sh
BASIN_BIND=127.0.0.1:5433 \
BASIN_DATA_DIR=/tmp/basin \
BASIN_TENANTS='alice=*,bob=*' \
cargo run -p basin-server
```

`alice=*` and `bob=*` allocate fresh tenant ULIDs at startup; the server logs
them to stderr so you can also pin them with `BASIN_TENANTS='alice=01HABCD...'`
on the next run.

Connect with `psql` (uses simple-query for everything you type at the prompt):

```sh
psql -h 127.0.0.1 -p 5433 -U alice -d basin
```

```sql
CREATE TABLE events (id BIGINT NOT NULL, body TEXT NOT NULL);
INSERT INTO events VALUES (1, 'hello'), (2, 'world');
SELECT * FROM events WHERE id = 2;
```

Confirm Parquet hit disk under the tenant prefix:

```sh
find /tmp/basin/tenants -name '*.parquet'
```

### Known PoC limits

The router speaks **simple-query only**. Drivers that default to extended
protocol (`tokio-postgres::query`, `psycopg`, JDBC) need to be told to use
simple queries explicitly, or they will get a `0A000` error on `Parse`.
`psql` is fine. SQL coverage is `CREATE TABLE`, `INSERT … VALUES`,
`SELECT [WHERE]`, `SHOW TABLES` — no UPDATE/DELETE/JOIN/transactions yet.
Auth is cleartext-password and **the password is ignored**.
