# orm-live — live ORM compatibility harness

The gold standard beyond the recorded corpus
(`tests/integration/tests/orm_compat.rs`, 139 recorded shapes / 7 ORMs).
Where the corpus replays *captured* SQL, this harness runs **six real ORM
apps** — each driving its **own migration engine, query builder and
transaction manager** — against a **real `basin-server`** over pgwire.
If Prisma 7 or Django 5.2 changes the SQL it emits, this harness notices;
the recorded corpus cannot.

```
testing/orm-live/
├── README.md          ← you are here
├── run-all.sh         ← starts basin-server, runs every suite, scores, exits
├── scorecard.py       ← TAP parser + baseline comparison (python3)
├── baseline.json      ← expected pass/xfail per test; gaps NAMED, never silent
├── prisma/            ← Prisma 7  (migrate deploy/dev, client CRUD, nested writes, $transaction)
├── drizzle/           ← drizzle-orm + drizzle-kit (generate/migrate, relational queries)
├── typeorm/           ← TypeORM 1.x (migration runner, repositories, QueryRunner tx)
├── django/            ← Django 5.2 + psycopg2 (makemigrations/migrate, atomic, F(), bulk_create)
├── sqlalchemy/        ← SQLAlchemy 2.0 + Alembic (upgrade head, selectinload, begin_nested)
└── gorm/              ← GORM (AutoMigrate, Preload, CreateInBatches, OnConflict upsert)
```

## How to run

```sh
# 1. Build the server once (NOT done by the harness):
cargo build --release -p basin-server

# 2. Run everything (node / python3 / go suites auto-skip if the toolchain
#    is missing — skips are reported, never silent):
testing/orm-live/run-all.sh

# Variations:
BASIN_SERVER_BIN=/path/to/basin-server testing/orm-live/run-all.sh
BASIN_DSN=postgres://basin@127.0.0.1:5433/basin testing/orm-live/run-all.sh  # external server
testing/orm-live/run-all.sh --only=django
testing/orm-live/run-all.sh --update-baseline   # true-up baseline.json (then review + commit the diff)
```

`run-all.sh` launches the server with `BASIN_PROJECTS=basin=*` on
`127.0.0.1:${BASIN_PORT:-54329}` against a throwaway `BASIN_DATA_DIR`, waits
for pgwire, exports `BASIN_DSN`, runs each suite, then scores. Each suite
installs its own deps on first run (`npm install` / venv + `pip install` /
`go mod tidy`) — first run needs network; afterwards installs are cached in
`node_modules/` / `.venv/` / the Go module cache. Raw per-suite output is
teed to `results/<suite>.tap` (+ `.log`) so a long run is peekable mid-flight.

## Scorecard format

Every suite prints one TAP-ish line per test:

```
ok - django.savepoint-rollback
not ok - prisma.migrate-deploy # P3018: advisory lock ...
```

`scorecard.py` joins those against `baseline.json` and prints a table; the
verdict column is the contract:

| verdict    | meaning                                                     | CI effect |
|------------|-------------------------------------------------------------|-----------|
| PASS       | passed, expected to pass                                    | ok        |
| XFAIL      | failed, known gap (named in `baseline.json`)                | ok        |
| XPASS      | passed, was a known gap → **re-baseline & commit**          | ok + note |
| REGRESSION | failed, expected to pass                                    | **exit 1**|
| NEW-FAIL   | failed, not in baseline (add an entry naming the gap)       | **exit 1**|
| MISSING    | in baseline but produced no TAP line (suite died mid-run)   | **exit 1**|
| NEW-PASS   | passed, not in baseline (add an entry)                      | ok + note |
| SKIP       | suite toolchain unavailable on this box                     | ok, listed|

## baseline.json contract

* `"expected": "pass"` — failure is a regression and fails CI.
* `"expected": "xfail"` — a **known Basin gap**, with the gap **named** in
  `"gap"` (e.g. advisory locks for `prisma migrate`, `CREATE DATABASE` for
  the shadow DB, the `EXCLUDED`-alias upsert gap). Never skip silently: an
  xfail that starts passing is reported as XPASS so the baseline gets
  tightened, and an xfail's failure reason stays visible in the table.
* The shipped baseline is **provisional** (derived from the corpus gates in
  `orm_compat.rs`; the harness has not yet been run against a live server —
  the tree was build-frozen when it was written). First live run:
  `./run-all.sh --update-baseline`, then review the diff — every new xfail
  must get a real gap description before commit.

## What each suite covers

* **prisma** — `prisma migrate deploy` (committed migration; relations +
  enum + Json + DateTime DDL), `prisma migrate dev`, client CRUD via the
  Prisma 7 driver-adapter (`@prisma/adapter-pg`), nested writes with
  `include`, interactive `$transaction` commit + rollback, skip/take
  pagination + `count()`.
* **drizzle** — `drizzle-kit generate` (offline) + `drizzle-kit migrate`,
  insert/`returning`, `where`, relational queries (`db.query…with`),
  `onConflictDoUpdate` upsert, `db.transaction` commit + rollback,
  limit/offset pagination, delete/`returning`.
* **typeorm** — `DataSource.initialize`, `runMigrations()` (tracker table),
  repository CRUD, eager many-to-one + `relations:` one-to-many loading,
  QueryRunner-managed transactions (commit + rollback).
* **django** — `makemigrations` + `migrate` (FK, `unique_together`,
  `JSONField`, `DateTimeField` DDL through Django's schema editor), ORM CRUD,
  `select_related`, `transaction.atomic` (commit + nested savepoint
  rollback), `F()` server-side update, `bulk_create` incl.
  `ignore_conflicts`.
* **sqlalchemy** — `alembic upgrade head`, 2.0-style ORM CRUD,
  relationship cascade insert + lazy load, `selectinload`, session rollback,
  `begin_nested()` savepoint rollback.
* **gorm** — `AutoMigrate` (information_schema introspection + DDL), `Create`
  with associations (RETURNING), `Preload`, `CreateInBatches`,
  `clause.OnConflict` upsert, `db.Transaction` commit + rollback, delete.

Each suite generates unique row keys per run, so re-running against a durable
server does not collide.

## CI

Build the server, then run the harness; nonzero exit = regression vs
baseline. Suggested job:

```yaml
orm-live:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-node@v4
      with: { node-version: 24 }
    - uses: actions/setup-python@v5
      with: { python-version: "3.12" }
    - uses: actions/setup-go@v5
      with: { go-version: "1.24" }
    - uses: dtolnay/rust-toolchain@stable
    - uses: Swatinem/rust-cache@v2
    - run: cargo build --release -p basin-server
    - run: testing/orm-live/run-all.sh
    - if: always()
      uses: actions/upload-artifact@v4
      with:
        name: orm-live-results
        path: testing/orm-live/results/
```
