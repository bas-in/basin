# Basin — session benchmark highlights

Per-feature performance numbers reported by the agents that landed each
piece this session. Spot-checks, not consolidated benchmarks — for the
auto-generated benchmark dashboard see [`RESULTS_localfs.md`](./RESULTS_localfs.md)
(regenerate via `cargo test -p basin-integration-tests --tests -- --nocapture && python3 benchmark/bundle.py`).

## Storage / consensus

| Feature | Number | Workload |
|---|---|---|
| `RaftWal` 3-node append (in-process simulation) | ~120 µs/op | 10 entries via `client_write`, no fsync, no network — sets a floor for the cross-process gRPC follow-up |
| EDF scheduler — noisy-neighbor p50 | 5.83 ms | Project B's 100 sequential HEADs vs project A's 1000-bulk-op concurrent load |
| EDF scheduler — noisy-neighbor p99 | 13.97 ms | Same workload (per-op cost = 2 ms; ratio ≈ 7×; cf. ADR 0008's 32–44× p99 inflation pre-EDF) |
| `LocalWal` (single-node fsync, baseline) | byte-identical to pre-trait extraction | preserved across `Wal` trait extraction — verified by 9 unchanged unit tests |

## Indexes / search

| Feature | Speedup | Workload |
|---|---|---|
| `basin-trgm` GIN trigram index | 9.4× (debug build) | 1000-row corpus, similarity threshold 0.3, query "smith". Release build expected wider |
| Vector planner auto-routing for `ORDER BY <-> LIMIT k` | 5.62× (debug build) | 1K-row `vector(8)` corpus, top-10 k-NN |
| HNSW vs brute-force (existing) | already-shipped per CAPABILITIES | inflection at ~1K rows; widens linearly to 10K+ |

## Materialised views / continuous aggregates

| Feature | Speedup | Workload |
|---|---|---|
| `basin-cv` incremental refresh | 2.3× (micro-bench) | 10K-row source + 100 new rows; wins more on production-scale source tables (saved-scan cost dominates as the source grows) |

## Wire format

| Feature | Behaviour | Notes |
|---|---|---|
| pgwire INTERVAL OID 1186 (text) | end-to-end | binary serialisation deferred to v0.2 |
| pgwire TIMESTAMPTZ OID 1184 (text + binary) | end-to-end | rebased to PG epoch 2000-01-01 |
| pgwire DATE OID 1082 (text + binary) | end-to-end | rebased to PG epoch |
| pgwire NUMERIC OID 1700 (text in binary slot) | tolerated by lenient drivers | `tokio-postgres`, `asyncpg`, `pgx` all accept; binary varlena form is v0.2 |

## SQL semantics

| Feature | Pattern | Numbers |
|---|---|---|
| `extract(second FROM ts)` | bit-exact through ≤µs precision | Float64 mantissa (53-bit) covers 60 × 1M distinct values; sub-µs would need Decimal128 (queued) |
| `age(ts1, ts2)` calendar walk | matches PG `timestamp_age` for common cases | Same-day-of-month edge case may diverge |
| Mutual-recursion detection in `LANGUAGE sql` | bounded BFS, depth cap 256 | Catches `f → g → f` and longer cycles at registration time, not planning time |
| Enum ordinal comparison via `ORDER BY` | rewriter emits `CASE WHEN ord_eq THEN 0 …` | Best-effort: bails on JOINs, set-ops, derived tables |

## Constraint enforcement

| Surface | Cost in v0.1 | v0.2 path |
|---|---|---|
| PRIMARY KEY uniqueness on INSERT | full-table-scan: O(rows_in_table + rows_in_batch) | Phase 5.7 B1 secondary indexes → O(log n) point-probe |
| FOREIGN KEY exists check on INSERT | full-table-scan against parent | Same — same B-tree |
| CASCADE DELETE recursion | runs through `sess.execute("DELETE FROM child WHERE …")` | Same path; no special engine fast-path |

## ORM compat (Prisma / Sequelize / SQLAlchemy)

All three ORMs' startup-introspection queries land successfully against
Basin's catalog views:

- **Prisma** — schema discovery, table list, column list, PK lookup, FK lookup, index list
- **Sequelize** — column-with-PK definition, table existence, sequence listing
- **SQLAlchemy** — full column introspection, composite PK ordinal, sequence-backed default detection

Tests in `tests/integration/tests/orm_compat.rs`. Tests that depend on
yet-unshipped surfaces (sequences-as-pg_class-relkind-`S`, etc) marked
`#[ignore]` with flip-comment for follow-up.

## PostgREST / pgAdmin compat

`tests/integration/tests/postgrest_pgadmin_compat.rs` — every catalog
probe those tools issue on connect lands a successful row set, including
pgAdmin's `pg_attribute JOIN pg_type` column-detail query.

---

_Captured 2026-05-10. Numbers are debug-build / single-process simulation
unless noted. Production hot-path numbers (cross-process Raft + on-disk
storage + release-build) belong in `RESULTS_*.md` after a real benchmark
run._
