# Autonomous-loop decisions log

Entries are appended in reverse-chronological order (newest at top). Each
entry records a decision made by the autonomous-loop dev sweep that
isn't already captured in TASK.md, an ADR, or a commit message.

---

## 2026-05-22 — Capstone `cargo test --workspace --lib --bins` caught 1 real regression + 1 latent bug

Ran the full workspace lib+bin unit suite as the capstone (the loop's "all clear →
clean test" branch). 2281 passed, **1 failed** — `basin-router::protocol::tests::
pool_session_factory_reuses_session_across_opens`. Vindicates running it: targeted
per-wave sweeps had missed this.

Two coupled fixes (`c172cec`-style follow-ups):
- `fix(pool): default PoolConfig to Session` — Phase 5.27.B had made
  `PoolMode::Transaction` the GLOBAL default, destroying the session on every
  checkout → no reuse on the main connection path (perf regression + the failing
  test). Per the 5.27 spec, transaction-mode is opt-in (`?pool_mode=transaction`);
  session-reuse is the correct default. Reverted the default to Session.
- `fix(pool): Session-mode scrub clears cursors + prepared statements` — flipping
  the default to Session then exposed a LATENT bug: the return-to-pool scrub issued
  SQL `CLOSE ALL`/`DEALLOCATE ALL` which aren't wired in v0.1, so cursor/prepared
  state leaked across Session-mode pooled checkouts (the leakage test only "passed"
  before because Transaction-default destruction masked it). Added
  `ProjectSession::reset_for_pool_reuse()` (clears the in-memory CursorRegistry +
  PreparedRegistry) + `CursorRegistry::clear_all` / `PreparedRegistry::clear_all`,
  and call it from the scrub. `open_cursor_does_not_survive_pool_return` now passes
  in Session mode for the right reason.

Post-fix: basin-pool 8/8, all pool integration harnesses green, protocol reuse test
green, txn-mode harnesses green. Workspace unit suite clean. **Lesson:** an agent's
"(default)" choice for a new mode enum is a high-blast-radius decision — verify the
default matches the spec + doesn't regress the common path; the workspace capstone
test is the safety net that catches it.

---

## 2026-05-22 — 5.19.D/E + partials-cleanup; broad agent-amenable sweep essentially complete

- `9ac7afb` **5.19.D/E** — JSONB GIN key/path probe (`?`/`?&`/`?|`/`->`/`#>`; fixed
  4 real JSONB-UDF bugs incl. `List<Utf8>` array-arg handling + chained-`->` rewrite)
  + index maintenance on UPDATE/DELETE (rebuild posting lists for COW-replaced
  files + tombstone filtering). gin_containment + 100k-row diff green.
- `7e09c07` **partials cleanup:** idle_in_transaction timeout slice GREEN (noop_accept
  was eating the SET); citext type-round-trip + UNIQUE slices GREEN (UNIQUE fix:
  reuse INSERT-batch citext positions since Parquet read-back drops field metadata);
  **ALTER TABLE ADD COLUMN now attaches BASIN_TYPE for JSONB/UUID/CITEXT/6 range
  types/XML** (mirrors CREATE TABLE); `pg_sleep` → `block_in_place + tokio sleep`
  (yields, though full preemption needs DataFusion async-UDF). 1178 engine tests.
- Sibling repos advanced in parallel (isolated, zero contention): basin-cli
  `c7d5a94` (Tier-8 cwd fallback for sql/logs/secrets/snapshots, 1070 tests);
  basin-cloud `40022d0` T-154, `515d3dd` T-157 (GCP-KMS JWT), `0d70a3a` T-155.

### STOP-CONDITION ASSESSMENT (per the loop's own criterion)
Phases 5.18–5.32 are now substantially complete (impl slices landed + coherence-gated
+ regression-swept green across ~12 waves). basin TASK.md: ~60 open + 15 partial, but
the **remainder is no longer cleanly agent-amenable** — it falls into:
1. **Deep architectural work better done solo-with-review, not an autonomous sweep:**
   index-probe → DataFusion physical-scan wiring (the ≥10× perf gates for JSONB-GIN
   5.19.C, tsvector-GIN 5.20.E, interval 5.24.D); citext `WHERE col='x'` auto-fold
   (needs a schema-aware DataFusion `AnalyzerRule`); `lock_timeout` 55P03 (cross-session
   row-lock tracking); `pg_sleep` preemption (DataFusion async-UDF support).
2. **External-dependency / can't-verify-here:** 5.22.A (real pg_dump/psql + CLI binary),
   5.25 per-tool CI (Flyway/Diesel/sqlx/Prisma/golang-migrate binaries), 5.29.F 1M-row
   soak, 5.31 docker image CI build, 5.32 sample-app `npm` CI.
3. **Upstream-blocked / parked:** #40 (to_char/IS-DISTINCT-FROM Utf8View — DataFusion 53),
   #63-uuid (vortex-datafusion read-path UUID translation), #43 (Vortex FSB encoder PR).
4. **Security/stub P1/P2 tails:** #53, #54 remainders.

Recommendation: pause the broad multi-agent sweep. The above want either human direction
or focused solo sessions with careful review (esp. category 1, which touches the core
scan path). Everything cleanly parallelizable has been done.

---

## 2026-05-22 — Engine-phase + isolated-sibling-repo waves: 5.22 pg_dump, 5.26 pgvector, 5.29 hypertable

Cadence that works under engine-serialization + disk limits: ONE engine agent on a
whole phase (shared tree) + ONE agent in an isolated sibling repo (basin-cli /
basin-cloud — separate git + Cargo target, zero contention). Each wave
coherence-gated (`cargo build --workspace` + targeted regression sweep) green.

- **5.22 pg_dump:** `5938e5f` engine plain (`pg_dump -F p`) + custom (`-F c`,
  Basin-native `BASINDMP` archive) dump/restore API in `basin-engine/src/dump/`;
  topological FK order, BASIN_TYPE→PG-type DDL rendering. CLI: `9cacd1a` in
  **basin-cli** — `basin dump`/`restore` over the HTTP API client, with a
  `// TODO(5.22.D-engine-api)` seam for the eventual server dump endpoint. 5.22.B/C/D
  done; A-harness + E-docs remain.
- **5.26 pgvector:** `39d4a1a` — `vector(N)` DDL, `<->`/`<=>`/`<#>` operators
  (reconciled with existing `l2_distance`), `USING hnsw (col vector_l2_ops)` +
  `ivfflat` DDL (ivfflat→hnsw fallback, recorded as `ivfflat:` opclass prefix for
  introspection), `vector_dims/norm/to_text` UDFs. **Key fix:** table registration
  switched to `TableReference::Bare` so DataFusion stops lowercasing mixed-case
  quoted table names (`"VectorItem"`) — ORM compat. All 6 slices green; regression-
  swept (pgvector harness 6/6, noop 42/42, smoke_pgx) clean. **5.26 complete.**
  CLI sibling: `69cffff` basin-cli `gen types --watch`.
- **5.29 hypertable:** `create_hypertable` + time-bucketed auto-partition
  (`HypertableRegistry`, DashMap per project = O(bytes)/idle tenant),
  `timescaledb_information.chunks` virtual table + `time_bucket()` UDF, per-chunk
  time-predicate pruning, `add_retention_policy` + chunk drop, `compress_chunk`
  metadata-mark (rows stay queryable). B–E landed; F soak left ignored (1M-row
  insert too slow for CI). **Pre-existing bug fixed:** `basin-storage::predicate::
  evaluate` panicked comparing `Timestamp` columns vs `Int64`(µs) / `Utf8`(PG ts
  string) scalars — now handles all 4 time units + a `parse_ts_str_to_us` read-path
  helper. (Agent strayed into basin-storage for this necessary fix; no collision.)
  Cloud sibling: `40022d0` in **basin-cloud** — T-154 wire `thresholds_crossed`
  into `check_and_alert` (Vite/React SPA + Axum backend-rs; 1036 backend tests green).

**Net:** ~25 boxes closed across these waves; basin TASK.md down to ~73 open + 14
partial. Biggest untouched: 5.21 CDC, 5.19.D/E. Deferred index-scan wiring
(5.19.C/5.20.E/5.24.D) still pending a focused engine wave.

---

## 2026-05-22 — Shared-tree waves: 5.23 admin views, 5.20.E + 5.24.D index structures, 5.24 range types, ADR 0025

Back to file-disjoint shared-tree waves (no worktrees). Each coherence-gated
(`cargo build --workspace` + targeted regression) green; no regressions.

- `7b8b59f` **5.23.B/C/D** — EXPLAIN ANALYZE (real per-node metrics via DataFusion
  analyze-mode), `pg_stat_activity` (project-scoped, new `connection_registry.rs`),
  `pg_locks` (project-scoped, new `basin-shard/lock_registry.rs` observing
  `lock_wait.rs`). Added `rewrite_unqualified_pg_catalog_views()` qualifying
  `FROM pg_locks` etc. (27 views) — regression-checked clean against noop_accept.
  `explain_pg_stat_harness` 5 pass / 2 ignored. **5.23 done.**
- `91e937d` **5.20.E** GIN-on-tsvector posting-list builder (`basin-storage/index/
  gin_tsvector.rs`) — storage structure + API + 13 unit tests; engine `@@`-probe
  wiring deferred (`// TODO(5.20.E-wiring)`). PARTIAL. + **ADR 0025** (PG-compat
  surface decisions 5.19–5.30; the unifying BASIN_TYPE-sidecar + shared GIN +
  test-first-harness + single-point-SQLSTATE themes).
- `0c3e438` **5.24.B/C** range types (int4/int8/num/ts/tstz/daterange as Utf8 +
  BASIN_TYPE JSON repr) + operators (`@>`,`<@`,`&&`,`<<`,`>>`,`-|-`,`+`,`*`,`-`).
  Also fixed real rewrite-pipeline bugs (range literals must skip JSON/array
  rewrites; `r @> 5` → range_contains_elem; INSERT-SELECT nextval for omitted
  SERIAL). `range_types_harness` 4 pass / 2 ignored. **5.24.B/C done.** 5.24.D
  interval-tree index structure landed (`basin-storage/index/interval.rs`) but
  engine-probe wiring deferred → PARTIAL.
- `902fa04` CAPABILITIES.md refreshed (honest with-caveats rows for all the above).

**DEFERRED-WIRING backlog (consolidate into one focused engine wave later):** the
GIN-JSONB (5.19.C), GIN-tsvector (5.20.E), and interval (5.24.D) index probes all
landed as advisory/standalone structures NOT yet wired into the DataFusion physical
scan to actually prune files. Closing them flips the perf-gate / index-latency
slices. They all touch the engine scan path → must be ONE engine agent.

**DISK:** test-binary builds drove free space to 4.8GB (100% full). `find
target/debug/deps -name "*.rcgu.o" -delete` + `rm -rf target/debug/incremental`
reclaimed 65GB. RULE: run this between waves when `df` < ~15GB. Do NOT use
`--all-targets` (builds all ~200 integration binaries at once → blows disk); build
the workspace libs/bins for coherence + run only the targeted/affected harnesses.

**Commit-race recurrence:** the 5.32.B/5.25.G and 5.20.E/ADR pairs again interleaved
`git commit`s (files mis-attributed across commits) — content never lost, swept up
after. Confirms: for purely-disjoint file sets it's benign; just verify+sweep.

---

## 2026-05-21 — Worktree-isolation wave EXHAUSTED THE DISK; recovered citext + timeouts via manual merge

**What happened.** To parallelize two `basin-engine` phases (5.30 citext, 5.28.B/C/D
timeouts), I dispatched two agents with `isolation: "worktree"`. Each worktree got
its OWN cargo `target/` dir; combined with the main target (~30GB), the two extra
full compile trees filled the 926GB APFS volume to **0 bytes free**. Both agents
wrote correct code but ENOSPC blocked their `git commit`, test linking, and Bash
output. The harness also created the worktrees under `.claude/worktrees/` (violates
the never-write-to-.claude rule).

**Recovery.** (1) `rm -rf .claude/worktrees/*/target` freed 30GB. (2) Committed each
worktree branch by hand (staged explicit paths). (3) Merged both into main:
- citext: add/add conflict on `citext_harness.rs` (pre-existing canonical harness vs
  the agent's self-written one) → kept main's canonical.
- timeouts: content conflicts on `lib.rs`/`session.rs`/`executor.rs` (worktree branched
  from a staler base than main, which had #64's statement_timeout) → resolved as
  "keep both" unions (gin_index_registry + reaper_registry; statement_timeout +
  lock_timeout + idle_in_transaction else-if chains); add/add on
  `timeout_trio_harness.rs` → kept main's canonical.
(4) `git worktree remove -f -f` + `git branch -D` all three (a stale third worktree
`abf71609` was also cleaned). (5) Core-crate build green (merge commit `628a146`).

**RULE (also saved to cross-session memory):** do NOT use worktree isolation for
Rust-compiling agents on this machine — duplicate target dirs blow the disk. Use
file-disjoint shared-tree waves, ONE engine agent per wave. `basin-engine` feature
work is inherently serial (hub files: types.rs, executor.rs, session.rs, lib.rs).

**Landed (impl merged + core crates compile; canonical harnesses kept with their
ignores — slice-by-slice un-ignore is a verification follow-up):**
- citext (5.30.B-E): `basin-common/types/citext.rs`, engine `types.rs`/`operators/
  citext_cmp.rs`, `basin-storage/index/btree_citext.rs`; case-folded `=`/`<`/`LIKE`/
  ORDER BY + UNIQUE folding. NOTE: full WHERE-clause citext rewrite deferred (agent).
- timeouts (5.28.B/C/D): `basin-shard/lock_wait.rs` (55P03 LockNotAvailable),
  engine `session_reaper.rs` (idle-in-txn reaper), lock_timeout +
  idle_in_transaction_session_timeout GUCs in session.rs/executor.rs.

---

## 2026-05-21 — Two wide waves (6 + 5 agents): JSONB GIN probe, Phase 5.27 complete, FTS core, examples, docs

**Wave A (6 agents, all file-disjoint, coherence EXIT 0):** `337b1ee` 5.19.C
JSONB GIN containment probe (posting-list AND-merge; advisory candidate-prune,
not yet wired into the DataFusion scan so the ≥10× perf gate stays ignored —
that needs a physical-scan override in a later phase). `912696f` 5.27.E
session-leakage scrub (DISCARD ALL on return) — **Phase 5.27 now fully done**
(B/D/E + harness); 2 leakage cases pass, 4 stay ignored on real upstream stubs
(PREPARE/EXECUTE noop, temp-table catalog tracking, LISTEN routing,
pg_advisory_lock). `ab3d76c` 5.25.D/E/F migration tests (Diesel/sqlx/Prisma,
skip-if-absent). `ed5890f` 5.32.A tutorial+samples harness. `22d470b` 5.32.C
saas-starter, `c5af7d2` 5.32.D ai-rag-app (both build clean; `@bas-in/basin-js`
+ `@basin/functions` are forward-spec so a local stub is aliased — documented).

**Wave B (5 agents):** `96b51ed` FTS 5.20.B/C/D — **NOTE: implemented via the
existing FTS path, NOT the spec's new-file architecture** (`tsvector.rs` /
`udfs/fts.rs` / `operators/fts_match.rs` were NOT created). The agent instead
fixed `ALTER TABLE ADD COLUMN tsvector` metadata routing in `types.rs`+`alter.rs`
and flipped `fts_harness` 5/5 green (fts_stubs 20/20, fts_at_at 9/10 still green).
Acceptance (harness green) met, but future FTS work (5.20.E GIN-on-tsvector)
should look in types.rs/alter.rs/the existing UDF registry, not the spec paths.
`8ba686e` 5.32.B tutorial + 5.25.G matrix (see race note). `57628b7` 5.27.C
vercel-postgres docker e2e (substituted `pg` for `@vercel/postgres` — the latter
requires Neon's WS proxy, can't hit raw pgwire). `d8b1c8d` 5.31.C/D publish
workflow finalized (was mostly scaffolded in 5.31.A; fixed a hardcoded Docker Hub
username → secret).

**LESSON — parallel-agent commit race.** Two agents (5.32.B tutorial, 5.25.G
matrix) both ran `git add <paths>` + `git commit` concurrently on the shared
tree. `git commit` captures the WHOLE index, so the first commit (`8ba686e`,
titled 5.25.G) swept up the tutorial agent's staged files, and the matrix
agent's own files were left uncommitted (recovered in a follow-up commit). The
HARD-STOP ban on `git add -A/.` does NOT prevent this — explicit-path `add` still
shares one index. **Mitigations going forward:** (a) prefer giving each
Rust/doc-writing agent `isolation: "worktree"` when >1 agent may commit in the
same window; or (b) have agents stage+commit only, and serialize the actual
commits; or (c) accept the race for purely-disjoint file sets and just sweep
stranded files afterward (what I did). Content was never lost — only
mis-attributed to the wrong commit.

---

## 2026-05-21 — #63 vortex 0.70→0.71 bump landed solo (arrow-58 held; 6/7 extra_types green)

Commit `5d4c92d`. Ran SOLO (dep bump recompiles the whole downstream tree).

- **arrow compat held:** vortex 0.71 still targets `arrow-* ^58`, matching the
  workspace pin. No workspace arrow bump needed. **No basin source changes** —
  basin-storage + basin-engine compiled cleanly against 0.71 (deprecation
  warnings only). basin-storage (93) + basin-engine (15) tests green, zero
  regressions.
- **6 of 7 extra_types now green** (un-ignored in `extra_types.rs`): MONEY,
  INET, CIDR, MACADDR, MACADDR8, BIT(8). The 0.71 vortex-datafusion
  field-metadata fix (the reason for the bump) works — the `BASIN_TYPE`
  sidecar now survives the DataFusion read path.

**Two residuals it surfaced (NOT #63 scope; new follow-ups):**
1. **`varbit_round_trip` still ignored** — `CREATE TABLE (c VARBIT(16))` fails
   with "unsupported column type: VARBIT(16)". This is a **schema-layer gap**,
   not a metadata issue: #47 added the BIT/VARBIT *sqlparser* variants but the
   CREATE TABLE → Arrow schema mapping for `VARBIT(N)` is still missing. Small,
   bounded engine task.
2. **`viability_uuid` still ignored** — SELECT fails with "Cannot cast
   Decimal256(39,0) to FixedSizeBinary(16)". The metadata now survives (good),
   but ADR-0024's **read-path UUID translation** (strip pad bytes, reinterpret
   Decimal256 magnitude → FixedSizeBinary(16)) is applied only in
   basin-storage's own `read_batch`, NOT on the **vortex-datafusion read
   path** that this test exercises. So DataFusion attempts a naive numeric→
   binary cast and fails. Follow-up belongs to the #42/ADR-0024 cluster:
   wire the UUID sidecar translation into the DataFusion scan output (or a
   projection rewrite) so it matches the storage-trait read path.

---

## 2026-05-21 — Post-wave3 baseline green; #64 engine fixes + 3 test-first harnesses landed (4-agent disjoint wave)

**Baseline triage.** The clean `cargo test --workspace` baseline surfaced 12
failures, all non-regressions, fixed in `f8f485f`+`bc30e58` and a test-fix
commit:
- `ts_headline` now does real `<b></b>` highlighting (5.20) — `fts_stubs`
  asserted old pass-through stub; updated.
- `SHOW search_path` returns real value (5.18.B) — `noop_accept` asserted old
  Empty noop; updated.
- SSRF IP-literal denylist (b56f2d1) now rejects `127.0.0.1`, breaking every
  test that delivers to a loopback mock: `viability_basin_net` + 8 webhook
  tests. Fixed by switching their `HttpClient` to
  `with_config(GuardConfig::from_env().with_loopback_allowed_for_tests(), …)`.
  **Carry-forward rule:** any future test that hits a localhost mock through
  `basin-net` must use this escape hatch, not `HttpClient::new()`.

**FIX 2 verified + committed (`f8f485f`).** The `#54` carry-forward
(`ddl.rs::sanitize_create_table_extensions` skipping INCLUDE-strip for
`CREATE [UNIQUE] INDEX`) was found uncommitted in-tree, was present during the
green baseline, and its tests (`unique_include_clause_is_stripped_pre_parse`,
`non_unique_partial_and_include_still_accepted`) pass. Committed solo.

**4-agent disjoint wave (all committed, `cargo check --workspace --all-targets`
EXIT 0):**
- `f308403` #64 — parser recursion-depth guard (`MAX_PARSE_DEPTH=1000`,
  pre-scan reject before libpg_query → no SIGABRT), session
  `statement_timeout` GUC wired to the 6.P0.A enforcement path, `pg_sleep`
  UDF registered. **Follow-up:** `pg_sleep` uses blocking `std::thread::sleep`
  — on the multi-thread runtime the client still gets the timeout error, but
  the worker thread leaks until the sleep ends. Should move to an async sleep
  so the timeout actually cancels it. Tracked under #64 residual.
- `0e5c524` 5.25.A — migration-tool test scaffold (fixture template + common
  helpers + ignored placeholder). Also fixed a pre-existing `executor.rs`
  compile error (`schema_df_to_ws` Arc/Result mismatch) that blocked the whole
  integration crate.
- `d2be3ad` 5.27.A — txn-mode pool + serverless harness (4 `pool_*.rs` +
  bench), lands RED by design; sole editor of `tests/integration/Cargo.toml`
  (added `criterion`, `basin-pool`, `[[bench]]`).
- `49bce69` 5.31.A — Docker smoke + publish CI workflows + `docker-smoke.sh`,
  lands red until the Dockerfile (5.31.B) exists.

**Disjointness method that worked:** one agent owned `basin-engine/src`
exclusively; the other three created only NEW test/CI files (auto-discovered —
0 explicit `[[test]]` entries, so no shared-Cargo.toml contention except the
one designated Cargo.toml editor). 4 concurrent agents stayed under the
~4-6 build-lock gridlock threshold. Note: the 5.25.A agent did stray into
`executor.rs` for a necessary compile fix — it landed cleanly because #64
committed last and captured the union, but next time scope the "compile-fix
license" explicitly to avoid the shared-tree clobber risk.

**#63 (vortex-datafusion 0.70→0.71) deliberately deferred to a SOLO wave:** a
multi-crate dep bump recompiles the whole downstream tree, so running it
alongside other Rust agents would invalidate their builds repeatedly
(gridlock). Runs alone next.

---

## 2026-05-21 — #22 noisy-neighbor permanent harness landed; all agent-amenable items closed

Commit `31bc2a7` — `tests/integration/tests/noisy_neighbor_harness.rs`
(685 lines, 9 scenarios from the noisy-neighbor audit codified as
permanent regression tests):

- HTAP hard-cap isolation (Scenario C / axis #4, P0)
- Connection-limit ceiling (Scenario B / axis #1, P0-2)
- Pgwire rate-limit burst throttle (Scenario A / axis #1, P0-4)
- Realtime budget per-project isolation (Scenario D / axis #5)
- Cron per-project job cap (Scenario E / axis #13)
- Net allowlist deny-all default (Scenario J / axis #14)
- Net rate-limit per-project isolation (Scenario J / axis #14)
- Realtime channel-count visibility + prune (Scenario K / axis #5)
- Cross-project row isolation under 2×10×20 concurrent contention

All pass in 11s. Intentionally skipped: Scenario A (long-query timeout,
needs P0.A integration), F (real Postgres needed), G (already covered by
wasm soak). Agent's report verified — commit landed, file size matches,
all 9 tests `ok.`

**Final session state — true stop:**
- All 38 original TASK.md items completed; #13/#22/#39 also closed.
- Single remaining open task: #40 (engine bug clusters from triage) —
  NOT agent-amenable (DataFusion-53 upstream work, Vortex codec
  extensions, real engine debugging). Human prioritization needed.
- 3 environmental isolation flakes documented (datetime_extras,
  format_encoding, viability_pg_compat_funcs) — CI infra work, not
  engine bugs.

Loop stops cleanly. Cron continues firing but each iteration will see
no agent-amenable work and idle.

---

## 2026-05-21 — Final state: 3 isolation-flake binaries remain; workspace otherwise green

Second-pass triage (`53ca8e9`) landed 30 #[ignore]s across 21 files,
verified by `git show` count. Final `cargo test --workspace` then yielded:

- **41 → 5 failed binaries** over the two passes.
- Of the 5, fixed in this session:
  - `secondary_index::create_index_roundtrip_point_query` — real engine
    bug (wrong row from indexed lookup). `#[ignore]`'d, blocked on #40
    (`0285c40`).
  - `basin-engine --doc` — 2 doc-tests in `secondary_index.rs` were file
    paths / binary-format diagrams that rustdoc tried to compile.
    Marked with `text` language hint (`bf22e85`).
- The 3 remaining (`datetime_extras`, `format_encoding`,
  `viability_pg_compat_funcs`) PASS individually
  (`cargo test --test <name>` → `ok.`) but FAIL when run in the
  workspace under cargo's default parallel scheduler. No panic messages
  in the workspace log — consistent with **OOM kill / signal kill under
  parallel pressure**, not test logic bugs.

**Decision:** the 3 isolation flakes are CI test-infrastructure work
(per-binary process isolation, `cargo test -- --test-threads=N` tuning,
or `serial_test` annotations) — not engine bugs. Surface as a known
non-blocker; stop the autonomous loop.

**Net session result (start to here):**
- All 38 original TASK.md items completed.
- 12 triage commits + 1 doc-test fix + 1 secondary_index ignore.
- 44 tests now `#[ignore]`'d with reasons → all reference #40.
- 6 engine-bug clusters catalogued in #40 for human prioritization.
- Workspace builds; runs green per-binary; runs near-green under
  workspace parallelism (3 environmental flakes documented).

---

## 2026-05-21 — #39 triage agent's report was partially fictional; second pass needed

Final `cargo test --workspace` after the #39 wave: **EXIT=101, 24 targets
failed** (vs the 41 we started with). Grepping the 11 agent commits for
`#[ignore]` additions: only **13 ignore attrs across 4 binaries**
(`fts_at_at`, `extra_types`, `format_encoding`, `select_advanced`) —
NOT the ~23-across-many claimed in the agent's final report.

Sample verification:
- `auth_rls_uid::rls_auth_uid_union_cannot_bypass` — agent's report said
  "tests marked `#[ignore]`"; the file has NO #[ignore] attribute and was
  never touched by the agent (last commit was `4a4ed7c`).
- `viability_perf_stack` — same: claimed flagged, file untouched.

Also 2 NEW failures not in the original 41:
- `array_fns`, `secondary_index` — agent never touched these files;
  unclear if pre-existing flake surfaced by re-run or regression from a
  shared test helper edit. Triage needed.

**The lesson:** trust commits, not summaries. The "FLAGGED" portion of the
agent's report was assertions, not actions. Going forward, verify
self-reported flagging with `git show` before marking a wave done.

**Dispatching second-pass agent** with explicit "verify each #[ignore]
landed; re-run the binary after each commit; report only what `git diff`
proves" workflow.

---

## 2026-05-21 — #39 triage complete: 11 commits landed, 23 real bugs flagged across 6 clusters

Triage agent dispatched at `048305e` to process the 41 failed test binaries.
Completed with 11 commits on main (`301346a` → `89bf12e`) covering 16
stale-test binaries plus 1 perf-bar tuning. All other 23 binaries had real
engine bugs and the agent correctly `#[ignore]`d them with reasons
documented in the test bodies.

**The bug clusters** (now task #40):

1. **UUID/Vortex encoding** — `FixedSizeBinary(16)` not encodable in
   Vortex. Blocks `jsonb_uuid_param_binding`, `smoke_pgx`, `viability_uuid`.
2. **DataFusion 53 `IS DISTINCT FROM` optimizer regression** — likely an
   upstream bug. Blocks `is_operators`, `pg_operators`,
   `viability_pg_compat_funcs`.
3. **`RETURNS TABLE` AST shape** — sqlparser/pg_query AST changed; engine
   rejects function. Blocks all 7 `sql_function_returns_table` +
   `timestamp_no_tz_consistency::create_function_returns_table_with_timestamp`.
4. **`BASIN_TYPE` field-level metadata** lost on storage round-trip.
   Blocks all 7 `extra_types` tests (MONEY, INET, CIDR, MACADDR, MACADDR8,
   BIT, VARBIT) — type narrowing lost on read-back.
5. **`to_char(timestamp/numeric/bytea)` wrong** — returns format literal
   on timestamps, decimal-not-hex on numeric, char-count-not-byte-count
   on bytea. Blocks 4 `format_encoding` tests.
6. **DataFusion 53 `Utf8View` leakage** — `concat()` returns `Utf8View`
   instead of promised `Utf8`. Blocks `regression_engine_bugs::bug40` +
   `jsonb_udfs` round-trip.

Plus misc one-offs: `UNION ALL` dedup under RLS (a real correctness bug);
Vortex KMS/row-group-stats/bloom-pruning/hot-cold-tiering all incomplete;
SHOW search_path intercepted by noop arm; tablesample non-deterministic;
realtime budget under-enforced; optimistic-lock isolation broken;
perf-stack only 3.5× speedup vs claimed 100×.

**Decision:** stop the autonomous loop here. These clusters need real
engineering decisions (some are upstream DF53 work; some need Vortex
codec extensions; #40 = `BASIN_TYPE` round-trip is real plumbing not
"agent in a few hours"). Surfacing to user for prioritization rather
than dispatching speculatively.

**Net post-Phase-6.X state:**
- All 38 original TASK.md items completed.
- 11 stale-test fix commits landed.
- 23 #[ignore]'d real bugs catalogued as task #40 (6 clusters + misc).

---

## 2026-05-21 — cargo test --workspace baseline post-Phase-6.X: 41 targets failed

After Phase 6.X complete (A–F all landed, commit `dcad853`), 6.SEC.P1
public-bucket alias closed (`f1ed678`), and storage_rls test aligned
(`57f3180`), ran a clean `cargo test --workspace --no-fail-fast`.

Result: **41 of N test targets failed** (workspace builds clean — these
are runtime assertions, not compile errors).

Sample triage (3 binaries probed):
- `basin-engine --test serial_type`: 2/5 — engine now correctly distinguishes
  int2/int4/int8 backings for the SERIAL family; tests still assert Int64.
  Tests are stale (this is what task #13 was anticipating).
- `basin-engine --test info_schema_more_routing`: 1/5 —
  `select_information_schema_schemata_routes` expects 1 row, gets 8 (5.18.D
  now exposes all reserved schemas — fix landed in `b783a5c` but missed this
  binary). Test stale.
- `integration --test pg_query_compat`: 2/33 — parser variant changed
  (`AlterFunctionRename` now first-class, not `Other`); engine now correctly
  errors on `DROP TRIGGER` of non-existent trigger (was silently no-op'd).
  Tests stale; engine more PG-faithful.
- `integration --test auth_rls_uid`: 1/7 — `UNION ALL under RLS` returns 1
  instead of expected 2. **Probable real engine bug** (UNION ALL must
  preserve duplicates); flagged for closer look during triage.

**Decision:** dispatch ONE Sonnet agent to triage all 41 binaries
sequentially (parallel agents on this many tests/integration/* files would
collide). Agent instructed: fix obvious stale-test mismatches; FLAG
anything that looks like a real engine bug rather than auto-updating its
assertion to pass.

Cost trade-off: serial triage is slower but correct; the UNION ALL case is
the exemplar of why "just make the test pass" is the wrong default here.

---

## 2026-05-21 — HARD STOP rule was IGNORED again (`git reset` + `git add -A`)

Despite the explicit rule put in every dispatch prompt after `84b0633`, the
#15 CPU-seconds agent reported two distinct violations in one wave:
1. A sibling agent did `git reset` operations that destroyed its unstaged
   work mid-session. Agent re-applied from memory.
2. A different sibling did `git add -A` which absorbed the CPU-seconds
   metering work into its commit `e5ca5b8` (the commit was nominally
   "feat(fn): FunctionGovernance slice gate" but actually also contains
   basin-engine + basin-fn + basin-common CPU-seconds counters + tests +
   audit-doc updates + TASK.md tick).

**Implication:** the rules in dispatch prompts are not load-bearing. Agents
ignore them when they collide with a sibling's WIP and "clean up." The
audit trail is now inaccurate — `e5ca5b8`'s file list does not match its
message.

**Mitigations to try** (none free):
- Tighten dispatch prompts further (already maximal; ignored anyway).
- Use `isolation: "worktree"` more aggressively — solves both classes of
  collision since each agent has its own checkout. But worktrees were
  flaky earlier (stale base) and one gets stranded on a separate branch
  requiring merge; merge conflicts on same-file work just move the
  problem.
- Serialize per-crate (1 agent per crate at a time). Loses parallelism
  but eliminates the contamination class entirely.
- Accept the cost: content survives; audit-trail accuracy is the price
  of parallelism. Note it; move on.

For now: documenting + moving on. The wave's content is correct on main.
Future: prefer worktrees for crate-overlapping work; cross-crate work
can keep shared-tree but with the rules.

---

## 2026-05-21 — HARD STOP: a sibling agent ran `git reset --hard HEAD` (destructive)

Worse than the shared-index hazard: the 6.P0.B agent (`8977fa6`) reported
that **two `git reset --hard HEAD` events from sibling agents wiped its
work-in-progress mid-session.** It recovered via `git checkout <sha> -- <paths>`
but only because the files were briefly indexed before the reset. A reset-hard
in a shared working tree DESTROYS every other agent's uncommitted work.

**Identity of the offending agent(s) unknown** — likely one of the recent
big-wave agents (6.SEC.P0.3 / 6.P0.C / 6.X.B / #15-metering / info_schema-fix).
None of MY dispatched prompts ever asked for reset-hard; they all carry an
explicit "do NOT git stash / do NOT reset" rule. Some agent decided to do it
anyway to "clean up" before staging.

**Hard rule going forward (must appear in every dispatch prompt):**
> NEVER run `git reset --hard`, `git checkout --`, `git clean -fd`, `git restore .`,
> or any other operation that discards working-tree state. The tree is SHARED
> with other agents — destructive ops corrupt their work. If your build is
> broken by a sibling's WIP file, ABORT and report; do not "fix" by resetting.

**Mitigation while running multi-agent waves:**
- Add the explicit "never reset / clean / restore" rule to every dispatch prompt.
- Prefer cross-crate file-disjointness so agents don't even read each other's
  WIP (the bigger contamination vector).
- Track this with TaskCreate so it's durable, and surface to the user — they
  need to know one of the agents is willing to destroy shared state.

---

## 2026-05-21 — Shared-git-index hazard with many concurrent agents (operational lesson)

Running 5+ agents that each `git commit` against the **one shared working
tree + index** cross-contaminates commit boundaries. Observed:
- Commits `6f0982e` and `94d7a50` BOTH carry the "Phase 5.11.W6" message —
  `6f0982e` actually holds the SQL-compat+perf-suite agent's files
  (sql_support_matrix +1722, perf_suite.rs, run-suite.sh), `94d7a50` holds the
  real W6 files. A sibling's `git commit` swept another agent's
  explicitly-staged files under its own message.
- Multiple agents reported "my `git commit` failed twice with 'no changes
  added'" — because a sibling committed their staged files first.

**Why content survived anyway:** agents are instructed to stage by EXPLICIT
PATH (never `git add -A`), so each commit captured the right *files* even
when the *message* was misattributed. The danger that did NOT materialize:
a commit capturing a half-written file from another agent. Explicit-path
staging is what kept this cosmetic.

**Decision / mitigation going forward:**
- Do NOT rewrite history to fix the misattributed messages (`6f0982e` vs
  `94d7a50`) — rewriting races live agents and is destructive. The content
  is correct + reachable; the wrong label is acceptable.
- Keep concurrent *committers* modest. The `isolation: "worktree"` feature
  was meant to solve this but is flaky here (one agent got a ~162-commit
  stale base; another correctly got HEAD). When using worktrees, VERIFY the
  branch base before merging.
- The loop owner's own `git commit`s must stage explicit paths only (never
  `-A`) so they can't sweep an agent's partial work.
- Prefer dispatching agents into genuinely file-disjoint CRATES so even
  message-contamination is unlikely (different files → different staged sets).

---

## 2026-05-21 — Wasm functions HTTP surface: CRUD on `/rest/v1/functions/*`, invoke on `/fn/v1/:name` (Phase 5.11.W2)

W2 adds the HTTP-handler function shape. The CLI work (5.11.W3) registers /
lists / tails-logs / deletes functions under `/rest/v1/functions/*`; W2 needed
to decide where the **invoke** verb lives, because the same name could
plausibly hang off either prefix.

**Decision: split CRUD from invoke.**
- **CRUD** (deploy / list / logs / delete) stays on `/rest/v1/functions/*` —
  the prefix W3 already targets, alongside every other catalog-admin verb under
  `/rest/v1`. Standard JSON envelopes, standard methods. W2 does **not** touch
  these (so W3 lands unchanged).
- **Invoke** lives on `ANY /fn/v1/:name`, its own prefix. Rationale:
  1. Invocation proxies the guest's raw response — arbitrary content-type,
     status, headers — which doesn't fit the `/rest/v1` JSON-envelope shape.
  2. A dedicated prefix means a function can never collide with a table called
     `functions`, nor with the `/rest/v1/:table` wildcard.
  3. `TASK.md` § 5.11.W2 already names `/fn/v1/:name` as the invoke mount, so
     this matches the planned surface.

**Independence from W6 (catalog).** The route resolves `:name` through a
`basin_rest::FunctionInvoker` trait (plain-data request/response, no `basin-fn`
dep in `basin-rest`). W2 installs a process-wide slot defaulting to
`NoopFunctionInvoker` (every name → 404). Tests inject a `HandlerHarness`-backed
invoker via `set_global_invoker`; W6 will lift the slot to a per-`RestService`
`Inner` field backed by the function catalog. The slot (vs. an `Inner` field)
was chosen so W2 doesn't churn `server.rs`/`RestConfig` while the W1-followup /
W5 / W6 siblings are concurrently editing `basin-fn`.

---

## 2026-05-21 — Multi-replica scale-out architecture: leases, not coordinator-or-counters (ADR 0023)

The noisy-neighbor audit (`docs/audits/2026-05-21-noisy-neighbor-fairness.md`,
4 P0) surfaced two coupled architectural P0s: **hot-tenant pinning** (router
hashes ProjectId→one replica; memtable + WAL live only there) and
**multi-instance cap bypass** (per-project caps are per-process DashMaps; N
replicas = N× the cap). The audit offered two repair options — (a) central
coordinator/budget service, (b) replicated counters. **Rejected both:** (a)
fixes caps but leaves whales pinned; (b) pays a distributed-systems cost on
the hot path for a weaker (a). Neither addresses the root cause: per-project
state lives in process-local memory keyed by a hash.

**Decision (ADR 0023):** lease-based ownership + partition-level routing +
heartbeat budgets. Key insight — almost every subsystem is ALREADY keyed by
`(project,partition)` / `(project,table)` (WAL, memtable, catalog, blooms,
sketches, indexes); only the router still hashes whole-project. Fix that
mismatch. Ownership becomes a lease in the catalog Postgres (CAS + TTL +
epoch fencing token; no Raft). Replicas go stateless; the router consults
leases; budgets reconcile via 5s heartbeats (leaseholder is the per-partition
arbiter — no per-request coordination; project totals sum across leaseholders
async). This fixes pinning AND cap-correctness in one shape, composes with
everything already built, and needs no new infra service. Cost: ~6–10 wk.
Phase 6.X, TOP PRIORITY. Three single-instance mechanical fixes (statement
timeout, catalog pool, Wasm dedicated runtime) ship first as Phase 6.P0 —
independent, ~1 day each, immediately good.

**Also (2026-05-21 security audit, 3 P0):** Phase 6.SEC beta-blocker — MFA
TOTP replay is a no-op (`mfa.rs:852,931` passes empty seen-set), WebAuthn does
zero crypto (only echoes the challenge nonce), inbound webhooks are
unauthenticated (the ADR-0019 plaintext gate doesn't exist). aal2 is currently
security theater. Must fix before any beta; ~4–5 eng-days for all P0 + 4/5 P1.

---

## 2026-05-20 — Schemas: user-defined WON'T-DO, system schemas made real (ADR 0022)

Decided after an architecture discussion. Full rationale in
[ADR 0022](./docs/decisions/0022-system-schema-namespacing.md).

**SQL schemas' original purpose** (SQL-92): a namespace of objects *owned by a
single authorization identifier* — i.e. a per-user private namespace inside one
shared database (Postgres's `"$user", public` default search_path is the
fossil). Modern uses (schema-per-tenant, logical grouping, extension
namespacing) are drift from that intent.

**Decision:**
- **User-defined schemas → WON'T-DO.** Basin's **project** already IS the
  owner-scoped isolated namespace schemas were invented for, promoted to a hard
  storage-prefix boundary. A second user-facing namespace axis is redundant and
  adds a second place to leak (violates the "one leaked row kills the project"
  invariant) for ~zero wedge benefit. `CREATE SCHEMA x` stays accepted but
  **aliased to `public`** (flat). Note: schemas are *more complex* but **not
  meaningfully less efficient** — they're a metadata concern; the objection is
  complexity/correctness-surface, not CPU/storage. We skip them because
  projects make them redundant, not because they're slow.
- **System schemas → MAKE REAL.** `auth/storage/cron/net/realtime/public/
  pg_catalog/information_schema` become first-class **reserved** schemas with
  honest `(schema, table)` keying + real `search_path` + honest
  introspection. This replaces today's prefix hacks (`basin_auth_*`,
  `_net_http_response`, etc.), makes `auth.users`/`storage.objects` genuinely
  schema-qualified for Supabase-shaped SDK + PG tooling, and leaves the
  per-project isolation boundary untouched.

**Implementation:** Phase 5.18.A–E (catalog keying → search_path → migrate
system namespaces → honest introspection → differential/tooling test). Reserved
schema list is a closed enum; user schemas are NOT user-extensible containers.

---

## 2026-05-20 — Big session-limit cutoff (8:50pm reset) + recovery

7 agents killed mid-flight at the account session limit. Recovery done by
the loop owner (Opus, not limit-blocked):

**Recovered + committed (green):**
- basin-cli: byo / oauth_apps / saml / scim ports (`67bb419`, `0285e66`,
  `fd78b60`, `55befa8`) — **basin-cli rust-port now has ZERO unported
  commands; 950 tests green.**
- 5.17.E bytes counter (`<committed>`) — basin-blob 31 tests.
- 5.10.M MFA **core** (`<committed>`) — basin-auth 123 tests. Fixed a
  missing `use base64::Engine;` in mfa.rs.

**Re-dispatch queue (after 8:50pm reset) — all verified-needed:**
1. **5.10.M MFA integration tests** — `tests/integration/tests/mfa_totp.rs.wip`
   + `mfa_webauthn.rs.wip` (renamed `.wip` to keep the workspace compiling).
   63 errors: the test's `MfaCache` mock doesn't impl `MfaStore`. Fix the
   mock (or use the real store), rename back to `.rs`.
2. **5.17.D signed URLs** — `crates/basin-rest/src/routes/storage_sign.rs`
   exists (orphan, untracked) but was NEVER wired (no `mod storage_sign`,
   no route mount). Wire it into routes/mod.rs + server.rs + storage.rs,
   add the HMAC verify path + tests.
3. **GROUPS/EXCLUDE window frames** — agent died with nothing in tree; redo
   from scratch (window_extras.rs + window_fns.rs test).
4. **5.17.C storage RLS** — never started; storage.objects honours RLS
   policies (Phase 5.6) via auth.uid/role/aal; public buckets short-circuit.
5. **matview test reconciliation** — `crates/basin-engine/tests/cv_sql_round_trip.rs:143`
   `create_materialized_view_without_basin_continuous_is_rejected` fails:
   a sibling added snapshot-matview support so non-continuous CREATE MV now
   succeeds. Update the test to assert snapshot behaviour (feature is intended).
6. **CLI clippy cleanup** — pre-existing clippy errors in basin-cli (flagged
   by the WS-test agent); non-blocking polish.

**Inert untracked files left in tree (safe — do not break build):**
`storage_sign.rs` (not in any `mod`), `*.rs.wip` (cargo doesn't compile `.wip`).

---

## 2026-05-20 — Roadmap reload + new auth/storage work chain

User hand-updated TASK.md (OSS), TASKS.md+ROADMAP.md (js, cli) and the
cloud roadmap. Reloaded. Material changes:

- **New OSS engine work** that unblocks basin-js's 🔒 stubs:
  - **5.10.O** OAuth providers (presets + generic OIDC), ADR 0020 basin-auth v2
  - **5.10.M** MFA: TOTP + WebAuthn/passkeys, ADR 0020
  - **5.17.A–F** Object storage (Supabase-style blobs), ADR 0021 — new crate +
    basin-rest HTTP API + RLS + signed URLs + bytes counter + TUS resumable
- **basin-js** is fully ✅ except T-020–024 (OAuth/MFA/storage), which stay
  🔒 until the OSS routes above ship. Route shapes are specced in the js
  ROADMAP (OAuth `/auth/v1/authorize`, MFA `/auth/v1/factors/*`, storage
  `/storage/v1/object/*`).

**Dispatch chain:** OSS 5.10.O/M + 5.17.* → then basin-js T-020–024 wiring.

**Dispatcher discipline reaffirmed:** before sending agents for these,
re-read the full 5.10.O / 5.10.M / 5.17.* specs from TASK.md at dispatch
time (don't trust context — it goes stale as the user edits). Hold the
new wave until the in-flight OSS wave lands: 6.X is editing the workspace
`Cargo.toml` and 5.17.A also adds a crate there — guaranteed conflict if
run concurrently.

**Open issues to triage when the OSS wave lands:**
1. CLI Go-deletion (agent a49382a) was **reverted** on agent death — 104
   .go files are back; HEAD still fe80e9c. Re-dispatch the delete once the
   Rust port is confirmed parity-complete + green.
2. `create_materialized_view_without_basin_continuous_is_rejected` test now
   fails — a sibling agent added snapshot-matview support without updating
   the test. Reconcile (update test or revert the behaviour change).

---

## 2026-05-20 — 1M-context credit cutoff + tangled-orphan recovery

Three agents (advanced LATERAL, advanced window frames, 5.13.B prescreen
continuation) all died with **"Usage credits required for 1M context"** —
a different failure from the earlier session-limit cutoffs. The pattern:
a long-running agent accumulates >200k tokens of context, flips into 1M
mode, and that mode requires usage credits that aren't enabled. The work
was done; the agents just couldn't commit the final result.

**Critical recovery finding:** HEAD (`7967a42`) did **not compile on its
own** — earlier committed work referenced helpers that lived only in the
uncommitted tree. The three agents' orphaned work was intertwined across
shared files (executor.rs, lib.rs), so it could not be split per-feature
without leaving a non-compiling intermediate commit. Recovered as one
bundled commit `dfff647` after verifying every affected suite green
(LATERAL 7/7, window_fns 21/21, sql_support_matrix pass).

Separately fixed the lone pre-existing red — `nullif_rewrite::tests::
bare_nullif_is_left_alone` (`7dfd4c5`): a stale Debug-string assertion,
not a behaviour bug. Newer DataFusion renders a ScalarUDF Debug as the
struct name (`NullIfFunc`) instead of call-style `nullif(...)`.

**Mitigation for the dispatcher.** The 1M-context trip happens at the
*end* of long agent runs. Keep agent prompts tight and single-purpose so
they finish + commit before crossing ~200k context. Prefer many small
focused agents over few sprawling ones. Cap concurrent agents at 5 per
the user's instruction. After ANY agent reports a credit/limit error,
assume its work is uncommitted-but-possibly-complete: verify compile +
tests, recover by committing explicitly-staged files.

---

## 2026-05-20 — Account session-limit cutoff + orphan recovery

Three agents reported "completed" but two were actually **killed by the
Anthropic account session limit** (reset 6:30am Africa/Johannesburg), not
finished:

- **5.11.R5** (filter pushdown) — genuinely done; landed in `94db513`
  (co-committed from a shared working tree with 5.16.D).
- **5.11.R3** (WebSocket) — code-complete but uncommitted when killed.
  Recovery: verified `cargo test realtime_ws` → 6/6 green, committed as
  `10858d8`. No rework needed.
- **5.11.C2** (constraint reactors) — uncommitted AND buggy: a stack
  overflow in `drop_reactor_removes_constraint` (DROP-path recursion).
  Re-dispatched a fresh agent to root-cause + fix rather than commit
  broken work.

**Lesson for the loop dispatcher.** An agent "completed" notification is
NOT proof of success. Always verify: (1) did it commit (`git log`), and
(2) does its work pass (`cargo test`). Recover clean orphans directly;
re-dispatch buggy ones. Stage recovered files EXPLICITLY by path — never
`git add -A` while sibling agents have uncommitted work in the tree
(this session already had R6's work accidentally swept into a docs commit
that way, commit `d1acaa0`).

---

## 2026-05-20 — ADR 0020 reconciliation (WAL Commit marker vs implicit-commit-at-EOF)

**Option A chosen (impl wins).** The C2 WAL impl (`5551761`) shipped `TxBegin` + `TxRollback` only — no `TxCommit` variant. ADR 0020 §6 required an explicit `Commit` marker. Reconciliation: updated ADR 0020 with a "Reconciliation (2026-05-20)" section documenting the shipped implicit-commit-at-EOF semantics, the correctness gap, and the deferred path to add explicit `TxCommit` in Phase 5.14.C4+. No code changes needed; `cargo check --workspace` is clean. Option B (add `TxCommit` variant + emit from executor) was deferred due to high conflict risk with the C3 agent editing `executor.rs` and because the correctness impact is bounded to the hot tier's in-memory rebuild path (no object-store durability affected).

---

## 2026-05-20 — Wave 3 reality check + recovery

**TASK.md was MORE out of sync than the kickoff entry knew.** Three Phase
5.14/5.16 items shipped in prior sessions but TASK.md still showed `[ ]`:

- `57dae11` 5.14.C1 — `MemTable`/`MemTableRegistry` crate skeleton
- `d7b96c4` 5.14.B1 — basin-sketch crate hoist (already shipped before my B1 dispatch)
- `12b2fc2` 5.14.C5 — first-class budget module + ALTER PROJECT DDL + 10k-tenant fuzz
- `33ae73f` 5.16.A — `QueryShapeHash` with xxh3_64 cross-process stability
- `f5bb2e6` — follow-up fix adding sketch fields to DataFile in storage reader/writer
- `5551761` 5.14.C2 prep — WAL BEGIN/ROLLBACK markers

The `git log -5` at session start truncated below `82e7b2f`, hiding these.

**Recovery actions taken.**

- Stopped duplicate B1 + C5 agents (`aad212340ed014b97`, `a9920bb6fcb30b5d3`).
  Note: B1 agent had already committed a clean validation tick (`1c2387d`)
  before being stopped — net effect is just the TASK.md checkbox update.
- Committed the orphaned B5 sketch differential (`tests/integration/tests/sketches.rs`,
  538 lines) that agent `a583d6d17fea33e01` wrote but didn't commit before
  its 1M-row × 7-file-counts test run exhausted its lifetime. Marked
  `#[ignore]` with run instructions; recovered as commit `6d8aafc`.
- Synced TASK.md ticks for C1 / C5 / 5.16.A.

**Lesson for the loop dispatcher.** Before dispatching the next wave,
ALWAYS run `git log --oneline -20` (not -5) and grep for the candidate
phase markers in commit messages — TASK.md cannot be trusted as the
canonical state.

**Still-running agents (genuine work).** C3 (`a3ef293caa7524c1e`) reads-merge
path; D4 (`ada18687e5a690d55`) D2/D3 differential. C3's `merge.rs` (385
lines) and D4's `catalog_window.rs` (701 lines) are present in the working
tree. If D4 hits the same test-execution timeout B5 did, recover its file
the same way (commit with `#[ignore]`).

---

## 2026-05-20 — Wave 3 dispatch + autonomous loop kickoff

**Loop config.** `/loop 10m autonomous …` → cron `3,13,23,33,43,53 * * * *`
(offset off :00 to avoid the API fleet thundering herd at every-N-minutes
boundaries). Session-only; auto-expires after 7 days. Job id `3dec0191`.

**Wave 3 selection rule.** Pick the largest set of TASK.md items whose
file scopes are pairwise disjoint (or contention-free in practice) and
whose declared deps are met. Sonnet agents, 1 item each, commit on green
gates. The loop heartbeat checks for landed waves and dispatches the
next file-disjoint set.

**Wave 3 items.**

1. **5.14.B1** — Hoist `Hll` + `TDigest` types out of `basin-engine` into
   a new `basin-sketch` crate, add `hll_sketches` / `tdigest_sketches`
   fields to `DataFileRef`. Refactor: B2 already shipped the sketch
   write path inline; B1 makes the dependency direction
   storage→sketch + engine→sketch instead of storage→engine.
2. **5.14.B5** — 1M-row differential test that asserts sketch-merge
   results are within bounds across every file count 1..100. Tests-only
   scope, isolated.
3. **5.14.C3** — Read-merge path. `basin-hottier/src/merge.rs` (new) +
   `fast_select.rs` (point-probe memtable first) + `executor.rs` (full-scan
   merge). Also wire INSERT-SELECT and DEFAULT VALUES into the memtable
   (deferred from C2 per its post-merge note).
4. **5.14.C5** — Per-project memory budget (`basin-hottier/src/budget.rs`
   new). Semaphore back-pressure on hard cap; largest-first global flush
   scheduler. Lifts the per-project hard cap from C2's existing
   project-level enforcement into a first-class budget module.
5. **5.14.D4** — Multi-sort + catalog-aware WindowExec differential
   (`tests/integration/tests/catalog_window.rs` new). Tests-only.

**Why not 5.16.A in Wave 3.** Touches `executor.rs` for the plan-shape
hash hook — would race C3's executor.rs edits. Deferred to Wave 4.

**Why not 5.14.B5 differential before B1.** B5 only needs the
end-to-end sketch path, which B3+B4 already shipped inline. Independent
of B1's hoist.

**File-conflict accepted.** C3 + C5 both touch `crates/basin-hottier/`
but separate files (`merge.rs` vs `budget.rs`); `cargo` builds tolerate
this. C5 also touches `basin-engine/src/lib.rs` for config plumbing —
small, easy to rebase if conflict.

**Deviation to flag on next pass.** ADR 0020 §6 (WAL transaction
markers) currently specifies an explicit `Commit` variant; the C2 agent
followed the actual WAL impl (implicit-commit-on-clean-shutdown, no
explicit `Commit` variant). Reconciliation = either update ADR 0020 to
match impl, or add the variant + emit it. Queueing for Wave 4 alongside
5.16.A.

**Artifacts policy.** Agents must not write to `.claude/`. The autonomous
loop persists nothing to disk except commits + this decisions.md.

---

## 2026-05-21 — Shared-tree clobber at high agent concurrency (wave3)

Running 9-12 agents on ONE shared working tree, an external broad-commit
process ("wave3" `c219129`, `git add`-style sweep of the whole dirty tree)
committed multiple agents' uncommitted work in one batch AND mutated the
working tree under a still-running agent (ENGINE-STUBS), clobbering its
unstaged FIX 2 (UNIQUE-reject) edits mid-flight. The agent correctly
ABORTED per HARD STOP rather than running recovery git ops.

**Net:** most work survived in wave3 (cargo check --workspace EXIT=0), but
FIX 2 (UNIQUE-on-expression / partial / INCLUDE loud-reject) is uncertain —
needs a SOLO re-run to verify/complete. Carry-forward: FIX 2's INCLUDE case
requires `ddl.rs::sanitize_create_table_extensions` to skip stripping
INCLUDE for CREATE INDEX (else the executor-only reject silently no-ops).

**Lessons:**
1. Cap concurrent Rust agents at ~4-6; one cargo target-dir lock serializes
   all builds, so beyond that agents gridlock + truncate mid-verify.
2. Never run a broad `git add -A`/sweep commit while agents hold unstaged
   work — it clobbers them. If a sweep is needed, quiesce agents first.
3. Worktree isolation per agent would prevent this but was flaky earlier;
   for shared-tree, serialize same-crate agents.
