# Basin — next-session handoff (2026-05-22)

> **SUPERSEDED — 2026-05-25.** This 2026-05-22 handoff is stale (its §0
> uncommitted-WIP and in-flight items are long resolved). Current state:
> the technical critical path is **complete** — HTAP fastpaths default-on,
> JSONB `->>` selective-read pushdown, and the perf wave all shipped (see
> CHANGELOG 2026-05-25 and the STATUS banner atop `NEXT_WAVES.md`). The
> live priority is **distribution / first reference customer** (Phase 0
> interviews, `docs/customer-interview-script.md`), not engine work. Treat
> the rest of this file as historical; regenerate it at the start of the
> next working session.

Hand this to a fresh chat. Full per-wave history is in `decisions.md` (newest at
top); roadmap detail in `TASK.md`. Counts as of writing: **345 done / 62 open /
9 partial** boxes in `TASK.md`.

---

## 0. FIRST THING: there is uncommitted WIP in the tree — verify or discard it

Three agents (advisory locks / GIN-completeness-at-scale / pg_sleep) wrote files
this session but were **not committed**. `git status` shows ~7 modified files:

- **#1 lock_timeout/advisory locks** (per ADR 0026): `crates/basin-engine/src/advisory_lock.rs`, `session.rs`, `crates/basin-shard/src/lock_registry.rs`
- **#2 GIN completeness-at-scale + perf gates**: `crates/basin-engine/src/index_probe.rs`, `crates/basin-storage/src/index/gin_tsvector.rs`
- **#3 pg_sleep cooperative cancel**: `crates/basin-engine/src/executor.rs`, `crates/basin-engine/src/udf.rs`

**Action:** `cargo build --workspace` then run the relevant harnesses
(`timeout_trio_harness`, `jsonb_index_harness`, `fts_harness`). If green, commit
each task as its own commit (stage explicit paths). If incoherent, the diffs are
small — finish or `git checkout` the broken file (these were never committed, so
discarding is safe). Decisions already recorded: **ADR 0026** (`docs/decisions/0026-*`)
governs #1 — advisory-lock blocking manager + 55P03, NO row-lock manager, row
writes stay optimistic (40001 at commit).

---

## 1. TOP PRIORITY — mount the missing HTTP routes (unblocks cloud gates)

The cloud calls routes that basin-rest doesn't mount. Handler **logic exists in
the crates**; it just needs route handlers in `crates/basin-rest/src/routes/auth.rs`
+ `.route(...)` wiring in `crates/basin-rest/src/server.rs` (see the existing
`/auth/v1/signin` etc. block ~lines 183-206 and the `/admin/v1/functions/*` block
~228-246 as the template).

- **OAuth (14 providers)** — `basin-auth/src/oauth.rs` is complete (google, github,
  apple, bitbucket, discord, figma, gitlab, linkedin, microsoft, notion, slack,
  spotify, twitch, twitter_x). **MISSING routes:** `GET /auth/v1/oauth/:provider/authorize`
  (build authorize URL + state) and `GET /auth/v1/oauth/:provider/callback` (exchange
  code → identity → session). Cloud's `basin_auth_client.rs` returns
  `oauth_via_engine_http_not_available` until these exist.
- **MFA** — `basin-auth/src/mfa.rs` complete (factors/TOTP/step-up). **MISSING routes:**
  `/auth/v1/factors*` (enroll/list/verify/challenge). 
- **Functions deploy/list/logs** — ✅ ALREADY MOUNTED (`/admin/v1/functions/*` in
  server.rs ~228-246). The earlier "still gated" audit was STALE. `/fn/v1/:name`
  (invoke) also mounted. No action.

Scope: basin-rest only (+ thin glue to basin-auth). Add integration tests hitting
the new routes. Check off the corresponding cloud-gate tasks when done.

---

## 2. Remaining `TASK.md` work, by category

**Phase impl essentially complete (5.18–5.32):** JSONB-GIN, FTS, range types,
citext, pgvector, CDC/pgoutput, hypertables, EXPLAIN/pg_stat/pg_locks, pg_dump,
timeouts, Docker/examples, ADR 0025/0026. Index-scan-wiring **trilogy done**
(JSONB-GIN 85d156a, tsvector-GIN 21d8a6e, interval/GIST 2592a6e — interval perf
gate green at 28×; JSONB/tsvector perf gates are the #2 WIP above).

**A. Engine, larger / needs care (one solo engine agent each, full test pass after):**
- `lock_timeout` 55P03 = #1 WIP above (ADR 0026).
- JSONB/tsvector perf gates = #2 WIP above (per-file completeness so pruning works
  at scale, not just on tiny fixtures).
- `pg_sleep` preemption = #3 WIP above (cooperative deadline poll).
- 5.24.F range exclusion-constraint (still `#[ignore]`'d).

**B. External-tool-gated — wire into CI, can't verify in this sandbox:**
5.22.A (real pg_dump/psql round-trip), 5.25 migration-tool CIs (Flyway/Diesel/sqlx/
Prisma/golang-migrate binaries), 5.29.F (1M-row hypertable soak), 5.31 (docker image
build/publish CI — Dockerfile + workflows exist), 5.32 (sample-app `npm` CI).

**C. Upstream-blocked / parked:**
- #40 — `to_char` family + `IS DISTINCT FROM`/Utf8View (DataFusion 53 limitation).
- #63 — already DONE-ish: vortex bumped to 0.71 (6/7 extra_types green). Residual:
  `viability_uuid` — ADR-0024 UUID Decimal256→FixedSizeBinary translation not applied
  on the vortex-datafusion read path (engine read-path translation needed).
- #43 — upstream Vortex FixedSizeBinary(N) encoder PR. **PARKED — do NOT work on it**
  (standing user instruction; roadmap-only).

**D. Security/stub tails (need the original findings lists):**
- #53 — SEC-AUDIT `a41436a` findings (5 P0 + 10 P1 + 8 P2). P0s were closed earlier;
  P1/P2 remainder. Recover the findings via `git show a41436a` or the audit commit.
- #54 — STUB-HUNT `a7e5a6b` findings (3 P0 + 7 P1 + 15 P2). Same.

**E. Cross-repo (separate git repos, separate Cargo targets — zero contention):**
- `/Users/pc/code/basin-cli` — `TASKS.md`; ~5 tasks landed this session
  (dump/restore, gen-watch, cwd-fallback, projects tests). HTTP-API-client CLI.
- the private cloud repo — `TASKS.md`, T-127..T-144 (paired UI) + T-15x
  backend; ~5 landed (T-154/155/156/157/158). Vite+React SPA + Axum `backend-rs`.

---

## 3. How to run the autonomous loop (proven cadence + hard-won lessons)

**Cadence per wave:**
1. `git log/status` — confirm clean tree; if a wave's agents landed, verify+commit.
2. Pick a **file-disjoint** set. Dispatch agents (sonnet). Each agent: implement,
   self-verify (`cargo test -p <crate>` + its harness), commit its OWN files by
   explicit path, report SHA.
3. **Coherence gate:** `cargo build --workspace` (NOT `--all-targets` — see disk
   lesson) + a **targeted regression sweep** on the harnesses the change could
   affect (always include `noop_accept` + `smoke_pgx` for any query-path change).
4. Mark `TASK.md` boxes (`- [x]` done, `- [~]` partial). Log non-obvious decisions
   to `decisions.md` (newest at top). Reclaim disk if `df` < 15GB.

**Engine-serialization reality:** almost all remaining feature work is in
`basin-engine` (hub files: `executor.rs`, `session.rs`, `types.rs`, `lib.rs`,
`index_probe.rs`). **Run ONE basin-engine-source-modifying agent per wave.** Pair
it with: a different-crate agent (basin-pool/shard/storage/catalog), new-file
agents (tests/docs/CI), and/or **sibling-repo agents** (basin-cli, the cloud
repo — fully isolated, the best parallelism). To run >1 engine agent, give each a STRICT
disjoint file scope and tell it which files NOT to touch (worked this session for
advisory/perf/pg_sleep).

**HARD-STOP rules in EVERY agent prompt:** never `git reset --hard` / `checkout --`
/ `clean -fd` / `restore .` / `stash` / `add -A` / `add .`; stage explicit paths;
never write to `.claude/`.

**Lessons (full detail in `decisions.md` + cross-session memory):**
- **NO worktree isolation for Rust agents** — each worktree gets its own multi-GB
  cargo target → exhausts the 926GB volume → ENOSPC mid-run. Use shared-tree
  file-disjoint waves. (Memory: `feedback-no-worktree-isolation-rust`.)
- **Disk:** `cargo test --workspace --all-targets` builds ~200 integration binaries
  at once → fills disk. Use `cargo build --workspace` for coherence + targeted
  `--test X` runs. Reclaim: `find target/debug/deps -name '*.rcgu.o' -delete && rm -rf target/debug/incremental`.
- **Commit races:** two agents doing explicit-path `git add`+`commit` concurrently
  can interleave (one commit sweeps the other's staged files). Benign for disjoint
  file sets — just `git status` after each wave and sweep stranded files.
- **Always run the workspace capstone** (`cargo test --workspace --lib --bins`)
  periodically — it caught a real pool default-mode regression that per-wave
  targeted sweeps missed.
- **Index-prune pattern (reusable):** populate registry on write + `mark_file_indexed`
  → `detect_*` probe at query → `apply_*_pruning_for_query` re-registers a pruned
  ListingTable, gated by `indexed_files ⊇ live_files` (else full-scan fallback,
  correctness-safe). See commits 85d156a / 21d8a6e / 2592a6e.
- **No backcompat shims** — fresh project, rewrite in place (memory:
  `feedback-no-backcompat-shims`). **Autoscale is wanted**, not scope creep.
- **Trust commits, not agent summaries** — verify the actual diff/tests.

**Standing user prefs:** record decisions in `decisions.md`; comprehensive testing
(security/compat/ORM/perf, 3-way Neon/Supabase/Basin); "keep going till complete";
30-min self-resuming loop reminder; pause on rate-limit then resume.
