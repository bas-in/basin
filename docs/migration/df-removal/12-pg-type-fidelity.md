---
title: Postgres Type System Fidelity
nav_section: migration
sidebar_position: 12
summary: Authoritative spec for Basin's logical Postgres type system after DataFusion removal — typmod enforcement, NUMERIC, arrays, coercion, NULL semantics, wire-protocol type reporting — plus the tests that currently pin non-Postgres behaviour and must be fixed before migration.
tags:
  - migration
  - types
  - postgres-compatibility
  - coercion
  - wire-protocol
---

<!-- STATUS: IN PROGRESS — filled incrementally, newest findings appended. -->

# Postgres Type System Fidelity

Companion to [08 — owned IR and engine design](./08-ir-design.md), whose §2
sketches `PgType { oid, typmod }` and defers deep semantics here. This document
is the authority on what "Postgres-compatible" means for Basin's type system.

## 0. BLOCKER: tests that pin non-Postgres behaviour

A missing test permits a fix. A **wrong** test forbids one: the correct engine
fails CI, and the pressure is to reproduce the bug. These must be corrected
**before** engine work starts, not during it.

### 0.1 Float→integer cast: Basin pins truncation, Postgres rounds — VERIFIED

Postgres's `float8 → int8/int4/int2` cast rounds (`rint()`, round-half-to-even).
Its `numeric → int` cast also rounds (half away from zero). Postgres **never**
truncates toward zero in a cast; truncation is what `trunc()` is for.

```
postgres=# SELECT 3.9::int, (-2.7)::int, CAST(3.9 AS bigint);
 int4 | int4 | int8
------+------+------
    4 |   -3 |    4
```

Two test sites asserted the opposite. **Both are now fixed** — corrected to
Postgres semantics and marked `#[ignore]`, since Basin does truncate; see
[§8](#8-tests-that-pinned-non-postgres-behaviour) for the full record. The
original findings are kept below because the *engine* behaviour they describe is
still current:

- `tests/integration/tests/pg_type_casts.rs:150-152`
  `assert_eq!(v, 3, "CAST(3.9 AS BIGINT) truncates to 3");`
  Postgres yields **4**.
- `tests/integration/tests/pg_type_casts.rs:337-371`
  (`cast_float_to_integer_truncates`)
  `assert_eq!(vals, vec![3, -2], "float-to-int truncates toward zero");`
  Postgres yields **`[4, -3]`**.

Both carry comments claiming the behaviour matches Postgres ("PG truncates
toward zero: 3.9 → 3, -2.7 → -2", `:370`). The comment is wrong, so the error
will survive a casual reading of the test. This is inherited DataFusion
behaviour: DataFusion's float→int cast follows Rust's `as` semantics, which
truncate.

Note also that in Postgres the *literal* `3.9` is `numeric`, not `float8`, so
`CAST(3.9 AS BIGINT)` goes through the numeric→int8 cast (round half away from
zero) rather than the float path. Both round; they differ only at exact `.5`
ties, where `float8` rounds to even (`0.5::int` → `0`, `1.5::int` → `2`) and
`numeric` rounds away from zero (`0.5::numeric::int` → `1`, `1.5::numeric::int`
→ `2`). A conforming engine must implement **both** rules, keyed on the source
type — a single shared rounding helper is already a fidelity bug.

**Action:** correct both assertions and their comments to Postgres's values,
then mark them as expected-fail against the DataFusion engine (or gate on the
new engine flag) so the fix direction is encoded in CI rather than the bug.

### 0.2 numeric(p,s) INSERT rejects excess scale instead of rounding — VERIFIED

Postgres rounds an incoming `numeric` to the column's declared scale
(half away from zero) and errors *only* if the integer part will not fit:

```
postgres=# CREATE TABLE t (n numeric(10,2));
postgres=# INSERT INTO t VALUES (1.005);   -- OK, stores 1.01
postgres=# INSERT INTO t VALUES (99999999.99);  -- OK
postgres=# INSERT INTO t VALUES (999999999.9);  -- ERROR 22003 numeric field overflow
```

Basin raises an error on the first case. `crates/basin-engine/src/dml.rs:2688-2696`
(`parse_decimal_to_i128`), when the literal's scale exceeds the column scale,
only divides out **trailing zeros** and otherwise fails:

```rust
for _ in 0..drop {
    if value % 10 != 0 {
        return Err(format!(
            "literal has more fractional digits than column scale {target_scale}"
        ));
    }
    value /= 10;
}
```

The doc comment above it (`:2617-2620`) states this is deliberate — "fractional
digits that would require *more* scale than the column allows are an error so
the user sees the precision-loss explicitly". That is a defensible design choice
and *not* Postgres. Any ORM or client that computes a value at higher precision
than the column declares (currency division, averages, `round()` upstream) gets
a hard failure on SQL Postgres accepts. High-severity, high-frequency.

Secondary issues at the same site:
- The error is `BasinError::InvalidSchema`, not SQLSTATE **22003**
  (`numeric_value_out_of_range`) / **22P02**. Clients keying on SQLSTATE
  mis-handle it.
- Rounding mode must be **half away from zero** for `numeric` (PG's
  `numeric_round`), distinct from the float path's half-to-even (§0.1).

### 0.3 Other tests asserting conversion / rounding / overflow / ordering

<!-- sweep in progress -->

## 1. Inventory: Basin's type support today

### 1.1 How the logical type is represented today

There is no logical type. There is an Arrow `DataType` plus a **string sidecar in
`Field` metadata**. `crates/basin-engine/src/types.rs:20-182` defines ~25
`BASIN_TYPE_*` marker constants, and the marker value is parsed back out of a
string at every use site:

| Logical PG type | Arrow physical | Marker | Source |
|---|---|---|---|
| `jsonb` / `json` | `LargeBinary` | `BASIN_TYPE=JSONB` | `types.rs:21`, `:690` |
| `uuid` | `FixedSizeBinary(16)` | `BASIN_TYPE=UUID` | `types.rs:22`, `:708` |
| `tsvector` / `tsquery` | `Utf8` | `TSVECTOR` / `TSQUERY` | `types.rs:26-29`, `:729` |
| `inet` / `cidr` / `macaddr` / `macaddr8` | `Utf8` | 4 markers | `types.rs:33-39`, `:806-809` |
| `bit(n)` / `varbit(n)` | `Utf8` | `BIT(n)` / `VARBIT(n)` | `types.rs:45-49`, `:959-968` |
| `money` | `Decimal128(20,2)` | `MONEY` | `types.rs:56`, `:797` |
| `xml` | `Utf8` | `XML` | `types.rs:59`, `:801` |
| `point` / `geometry(point,srid)` | `FixedSizeBinary(21)` WKB | `POINT` + `BASIN_SRID` | `types.rs:66-85`, `:930-943` |
| `citext` | `Utf8` | `CITEXT` | `types.rs:92` |
| 6 range types | `Utf8` holding JSON | 6 markers | `types.rs:97-102`, `:813-818` |
| ENUM | `Utf8` | `BASIN_ENUM_TYPE` + `BASIN_ENUM_OID` | `types.rs:154-162` |
| DOMAIN | base type | `BASIN_DOMAIN` | `types.rs:168` |
| `varchar(n)` / `char(n)` | `Utf8` | `BASIN_CHARLEN=varchar(n)` | `types.rs:182`, `ddl.rs:601` |
| `vector(n)` / `halfvec(n)` | `FixedSizeList<Float32>` | — | `types.rs:838-870` |

Consequences of the sidecar design, all of which `PgType { oid, typmod }` fixes
by construction:

1. **Markers are stringly typed.** `"varchar(10)"` is `format!`-ed at DDL time
   (`types.rs:204-226`) and re-parsed with `strip_prefix` at every read
   (`types.rs:230-241`, `:408-430`). A typo is a silent "no limit".
2. **Markers live on `Field`s, so they survive only where `Field`s survive.**
   Any expression result is a fresh Arrow array with no metadata. `SELECT
   citext_col` keeps case-insensitivity; `SELECT lower(citext_col) || 'x'` does
   not, and neither does a `CASE` over it. Same for `varchar(n)` — see §2.1.
3. **The mapping is many-to-one and lossy in the other direction.** Six range
   types, `inet`, `cidr`, `macaddr`, `xml`, `citext`, `bit`, `tsvector` and plain
   `text` all land on `Utf8`. Without the marker they are indistinguishable, so
   the wire layer cannot recover the OID for a computed column.
4. **Two independent mappings exist.** `basin-engine`'s markers and a matching
   set in `basin-router` (`types.rs:162` notes "The router reads the same key (a
   matching local const)"). Two copies of a type table can disagree.

### 1.2 Type-mapping divergences from Postgres, as declared in the code

Each of these is stated in a comment in `types.rs`, so they are known, not
discovered here:

- **`NUMERIC` with no precision → `Decimal128(38,0)`** (`types.rs:983-1013`,
  `decimal128_from_exact_number_info`). Postgres's bare `numeric` is
  arbitrary-precision **with arbitrary scale**; Basin's is a 38-digit integer.
  `SELECT 0.5::numeric` therefore loses the fraction entirely on a bare-`numeric`
  column. Precision > 38 is a hard `InvalidSchema` (`clamp_precision`,
  `types.rs:1006-1013`). See §2.2.
- **`JSON` is aliased to `JSONB`** (`types.rs:690`, comment at `:684-689`).
  Postgres's `json` preserves whitespace, key order, and duplicate keys; `jsonb`
  does not. Basin canonicalises both, and reports one OID for both.
- **`timestamp(n)` / `time(n)` precision is discarded.** `types.rs:760-766`
  matches `SqlDataType::Timestamp(_, tz_info)` — the `_` is the precision — and
  always produces `Timestamp(Microsecond, ...)`. `types.rs:674-676` does the same
  for `TIME(n)`. So `timestamp(0)` does not truncate to seconds and does not
  report its typmod. See §2.1.
- **`timetz` is rejected outright** (`types.rs:674`, only `TimezoneInfo::None |
  WithoutTimeZone` accepted).
- **`citext` reports OID 25 (`text`)** (`types.rs:88-92`). Defensible — citext is
  an extension type — but it means clients cannot tell the column is
  case-insensitive, and the case-folding is implemented ad hoc per operator
  rather than by a collation.
- **`money` → `Decimal128(20,2)`** (`types.rs:51-56`). Postgres `money` is an
  int64 scaled by `lc_monetary`'s fractional digits; pinning scale 2 is wrong
  for locales with 0 or 3 fractional digits.
- **`sparsevec(n)` is an explicit 0A000** (`types.rs:878-881`).
- **`geometry` accepts only the `POINT` subtype** (`types.rs:940-943`).
- **Arrays are `List<T>`, not PG arrays.** `types.rs:778-783` maps `INT[]` to
  `List`, and nested `INT[][]` to nested `List`. Postgres arrays are *not*
  nested lists — see §2.3, this is the largest structural gap in the inventory.

### 1.3 What Basin does enforce today

Worth stating plainly, because [08](./08-ir-design.md) §2 implies typmod is
unenforced and that is only half true:

- **`varchar(n)` / `char(n)` length IS enforced on write.** `enforce_charlen`
  (`types.rs:256-301`) raises `BasinError::StringTooLong` (SQLSTATE 22001) and
  blank-pads `char(n)`. It is called as a post-pass on every built Utf8 column
  from four sites: `dml.rs:544` (INSERT), `dml_mutate.rs:7340` and `:7353`
  (UPDATE), `copy_ingest.rs:555` (COPY). Length is measured in `chars()`, which
  matches Postgres's per-character semantics.
- The trailing-space rule is implemented (`types.rs:263-282`): over-length
  *spaces* are truncated rather than erroring, which is Postgres's `varchar`
  behaviour.

So the enforcement gap is narrower than "typmod is not enforced": the write path
is broadly right for character types, and the gaps are (a) reporting, (b) cast
expressions, (c) `numeric` rounding, (d) `timestamp(n)`. See §2.1.

## 2. Target logical type system

### 2.1 typmod enforcement

### 2.2 NUMERIC / arbitrary precision

### 2.3 Arrays as first-class Postgres types

### 2.4 Domains, composites, ENUMs, ranges

### 2.5 The `unknown` type and late literal resolution

## 3. Coercion and cast fidelity

## 4. NULL and edge semantics

## 5. Wire-protocol type reporting

## 6. Prioritized fidelity gap table

## 7. Open questions

## 8. Tests that pinned non-Postgres behaviour

A test that asserts behaviour Postgres does not have is worse than a missing
test. A missing test *permits* a fix; a wrong test *forbids* one — it converts a
fidelity gap into a defended invariant, and any engine work built against it
inherits the defect. This section is the running record of every such site found
in the suite, what it claimed, what Postgres actually does, and where it now
stands.

**Verification method.** Every "what Postgres does" row below was confirmed
against a live server, not from memory:

```
PostgreSQL 18.2 (Homebrew) on aarch64-apple-darwin24.6.0
```

Doc citations accompany each rule. Where Basin's engine genuinely disagrees, the
corrected test is left asserting *Postgres* semantics and marked `#[ignore]`
with a reason pointing back here. The assertion is never weakened to match the
engine, and the engine is not changed to chase the test — the point of the
exercise is to convert a hidden wrong behaviour into a visible, tracked gap.

### 8.1 Reference: the rounding rules, as measured

Float-to-integer and numeric-to-integer casts **both round** — neither
truncates — but they round *differently*, and conflating them is its own bug:

| Input | `::float8 → bigint` | `::numeric → bigint` |
|-------|--------------------:|---------------------:|
| `3.9`  | `4`  | `4`  |
| `-2.7` | `-3` | `-3` |
| `0.5`  | `0`  | `1`  |
| `1.5`  | `2`  | `2`  |
| `2.5`  | `2`  | `3`  |
| `-0.5` | `0`  | `-1` |
| `-1.5` | `-2` | `-2` |

- **float → int: round half to even** (banker's rounding). `0.5 → 0`,
  `2.5 → 2`. This follows the C library's `rint()`, which honours the current
  IEEE-754 rounding mode; that mode is round-half-to-even by default.
- **numeric → int: round half away from zero**. `0.5 → 1`, `2.5 → 3`,
  `-0.5 → -1`. `numeric` is a decimal type and does not go through `rint()`.

The two agree on every input whose fraction is not exactly `.5`, which is why
the distinction is easy to miss and easy to get wrong in a shared cast path.

Source: PostgreSQL docs, [§8.1 Numeric Types][pg-numeric] — "When rounding
values, the `numeric` type rounds ties away from zero, while (on most machines)
the `real` and `double precision` types round ties to the nearest even number."

[pg-numeric]: https://www.postgresql.org/docs/18/datatype-numeric.html

Neither rule is truncation. `trunc()`, `floor()`, and `ceil()` are the explicit
functions for that, and a cast is not a call to any of them.

### 8.2 Sites found

Four sites. Three asserted a rule Postgres does not have; the fourth permitted
the wrong answer alongside the right one, which is the same defect wearing a
disguise — a test that accepts either outcome cannot defend the correct one.

| # | Site | Asserted | Postgres actually | State |
|---|------|----------|-------------------|-------|
| 1 | `tests/integration/tests/pg_type_casts.rs` — `cast_float_to_integer_truncates` (was line 340) | `3.9 → 3`, `-2.7 → -2`, comment "PG truncates toward zero" | `3.9 → 4`, `-2.7 → -3`; rounds half to **even** | **fixed-and-ignored** |
| 2 | `tests/integration/tests/pg_type_casts.rs` — `cast_sql_standard_explicit` (was line 150-152) | `CAST(3.9 AS BIGINT) → 3`, comment "truncates to 3" | `4` — bare `3.9` is **`numeric`**, rounds half **away from zero** | **fixed-and-ignored** (split into a new test) |
| 3 | `tests/integration/tests/differential_pg.rs` — `diff_cast_matrix_basic` (comment at line 2302, title at 2275) | comment "numeric → int4 (truncation) — 3.7 truncates to 3 in PG" | `3.7::numeric::int4 → 4`; rounds half **away from zero** | **fixed** (passes) |
| 4 | `tests/integration/tests/coverage_errorpaths.rs` — `error_division_by_zero_in_select` (line 221) | accepted *either* an error *or* a row with NULL ("Accept either outcome") | raises SQLSTATE 22012 `division_by_zero`, unconditionally | **fixed** (passes) |

Sites 1 and 2 are in the same file and were the same misconception applied to
two different source types — which is why fixing them needed the §8.1 rule pair
rather than one blanket "casts round" edit. Site 1 is a `float8` column
(half-to-even); site 2 is a bare decimal literal, and `pg_typeof(3.9)` is
`numeric`, so it takes the half-away-from-zero path. Writing `CAST(3.9 AS
BIGINT)` and `CAST(3.9::float8 AS BIGINT)` selects different tie rules from the
same keyword.

#### Site 1 — float→int cast, `pg_type_casts.rs`

Renamed to `cast_float_to_integer_rounds`, since the old name encoded the wrong
rule and would have kept re-teaching it. The test now asserts PG's values and
was extended with the exact-tie inputs `0.5, 1.5, 2.5, -0.5`, because the
original two-value case (`3.9`, `-2.7`) cannot distinguish half-to-even from
half-away-from-zero at all — only ties can.

Run against the current engine:

```
left:  [3, -2, 0, 1, 2, 0]     <- Basin (truncation)
right: [4, -3, 0, 2, 2, 0]     <- PostgreSQL 18.2
```

**Basin genuinely truncates.** This is the intended outcome of the exercise: a
hidden wrong behaviour is now a visible, named gap. The test is marked
`#[ignore]` with a reason pointing here; the assertion still states Postgres
semantics. It was *not* weakened back to truncation, and the engine was *not*
changed to chase it. Deleting the `#[ignore]` is the acceptance criterion for
implementing round-half-to-even in the cast path.

Note that `0.5 → 0` and `-0.5 → 0` agree under both rules, so a test built only
from those two ties would have passed while the engine was still wrong. The
`1.5`/`2.5` pair is what carries the proof.

#### Site 2 — decimal literal→int cast, `pg_type_casts.rs`

The wrong assertion sat inside `cast_sql_standard_explicit`, a test whose real
job is proving that `CAST(x AS type)` *syntax* reaches the planner. Rather than
`#[ignore]` the whole test and lose that syntax coverage, the rounding
assertion was split out into a new `cast_numeric_literal_to_integer_rounds`,
which carries the ties (`0.5 → 1`, `2.5 → 3`, `-2.5 → -3`) and the `#[ignore]`.
`cast_sql_standard_explicit` keeps its identity-cast check and stays green.

Bundling a fidelity gap into an unrelated test is how gaps become invisible: the
whole test must then be either ignored (losing the unrelated coverage) or
weakened (losing the gap). Splitting keeps both honest.

#### Site 3 — numeric→int cast, `differential_pg.rs`

This is the site §8.1's rule pair was needed to catch, and it is instructive
because **the test passed and still passes**. It is oracle-driven: expectations
come from a live Postgres over `PG_DIFF_TEST_DSN` via `run_assert_match`, not
from hand-coded literals. So the wrong claim lived entirely in the comment and
the test title, where it could never fail — and from there it propagated into
`07-conformance-tests.md` §G5, which cited this line as authority that
"only `numeric`→int truncates". A wrong comment on a passing oracle test is a
particularly durable kind of wrong: nothing mechanical will ever contradict it.

Fixed by correcting the comment and title to the measured rule, and by adding
the tie cases for `numeric` *and* `float8` side by side in one test, so the
half-to-even/half-away-from-zero distinction is visible at the point of use
rather than inferred. The stale citation in `07-conformance-tests.md` §G5 was
corrected in the same pass.

While in the file, the adjacent `diff_cast_int8_to_int4_overflow` was switched
from `run_assert_match` to `run_assert_both_error(..., Some("22003"))`. It was
not asserting anything false, but "compare whatever both sides produced" is
weaker than the verified rule: PG raises `22003` here
(`ERROR: 22003: integer out of range`, measured with `VERBOSITY verbose`). The
stronger form fails loudly with "Basin succeeded but expected an error" if Basin
wraps silently, which is the self-flagged suspicion in that test's own doc
comment.

Caveat: every test in this file is DSN-gated — `make_runner` returns `None` and
the test exits green when `PG_DIFF_TEST_DSN` is unset. These fixes therefore
improve what the harness *says* and what it checks *when run*, but they are not
load-bearing in a default `cargo test`.

#### Site 3 — division by zero, `coverage_errorpaths.rs`

The doc comment said the query "must produce an error (not a panic or silently
return NULL)" while the body's `Ok(ExecResult::Rows { .. })` arm printed a note
and passed. The test was named for an axis it did not test.

Tightened so the row-returning arm panics. **It passes** — Basin does raise on
integer division by zero, so the permissive arm was protecting nothing and cost
a real invariant. No `#[ignore]` needed.

#### Related hazard (not a wrong assertion)

`tests/integration/tests/plan_floor_caches.rs:86` — the `scalar_i64` helper
coerces a NULL first cell to `0` ("treating a NULL (empty table SUM) as 0").
This does not *claim* Postgres semantics; the surrounding tests are about plan
floor caching and the aggregate value is incidental. But it makes a
`SUM`-over-empty of `0` (wrong) indistinguishable from `NULL` (right), so it
must not be reused as a semantics oracle. Left as-is deliberately — changing it
would alter tests unrelated to type fidelity. Recorded here so the next reader
does not mistake it for a pinned rule.

#### Categories swept clean

Checked and found either correct or simply untested (a gap, not a landmine):

- **Default NULL ordering** — every site states the right rule (ASC → NULLS
  LAST, DESC → NULLS FIRST): `array_agg_perf_shape.rs:214`,
  `topk_late_materialize.rs:321`, `pg_agg_udf.rs:1918-1928`,
  `differential_pg.rs:2172`. `array_agg_perf_shape.rs`'s expectation
  `{c, NULL, a}` was re-verified against live PG and matches.
- **Empty-input aggregates** —
  `where_null_3vl_fold.rs:164 aggregate_under_null_eq_returns_empty_relation_answer`
  correctly asserts `COUNT(*) = 0` with `MIN`/`MAX`/`SUM` all NULL.
- **Integer overflow** — no test asserts a wrapped or saturated value. Genuinely
  untested rather than mis-asserted (survey §G3).
- **NaN / -0.0 ordering** — no SQL-level `NaN` assertion exists to be wrong; the
  `gapfill.rs` / `fast_select.rs` hits cited in survey §G4 contain no `NaN`
  tokens at all. Untested, not mis-asserted.
- **String truncation** — `crates/basin-engine/src/types.rs:248-288` documents
  and implements the correct split: 22001 on over-length, with over-length
  *trailing spaces* truncated rather than raised. Matches PG.
- **`trunc`/`date_trunc`/`floor`** — the many "truncat" grep hits are the
  `TRUNCATE` statement or `date_trunc`, where truncation is the correct
  behaviour. Dismissed on inspection.
- `viability_pg_compat_funcs.rs:724` asserts
  `extract(second FROM …'42.5') + 0.5` renders as `43` where PG renders
  `43.000000` (numeric). That is a type/rendering divergence, and the file's
  stated policy is to document such divergences rather than coerce them. Noted,
  not changed — it is a wire-format question for §5, not a rounding rule.

### 8.3 Sweep log

- Live-Postgres reference table for the sweep (all measured on PG 18.2):
  - **Integer overflow** — `9223372036854775807::bigint + 1` → `ERROR: bigint
    out of range`. Out-of-range float→int casts likewise:
    `CAST(1e20::float8 AS bigint)` → `ERROR: bigint out of range`. Postgres
    *errors*; Arrow's native arithmetic and cast kernels may wrap or saturate
    silently.
  - **Division by zero** — `SELECT 1/0` → `ERROR: division by zero`. Not NULL,
    not infinity.
  - **Empty-input aggregates** — over zero rows, `sum()` is NULL, `avg()` is
    NULL, and only `count()` is `0`.
  - **NaN ordering** — Postgres sorts `NaN` as **larger than all other float
    values**, including `Infinity`, and treats `NaN = NaN` as **true** for
    sort/index purposes. `0.0 = -0.0` is also true.
  - **Default NULL ordering** — `ORDER BY x` puts NULLs **last**; `ORDER BY x
    DESC` puts them **first**. (NULLs sort as if larger than everything.)
  - **varchar over-length** — an explicit cast `'abcdef'::varchar(3)`
    *truncates* to `abc`, but storing an over-length value into a `varchar(3)`
    *column* → `ERROR: value too long for type character varying(3)`. The two
    paths genuinely differ; a test asserting one must not be read as licensing
    the other.
