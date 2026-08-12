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

Two test sites assert the opposite:

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

_(table filled in as the sweep proceeds; see §8.3 for the running log)_

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
