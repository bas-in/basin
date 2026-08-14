# `extract_fns` — integration notes

Slice: `OID_EXTRACT_DATE OID_EXTRACT_INTERVAL OID_EXTRACT_TIME
OID_EXTRACT_TIMESTAMP OID_EXTRACT_TIMESTAMPTZ OID_EXTRACT_TIMETZ
OID_OP_DATE_MI_DATE OID_OP_DATE_MII OID_OP_DATE_PLI OID_OP_INT_PL_DATE`

Verified against live PostgreSQL 18.2 at `postgres://pc@127.0.0.1:5432/postgres`.

---

## STATUS: 0 of 10 registered. **Do not add `pub mod extract_fns;` to `funcs/mod.rs` yet.**

Both halves of this slice are blocked by the `ScalarFunc` ABI, for two
*different* and independent structural reasons, both proved below from the
tree rather than argued:

| oids | blocker |
|---|---|
| 6199–6204 `extract` | the result's `Decimal128` **scale** is a function of the unit *value*, and `Project::new` fixes the output schema by evaluating the expression against a **zero-row probe batch**, at which point the evaluated unit array is empty and the unit is unrecoverable. |
| 1099/1100/1101/2555 date arithmetic | these are **`pg_operator` oids, not `pg_proc` oids**. They are dispatched in `eval_binary`, which never consults the registry, and `register_scalar` would panic on them because `basin_pgtype::func::FUNCS` has no such rows. |

`crates/basin-exec/src/funcs/extract_fns.rs` is written and parses. It is the
*prepared* port: six impls that delegate to `eval.rs` in the `dt_fns.rs` style,
plus the tests. It is **not** wired into `funcs/mod.rs` and must not be until
§5 and §6 land, or the crate will not build (it names a helper that does not
exist yet) and `extract` will break in every query that uses it.

---

## 1. Registration lines — **BLOCKED, do not paste yet**

When §5 *and* §6 have landed, append to `funcs::builtins()`:

```rust
        // extract_fns — the six `extract` oids. REQUIRES the plan-time
        // constant side-channel (§6); without it every projection containing
        // an `extract` gets the wrong output scale.
        r.register_scalar(Box::new(extract_fns::ExtractDate));
        r.register_scalar(Box::new(extract_fns::ExtractTime));
        r.register_scalar(Box::new(extract_fns::ExtractTimetz));
        r.register_scalar(Box::new(extract_fns::ExtractTimestamp));
        r.register_scalar(Box::new(extract_fns::ExtractTimestamptz));
        r.register_scalar(Box::new(extract_fns::ExtractInterval));
```

and the module declaration, in the existing alphabetical block:

```rust
pub mod extract_fns;
```

`funcs::mod.rs`'s `the_registry_reports_what_is_actually_hosted` asserts
`builtins().len() == 24` today. Six more makes it **30** — but that number is
being moved by five other agents in this tree, so re-read it rather than
trusting this line.

**There are no registration lines for 1099/1100/1101/2555.** They cannot be
registered at all — see §4.

---

## 2. `match` arms to delete — **BLOCKED, do not delete yet**

### 2a. `eval_scalar_fn`, in `crates/basin-exec/src/eval.rs` (delete with §1)

Exact current text, comment included:

```rust
        // All six `extract` oids share ONE implementation, which reads the
        // argument's actual Arrow type rather than trusting the oid. See
        // [`eval_extract`] for why that is the correct dispatch and not a
        // shortcut.
        OID_EXTRACT_DATE
        | OID_EXTRACT_TIME
        | OID_EXTRACT_TIMETZ
        | OID_EXTRACT_TIMESTAMP
        | OID_EXTRACT_TIMESTAMPTZ
        | OID_EXTRACT_INTERVAL => {
            let value = a(1)?;
            eval_extract(args.first(), &value, session)
        }
```

The six `const OID_EXTRACT_*` declarations **stay**: `eval.rs`'s own tests
(`sf(OID_EXTRACT_TIMESTAMP, …)` and the six-oid loop) still name them.

### 2b. `eval_binary` — **DO NOT DELETE. EVER, under this ABI.**

```rust
    match op.0.get() {
        OID_OP_DATE_PLI => return date_offset_days(&l, &r, 1),
        OID_OP_INT_PL_DATE => return date_offset_days(&r, &l, 1),
        OID_OP_DATE_MII => return date_offset_days(&l, &r, -1),
        OID_OP_DATE_MI_DATE => return date_diff_days(&l, &r),
        _ => {}
    }
```

This block is in `eval_binary`, not `eval_scalar_fn`. Nothing in
`funcs::FuncRegistry` is reachable from `eval_binary`. Deleting it deletes
`date + integer`, `integer + date`, `date - integer` and `date - date` from
the engine outright. See §4.

---

## 3. Why `extract` (6199–6204) cannot be ported under this ABI

`dt_fns.rs`'s module doc already refuses this port, on the grounds that
`eval_extract` takes the **unevaluated** field argument and so can refuse a
non-literal unit, which `&[ArrayRef]` cannot reproduce. That argument is
correct but understates the problem — it reads as an *acceptance-set* change,
which one could argue is tolerable. It is not. The real blocker is the output
**type**, and it fires on every query, not on an exotic one:

`crates/basin-exec/src/project.rs`, `Project::new`:

```rust
        let input_schema = input.schema();
        let probe = RecordBatch::new_empty(Arc::clone(&input_schema));

        let mut fields = Vec::with_capacity(exprs.len());
        for (expr, name) in &exprs {
            let array = eval::eval(expr, &probe)?;
            fields.push(Field::new(
                name,
                array.data_type().clone(),
                expr_is_nullable(expr, &input_schema),
            ));
        }
```

The projection's output schema is **the data type of the array the expression
produces over a zero-row batch**, and `Project::next_batch` then assembles
real batches with `batch_with_row_count(…)` → `RecordBatch::try_new`, which
*validates* each array against that schema.

Now follow an `extract` through it, after a port:

1. `eval_literal(PlanDatum::Utf8("second"), TEXT, 0)` is
   `StringArray::from(vec!["second"; 0])` — **length 0**. (`eval.rs`,
   `eval_literal`; the literal is broadcast to `batch.num_rows()`.)
2. `eval_scalar_fn` hands `invoke` a length-0 `StringArray` and a length-0
   value array. The unit string is *gone*: an empty array carries no value.
3. `extract_scale(unit, kind, raw)` cannot be called. `invoke` must return
   either an error — `Project::new` fails, so the whole plan fails to build —
   or an empty `Decimal128` at a guessed scale.
4. If it guesses: `Project` declares e.g. `Decimal128(38, 0)`, the first real
   batch produces `Decimal128(38, 6)` for `second`, and `RecordBatch::try_new`
   rejects it. A working query becomes a runtime error.

Today this works *precisely because* the unit comes from the `Expr`, which is
present at probe time when the data is not. `eval.rs` already pins the
resulting type:

```rust
        assert_eq!(got.data_type(), &DataType::Decimal128(38, 6));
```

Two corollaries worth recording:

* **The scale really is unit-dependent, measured on 18.2** — one column, two
  `dscale`s, from a non-literal unit:

  ```text
  SELECT pg_catalog.extract(u, timestamp '2024-03-05 14:07:09.123456')
    FROM (VALUES ('year'),('second')) v(u);
   extract
  ---------
      2024
  9.123456
  ```

  PostgreSQL is happy with a varying unit because `numeric` carries a
  per-*value* `dscale`. One Arrow `Decimal128` carries one scale per *array*.
  This is not a Basin defect; it is a representation mismatch, and it is why
  `eval_extract` insists on a literal.

* **`ScalarFunc::return_type` does not help and currently has no callers.**
  `grep -rn return_type crates/basin-exec/src crates/basin-plan/src` finds the
  definition, three doc mentions and two unrelated `basin-plan` tests — nothing
  calls it. `Project` uses the zero-row probe instead. Even if it were called,
  its signature is `&[PgType]`: argument *types*, not argument *values*, so it
  cannot see `"second"` either. The trait doc names `extract(… FROM interval)`
  (6204) as the reason `return_type` is overridable; that reason does not
  actually work. (`arr_fns.integration.md` independently recorded the
  no-callers half of this.)

### The scale table, re-measured on 18.2

Given in the brief for `interval`; re-measured here over
`interval '1 year 2 mons 3 days 4:05:06.789123'`, and confirmed exactly:

```text
unit            scale   value
epoch             6     37015506.789123
second            6     6.789123
seconds           6     6.789123
millisecond       3     6789.123
milliseconds      3     6789.123
microsecond       0     6789123
microseconds      0     6789123
minute/hour/day/week/month/quarter/year/decade/century/millennium
                  0
```

So: `second`/`epoch` → 6, `millisecond(s)` → 3, everything else → 0. Identical
to the rule `extract_scale` already applies to `timestamp`, which is the one
piece of good news in this slice.

---

## 4. Why the four date-arithmetic oids cannot be ported under this ABI

They are **operator** oids. Read off live 18.2:

```sql
SELECT o.oid AS op_oid, o.oprname, lt.typname, rt.typname, res.typname,
       o.oprcode::oid AS proc_oid, p.proname
FROM pg_operator o
JOIN pg_type lt ON lt.oid=o.oprleft   JOIN pg_type rt  ON rt.oid=o.oprright
JOIN pg_type res ON res.oid=o.oprresult JOIN pg_proc p ON p.oid=o.oprcode
WHERE p.proname IN ('date_pli','date_mii','date_mi','integer_pl_date');
```

```text
op_oid | oprname | lhs  | rhs  | ret  | proc_oid | proname
  1099 | -       | date | date | int4 |     1140 | date_mi
  1100 | +       | date | int4 | date |     1141 | date_pli
  1101 | -       | date | int4 | date |     1142 | date_mii
  2555 | +       | int4 | date | date |     2550 | integer_pl_date
```

Three consequences:

1. **The registry is keyed on `pg_proc.oid`.** `register_scalar` asserts
   `catalog_row(oid).is_some()` against `basin_pgtype::func::FUNCS`, which has
   no rows for 1099/1100/1101/2555 (`grep -n 'date_pli\|date_mii\|integer_pl_date'
   crates/basin-pgtype/src/func.rs` → nothing). Registering any of them
   **panics at `builtins()` construction**, i.e. at first use, in every test.
2. **The two oid spaces are different namespaces and Basin already holds both.**
   `basin_pgtype::operator::OPERATORS` tabulates these four as
   `OperatorSig::binary(1100, "+", oid::DATE, oid::INT4, oid::DATE)` etc. The
   *function* oids for the same operators are 1140/1141/1142/2550. Putting a
   `pg_operator` oid into a `pg_proc`-keyed `HashMap<Oid, …>` is a collision
   waiting to happen: nothing prevents some future `pg_proc` row from having
   oid 1100, and the failure mode would be the registry silently answering the
   wrong function — exactly what `funcs/mod.rs`'s "Keyed on OID, not on name"
   note is trying to prevent.
3. **`eval_binary` never consults the registry.** Only `eval_scalar_fn` does.
   Even with a `FUNCS` row added, the arms in §2b would still be the only live
   code and the registration would be unreachable — the "committed, tested and
   completely inert" pattern `str_fns.rs`'s module doc names three prior
   instances of.

The honest port for these four is an **`OperatorRegistry`** keyed on
`pg_operator.oid`, consulted from `eval_binary`, that is a sibling of
`FuncRegistry` rather than a reuse of it. That is a design decision above this
slice's pay grade, so it is reported, not invented. Doc 27 should grow a
section on it: the operator half of `eval.rs` is a second hard-coded `match`
with the same parallelisation problem the function half had, and nothing in
the current ABI addresses it.

---

## 5. Exact `eval.rs` changes needed (owner's lane — I did not make them)

**One function.** In `crates/basin-exec/src/eval.rs`, change

```rust
fn eval_extract(
    field: Option<&Expr>,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let raw = match field {
        Some(Expr::Literal(PlanDatum::Utf8(s), _)) => s.as_str(),
        _ => {
            return Err(ExecError::Internal(
                "extract with a non-literal field is not implemented — the result's numeric \
                 scale depends on the field, and one Arrow array carries one scale"
                    .into(),
            ))
        }
    };
    let kind = temporal_kind(values, "extract")?;
```

to

```rust
pub(crate) fn eval_extract(
    raw: &str,
    values: &ArrayRef,
    session: &EvalSession,
) -> Result<ArrayRef, ExecError> {
    let kind = temporal_kind(values, "extract")?;
```

i.e. **`pub(crate)`, take the unit as `&str`, and delete the literal-unwrap
prelude** — the caller supplies the unit now. Everything from `let kind = …`
onward is unchanged. The refusal message moves verbatim into
`extract_fns::non_literal_unit`, so no string is lost.

No other helper needs to change visibility: `extract_fns.rs` deliberately
touches nothing else in `eval.rs`, for the reason `dt_fns.rs` gives — copying
`parse_date_unit` / `DateUnit` / `extract_scale` / `temporal_kind` /
`temporal_readings` / `date_part_of` across would leave two copies of
Postgres's unit vocabulary in one crate, free to drift. `downcast_array` and
`EvalSession` are already `pub(crate)`.

After the change `eval_extract` has exactly one caller (the registry impls),
and the deleted prelude has no caller at all — which is why the signature is
*changed* rather than a second entry point being added beside it.

**Not needed, contrary to the brief:** an `Interval` arm in `temporal_kind`
does **not** wire up 6204. `extract`'s body is `temporal_readings` →
`extract_value` → `date_part_of`, all of which read a *civil time*
(`NaiveDateTime` + optional offset + epoch micros). An interval has no
position on the calendar and Postgres computes it with a completely different
C function, `interval_part`, whose rules Basin already reproduces separately
in `eval_date_part_interval` — the `quarter` sign branch on `months` rather
than `months % 12`, and the load-bearing summation order for `epoch`. Routing
an interval through `temporal_readings` would produce *wrong answers*, not
missing ones. 6204 needs its own branch in `eval_extract`:

```text
extract(unit, interval)  ==  eval_date_part_interval(unit, interval)
                             re-expressed as Decimal128 at
                             scale 6 for second/epoch, 3 for millisecond(s),
                             0 for everything else
```

with the eight `not supported for type interval` refusals (§7.2) applied
before the scale is chosen. That is a *new implementation*, not a port, and is
deliberately not in `extract_fns.rs`: `ExtractInterval` delegates to
`eval_extract` exactly as the deleted `match` arm did, and therefore still
fails closed on an interval argument with `temporal_kind`'s
`"extract on Interval(MonthDayNano) is not implemented"`. Behaviour moved,
not improved.

---

## 6. The ABI change that unblocks `extract`

Minimal, and it does **not** reintroduce the double-evaluation bug (#151) that
`&[ArrayRef]` exists to prevent — the side-channel carries `Datum`s lifted
straight off the `Expr` tree with nothing evaluated, and cannot be used to
evaluate anything.

In `crates/basin-exec/src/funcs/mod.rs`, add to `ScalarFunc` (default method,
so no existing impl changes):

```rust
    /// Arguments the planner can prove are constants, positionally — `None`
    /// where the argument is not a plan-time constant. Lifted off the `Expr`
    /// tree WITHOUT evaluating anything, so it cannot reintroduce the double
    /// evaluation `invoke`'s `&[ArrayRef]` exists to prevent (#151).
    ///
    /// Needed only where the result's Arrow *type* depends on an argument's
    /// *value*. `extract` (6199-6204) is the case: its `numeric` scale is a
    /// function of the unit, and `Project::new` fixes the output schema from a
    /// ZERO-ROW probe batch, where the evaluated unit array is empty.
    fn invoke_bound(
        &self,
        args: &[ArrayRef],
        consts: &[Option<&PlanDatum>],
        session: &EvalSession,
    ) -> Result<ArrayRef, ExecError> {
        let _ = consts;
        self.invoke(args, session)
    }
```

and in `eval_scalar_fn`, change the registry consult to build the constants
alongside the already-evaluated arguments:

```rust
    if let Some(hosted) = crate::funcs::builtins().scalar(func.0) {
        let evaluated = args
            .iter()
            .map(|e| eval_with(e, batch, session))
            .collect::<Result<Vec<_>, ExecError>>()?;
        let consts: Vec<Option<&PlanDatum>> = args
            .iter()
            .map(|e| match e {
                Expr::Literal(d, _) => Some(d),
                _ => None,
            })
            .collect();
        return hosted.invoke_bound(&evaluated, &consts, session);
    }
```

`extract_fns.rs` then overrides `invoke_bound` and `invoke` becomes the
zero-row-hostile path it already documents. Until this lands, keep §2a.

**Note the acceptance set is preserved exactly by this shape** and would *not*
be by a "read the unit out of row 0 of the evaluated array" shortcut:
`Expr::Literal` is the same test the deleted prelude applied, so a computed
constant (`extract(lower('YEAR') FROM ts)`) is still refused, as it is today.
`extract_fns.rs` as written uses the shortcut, guarded, and pins the
difference — see §7.5.

---

## 7. PostgreSQL 18.2 divergences and measurements

### 7.1 `extract(… FROM date)` — the eight units `date` refuses

Measured, all eight verbatim:

```text
SELECT pg_catalog.extract('hour', date '2024-03-05');
ERROR:  unit "hour" not supported for type date
```

…identically for `minute`, `second`, `millisecond`, `microsecond`,
`timezone`, `timezone_hour`, `timezone_minute`. Everything else answers at
scale 0:

```text
epoch 1709596800   julian 2460375   dow 2       doy 65      isodow 2
isoyear 2024       year 2024        quarter 1   week 10
decade 202         century 21       millennium 3
```

`extract_scale`'s `TemporalKind::Date` arm already refuses exactly those eight
with `unit_not_supported(raw, "date")`. **Agrees.** Note this is where
`extract` parts company with `date_part(text, date)`, which answers `hour` as
`0` — Postgres reaches `date_part` from a `date` by an implicit cast to
`timestamp` and `extract_date` has no such cast.

### 7.2 `extract(… FROM interval)` — the eight refusals, and the `not recognized` boundary

```text
extract('dow',             interval '3 days 4:05:06')  ERROR:  unit "dow" not supported for type interval
extract('doy',             …)                          ERROR:  unit "doy" not supported for type interval
extract('isodow',          …)                          ERROR:  unit "isodow" not supported for type interval
extract('isoyear',         …)                          ERROR:  unit "isoyear" not supported for type interval
extract('julian',          …)                          ERROR:  unit "julian" not supported for type interval
extract('timezone',        …)                          ERROR:  unit "timezone" not supported for type interval
extract('timezone_hour',   …)                          ERROR:  unit "timezone_hour" not supported for type interval
extract('timezone_minute', …)                          ERROR:  unit "timezone_minute" not supported for type interval
extract('bogus',           …)                          ERROR:  unit "bogus" not recognized for type interval
```

`not supported` = a real unit that means nothing for this type; `not
recognized` = not in the vocabulary at all. `eval.rs` already has both
(`unit_not_supported` / `unit_not_recognized`) and `eval_date_part_interval`
already applies exactly this split for oid 1172.

Also measured, and worth having: `j` is an **alias for julian**, so it lands in
the refused set with the *refused* message, not the unknown one:

```text
extract('j',  interval '3 days 4:05:06')  ERROR:  unit "j" not supported for type interval
extract('m',  interval '3 days 4:05:06')  ->  5     (MINUTE)
extract('mm', interval '3 days 4:05:06')  ->  5     (MINUTE)
extract('h',  interval '3 days 4:05:06')  ->  4     (HOUR)
```

**KNOWN DIVERGENCE (inherited, family-wide).** `parse_date_unit` knows none of
`m`, `mm`, `h`, `j`, so Basin says `not recognized` for all four where the
server answers (or, for `j`, says `not supported`). This is the same gap
`dt_fns.rs::date_part_rejects_four_unit_aliases_postgres_accepts` already
pins for `date_part`; it is inherited by `extract`, which shares
`parse_date_unit`. Not fixed here — the fix cannot be a bare table addition,
because `date_trunc('m', …)` is accepted by the server while
`date_trunc('mm', …)` is `not recognized`, so one shared table cannot express
it.

### 7.3 `extract` from `time` / `timetz` (6200 / 6201) — unimplemented, both sides fail closed

```text
extract('hour',     time   '12:34:56.789123')  -> 12
scale(extract('second', time '12:34:56.789123'))  -> 6
extract('epoch',    time   '12:34:56.789123')  -> 45296.789123
extract('timezone', timetz '12:34:56+05:30')   -> 19800
extract('hour',     timetz '12:34:56+05:30')   -> 12
```

`temporal_kind` handles only `Date32` and `Timestamp(Microsecond, _)`, so both
oids error with
`"extract on … is not implemented — only date, timestamp and timestamptz are"`.
Unchanged by this slice: `ExtractTime`/`ExtractTimetz` delegate to the same
place the deleted arm did and fail identically. Registering them is still
correct once §6 lands — they were on the `match` and must leave it together,
or the `match` becomes a graveyard of five-sixths of one function.

### 7.4 `extract('julian', timestamp)` — Basin refuses, Postgres answers

Pre-existing, documented in `extract_scale`, restated here because it is the
only unit `extract` refuses for a reason that is *not* Postgres refusing it:
Postgres computes it as a `numeric` division whose `dscale` floats (measured 20
digits for `2024-03-15 12:34:56.789123`, 28 for the same date at midnight), and
no single `Decimal128` scale reproduces that. Basin returns
`ExecError::Internal`. On a `date` there is no division and Basin does answer
(2460375, §7.1).

### 7.5 The acceptance-set change the shortcut introduces — **pinned, not shipped**

`extract_fns.rs` recovers the unit from row 0 of the evaluated array, after
checking that every row is non-null and equal. That accepts one thing the
deleted arm refused: a **computed constant**, e.g.
`extract(lower('YEAR') FROM ts)`, which PostgreSQL accepts and which the old
code refused with the `non-literal field` message.

It also **refuses one thing the deleted arm accepted: a zero-row batch** —
which is §3, i.e. every plan built through `Project::new`. That is the whole
blocker, and it is why the shortcut is a stopgap for direct `eval()` callers
and the real fix is §6's `Expr::Literal` test, which reproduces the old
acceptance set exactly.

Both directions are pinned in `extract_fns.rs`'s tests
(`a_computed_constant_unit_is_now_accepted_where_the_match_refused_it`,
`an_empty_batch_cannot_recover_the_unit_which_is_the_blocker`).

### 7.6 Date arithmetic (1099/1100/1101/2555) — measured, and one real divergence

```text
date '2024-01-15' - date '2024-01-01'   ->  14           integer   (NOT interval)
date '2024-01-15' + 1                   ->  2024-01-16   date
1 + date '2024-01-15'                   ->  2024-01-16   date
date '2024-01-15' - 1                   ->  2024-01-14   date
date '2024-01-15' + interval '1 day'    ->  2024-01-16 00:00:00
                                                         timestamp without time zone
```

NULL propagates on either side in all four (`NULL::date - 1`,
`NULL::date - date '2024-01-01'`, `date '2024-01-01' + NULL::int` are all
NULL). `date_offset_days` / `date_diff_days` reproduce every row above.

**KNOWN DIVERGENCE — Basin answers outside Postgres's `date` range.**
Postgres's `date` spans `4713-11-24 BC` … `5874897-12-31`, which in days from
the epoch is:

```text
SELECT date '5874897-12-31' - date '1970-01-01';   ->  2145042905
SELECT date '4713-11-24 BC' - date '1970-01-01';   ->    -2440222
SELECT date '2024-01-15' - 3000000;   ERROR:  date out of range
SELECT date '2024-01-15' + 2147483647; ERROR:  date out of range
```

`date_offset_days` checks only `i32` overflow (`checked_mul` / `checked_add`,
then `ExecError::Overflow("date")`). So `date '2024-01-15' - 3000000` is day
number `19737 - 3000000 = -2980263`, a perfectly good `Date32`, and Basin
returns roughly `6191-02-…  BC` where the server raises `date out of range`.
The `+ 2147483647` case errors on both sides but with different messages
(`Overflow("date")` vs `date out of range`).

This is the same shape as the divergence `dt_fns.rs` already pins for
`make_date` (`make_date_refuses_a_year_postgres_accepts`) and has the same
root cause: Basin has no `date` range check anywhere, only Arrow/chrono's. The
fix is one shared range predicate for the whole date family — `make_date`,
`date_pli`/`date_mii`, `date` literals, casts — which is its own commit.
Recorded here because this slice measured it; **not** fixed, and the arms in
§2b are untouched.

---

## 8. NEEDS VERIFICATION

Nothing in this list was checked, because this agent cannot run `cargo` and
Basin has no live server on this box to `psql` into. Each is a claim someone
with a build should confirm before acting on it.

1. **`extract_fns.rs` has never been compiled.** It is proof-read for syntax
   only. It also names `eval::eval_extract(raw: &str, …)`, which does not exist
   until §5 lands, so it *cannot* compile today. Do not add it to `mod.rs`
   before §5.
2. **The §6 patch is untested.** In particular `PlanDatum` is `eval.rs`'s alias
   for `basin_plan::Datum`; `funcs/mod.rs` would need its own import, and the
   `&[Option<&PlanDatum>]` borrow may need to be `Option<PlanDatum>` (cloned)
   if the borrow checker objects to holding `&args[i]` across the `evaluated`
   collect. Cheap either way.
3. **The `builtins().len()` constant.** §1 says 30; five other agents are
   registering functions in the same tree. Read the current value, do not
   trust that number.
4. **BC dates through `extract`.** `date_part_of`'s `pg_year` closure maps
   chrono's astronomical year to Postgres's (`y <= 0 → y - 1`), and hand-checking
   it against the server for `date '0044-03-15 BC'` agrees on all seven units
   measured (`year -44`, `century -1`, `decade -5`, `millennium -1`,
   `isoyear -44`, `julian 1705428`, `epoch -63517824000`). But it was checked
   *on paper* against the formulas, not by running Basin. Worth a real test.
5. **Whether an empty `RecordBatch` reaches `eval_scalar_fn` outside the probe.**
   `project.rs` says `filter_record_batch` "always returns a batch, even an
   empty one, rather than `None`", which implies yes — a filter that matches
   nothing feeds an empty batch to the next `Project`. If so §3's failure is
   worse than "at plan time only". Confirm before relying on it either way.
6. **Whether any other ported function is exposed to §3.** Any `ScalarFunc`
   whose output Arrow type depends on an argument *value* rather than an
   argument *type* has the same zero-row-probe bug. `dt_fns`'s and `num_fns`'s
   ports look safe (fixed return types); the `numeric` round/trunc-with-N
   family in the `match` (`OID_ROUND_NUMERIC_N`, `OID_TRUNC_NUMERIC_N`) should
   be checked before anyone ports it — `decimal_round_per_row` takes `ndigits`
   as an array, which smells like the same trap.

---

## 9. As built — what is in `extract_fns.rs`

Written, proof-read, never compiled. Two free functions, six unit structs, ten
tests.

```text
fn non_literal_unit() -> ExecError        the deleted arm's refusal, verbatim
fn literal_unit(&ArrayRef) -> &str        the stopgap: constant-column check
fn extract_one(&[ArrayRef], u32, &EvalSession)   the body all six share

pub struct ExtractDate         6199   answers
pub struct ExtractTime         6200   fails closed (temporal_kind: no Time64)
pub struct ExtractTimetz       6201   fails closed
pub struct ExtractTimestamp    6202   answers
pub struct ExtractTimestamptz  6203   answers, session-dependent
pub struct ExtractInterval     6204   fails closed (temporal_kind: no interval)
```

Tests, and what each one is holding down:

| test | pins |
|---|---|
| `the_six_extract_oids_are_deliberately_not_registered_yet` | the status in §0 — flip to `is_some()` when §6 lands, do not delete |
| `each_impl_reports_its_own_oid` | six near-identical impls, no `register_scalar` check to catch a typo |
| `the_output_scale_depends_on_the_unit_not_on_the_argument_type` | `second` → `Decimal128(38,6)` value `9123456`; `year` → `(38,0)` value `2024`; the two types differ — §3 in one assertion |
| `an_empty_batch_cannot_recover_the_unit_which_is_the_blocker` | **the blocker** |
| `a_computed_constant_unit_is_now_accepted_where_the_match_refused_it` | acceptance-set change, direction 1 (§7.5) |
| `a_varying_unit_is_refused_because_one_array_carries_one_scale` | the fail-closed case, plus a NULL unit |
| `a_null_value_yields_a_null_row_and_the_length_is_preserved` | NULL in/NULL out, output length == input length |
| `extract_from_a_date_refuses_the_eight_units_postgres_refuses` | §7.1, both halves — the eight refusals and the twelve accepted values at scale 0 |
| `extract_rejects_four_unit_aliases_postgres_accepts` | §7.2's `m`/`mm`/`h`/`j` divergence, inherited from `parse_date_unit` |
| `extract_from_an_interval_still_fails_closed` | 6204 unchanged; the fix is not a `temporal_kind` arm (§5) |
| `a_missing_argument_is_reported_as_a_planner_bug` | arity is a planner bug, not user error |

The accepted-unit values in the `date` test were read off 18.2 and cross-checked
against `date_part_of`'s formulas by hand: `epoch 1709596800`, `julian 2460375`,
`doy 65`, `dow 2`, `isodow 2`, `isoyear 2024`, `year 2024`, `quarter 1`,
`week 10`, `decade 202`, `century 21`, `millennium 3` for `DATE '2024-03-05'`
(`Date32` day number 19787).

### Reproducing the measurements

```sh
psql postgres://pc@127.0.0.1:5432/postgres -X -A -c "<query>"
```

* scales: `SELECT scale(pg_catalog.extract(u, <value>)), pg_catalog.extract(u, <value>) FROM unnest(ARRAY[…]) u;`
* refusals: one `SELECT pg_catalog.extract('<unit>', <value>);` per unit, reading `ERROR:` verbatim
* operator oids: the `pg_operator ⋈ pg_proc` join in §4
* date range: `SELECT date '5874897-12-31' - date '1970-01-01';` and the BC bound
