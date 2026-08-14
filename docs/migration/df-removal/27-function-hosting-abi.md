---
title: "DF removal — the function hosting ABI"
nav_section: migration
sidebar_position: 27
summary: "IN PROGRESS. Specifies the seam through which the owned engine hosts SQL functions, replacing DataFusion's SessionContext UDF/UDAF/UDTF registry. Leads with the finding that the registry is not the only blocker: basin-plan cannot type a function call at all (schema.rs:326), so the ABI must carry return-type resolution, not just execution."
tags: [migration, datafusion, functions, abi, design]
---

# 27 — The function hosting ABI

Status: **IN PROGRESS** — sections are appended as they are verified. Do not
implement against a section that is not marked VERIFIED or DESIGN.

Every count below is followed by the command that produced it. Measured on
`feat/own-engine-remove-datafusion` at the working tree of 2026-08-14 (dirty:
`git status --porcelain` lists 6 modified, 5 untracked paths). No `cargo
build`/`check`/`test` was run — anything that would need one is on the NEEDS
VERIFICATION list in §9.

---

## 0. The census, before anything else

[24](./24-sessioncontext-replacement.md) names function hosting as the head of
the critical path and quotes 372 / 245 / 329. Re-measured here:

```
$ grep -rn "impl ScalarUDFImpl for"    crates/ --include='*.rs' | wc -l   # 245
$ grep -rn "impl AggregateUDFImpl for" crates/ --include='*.rs' | wc -l   #  11
$ grep -rn "impl WindowUDFImpl for"    crates/ --include='*.rs' | wc -l   #   0
$ grep -rn "impl TableFunctionImpl for" crates/ --include='*.rs' | wc -l  #   6
$ grep -rn "register_udf(\|register_udaf(\|register_udwf(\|register_udtf(" \
    crates/ --include='*.rs' | wc -l                                      # 372
$ grep -rn "impl ScalarUDFImpl for" crates/ --include='*.rs' \
    | cut -d/ -f2 | sort | uniq -c                                        # 245 basin-engine
```

245 / 11 / 0 / 6 confirmed; 372 confirmed. Two corrections to how those numbers
have been read:

1. **Every single `ScalarUDFImpl` lives in `basin-engine`.** Not one is in
   `basin-fn`, `basin-geo`, `basin-vector`, `basin-trgm` or any other leaf
   crate. The hosting problem is therefore entirely a `basin-engine` →
   `basin-exec` migration, with no cross-crate trait-visibility problem to
   solve first. That is good news and it is not written down anywhere else.
2. **There are zero `WindowUDFImpl`s.** Window functions are not part of the
   rehosting surface at all — whatever window functions Basin answers today,
   it answers through DataFusion's built-ins, not through Basin code. A window
   arm in the hosting ABI would be hosting *nothing*. §3 draws the consequence.

`372 registration sites > 262 impls` because a single impl is registered under
several names/arities, and because some sites register DataFusion's own
built-ins. 372 is a count of *call sites*, not of functions; do not schedule
against it.

### What `basin-exec` answers today

```
$ grep -c "const OID_" crates/basin-exec/src/eval.rs                      # 130
```

130 OID constants, all in one file, all consumed by one `match oid` at
`crates/basin-exec/src/eval.rs:2723-3093` (371 lines) inside `eval_scalar_fn`.
Its fallback arm is explicit about what it is:

```rust
other => Err(ExecError::Internal(format!(
    "scalar function oid {other} is not implemented in eval yet — the bridge should \
     fall back to DataFusion for it rather than guess"
))),
```

130 constants is not 130 functions — several constants share one arm
(`OID_LENGTH_TEXT | OID_CHAR_LENGTH_TEXT | OID_CHARACTER_LENGTH_TEXT`), and
some name one function at two arities. It is the right order of magnitude for
"about a hundred names answered natively".

---

## 1. The lead finding: the registry is not the blocker. The **planner** is.

The brief invited this conclusion and the code supports it. Registration is a
real gap, but it is the *second* gap. The first is that **`basin-plan` cannot
determine the return type of a function call at all**:

`crates/basin-plan/src/schema.rs:322-326`

```rust
Expr::ScalarFn { .. }
| Expr::Aggregate { .. }
| Expr::Window { .. }
| Expr::SetReturning { .. } => Err(SchemaError::Unimplemented("function return type")),
```

with the comment above it saying, honestly, "There is no catalog yet, so this
cannot be more than 'not yet'."

The consequence is sharp. `expr_type` is how the owned planner computes an
operator's output schema. If *any* projected expression is a function call,
schema derivation for that plan node fails. So today the owned engine can
evaluate `lower(x)` (there is an OID arm for it) but cannot *type* a plan that
projects it. Porting 245 more function bodies into `eval.rs` does not move this
one inch: the bodies were never the missing piece for the planner.

**Therefore the ABI's first obligation is not `fn invoke`. It is `fn
return_type`.** A hosting design that specifies only execution would let ten
agents port 245 bodies into an engine that still cannot plan a query that calls
any of them. That is the failure mode this document exists to prevent, and it
is why §4's trait leads with signature/return-type and treats invocation as the
second method.

A second, smaller shape problem, same family: `eval_scalar_fn` is a free
function taking `(FuncId, &[Expr], &RecordBatch, &EvalSession)`. It has no
`&self`, no registry parameter, and 130 constants baked into its `match`. Every
caller of `eval`/`eval_with` in the workspace passes no registry, so introducing
one is a signature change that ripples:

```
$ grep -rn "eval_with(\|eval(" crates/basin-exec/src --include='*.rs' | wc -l
```

— counted in §7; the number decides whether the registry is threaded as a new
parameter or hung off `EvalSession`. §5 recommends the latter for exactly this
reason, and it is the single most consequential decision in this document.

(sections continue — being appended)

---

## VERIFICATION NOTE (main loop, appended after the authoring agent stalled)

§1's conclusion is **right in its recommendation and wrong in its mechanism**,
and the difference matters enough to record before ten agents design against it.

**What is confirmed.** `crates/basin-plan/src/schema.rs:322-326` does return
`Err(SchemaError::Unimplemented("function return type"))` for all four function
expression kinds. Verified by reading it.

**What is not.** §1 says "if *any* projected expression is a function call,
schema derivation for that plan node fails", and concludes the owned planner
cannot type a query that calls a function. That cannot be true as stated:
`SELECT name, sum(amt) FROM t GROUP BY name` is an `Expr::Aggregate` in the
target list and the probe reports it **served** today.

The reason is that `schema.rs::output_schema` is a **stub the live path
deliberately routes around**, and the code says so in its own comments:

```
crates/basin-plan/src/opt/projection.rs:165
    /// Not `basin_plan::schema::output_schema` — that is still a stub
crates/basin-plan/src/opt/decorrelate.rs:1141
    /// — without needing `crate::schema::output_schema` (which fails on anything ...)
```

`grep -rn "expr_type"` outside `schema.rs` returns nothing in `basin-exec` or
`basin-engine`: **`expr_type` has no caller on the execution path at all.**

The live derivation is `select_output_schema` in `lower/select.rs`, and its own
doc states the actual behaviour:

```
crates/basin-plan/src/lower/select.rs:672-676
    /// ... a computed column such as an aggregate or
    /// window result reports [`PgType::UNKNOWN`] rather than a guess ...
```

**So the true state is not "cannot type" but "types as UNKNOWN".** The plan is
built; the type is simply absent.

**Why §1's recommendation survives the correction, and is arguably stronger.**
UNKNOWN is not inert. It is an input to overload resolution, so every function
result feeding another call resolves against UNKNOWN arguments — which produces
MISRESOLUTION, not merely a gap. That is the same root cause the
`func_select_candidate` work is hitting from the other end: resolution quality
is capped by argument-type quality, so porting more resolution stages cannot
fix what bad argument types cause.

So: **the ABI must still lead with `return_type` rather than `invoke`**, and
§4's ordering stands. But the failure it prevents is *silent wrong overload
selection on nested calls*, not *unplannable queries*. A design justified by the
wrong mechanism tends to get "simplified" later by someone who notices the
mechanism is false — which is how this program has produced fourteen documents
that were confidently wrong.

Neither number in §1's closing paragraph should be carried forward unchecked
either: `grep -rn "eval_with(\|eval(" crates/basin-exec/src` returns **303**,
not a figure "counted in §7", which was never written.

**Status: this document is INCOMPLETE.** Sections 2-7 (the trait signatures,
registration/lookup, the coexistence story, worked examples, the slicing plan)
do not exist yet. It is committed in this state only because four consecutive
agents died authoring it and losing 126 verified lines a fifth time is worse
than committing a partial document that says plainly it is partial.

---

# Sections 2-7 (main loop)

Written by the main loop after five consecutive agents died authoring this
file. Everything below is designed against signatures read from the tree; the
line references were checked at `50e0ac69`.

## 2. What exists to design with

```rust
// crates/basin-exec/src/eval.rs:2709
fn eval_scalar_fn(func: FuncId, args: &[Expr], batch: &RecordBatch,
                  session: &EvalSession) -> Result<ArrayRef, ExecError>
```

Three facts fall out of that signature and they decide most of what follows.

**It takes `&[Expr]`, not evaluated arrays.** Every arm evaluates its own
arguments. That is real generality — an arm *could* be lazy — but no
PostgreSQL *function* is lazy. The lazy constructs (`CASE`, `COALESCE`,
`AND`/`OR` short-circuit) are `Expr` variants in their own right, not `pg_proc`
rows. So the generality is unused, and it costs every arm the same three lines
of argument-evaluation boilerplate.

**It is a free function with no `&self` and no registry parameter.** Nothing
can be registered because there is nowhere to register into.

**`EvalSession` is already the session carrier** (`eval.rs:681`): time zone,
transaction timestamp, statement timestamp. It is threaded to every evaluation
already.

And from `basin-pgtype` (`func.rs:254`), `FuncSig` already carries
`{oid, name, args, ret, kind}` with `FuncKind` ∈ {Scalar, Aggregate, Window,
SetReturning}. **The catalog half of hosting already exists.** What is missing
is an executable bound to an oid.

## 3. Where resolution ends and hosting begins

**The ABI is keyed on OID, not on name.** `basin-pgtype::func::resolve` maps
`(name, arg_types) -> FuncSig`; hosting maps `FuncSig.oid -> impl`. Two
reasons, and the second is the load-bearing one:

* An oid is what `pg_proc` keys on, what the orphan battery already probes by,
  and what `FuncId` already carries.
* Resolution is **not correct yet** and will not be for some time.
  `func_select_candidate` stages 2-4 landed at `50e0ac69`, but
  `to_char(unknown, unknown)` still diverges because Basin tabulates 2 of
  PostgreSQL's 8 rows, and computed columns still type as `PgType::UNKNOWN`
  (see the verification note above). **Keying hosting on oid makes resolution
  quality somebody else's bug.** A name-keyed ABI would couple 329 ports to a
  moving target.

So: a function author implements against an oid and never thinks about
overloads. A resolution bug shows up as *the wrong function being called*, not
as a broken port.

## 4. The traits

Object-safe, because the registry stores `Box<dyn _>`. `return_type` comes
first in each — that ordering is the point of §1, as corrected.

```rust
pub trait ScalarFunc: Send + Sync {
    fn oid(&self) -> Oid;

    /// Declared argument types in, result type out. Called at PLAN time.
    /// Most implementations return `FuncSig.ret` verbatim; the ones that
    /// cannot are why this is a method and not a table lookup — see §4.1.
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError>;

    /// Arguments arrive EVALUATED and length-aligned to the batch.
    fn invoke(&self, args: &[ArrayRef], session: &EvalSession)
        -> Result<ArrayRef, ExecError>;
}

pub trait AggregateFunc: Send + Sync {
    fn oid(&self) -> Oid;
    fn return_type(&self, args: &[PgType]) -> Result<PgType, ExecError>;
    fn accumulator(&self) -> Box<dyn Accumulator>;
}

pub trait Accumulator: Send {
    fn update(&mut self, args: &[ArrayRef], row: usize) -> Result<(), ExecError>;
    fn merge(&mut self, other: &dyn Accumulator) -> Result<(), ExecError>;
    /// Consuming would need `Box<Self>`, which is not object-safe here.
    fn finish(&mut self) -> Result<CellValue, ExecError>;
}
```

`invoke` takes **evaluated** `&[ArrayRef]`, not `&[Expr]` — deliberately
narrower than today's free function. It deletes the same boilerplate from 245
future impls, and it removes a foot-gun: an arm holding `&[Expr]` can evaluate
an argument twice, or zero times, and `now()` changing between two evaluations
inside one statement is a bug this program has already seen (#151).

Window functions are NOT in this list. They need frame state, peer groups and
`ORDER BY` visibility, which is a different shape from `Accumulator`, and
`window.rs` already implements them directly. **Porting windows through this
ABI is out of scope until a window operator exists that needs it** — inventing
a trait with no implementor is how `basin-pgcatalog` became 13,231 unreachable
lines.

### 4.1 Why `return_type` is a method

Most functions return `FuncSig.ret` and could use a table. The exceptions are
the reason the trait leads with it:

* `extract(... FROM interval)` (oid 6204) returns `numeric` with a **per-unit
  scale** — measured on 18.2: `second`/`epoch` scale 6, `milliseconds` 3,
  everything else 0. A single `ret` oid cannot express that.
* Polymorphic rows (`array_agg`, `min`/`max` over `anyelement`) are
  monomorphized in `FUNCS` today; a method lets a later increment compute them
  instead of enumerating them.

A default method returning the catalog's `ret` keeps the common case free.

## 5. Registration and lookup — hang the registry off `EvalSession`

**This is the most consequential decision in this document.**

```
$ grep -rn "eval_with(\|eval(" crates/basin-exec/src --include='*.rs' | wc -l
303
```

Threading a `&FuncRegistry` parameter through `eval` touches all 303. Hanging
it off `EvalSession` — which is already threaded everywhere — touches the
struct and its constructors:

```rust
pub struct EvalSession {
    time_zone: String,
    transaction_timestamp: Option<i64>,
    statement_timestamp: Option<i64>,
    funcs: Arc<FuncRegistry>,   // new; `Arc` so cloning a session is cheap
}
```

`EvalSession::default()` supplies the built-in registry, so **every existing
caller keeps compiling unchanged**. That property is what makes §6 possible.

The registry itself is deliberately dull — `HashMap<Oid, Box<dyn ScalarFunc>>`
plus one for aggregates. No priority, no shadowing, no versioning. A duplicate
oid is a build-time panic in the constructor, not a silent last-wins.

## 6. Coexistence — how 329 functions move without a flag day

```rust
fn eval_scalar_fn(func: FuncId, args: &[Expr], batch: &RecordBatch,
                  session: &EvalSession) -> Result<ArrayRef, ExecError> {
    if let Some(f) = session.funcs.scalar(func.0) {
        let evaluated = args.iter()
            .map(|e| eval_with(e, batch, session))
            .collect::<Result<Vec<_>, _>>()?;
        return f.invoke(&evaluated, session);
    }
    match func.0.get() { /* ... the existing 130-constant match, untouched ... */ }
}
```

**Registry first, `match` as fallback.** Consequences, and they are the whole
reason to prefer this shape:

* Moving a function is: write the impl, register it, DELETE its `match` arm.
  Three edits in three places, no coordination with anyone else.
* Two agents porting different oids never touch the same lines except the
  registry's constructor list — which is append-only and conflicts trivially.
* At every intermediate commit the engine works. There is no window where half
  the functions are hosted and nothing runs.
* **The migration is verifiable at each step by an oracle that already exists**:
  the orphan battery resolves by name and executes by oid, so a mis-registered
  function shows up as a battery failure, not as silence.

If a future increment wants the `match` gone entirely, the acceptance test is
`match` reduced to `_ => Err(...)`. Until then the two coexist without either
knowing about the other.

## 7. Slicing 329 functions into non-colliding batches

Slice by **oid range**, not by family or by file. Families collide (`date_part`
and `to_char` both touch datetime helpers); oid ranges do not, because the
registry constructor is append-only and each impl lives in its own new module.

Proposed slices, each one agent, each a new file under
`crates/basin-exec/src/funcs/`:

| Slice | Content | Rough size |
|---|---|---|
| `str_*`   | string functions | ~60 |
| `num_*`   | numeric/math | ~55 |
| `dt_*`    | date/time (`date_part`, `to_char`, `age`, `date_trunc`) | ~45 |
| `json_*`  | JSON/JSONB | ~40 |
| `arr_*`   | array | ~35 |
| `agg_*`   | aggregates | ~30 |
| `misc_*`  | everything else | ~60 |

Each agent: implement its slice against §4, register in the constructor, delete
the corresponding `match` arms, add a test per oid against live PostgreSQL,
report a NEEDS VERIFICATION list. **No agent runs cargo** — the main loop
batch-verifies, per the operational rule that ended a 30% agent death rate.

The counts above are ESTIMATES from the shape of `FUNCS` and have not been
derived by a command; the first agent into any slice should print its own count
and correct this table. The "329" in circulation traces to doc 24 and has not
been independently reproduced either — **treat it as unverified.**

## What is still missing from this document

* No worked example. §4 gives signatures, not a written-out `lower` /
  `date_part` / `sum`. The first slice agent should write one and paste it back
  here as the template the rest copy.
* Set-returning functions are unaddressed. `FuncKind::SetReturning` exists and
  SRFs in FROM already lower (`73429a38`), but a hosting shape for them is not
  designed here.
* `merge` on `Accumulator` is specified because parallel aggregation will want
  it, and is **not** required by any current operator. It should be a default
  method that errors until something calls it, rather than 30 hand-written
  implementations of a path nothing exercises.
