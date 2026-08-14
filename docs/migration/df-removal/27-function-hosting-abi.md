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
