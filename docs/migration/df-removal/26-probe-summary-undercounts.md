# The probe's summary line counts a different thing than its own table

`crates/basin-engine/tests/fallback_histogram.rs` prints two coverage numbers
and they do not agree. Measured on the working tree at `e762b942` + in-flight
agent edits:

```
  ...
  DML                  served   2 / 15  fallback  13  errored   0  panicked   0
  ...
─── owned-engine coverage over 231 representative queries ───
served   : 209
fallback : 10
reasons  : FallbackReasonCountersSnapshot { ineligible: 1, unsupported: 5,
                                            lowering_error: 0, build_error: 0,
                                            exec_error: 4 }
```

Summing the per-area table: **209 served + 21 fallback + 1 errored = 231**,
which reconciles exactly against the 231 queries the probe claims to run.

The summary line says `fallback : 10`. It is short by eleven.

## Why

The two numbers have different sources, which is the whole defect.

The per-area table is built by observation — the harness inspects each query's
outcome and increments on the `"FELL BACK"` marker (`fallback_histogram.rs:458`).

The summary line is read out of the engine (`fallback_histogram.rs:488-489`):

```rust
let served = eng.owned_engine_served_count();
let fallback = eng.owned_engine_fallback_count();
```

Those counters only move for queries that reach the owned engine. The engine
accounts for `209 + 10 = 219` queries; the harness observes 231. **Twelve
queries — eleven fallbacks and the one errored query — never touch an engine
counter at all**, because they are refused before the owned engine is entered.
Commit `2d21de4c` ("make DML lowering refuse honestly") is the likely route:
a statement refused upstream is correctly not served, but it is also never
counted as a fallback.

`served` is not affected: a query cannot be served without going through the
engine, so the served counter and the observed count agree at 209. Only the
fallback side is silently lossy — which is the worse direction, because the
number that under-reports is the one measuring what is left to do.

## What this invalidated

Every state line in this program of the form "probe N served / M fallback / 1
errored of 231" was mixing the two sources, and none of them summed to 231:

* "206 served / 13 fallback / 1 errored of 231" sums to 220.
* The per-area table published alongside it summed to 204, not 206.

So the headline and its own breakdown disagreed with each other and with the
total, in two different ways, and this went unnoticed across several cycles.
The reading recorded above (209 / 21 / 1) is the first internally consistent
one.

## The rule this earns

**Read the coverage numbers off the per-area table, not the summary line.** The
table is observed; the summary is reported by the subject under measurement.
When an instrument asks the thing being measured how it did, it cannot see the
cases where the thing being measured was never asked.

This is the same defect class as the orphan battery's hardcoded census and the
golden harness's plain invocation reporting "owned engine served 0 of 440": a
harness that appears to run, and reports a number that measures something other
than what its label claims.

## Not fixed here

Deliberately left alone. The probe is the program's steering instrument and it
is currently being read by several concurrent agents; changing what its numbers
mean mid-flight would invalidate their before/after comparisons. The fix is to
have the harness report its own observed totals rather than the engine's
counters, and it should land in one commit, alone, with a probe run either side
of it.
