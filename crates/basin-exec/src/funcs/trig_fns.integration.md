# `trig_fns` — integration instructions (wave-15 trig/math slice)

**STATUS: IN PROGRESS.** This file is written before and updated during the
port so the work survives even if the agent does not. Read it top to bottom
before touching `eval.rs` or `funcs/mod.rs` — this slice owns NEITHER of those
files and made NO edits to them.

The slice owns exactly two files, both new:

* `crates/basin-exec/src/funcs/trig_fns.rs`
* `crates/basin-exec/src/funcs/trig_fns.integration.md` (this file)

Nothing has been compiled: the agent that wrote it was forbidden from running
`cargo` (build-lock starvation). Everything below is a *hand-verified*
instruction list. See "NEEDS VERIFICATION" at the bottom.

---

## 1. Add the module

In `crates/basin-exec/src/funcs/mod.rs`, next to the existing `pub mod`
declarations (currently `dt_fns`, `num_fns`, `str_fns`):

```rust
pub mod trig_fns;
```

## 2. Registrations

Append inside `funcs::builtins()`'s `get_or_init` closure, after the `dt_fns`
block:

```rust
        // trig_fns — ported by the wave-15 trig/math slice.
        r.register_scalar(Box::new(trig_fns::AcosFloat8));
        r.register_scalar(Box::new(trig_fns::AcoshFloat8));
        r.register_scalar(Box::new(trig_fns::AsinFloat8));
        r.register_scalar(Box::new(trig_fns::AsinhFloat8));
        r.register_scalar(Box::new(trig_fns::AtanFloat8));
        r.register_scalar(Box::new(trig_fns::Atan2Float8));
        r.register_scalar(Box::new(trig_fns::AtanhFloat8));
        r.register_scalar(Box::new(trig_fns::CbrtFloat8));
        r.register_scalar(Box::new(trig_fns::CosFloat8));
        r.register_scalar(Box::new(trig_fns::CoshFloat8));
        r.register_scalar(Box::new(trig_fns::CotFloat8));
        r.register_scalar(Box::new(trig_fns::DegreesFloat8));
        r.register_scalar(Box::new(trig_fns::ExpFloat8));
        r.register_scalar(Box::new(trig_fns::LnFloat8));
        r.register_scalar(Box::new(trig_fns::LogFloat8));
        r.register_scalar(Box::new(trig_fns::Log10Float8));
        r.register_scalar(Box::new(trig_fns::RadiansFloat8));
        r.register_scalar(Box::new(trig_fns::SinFloat8));
        r.register_scalar(Box::new(trig_fns::SinhFloat8));
        r.register_scalar(Box::new(trig_fns::SqrtFloat8));
        r.register_scalar(Box::new(trig_fns::TanFloat8));
        r.register_scalar(Box::new(trig_fns::TanhFloat8));
        r.register_scalar(Box::new(trig_fns::PowFloat8));
        r.register_scalar(Box::new(trig_fns::PowerFloat8));
```

**24 registrations.** `pi()` (oid 1610) is the 25th requested function and is
NOT here — see section 5.

`funcs/mod.rs`'s `the_registry_reports_what_is_actually_hosted` test asserts
`builtins().len() == 24`. After this slice it must read **48** (24 + 24).

## 3. Arms to delete from `eval_scalar_fn`'s `match` in `eval.rs`

(pending — filled in as each function lands)

## 4. Divergences from PostgreSQL 18.2

(pending)

## 5. Not ported

(pending)

## 6. NEEDS VERIFICATION

(pending)
