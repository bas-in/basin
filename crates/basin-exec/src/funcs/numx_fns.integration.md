# `numx_fns` — integration instructions

Slice: the sixteen "extended numeric" arms — factorial, gcd/lcm, the
`numeric` shaping trio (`round`/`sign`/`trunc`, both arities), `to_hex`, and
the `bit_length`/`octet_length` quartet.

Owner file: `crates/basin-exec/src/funcs/numx_fns.rs` (new).
This document is written BEFORE the code and appended after every function,
so the integration survives even if the porting agent does not.

**STATUS: IN PROGRESS — see "Progress" at the bottom for what is actually
done. Do not integrate arms listed as not-yet-ported.**

---

## 1. Module declaration

In `crates/basin-exec/src/funcs/mod.rs`, alongside the existing
`pub mod dt_fns; pub mod num_fns; pub mod str_fns;`:

```rust
pub mod numx_fns;
```

## 2. Registrations

Append inside `builtins()` in `crates/basin-exec/src/funcs/mod.rs`:

```rust
// numx_fns — the extended numeric slice.
```

(exact lines appended below as each function lands)

## 3. Match arms to delete from `eval_scalar_fn` in `crates/basin-exec/src/eval.rs`

(listed below as each function lands)

## 4. Divergences from PostgreSQL 18.2

(listed below as each function lands)

## 5. NEEDS VERIFICATION

(listed below)

---

## Progress

- [ ] nothing yet — file just created
