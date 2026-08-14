# `numx_fns` — integration instructions

Slice: the sixteen "extended numeric" arms — factorial, gcd/lcm, the
`numeric` shaping trio (`round`/`sign`/`trunc`, both arities), `to_hex`, and
the `bit_length`/`octet_length` quartet.

Owner file: `crates/basin-exec/src/funcs/numx_fns.rs` (new).
This document is written BEFORE the code and appended after every function,
so the integration survives even if the porting agent does not.

**STATUS: IN PROGRESS.** See "Progress" at the bottom. Integrate only the
functions listed there as DONE.

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
        r.register_scalar(Box::new(numx_fns::OctetLengthText));
        r.register_scalar(Box::new(numx_fns::BitLengthText));
        r.register_scalar(Box::new(numx_fns::OctetLengthBytea));
        r.register_scalar(Box::new(numx_fns::BitLengthBytea));
        r.register_scalar(Box::new(numx_fns::ToHexInt4));
        r.register_scalar(Box::new(numx_fns::ToHexInt8));
        r.register_scalar(Box::new(numx_fns::Factorial));
        r.register_scalar(Box::new(numx_fns::GcdInt4));
        r.register_scalar(Box::new(numx_fns::GcdInt8));
        r.register_scalar(Box::new(numx_fns::LcmInt4));
        r.register_scalar(Box::new(numx_fns::LcmInt8));
```

All catalog rows were confirmed present in `basin_pgtype::func::FUNCS`
(`register_scalar` asserts this and panics otherwise): 1374, 1811, 720, 1810,
2089, 2090, 1376, 5044, 5045, 5046, 5047.

Note: `funcs/mod.rs`'s `the_registry_reports_what_is_actually_hosted` test
asserts `builtins().len() == 24`. It must be bumped by the number of
registrations added above (24 + 11 = 35 after batch 2).

## 3. Match arms to delete from `eval_scalar_fn` in `crates/basin-exec/src/eval.rs`

Around line 3046, delete these four lines (and the two-line comment above
them, which describes only these):

```rust
        // String and binary measurement. `bit_length` is `octet_length` times
        // eight and neither is `length` — see [`text_byte_length`].
        OID_OCTET_LENGTH_TEXT => text_byte_length(&a(0)?, false),
        OID_BIT_LENGTH_TEXT => text_byte_length(&a(0)?, true),
        OID_OCTET_LENGTH_BYTEA => bytea_byte_length(&a(0)?, false),
        OID_BIT_LENGTH_BYTEA => bytea_byte_length(&a(0)?, true),
```

Also then dead, and to be deleted with them:

* the constants at `eval.rs:523-526` —
  `OID_BIT_LENGTH_TEXT` (1811), `OID_BIT_LENGTH_BYTEA` (1810),
  `OID_OCTET_LENGTH_TEXT` (1374), `OID_OCTET_LENGTH_BYTEA` (720)
* `fn text_byte_length` (`eval.rs:6246`) and `fn bytea_byte_length`
  (`eval.rs:6265`) — **verify with a grep first**; they had no other callers
  at time of writing.

Around line 3051, delete:

```rust
        OID_TO_HEX_INT4 => eval_to_hex_i32(&a(0)?),
        OID_TO_HEX_INT8 => eval_to_hex_i64(&a(0)?),
```

Then dead: constants `OID_TO_HEX_INT4` (2089) and `OID_TO_HEX_INT8` (2090) at
`eval.rs:532-533`, and `fn eval_to_hex_i32` (`eval.rs:6310`) /
`fn eval_to_hex_i64` (`eval.rs:6320`).

Around line 3066, delete these five lines **and** the three-line comment
above them, which describes only this group:

```rust
        // Integer math. The overflow boundaries are the whole difficulty —
        // see [`pg_gcd_i64`]'s measured truth table.
        OID_FACTORIAL => eval_factorial(&a(0)?),
        OID_GCD_INT4 => eval_gcd_lcm_i32(&a(0)?, &a(1)?, true),
        OID_LCM_INT4 => eval_gcd_lcm_i32(&a(0)?, &a(1)?, false),
        OID_GCD_INT8 => eval_gcd_lcm_i64(&a(0)?, &a(1)?, true),
        OID_LCM_INT8 => eval_gcd_lcm_i64(&a(0)?, &a(1)?, false),
```

Then dead: constants `OID_FACTORIAL` (1376), `OID_GCD_INT4` (5044),
`OID_GCD_INT8` (5045), `OID_LCM_INT4` (5046), `OID_LCM_INT8` (5047) at
`eval.rs:536-540`, plus `fn eval_factorial` (`eval.rs:6345`),
`const DECIMAL128_LIMIT` (`eval.rs:6379`), `fn pg_gcd_i64` (`eval.rs:6402`),
`fn pg_lcm_i64` (`eval.rs:6423`), `fn eval_gcd_lcm_i32` (`eval.rs:6446`) and
`fn eval_gcd_lcm_i64` (`eval.rs:6470`).

**Caution — `pg_gcd_i64` had 7 references in `eval.rs` at time of writing**
(definition, `pg_lcm_i64`, the two `eval_gcd_lcm_*`, and doc-comment links).
Grep before deleting; if `eval.rs`'s own unit tests call it, those tests move
here or are deleted with the arms.

## 4. Divergences from PostgreSQL 18.2

Measured on `postgres://pc@127.0.0.1:5432/postgres`,
`PostgreSQL 18.2 (Homebrew) on aarch64-apple-darwin24.6.0`.

### 4.1 `bit_length` / `octet_length` — no divergence found

The quartet agrees with the server on everything tested.

```
$ psql -At -c "select 'length', length('héllo')::text
   union all select 'octet_length', octet_length('héllo')::text
   union all select 'bit_length', bit_length('héllo')::text
   union all select 'octet_length(emoji)', octet_length('😀')::text
   union all select 'bit_length(emoji)', bit_length('😀')::text
   union all select 'length(emoji)', length('😀')::text
   union all select 'octet_length(empty)', octet_length('')::text
   union all select 'bit_length(empty)', bit_length('')::text
   union all select 'octet_length(bytea)', octet_length('\xdeadbeef'::bytea)::text
   union all select 'bit_length(bytea)', bit_length('\xdeadbeef'::bytea)::text
   union all select 'octet_length(NULL)', coalesce(octet_length(NULL::text)::text,'NULL');"
length|5
octet_length|6
bit_length|48
octet_length(emoji)|4
bit_length(emoji)|32
length(emoji)|1
octet_length(empty)|0
bit_length(empty)|0
octet_length(bytea)|4
bit_length(bytea)|32
octet_length(NULL)|NULL
```

The `'héllo'` and `'😀'` rows are the ones that matter: they prove
`octet_length` counts BYTES while `length` counts CHARACTERS, so the port
must not be routed through `eval.rs`'s `text_char_length`. It is not.

### 4.2 `to_hex` — no divergence found

Two's complement on negatives, confirmed at both widths:

```
$ psql -At -c "select 'to_hex(255)', to_hex(255)
   union all select 'to_hex(0)', to_hex(0)
   union all select 'to_hex(-1 i4)', to_hex((-1)::int4)
   union all select 'to_hex(-2147483648)', to_hex((-2147483648)::int4)
   union all select 'to_hex(2147483647)', to_hex(2147483647::int4)
   union all select 'to_hex(-1 i8)', to_hex((-1)::int8)
   union all select 'to_hex(-9223372036854775808)', to_hex((-9223372036854775808)::int8)
   union all select 'to_hex(9223372036854775807)', to_hex(9223372036854775807::int8);"
to_hex(255)|ff
to_hex(0)|0
to_hex(-1 i4)|ffffffff
to_hex(-2147483648)|80000000
to_hex(2147483647)|7fffffff
to_hex(-1 i8)|ffffffffffffffff
to_hex(-9223372036854775808)|8000000000000000
to_hex(9223372036854775807)|7fffffffffffffff
```

Resolution, not this function — matches the note in the task brief:

```
$ psql -At -c "select to_hex('42');"
ERROR:  function to_hex(unknown) is not unique
HINT:  Could not choose a best candidate function. You might need to add
       explicit type casts.
```

The live server is ambiguous here too, so Basin's refusal to resolve
`to_hex('42')` is correct PostgreSQL behaviour and not a gap in this port.

### 4.3 `factorial` — DIVERGES on the overflow threshold and on the error class

Server side:

```
$ psql -At -c "select 'f(0)', factorial(0)::text
   union all select 'f(1)',factorial(1)::text
   union all select 'f(20)',factorial(20)::text
   union all select 'f(33)',factorial(33)::text
   union all select 'f(34)',factorial(34)::text;"
f(0)|1
f(1)|1
f(20)|2432902008176640000
f(33)|8683317618811886495518194401280000000
f(34)|295232799039604140847618609643520000000

$ psql -At -c "select length(factorial(20000)::text);"
77338
$ psql -At -c "select length(factorial(32000)::text);"
130271
$ psql -At -c "select length(factorial(32200)::text);"
ERROR:  value overflows numeric format
$ psql -At -c "select length(factorial(50000)::text);"
ERROR:  value overflows numeric format
```

**DIVERGENCE 1 — threshold.** PostgreSQL answers up to somewhere in
`(32000, 32200]` (`numeric`'s 131072-integral-digit ceiling). Basin's
`Decimal128(38, 0)` runs out at **33!** (37 digits); `factorial(34)` is 39
digits and raises `ExecError::Overflow("factorial")`. Both refuse rather than
truncate, so only the threshold differs. Inherited from `eval.rs`, NOT fixed
— fixing it needs arbitrary-precision decimals.

**DIVERGENCE 2 — error class on a negative.** With `VERBOSITY=verbose`:

```
$ psql -v VERBOSITY=verbose -At -c "select factorial(-1);"
ERROR:  22003: factorial of a negative number is undefined
LOCATION:  numeric_fac, numeric.c:3753

$ psql -v VERBOSITY=verbose -At -c "select factorial(100000);"
ERROR:  22003: value overflows numeric format
LOCATION:  numeric_fac, numeric.c:3763
```

Basin raises `ExecError::TypeMismatch("factorial of a negative number is
undefined")`, whose `Display` is `type mismatch: factorial of a negative
number is undefined` — the message is prefixed and the SQLSTATE is not
`22003`. Inherited, NOT fixed.

### 4.4 `gcd` / `lcm` — behaviour matches; only the SQLSTATE diverges

The full truth table in `eval.rs`'s `pg_gcd_i64` doc was re-measured live for
this port and every entry still holds:

```
$ psql -At -c "select 'gcd(0,0)', gcd(0,0)::text
   union all select 'gcd(-12,18)', gcd(-12,18)::text
   union all select 'gcd(12,-18)', gcd(12,-18)::text
   union all select 'gcd(-12,-18)', gcd(-12,-18)::text
   union all select 'gcd(0,-5)', gcd(0,-5)::text
   union all select 'gcd(INTMIN,1)', gcd((-2147483648)::int4,1)::text
   union all select 'gcd(INTMIN,-1)', gcd((-2147483648)::int4,-1)::text
   union all select 'lcm(0,5)', lcm(0,5)::text
   union all select 'lcm(-4,6)', lcm(-4,6)::text
   union all select 'lcm(INTMIN,0)', lcm((-2147483648)::int4,0)::text;"
gcd(0,0)|0
gcd(-12,18)|6
gcd(12,-18)|6
gcd(-12,-18)|6
gcd(0,-5)|5
gcd(INTMIN,1)|1
gcd(INTMIN,-1)|1
lcm(0,5)|0
lcm(-4,6)|12
lcm(INTMIN,0)|0
```

Errors:

```
select gcd((-2147483648)::int4,0);                  => ERROR: integer out of range
select gcd((-2147483648)::int4,(-2147483648)::int4);=> ERROR: integer out of range
select lcm((-2147483648)::int4,1);                  => ERROR: integer out of range
select lcm(2147483647,2);                           => ERROR: integer out of range
select gcd((-9223372036854775808)::int8,0);         => ERROR: bigint out of range
select lcm((-9223372036854775808)::int8,1);         => ERROR: bigint out of range
select lcm(9223372036854775807::int8,2);            => ERROR: bigint out of range
select gcd((-9223372036854775808)::int8,(-1)::int8);=> 1

$ psql -v VERBOSITY=verbose -At -c "select gcd((-2147483648)::int4,0);"
ERROR:  22003: integer out of range
LOCATION:  int4gcd_internal, int.c:1292
```

Findings worth naming:

* **`gcd(0,0)` is 0**, not an error and not NULL.
* **Results are always non-negative**, for every sign combination.
* **`lcm(INT_MIN, 0)` is 0, NOT an error** — the zero test must precede the
  gcd. `pg_lcm_i64`'s first statement is that test and it must stay first.
* **`gcd(BIGINT_MIN, -1)` is 1** — which is why the Euclid loop uses
  `wrapping_rem`; plain `%` panics on `i64::MIN % -1` in a debug build.

**DIVERGENCE — SQLSTATE, and the type name at `int8`.** The server says
`22003 integer out of range` / `22003 bigint out of range`. Basin raises
`ExecError::Overflow("integer")` for BOTH widths, so:

* the SQLSTATE is `XX000`, not `22003` (nothing maps the variant);
* at `int8` the message says `integer out of range` where the server says
  `bigint out of range`.

Both inherited from `eval.rs` unchanged. Fixing means an `ExecError` change
plus a `basin-router` mapping, i.e. two crates this slice does not touch.

## 5. NEEDS VERIFICATION

* **`bit_length` saturation at extreme lengths.** Basin's
  `bytes.saturating_mul(8)` clamps to `i32::MAX` for inputs over 268,435,455
  bytes rather than overflowing. What PostgreSQL does at that size was NOT
  measured — a 256 MB literal was not constructed on the live server. Inherited
  from `eval.rs` unchanged either way.
* **`i32::try_from(s.len()).expect(...)`** is an inherited panic path. It is
  unreachable through Arrow's 32-bit-offset `StringArray`/`BinaryArray`, but
  it is a panic rather than an `ExecError` and was not converted (a port moves
  behaviour).
* `map_arrow` is now copied into a THIRD module (`eval.rs`, `num_fns.rs`,
  `numx_fns.rs`). It should be hoisted to a shared `crate::funcs::error`
  module. Not done here: that is an edit to `eval.rs`.

---

## Progress

- [x] `octet_length(text)` 1374 — DONE
- [x] `bit_length(text)` 1811 — DONE
- [x] `octet_length(bytea)` 720 — DONE
- [x] `bit_length(bytea)` 1810 — DONE
- [x] `to_hex(int4)` 2089 — DONE
- [x] `to_hex(int8)` 2090 — DONE
- [x] `factorial(int8)` 1376 — DONE
- [x] `gcd(int4,int4)` 5044 / `gcd(int8,int8)` 5045 — DONE
- [x] `lcm(int4,int4)` 5046 / `lcm(int8,int8)` 5047 — DONE
- [ ] `round(numeric)` 1708 / `round(numeric,int)` 1707
- [ ] `sign(numeric)` 1706
- [ ] `trunc(numeric)` 1710 / `trunc(numeric,int)` 1709
- [ ] tests
