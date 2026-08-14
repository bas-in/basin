# `str_fns.rs` — integration notes for the second string slice

**Status: IN PROGRESS.** This file is written *before* the code and updated
after every function, because the work is worth nothing if it cannot be
integrated by someone who is not me.

This slice moves 8 named string functions (11 `pg_proc` oids) out of
`eval_scalar_fn`'s `match` in `crates/basin-exec/src/eval.rs` and into
`crates/basin-exec/src/funcs/str_fns.rs`.

**I did not touch `eval.rs` or `funcs/mod.rs`** — five other agents were editing
them at the same time. So steps 2 and 3 of the port (register, delete the arm)
are written out here rather than applied. **The port is not finished until
someone applies this file.** Until then the impls in `str_fns.rs` are tested
but unreachable, which is exactly the failure mode `funcs/mod.rs`'s own module
doc warns about (13,231 tested-but-unreachable lines in `basin-pgcatalog`).

---

## 1. What landed in `str_fns.rs`

| function | oid(s) | struct(s) | eval.rs helper it copies |
| --- | --- | --- | --- |
| `upper(text)` | 871 | `Upper` | `text_unary` + `str::to_uppercase` |
| `initcap(text)` | 872 | `Initcap` | `pg_initcap` |
| `lpad(text,int[,text])` | 879, 873 | `Lpad2`, `Lpad3` | `eval_pad`/`pg_pad` |
| `rpad(text,int[,text])` | 880, 874 | `Rpad2`, `Rpad3` | `eval_pad`/`pg_pad` |
| `repeat(text,int)` | 1622 | `Repeat` | `eval_repeat`/`pg_repeat` |
| `split_part(text,text,int)` | 2088 | `SplitPart` | `eval_split_part`/`pg_split_part` |
| `replace(text,text,text)` | 2087 | `Replace` | `eval_replace` |
| `strpos`/`position(text,text)` | 868, 849 | `Strpos`, `Position` | `eval_strpos`/`pg_strpos` |

All 11 oids are tabulated in `basin_pgtype::func::FUNCS` (checked by reading
`crates/basin-pgtype/src/func.rs` lines 299, 1572, 1573-1607, 417-434, 343-356),
so `register_scalar`'s `has no pg_proc row in FUNCS` assertion will not fire.

---

## 2. Exact lines to add to `funcs/mod.rs`

Inside `builtins()`, after the existing `r.register_scalar(Box::new(str_fns::Lower));`:

```rust
        r.register_scalar(Box::new(str_fns::Upper));
        r.register_scalar(Box::new(str_fns::Initcap));
        r.register_scalar(Box::new(str_fns::Lpad2));
        r.register_scalar(Box::new(str_fns::Lpad3));
        r.register_scalar(Box::new(str_fns::Rpad2));
        r.register_scalar(Box::new(str_fns::Rpad3));
        r.register_scalar(Box::new(str_fns::Repeat));
        r.register_scalar(Box::new(str_fns::SplitPart));
        r.register_scalar(Box::new(str_fns::Replace));
        r.register_scalar(Box::new(str_fns::Strpos));
        r.register_scalar(Box::new(str_fns::Position));
```

### 2a. …and the one test in `funcs/mod.rs` that this breaks

`the_registry_reports_what_is_actually_hosted` asserts the census:

```rust
        assert_eq!(
            builtins().len(),
            1,
```

`1` becomes **`1 + 11 = 12`** if this slice lands alone. If another slice lands
first, it is `1 + 11 + theirs` — read the number off the failure, do not guess.
`an_unported_oid_misses_and_falls_through_to_the_match` uses oid 1395
(`abs(float8)`), which this slice does not touch, so it still passes.

---

## 3. Exact arms to DELETE from `eval_scalar_fn` in `eval.rs`

Quoted verbatim from `eval.rs` at commit `18b59c2b` (line numbers as they were;
they will have moved). Every one of these is dead the moment the registration
above exists, because `eval_scalar_fn` consults the registry first.

```rust
        // line 2754
        OID_UPPER => text_unary(&a(0)?, str::to_uppercase),

        // lines 2818-2823
        OID_REPLACE => {
            let s = a(0)?;
            let from = a(1)?;
            let to = a(2)?;
            eval_replace(&s, &from, &to)
        }
        // lines 2824-2829
        // Same implementation, same argument order — see [`OID_POSITION`].
        OID_STRPOS | OID_POSITION => {
            let s = a(0)?;
            let needle = a(1)?;
            eval_strpos(&s, &needle)
        }

        // line 2831
        OID_INITCAP => text_unary(&a(0)?, pg_initcap),

        // lines 2833-2870, INCLUDING the four-line comment above OID_LPAD_2
        // ("The two-argument forms are not separate algorithms…") — that
        // comment documents `pg_pad`'s `None` fill and MOVES WITH IT; it is
        // reproduced on `Lpad2` in str_fns.rs, so delete it here.
        OID_LPAD_2 => {
            let s = a(0)?;
            let len = a(1)?;
            eval_pad(&s, &len, None, PadSide::Left)
        }
        OID_LPAD_3 => {
            let s = a(0)?;
            let len = a(1)?;
            let fill = a(2)?;
            eval_pad(&s, &len, Some(&fill), PadSide::Left)
        }
        OID_RPAD_2 => {
            let s = a(0)?;
            let len = a(1)?;
            eval_pad(&s, &len, None, PadSide::Right)
        }
        OID_RPAD_3 => {
            let s = a(0)?;
            let len = a(1)?;
            let fill = a(2)?;
            eval_pad(&s, &len, Some(&fill), PadSide::Right)
        }
        OID_REPEAT => {
            let s = a(0)?;
            let count = a(1)?;
            eval_repeat(&s, &count)
        }
        OID_SPLIT_PART => {
            let s = a(0)?;
            let delim = a(1)?;
            let field = a(2)?;
            eval_split_part(&s, &delim, &field)
        }
```

`OID_LEFT`/`OID_RIGHT`, the trims, `concat`/`concat_ws`, `substr`/`substring`
and the `length` trio are **NOT** in this slice. Leave their arms alone.

### 3a. Helper functions in `eval.rs` that become dead with those arms

Deleting the arms is step 3; deleting these is the same edit, because each is
now unreachable and will warn. I verified with `grep -rn <name> crates/ tests/`
that every one of them is referenced **only** from `eval.rs` and **only** from
the arms above (mention counts in parentheses, repo-wide, including doc links):

* `eval_replace` (2) — line 3372
* `eval_strpos` (2) + `pg_strpos` (2) — lines 3401, 3415
* `pg_initcap` (2) — line 3453
* `enum PadSide` (9) + `PAD_DEFAULT_FILL` (3) + `PAD_MAX_LEN` (3) +
  `eval_pad` (7) + `pg_pad` (4) — lines 3467-3571
* `eval_repeat` (2) + `pg_repeat` (2) — lines 3576, 3612
* `eval_split_part` (2) + `pg_split_part` (2) — lines 3628, 3672
* `PG_MAX_ALLOC_SIZE` (5) + `PG_VARHDRSZ` (3) — lines 3479, 3483. These two are
  shared by `pg_pad` **and** `pg_repeat` and by nothing else, so they die only
  because this slice takes *both*. If you integrate this file partially, keep
  them.

**`text_unary` (line 3146) must STAY.** It is not only `upper`/`initcap`'s: the
one-argument trims call it (`eval_trim_1`, line 3339) and the trims are not in
this slice. Its doc comment says "`lower(text)` / `upper(text)`", which is
already stale for `lower` and becomes staler here — reword, do not delete.

The `OID_*` constants (`OID_UPPER`, `OID_INITCAP`, …) must **STAY**: the
existing `eval.rs` tests below still use them to build `Expr::ScalarFunc` nodes,
which is precisely how those tests end up exercising the new seam. This matches
what the `lower` port already did — `OID_LOWER` (line 307) is still there with
no arm.

---

## 4. The existing `eval.rs` tests that will cross the new seam

This is the actual evidence that the port is behaviour-preserving. **No new
test in `str_fns.rs` is worth as much as one of these turning green through the
registry.** After integration, all of these run through `str_fns.rs` without a
character changed:

| test (`crates/basin-exec/src/eval.rs`) | line | covers |
| --- | --- | --- |
| `lower_and_upper_change_case` | 8173 | `upper` |
| `initcap_starts_a_word_after_any_non_alphanumeric_not_just_whitespace` | 8903 | `initcap` |
| `initcap_treats_non_ascii_letters_as_alphanumeric` | 8931 | `initcap` |
| `initcap_of_null_is_null` | 8949 | `initcap` |
| `lpad_truncates_before_it_checks_whether_the_fill_can_pad` | 8962 | `lpad` |
| `rpad_pads_on_the_right_but_truncates_from_the_same_end_as_lpad` | 8994 | `rpad` |
| `pad_fill_wraps_around_mid_string` | 9026 | both pads |
| `pad_length_is_in_characters_not_bytes` | 9038 | both pads |
| `pad_beyond_the_allocation_ceiling_errors_instead_of_allocating` | 9056 | `PAD_MAX_LEN` |
| `pad_of_null_is_null_even_at_a_length_that_would_error` | 9073 | NULL-before-error ordering |
| `repeat_with_a_non_positive_count_is_the_empty_string` | 9090 | `repeat` |
| `repeat_checks_bytes_times_count_not_the_count_alone` | 9117 | `repeat` ceiling |
| `repeat_of_null_is_null` | 9130 | `repeat` |
| `split_part_indexes_from_either_end_and_runs_off_quietly` | 9142 | `split_part` |
| `split_part_field_zero_errors` | 9172 | `split_part` |
| `split_part_of_null_is_null_even_at_the_field_position_that_errors` | 9188 | NULL-before-error ordering |
| `split_part_with_an_empty_delimiter_makes_exactly_one_field` | 9203 | `split_part` |
| `replace_substitutes_every_occurrence` | 8862 | `replace` |
| `strpos_is_a_character_position_not_a_byte_offset` | 8877 | `strpos` |
| `strpos_of_a_non_match_is_zero` | 8889 | `strpos` |
| the `position` block from line 9470 | 9470+ | oid 849 argument order |
| `the_new_string_functions_are_row_wise_over_a_column` | 9433 | `initcap`, both pads, `repeat` over a real column |

If any of these fails after integration, the port changed behaviour. Do not
"fix" the test.

---

## 5. PostgreSQL divergences

All measured against **live PostgreSQL 18.2 (Homebrew), aarch64-apple-darwin24,
`datcollate = datctype = en_US.UTF-8`**, at
`postgres://pc@127.0.0.1:5432/postgres` on 2026-08-15.

Everything here is **preserved, not fixed**. A port moves behaviour.

_(filled in per function below as each landed)_

---

## 6. NEEDS VERIFICATION

_(filled in as found)_
