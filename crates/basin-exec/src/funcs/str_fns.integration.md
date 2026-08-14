# `str_fns.rs` — integration notes for the second string slice

**Status: impls written and self-tested; steps 2 and 3 of the port are NOT
applied.** This file was written *before* the code and updated after every
function, because the work is worth nothing if it cannot be integrated by
someone who is not me.

Eleven new tests were added to `str_fns.rs` alongside the impls
(`every_oid_in_this_slice_has_the_catalog_row_registration_will_demand`,
`lower_also_diverges_on_the_greek_final_sigma`,
`upper_preserves_the_full_case_mapping_divergence`,
`initcap_agrees_with_postgres_on_sigma_but_not_on_expansion`,
`initcap_splits_on_any_non_alphanumeric_and_diverges_on_category_no`,
`pads_truncate_then_cycle_the_fill_by_character`,
`pad_checks_its_ceiling_per_row_and_only_after_the_null_test`,
`repeat_clamps_a_negative_count_before_it_checks_bytes`,
`split_part_indexes_from_either_end_and_only_zero_errors`,
`replace_leaves_an_empty_from_alone_unlike_str_replace`,
`strpos_and_position_are_character_positions_in_haystack_needle_order`) — but
see section 4: the 26 *existing* `eval.rs` tests crossing the new seam are the
real evidence, and they only start doing that once section 2 is applied.

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

Inside `builtins()`. The list is append-only and already grouped by slice
(`// num_fns — ported by the wave-15 numeric slice.` etc.), so put these
directly under `r.register_scalar(Box::new(str_fns::Lower));` with their own
group comment, keeping the `str_fns` lines together:

```rust
        // str_fns — the second string slice: case, padding, repetition,
        // splitting, substitution, searching.
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

`the_registry_reports_what_is_actually_hosted` asserts the census, which as of
`d65abd17` reads:

```rust
        assert_eq!(
            builtins().len(),
            24,
            "24 hosted: lower, the 12 numeric and the 11 date/time ports. Read \
             from the registry, never tracked by hand"
        );
```

`24` becomes **35** (`24 + 11`) and the message needs the string slice added.
But other slices are in flight in the same working tree (`numx_fns.rs` is
untracked as I write this), so if the arithmetic does not match, **read the
number off the failure** — that is the point of the test.

`an_unported_oid_misses_and_falls_through_to_the_match` uses oid 1395
(`abs(float8)`)… which the numeric slice has since ported, so that test may
already need a different oid. Not this slice's problem: none of the 11 oids
here is 1395.

---

## 3. Exact arms to DELETE from `eval_scalar_fn` in `eval.rs`

Quoted verbatim from the **working tree** `eval.rs` on 2026-08-15 (HEAD
`d65abd17` plus other agents' uncommitted edits). Line numbers are given for
orientation only — they moved once already while I was writing this, when the
numeric slice deleted the `abs`/`ceil`/`floor`/`round`-float8 arms above mine.
Match on the `OID_*` names, not on line numbers.

Every one of these is dead the moment the registration above exists, because
`eval_scalar_fn` consults the registry first.

```rust
        // line 2754
        OID_UPPER => text_unary(&a(0)?, str::to_uppercase),

        // lines 2803-2808
        OID_REPLACE => {
            let s = a(0)?;
            let from = a(1)?;
            let to = a(2)?;
            eval_replace(&s, &from, &to)
        }
        // lines 2809-2814
        // Same implementation, same argument order — see [`OID_POSITION`].
        OID_STRPOS | OID_POSITION => {
            let s = a(0)?;
            let needle = a(1)?;
            eval_strpos(&s, &needle)
        }

        // line 2816
        OID_INITCAP => text_unary(&a(0)?, pg_initcap),

        // lines 2818-2855, INCLUDING the four-line comment above OID_LPAD_2
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

Line numbers are from the working tree on 2026-08-15 and move constantly;
the names are what matters.

| test (`crates/basin-exec/src/eval.rs`) | line | covers |
| --- | --- | --- |
| `lower_and_upper_change_case` | 8100 | `upper` |
| `replace_substitutes_every_occurrence` | 8789 | `replace` |
| `strpos_is_a_character_position_not_a_byte_offset` | 8804 | `strpos` |
| `strpos_of_a_non_match_is_zero` | 8816 | `strpos` |
| `initcap_starts_a_word_after_any_non_alphanumeric_not_just_whitespace` | 8830 | `initcap` |
| `initcap_treats_non_ascii_letters_as_alphanumeric` | 8858 | `initcap` |
| `initcap_of_null_is_null` | 8876 | `initcap` |
| `lpad_truncates_before_it_checks_whether_the_fill_can_pad` | 8889 | `lpad` |
| `rpad_pads_on_the_right_but_truncates_from_the_same_end_as_lpad` | 8921 | `rpad` |
| `pad_fill_wraps_around_mid_string` | 8953 | both pads |
| `pad_length_is_in_characters_not_bytes` | 8965 | both pads |
| `pad_beyond_the_allocation_ceiling_errors_instead_of_allocating` | 8983 | `PAD_MAX_LEN` |
| `pad_of_null_is_null_even_at_a_length_that_would_error` | 9000 | NULL-before-error ordering |
| `repeat_with_a_non_positive_count_is_the_empty_string` | 9017 | `repeat` |
| `repeat_checks_bytes_times_count_not_the_count_alone` | 9044 | `repeat` ceiling |
| `repeat_of_null_is_null` | 9057 | `repeat` |
| `split_part_indexes_from_either_end_and_runs_off_quietly` | 9069 | `split_part` |
| `split_part_field_zero_errors` | 9099 | `split_part` |
| `split_part_of_null_is_null_even_at_the_field_position_that_errors` | 9115 | NULL-before-error ordering |
| `split_part_with_an_empty_delimiter_makes_exactly_one_field` | 9130 | `split_part` |
| `split_part_delimiter_can_be_multi_character_and_multi_byte` | 9148 | `split_part` |
| `split_part_counts_the_empty_field_a_trailing_delimiter_creates` | 9167 | `split_part` |
| `the_new_string_functions_are_row_wise_over_a_column` | 9360 | `initcap`, both pads, `repeat` over a real column |
| `position_takes_haystack_then_needle_not_the_in_syntax_order` | 9411 | oid 849 argument order |
| `position_counts_characters_and_finds_the_empty_needle_at_one` | 9431 | oid 849 |
| `position_of_a_null_argument_is_null` | 9443 | oid 849 |

That is **26 existing tests** covering the 11 oids, none of which needed a
character changed.

If any of these fails after integration, the port changed behaviour. Do not
"fix" the test.

---

## 5. PostgreSQL divergences

All measured against **live PostgreSQL 18.2 (Homebrew), aarch64-apple-darwin24,
`datcollate = datctype = en_US.UTF-8`**, at
`postgres://pc@127.0.0.1:5432/postgres` on 2026-08-15.

Everything here is **preserved, not fixed**. A port moves behaviour. Each one
is pinned by a test in `str_fns.rs` that states BOTH what Basin does and what
the server does, so none of them can be "fixed" silently and none can rot
unnoticed.

### D1 (NEW) — `lower('ΟΔΟΣ')`: the Greek final sigma

The `lower` port pinned one divergence (U+0130 expansion). There is a second,
found here, and it is a worse *kind*: same character count, different
character. Rust applies Unicode's `Final_Sigma` **conditional** mapping;
glibc's `towlower_l` has no notion of word position.

```console
$ psql -Atc "select lower('ΟΔΟΣ'), encode(convert_to(lower('ΟΔΟΣ'),'UTF8'),'hex');"
οδοσ|cebfceb4cebfcf83                 -- final char U+03C3 σ
$ psql -Atc "select encode(convert_to(lower('ΟΔΟΣ ΟΔΟΣ'),'UTF8'),'hex');"
cebfceb4cebfcf8320cebfceb4cebfcf83    -- both words, U+03C3 both times
```

`str::to_lowercase("ΟΔΟΣ")` is `οδος`, ending U+03C2 (hex `cf82`). Pinned in
`tests::lower_also_diverges_on_the_greek_final_sigma`; added to the module-doc
table. **This is a divergence in already-shipped code, not in anything this
slice wrote** — `Lower` is registered and live today.

### D2 — `upper`: Unicode full case mapping vs `towupper_l`

```console
$ psql -Atc "select upper('ß'), encode(convert_to(upper('ß'),'UTF8'),'hex');"
ß|c39f
$ psql -Atc "select upper('ﬁ'), encode(convert_to(upper('ﬁ'),'UTF8'),'hex');"
ﬁ|efac81
$ psql -Atc "select upper('ﬆ'), encode(convert_to(upper('ﬆ'),'UTF8'),'hex');"
ﬆ|efac86
$ psql -Atc "select upper('ᾀ'), encode(convert_to(upper('ᾀ'),'UTF8'),'hex');"
ᾈ|e1be88
```

Basin: `SS`, `FI`, `ST`, `ἈΙ` (`e1bc88 ce99`). The `ᾀ` row is not "the server
gave up" — it returns U+1F88, the **titlecase** character. glibc has an answer,
it is just a one-to-one one.

Agreement was checked too, so the gap is bounded: `upper('héllo世界 abc')` =
`HÉLLO世界 ABC` and `upper('ǅ')` = `Ǆ` (U+01C4) on both sides. Pinned in
`tests::upper_preserves_the_full_case_mapping_divergence`.

### D3 — `initcap`: `iswalnum` is `Nd`-only, `char::is_alphanumeric` is not

```console
$ psql -Atc "select initcap('½abc');"
½Abc
```

Basin: `½abc`. `½` is category `No`; glibc does not count it alphanumeric, so
the following `a` starts a word there and does not here. Already documented on
the old `eval.rs` arm; re-confirmed live for this port. Confirmed live
previously for `¹`, `²`, `①` (`No`) and `Ⅷ` (`Nl`); `٣` (`Nd`) agrees.

### D4 — `initcap`: the same expansion as D2, and the sigma it dodges

```console
$ psql -Atc "select initcap('ßeta'), initcap('ﬁne w'), initcap('ΟΔΟΣ ΟΔΟΣ');"
ßeta|ﬁne W|Οδοσ Οδοσ
```

Basin: `SSeta`, `FIne W`, and — **agreeing** — `Οδοσ Οδοσ`.

The agreement is the load-bearing part. `pg_initcap` lowercases with
`char::to_lowercase` (per character, no context), so `Final_Sigma` never fires
and D1 cannot reach `initcap`. **Anyone "tidying" `pg_initcap` to use
`str::to_lowercase` for consistency with `Lower` would introduce a new
divergence.** `tests::initcap_agrees_with_postgres_on_sigma_but_not_on_expansion`
exists to fail loudly if they do.

Also confirmed identical on both sides, and worth having on record because they
look like they should diverge: `initcap('İstanbul iSTANBUL')` = `İstanbul
Istanbul`, `initcap('ǅungla ǅungla')` = `Ǆungla Ǆungla`.

### D5 (NEW, performance not semantics) — `repeat('', 2147483647)`

```console
$ psql -Atc "set statement_timeout='2s'; select length(repeat('',2147483647));"
SET
ERROR:  canceling statement due to statement timeout
```

Basin returns `''` immediately (`str::repeat` with a zero-length subject).
PostgreSQL's `repeat` loops `count` times copying nothing, so it does not
return within 2s — the *value* is the same, the cost is not. Recorded because
`eval.rs`'s existing `repeat_checks_bytes_times_count_not_the_count_alone`
asserts the instant `''` and a future oracle diffing Basin against a live
server on this input would time out rather than mismatch. Noted in the test
message; **not** something to change.

### Non-divergences confirmed live (guards that are already right)

These are places where the obvious Rust implementation would be wrong and the
existing code already handles it. Re-verified so that a later "simplification"
has something to fail against:

* `replace('hello world', '', '0')` = `hello world` live. `str::replace` with an
  empty pattern would give `0h0e0l0l0o0 0w0o0r0l0d0`. The `!from.is_empty()`
  guard is why Basin agrees.
* `lpad('hello', 10, '')` = `hello` but `lpad('hello', 3, '')` = `hel` — the
  empty-fill rule is "cannot pad", not "return the input".
* `lpad(NULL, 2147483647)`, `lpad('a', 5, NULL)`, `split_part(NULL, ',', 0)`,
  `repeat(NULL, 1073741820)` are all NULL, **not** their respective errors.
  Strictness beats the error check, which is why those three impls walk indices
  instead of zipping iterators.
* `split_part('a,b,c', ',', -2147483648)` = `''`, not an error — the i64
  widening is what avoids a 32-bit negation overflow.
* `strpos('', '')` = 1, `strpos('abc', '')` = 1, `strpos('héllo世界', '世')` = 6
  (characters, not the byte offset 6+1=7).
* `pg_catalog.position('abc','b')` = 2 and `pg_catalog.position('b','abc')` = 0:
  the stored argument order is `(haystack, needle)`, the reverse of how
  `POSITION(x IN y)` is written.

---

## 6. NEEDS VERIFICATION

1. **Embedded NUL.** PostgreSQL cannot represent `U+0000` in `text` at all
   (`ERROR 22021`, "invalid byte sequence"), so none of these functions could be
   tested against the server with a NUL in any argument. Rust `String` accepts
   it, so Basin will happily `repeat`, `split_part` or `lpad` a string
   containing one. The divergence is upstream of these functions — it is a
   *type* question (what Basin lets into a `text` value) — but the six genuine
   `starts_with` battery failures come from exactly this, so whoever fixes the
   type will change behaviour here too. **Not tested, not pinned.**
2. **`repeat('a', 1073741819)`** — the accepted side of the byte ceiling. The
   arithmetic says it lands exactly on `MaxAllocSize` and the server accepts it,
   but the test deliberately does not exercise it: it would allocate a GiB in
   `cargo test`. Only the erroring side (1073741820) is pinned. Same for
   `lpad('hello', 268435454)`.
3. **Collation other than `en_US.UTF-8`.** Every case-mapping measurement here
   is from one database whose `datcollate` and `datctype` are both
   `en_US.UTF-8`. Basin has no notion of collation at all, so under `C`
   collation the server's answers would change and Basin's would not. Nobody
   has measured that; the divergence table would need a second column.
4. **`initcap` under a non-`en_US` `lc_ctype`.** The `Nd`-only claim (D3) is
   glibc's behaviour under this locale. Unverified elsewhere.
5. **The `numx_fns.rs` slice was untracked in the same working tree** while this
   was written. If it registers any oid in section 2, `register_scalar` panics
   with `registered twice` — that is the designed failure, not a mystery. I
   checked the committed `funcs/*.rs` and found no overlap; I could not check
   files that did not exist yet.

---

## 7. What was NOT done, and why

* **`eval.rs` and `funcs/mod.rs` are untouched.** Five other agents were in
  them. Sections 2 and 3 are the whole of the remaining work.
* **No behaviour was improved.** Every divergence above was preserved. Fixing
  one inside a port makes a later `git bisect` blame the port.
* **`str_fns.rs` was reformatted.** `rustfmt --edition 2021 --check` failed on
  the file at `d65abd17` (the `arg` and `Lower::invoke` signatures were
  hand-wrapped narrower than rustfmt wants), which fails CI's
  `cargo fmt --all -- --check`. The file is now clean; that is why the diff
  touches two pre-existing signatures.
* **`cargo` was never run** (build-lock starvation is what killed the earlier
  agents in this program). The file is syntax-checked by `rustfmt` parsing it,
  not by `rustc`. **Type errors are therefore possible and the first
  integration step should be `cargo test -p basin-exec funcs::str_fns`.**
