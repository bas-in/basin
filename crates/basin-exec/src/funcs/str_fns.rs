//! String functions hosted through [`crate::funcs::ScalarFunc`].
//!
//! # This file is the Phase 1 template
//!
//! `lower` is here as the WORKED EXAMPLE doc 27 said it owed. It is the
//! smallest function that exercises every part of the seam, and it was moved
//! first precisely because it already had tests: three cases in `eval.rs`
//! call it through `eval`, so they now run through the registry unchanged.
//! Existing tests passing over a new path is stronger evidence than any new
//! test written alongside the port.
//!
//! ## Porting a function is three edits
//!
//! 1. Write the impl here (or in the slice module for its family).
//! 2. Register it in [`crate::funcs::builtins`].
//! 3. **Delete its arm from `eval_scalar_fn`'s `match`.**
//!
//! Step 3 is not optional and not cosmetic. The registry is consulted first,
//! so a left-behind arm is unreachable code that still looks live — and this
//! repository has already shipped three features that were committed, tested
//! and completely inert, plus 13,231 unreachable lines in `basin-pgcatalog`.
//! Leaving the arm makes the `match` a graveyard nobody can safely read.
//!
//! ## What the impl must get right
//!
//! * **Arguments arrive evaluated.** Do not evaluate anything yourself; the
//!   call site did it once, deliberately (see `crate::funcs`).
//! * **NULL in, NULL out**, unless PostgreSQL says otherwise for that
//!   function. The `v.map(...)` shape below never calls the body for a NULL,
//!   which is what makes that automatic.
//! * **Length alignment.** The returned array must have the same length as
//!   the inputs. Collecting from the input's iterator gives that for free;
//!   constructing an array some other way does not.
//! * **Verify against live PostgreSQL 18.2**, not against what the function
//!   obviously does. `lower` looks obvious and is not: see the note on
//!   `to_lowercase` below.
//!
//! ## Step 2 and step 3 are owed, and written down
//!
//! The second slice (`upper`, `initcap`, `lpad`, `rpad`, `repeat`,
//! `split_part`, `replace`, `strpos`/`position`) wrote its impls here but
//! could not touch `eval.rs` or `funcs/mod.rs` — five agents were editing them
//! at once. The registrations it needs and the `match` arms it makes dead are
//! spelled out verbatim in `str_fns.integration.md`, next to this file.
//! **Until that file is applied, everything below except `Lower` is tested and
//! unreachable** — the exact defect `crate::funcs`' module doc names. Applying
//! it is the port; this is only the first of its three edits.
//!
//! The trims, `left`/`right`, `concat`, `substr`/`substring` and the `length`
//! trio are deliberately NOT in that slice and still live in the `match`.
//!
//! ## The one divergence this whole family shares
//!
//! Found by the slice that ported `upper` and `initcap`; stated once here
//! rather than three times below.
//!
//! PostgreSQL case-maps ONE CHARACTER AT A TIME through the C library
//! (`towupper_l`/`towlower_l`) under the database collation. Rust's
//! `str::to_uppercase`/`str::to_lowercase` implement Unicode's *full*
//! mappings, which may change the character count and may depend on
//! surrounding context. Measured on live PostgreSQL 18.2, `datcollate =
//! en_US.UTF-8`, against `rustc`'s own output for the same inputs:
//!
//! ```text
//!                    PostgreSQL 18.2      Basin
//! upper('ß')         ß        (c39f)      SS       (5353)
//! upper('ﬁ')         ﬁ        (efac81)    FI       (4649)
//! upper('ﬆ')         ﬆ        (efac86)    ST       (5354)
//! upper('ᾀ')         ᾈ        (e1be88)    ἈΙ       (e1bc88 ce99)
//! lower('İ')         i        (69)        i̇        (69 cc87)
//! lower('ΟΔΟΣ')      οδοσ     (…cf83)     οδος     (…cf82)
//! initcap('ßeta')    ßeta                 SSeta
//! initcap('ﬁne w')   ﬁne W                FIne W
//! ```
//!
//! The last two rows of the `lower` block are worth separating: `lower('İ')`
//! is the expansion the template already pins, but `lower('ΟΔΟΣ')` is a
//! *context* disagreement — Rust applies Unicode's `Final_Sigma` conditional
//! mapping and returns a final `ς`, while glibc's `towlower` has no notion of
//! word position and always returns `σ`. It was found by this slice, not by
//! the `lower` port, and is pinned in
//! [`tests::lower_also_diverges_on_the_greek_final_sigma`].
//!
//! `initcap` escapes the sigma half by accident: it case-maps with
//! `char::to_lowercase`, which is per-character and therefore contextless, so
//! `initcap('ΟΔΟΣ ΟΔΟΣ')` agrees with the server exactly. It does not escape
//! the expansion half, because `char::to_uppercase('ß')` is still `SS`.
//!
//! **None of it is fixed here.** A port moves behaviour. The fix is
//! collation-aware case mapping for the whole family — `lower`, `upper`,
//! `initcap`, `citext`, the `LIKE`/pattern paths — which is its own commit,
//! and doing it inside a port would make a later bisect blame this one.

use std::sync::Arc;

// `Array`'s `len`/`is_null` are trait methods, not inherent ones, and the
// implementations below that must raise a per-row error (`lpad`, `repeat`,
// `split_part`) walk indices rather than iterators so that a NULL row is
// skipped *before* the error can be raised. See [`Lpad2`] for why that
// ordering is load-bearing.
use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
use basin_pgtype::Oid;

use crate::eval::{downcast_array, EvalSession};
use crate::funcs::ScalarFunc;
use crate::operator::ExecError;

/// Fetch argument `i`, or report a planner bug.
///
/// A missing argument here is never user error: resolution already matched
/// the call against a `pg_proc` row with a fixed arity, so arriving with too
/// few means the planner built something the catalog does not describe.
pub(crate) fn arg<'a>(args: &'a [ArrayRef], i: usize, oid: u32) -> Result<&'a ArrayRef, ExecError> {
    args.get(i).ok_or_else(|| {
        ExecError::Internal(format!(
            "function oid {oid} invoked with only {} argument(s) — a planner \
             bug, not user error",
            args.len()
        ))
    })
}

/// `lower(text) -> text`, `pg_proc` oid 870.
pub struct Lower;

impl ScalarFunc for Lower {
    fn oid(&self) -> Oid {
        Oid(870)
    }

    /// `return_type` is not overridden: `lower` returns exactly its
    /// `pg_proc.prorettype`, so the catalog-backed default is correct. Only
    /// functions whose result type is not expressible as one return oid need
    /// to override it — `extract(... FROM interval)` is the live example.
    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let a = downcast_array::<StringArray>(arg(args, 0, 870)?, "text")?;
        // KNOWN DIVERGENCE, inherited from the `match` arm this replaces and
        // deliberately NOT changed here — a port must move behaviour, not
        // quietly alter it, or a later bisect blames the wrong commit.
        //
        // `str::to_lowercase` implements Unicode's *unconditional* full
        // lowercasing. PostgreSQL lowercases per the database collation.
        // They disagree on U+0130, measured on live 18.2 with
        // datcollate = en_US.UTF-8:
        //
        //   SELECT encode(convert_to(lower('İ'),'UTF8'),'hex')  ->  69
        //   str::to_lowercase("İ")                              ->  69 cc87
        //
        // PostgreSQL returns a bare `i` (length 1); Rust returns `i` plus a
        // COMBINING DOT ABOVE (U+0307), length 2. Every ASCII input agrees,
        // which is why this has never shown up in a test.
        //
        // Filed rather than fixed because the correct fix is collation-aware
        // case mapping for the whole string family, not a special case for
        // one code point — and because `upper`, `initcap`, `citext` and the
        // `LIKE`/pattern paths all share this assumption. See the module doc.
        let out: StringArray = a.iter().map(|v| v.map(str::to_lowercase)).collect();
        Ok(Arc::new(out))
    }
}

/// The shape every single-argument text-to-text function here reuses. Arrow
/// ships no case-conversion kernel at all, so this is a hand-written pass over
/// the materialized array rather than a kernel call.
///
/// NULL in, NULL out falls out of `v.map(&f)`: `f` is never called for a
/// `None`, so a NULL input cannot be turned into a value. Collecting from the
/// input's own iterator is also what keeps the output length equal to the
/// input length without anyone having to remember to check.
///
/// A copy of `eval.rs`'s `text_unary`, which stays there — the one-argument
/// trims still call it and they are not in this slice.
fn text_unary(arr: &ArrayRef, f: impl Fn(&str) -> String) -> Result<ArrayRef, ExecError> {
    let a = downcast_array::<StringArray>(arr, "text")?;
    let out: StringArray = a.iter().map(|v| v.map(&f)).collect();
    Ok(Arc::new(out))
}

/// `upper(text) -> text`, `pg_proc` oid 871.
///
/// KNOWN DIVERGENCE, inherited from the `match` arm this replaces and
/// deliberately NOT changed: `str::to_uppercase` is Unicode's *full*
/// uppercasing, PostgreSQL's is `towupper_l` one character at a time under the
/// database collation. Measured on live 18.2, `en_US.UTF-8` — the server
/// returns its input unchanged for all three of these, because no *single*
/// wide character is their uppercase:
///
/// ```text
///              PostgreSQL 18.2     Basin
/// upper('ß')   ß      (c39f)       SS   (5353)
/// upper('ﬁ')   ﬁ      (efac81)     FI   (4649)
/// upper('ᾀ')   ᾈ      (e1be88)     ἈΙ   (e1bc88 ce99)
/// ```
///
/// The `ᾀ` row is the interesting one: the server does *not* return the input,
/// it returns U+1F88, the **titlecase** character — glibc's one-to-one table
/// has an answer here, it is just not Unicode's two-character one. See the
/// module doc; pinned in [`tests::upper_preserves_the_full_case_mapping_divergence`].
pub struct Upper;

impl ScalarFunc for Upper {
    fn oid(&self) -> Oid {
        Oid(871)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        text_unary(arg(args, 0, 871)?, str::to_uppercase)
    }
}

/// `initcap(text) -> text`, `pg_proc` oid 872.
pub struct Initcap;

impl ScalarFunc for Initcap {
    fn oid(&self) -> Oid {
        Oid(872)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        text_unary(arg(args, 0, 872)?, pg_initcap)
    }
}

/// PostgreSQL's `initcap`: uppercase the first character of each "word",
/// lowercase everything else. The rule is not "after whitespace" — a new word
/// starts after ANY non-alphanumeric character. Verified against live
/// PostgreSQL 18.2:
///
/// ```text
/// initcap('hELLo-woRLD foo_bar')  = 'Hello-World Foo_Bar'  -- '-' and '_' both split
/// initcap('3abc a2b')             = '3abc A2b'             -- digits do NOT split
/// initcap('世界abc')              = '世界abc'              -- nor do CJK letters
/// initcap('İstanbul iSTANBUL')    = 'İstanbul Istanbul'    -- agrees with Basin
/// initcap('ǅungla')               = 'Ǆungla'               -- agrees: titlecase
///                                                          -- to uppercase, 1:1
/// ```
///
/// **Two known divergences, both preserved.**
///
/// 1. *Alphanumeric.* PostgreSQL calls `iswalnum` under `lc_ctype`, which
///    counts only Unicode category `Nd` as a digit; [`char::is_alphanumeric`]
///    is `Alphabetic | Nd | Nl | No`. So `initcap('½abc')` is `'½Abc'` live
///    (`½` does not split a word for glibc, so `a` begins one) and `'½abc'`
///    here. Confirmed live for `½`, `¹`, `²`, `①` (`No`) and `Ⅷ` (`Nl`); `٣`
///    (Arabic-Indic three, `Nd`) agrees. std exposes no `Nd`-only predicate.
/// 2. *Case mapping.* `char::to_uppercase` is still Unicode's full mapping, so
///    `initcap('ßeta')` is `'SSeta'` here against `'ßeta'` live.
///
/// It escapes the *third* trap by accident, and that is worth stating because
/// it is the reason this function must keep using `char::to_lowercase` rather
/// than borrowing `Lower`'s `str::to_lowercase`: per-character mapping has no
/// notion of word position, so Unicode's `Final_Sigma` rule never fires and
/// `initcap('ΟΔΟΣ ΟΔΟΣ')` is `'Οδοσ Οδοσ'` — character-for-character what the
/// server returns. `str::to_lowercase` would produce `'Οδος Οδος'` and break
/// agreement. See [`tests::initcap_agrees_with_postgres_on_sigma_but_not_on_expansion`].
fn pg_initcap(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_alnum = false;
    for c in s.chars() {
        if prev_alnum {
            out.extend(c.to_lowercase());
        } else {
            out.extend(c.to_uppercase());
        }
        prev_alnum = c.is_alphanumeric();
    }
    out
}

/// Which end [`pg_pad`] adds fill characters to.
#[derive(Clone, Copy)]
enum PadSide {
    Left,
    Right,
}

/// The fill the two-argument `lpad`/`rpad` use: a single ASCII space, taken
/// from the SQL body of PostgreSQL's own two-argument wrapper.
const PAD_DEFAULT_FILL: &str = " ";

/// PostgreSQL's `MaxAllocSize` (`0x3fffffff`), the ceiling `palloc` enforces.
const PG_MAX_ALLOC_SIZE: i64 = 0x3fff_ffff;

/// The 4-byte varlena header every `text` datum carries, which counts against
/// [`PG_MAX_ALLOC_SIZE`].
const PG_VARHDRSZ: i64 = 4;

/// The largest `len` `lpad`/`rpad` accept before raising `requested length too
/// large`: `(MaxAllocSize - VARHDRSZ) / 4`, where 4 is UTF-8's maximum bytes
/// per character. Confirmed from both sides on live PostgreSQL 18.2 —
/// `lpad('hello', 268435455)` raises the error, `lpad('hello', 268435454)`
/// does not (it runs until `statement_timeout` cuts it off, which is the
/// server genuinely building a gigabyte of padding).
const PAD_MAX_LEN: i32 = ((PG_MAX_ALLOC_SIZE - PG_VARHDRSZ) / 4) as i32;

/// `lpad(text, int [, text])` / `rpad(text, int [, text])`, shared by all four
/// oids. `fill` is `None` for the two-argument forms, meaning
/// [`PAD_DEFAULT_FILL`].
///
/// Strict in all three arguments, and **that strictness is load-bearing for
/// the length check**: `lpad(NULL, 2147483647)` is NULL on the live server,
/// not `requested length too large`, because a strict function is never
/// entered when an argument is NULL. That is why the ceiling is tested per row
/// inside [`pg_pad`], after the NULL test, rather than swept over the whole
/// `len` array up front — a whole-array pre-check would turn that NULL into an
/// error. It is also why this walks indices instead of zipping iterators: the
/// `?` has to be reachable only from a row that is known non-NULL.
fn eval_pad(
    text: &ArrayRef,
    len: &ArrayRef,
    fill: Option<&ArrayRef>,
    side: PadSide,
) -> Result<ArrayRef, ExecError> {
    let t = downcast_array::<StringArray>(text, "text")?;
    let l = downcast_array::<Int32Array>(len, "integer")?;
    let f = fill
        .map(|f| downcast_array::<StringArray>(f, "text"))
        .transpose()?;

    let n = t.len();
    let mut out: Vec<Option<String>> = Vec::with_capacity(n);
    for i in 0..n {
        let fill_null = f.map(|f| f.is_null(i)).unwrap_or(false);
        if t.is_null(i) || l.is_null(i) || fill_null {
            out.push(None);
            continue;
        }
        let fill = f.map(|f| f.value(i)).unwrap_or(PAD_DEFAULT_FILL);
        out.push(Some(pg_pad(t.value(i), l.value(i), fill, side)?));
    }
    Ok(Arc::new(StringArray::from(out)))
}

/// PostgreSQL's `text_pad`, on already-unwrapped values. `len` counts
/// *characters*, not bytes (`lpad('héllo', 8, 'é') = 'éééhéllo'` — verified
/// live), so this walks `chars()` rather than indexing the byte slice, which
/// is also what stops it from panicking mid-codepoint.
///
/// The order of the three steps is what makes the awkward cases fall out, and
/// every line below was re-verified against live PostgreSQL 18.2 for this
/// port:
///
/// 1. **Truncate first.** A `len` shorter than the input truncates rather than
///    doing nothing: `lpad('hello', 3) = 'hel'`. A negative `len` clamps to
///    zero, so `lpad('hello', -3) = ''` — not an error, not `'hello'`.
/// 2. **Then check the fill.** An empty fill cannot pad, so the *already
///    truncated* string comes back: `lpad('hello', 10, '') = 'hello'` but
///    `lpad('hello', 3, '') = 'hel'`. Reading the empty-fill rule as "return
///    the input unchanged" gets the second one wrong.
/// 3. **Then pad, cycling the fill character by character** — not by repeating
///    the whole fill and trimming. `lpad('a', 6, 'xyz') = 'xyzxya'` and
///    `lpad('a', 6, '世界') = '世界世界世a'`: the fill wraps mid-string.
fn pg_pad(s: &str, len: i32, fill: &str, side: PadSide) -> Result<String, ExecError> {
    if len > PAD_MAX_LEN {
        return Err(ExecError::TypeMismatch(
            "requested length too large".to_string(),
        ));
    }
    let len = len.max(0) as usize;

    let chars: Vec<char> = s.chars().collect();
    let keep = chars.len().min(len);
    let body: String = chars[..keep].iter().collect();

    let fill: Vec<char> = fill.chars().collect();
    if fill.is_empty() {
        return Ok(body);
    }

    let pad: String = (0..len - keep).map(|i| fill[i % fill.len()]).collect();
    Ok(match side {
        PadSide::Left => pad + &body,
        PadSide::Right => body + &pad,
    })
}

/// `lpad(text, integer) -> text`, `pg_proc` oid 879.
///
/// The two-argument forms are not a separate algorithm: on live PostgreSQL 18
/// `lpad(text,int)` is a SQL-language function whose body is `lpad($1, $2, '
/// ')` — its errors even arrive with `CONTEXT: SQL function "lpad" statement
/// 1`. So `None` means "the default fill", not "a different code path", and
/// all four pad oids share [`eval_pad`].
pub struct Lpad2;

impl ScalarFunc for Lpad2 {
    fn oid(&self) -> Oid {
        Oid(879)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_pad(arg(args, 0, 879)?, arg(args, 1, 879)?, None, PadSide::Left)
    }
}

/// `lpad(text, integer, text) -> text`, `pg_proc` oid 873.
pub struct Lpad3;

impl ScalarFunc for Lpad3 {
    fn oid(&self) -> Oid {
        Oid(873)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_pad(
            arg(args, 0, 873)?,
            arg(args, 1, 873)?,
            Some(arg(args, 2, 873)?),
            PadSide::Left,
        )
    }
}

/// `rpad(text, integer) -> text`, `pg_proc` oid 880. See [`Lpad2`] on why the
/// two-argument form is not a separate algorithm.
pub struct Rpad2;

impl ScalarFunc for Rpad2 {
    fn oid(&self) -> Oid {
        Oid(880)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_pad(arg(args, 0, 880)?, arg(args, 1, 880)?, None, PadSide::Right)
    }
}

/// `rpad(text, integer, text) -> text`, `pg_proc` oid 874.
pub struct Rpad3;

impl ScalarFunc for Rpad3 {
    fn oid(&self) -> Oid {
        Oid(874)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_pad(
            arg(args, 0, 874)?,
            arg(args, 1, 874)?,
            Some(arg(args, 2, 874)?),
            PadSide::Right,
        )
    }
}

/// `repeat(text, integer) -> text`, `pg_proc` oid 1622.
///
/// Strict, and — like [`eval_pad`] — checked per row rather than over the
/// whole array, so a NULL argument stays NULL instead of becoming a size
/// error. `repeat(NULL, 1073741820)` is NULL live, not `requested length too
/// large`.
pub struct Repeat;

impl ScalarFunc for Repeat {
    fn oid(&self) -> Oid {
        Oid(1622)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let s = downcast_array::<StringArray>(arg(args, 0, 1622)?, "text")?;
        let c = downcast_array::<Int32Array>(arg(args, 1, 1622)?, "integer")?;
        let n = s.len();
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if s.is_null(i) || c.is_null(i) {
                out.push(None);
                continue;
            }
            out.push(Some(pg_repeat(s.value(i), c.value(i))?));
        }
        Ok(Arc::new(StringArray::from(out)))
    }
}

/// PostgreSQL's `repeat`, on already-unwrapped values.
///
/// The count is clamped to zero *before* the size check, and the order
/// matters: a negative count is not an error, it is an empty string. Verified
/// live that `repeat('ab', -3)`, `repeat('ab', 0)` and even `repeat('ab',
/// -2147483648)` all return `''` rather than raising.
///
/// The size check is on *bytes* — the server measures the datum, not its
/// character count — and the boundary was confirmed from both sides on live
/// PostgreSQL 18.2: `repeat('a', 1073741820)` raises `requested length too
/// large` because `1 * 1073741820 + 4` exceeds `MaxAllocSize`, while
/// `repeat('a', 1073741819)` lands exactly on it and is accepted. A 3-byte
/// character moves the boundary by the same factor, as a byte-based reading
/// predicts (`repeat('世', 536870910)` errors).
///
/// The arithmetic is `i64` rather than `i32` deliberately. PostgreSQL reaches
/// the same verdict by *detecting* `int32` overflow in the multiply and the
/// add; widening first cannot overflow at all, and any product that would have
/// wrapped a signed 32-bit int is necessarily already past
/// [`PG_MAX_ALLOC_SIZE`], so the two agree on every input while this one has no
/// wrapping to reason about.
fn pg_repeat(s: &str, count: i32) -> Result<String, ExecError> {
    let count = i64::from(count.max(0));
    if s.len() as i64 * count + PG_VARHDRSZ > PG_MAX_ALLOC_SIZE {
        return Err(ExecError::TypeMismatch(
            "requested length too large".to_string(),
        ));
    }
    Ok(s.repeat(count as usize))
}

/// `split_part(text, text, integer) -> text`, `pg_proc` oid 2088.
///
/// Strict, and the zero-field error is raised per row *after* the NULL check
/// for the same reason [`eval_pad`]'s size check is: `split_part(NULL, ',', 0)`
/// is NULL live, not `field position must not be zero`.
pub struct SplitPart;

impl ScalarFunc for SplitPart {
    fn oid(&self) -> Oid {
        Oid(2088)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let s = downcast_array::<StringArray>(arg(args, 0, 2088)?, "text")?;
        let d = downcast_array::<StringArray>(arg(args, 1, 2088)?, "text")?;
        let f = downcast_array::<Int32Array>(arg(args, 2, 2088)?, "integer")?;
        let n = s.len();
        let mut out: Vec<Option<String>> = Vec::with_capacity(n);
        for i in 0..n {
            if s.is_null(i) || d.is_null(i) || f.is_null(i) {
                out.push(None);
                continue;
            }
            out.push(Some(pg_split_part(s.value(i), d.value(i), f.value(i))?));
        }
        Ok(Arc::new(StringArray::from(out)))
    }
}

/// PostgreSQL's `split_part`, on already-unwrapped values. Every branch below
/// re-verified against live PostgreSQL 18.2 for this port:
///
/// ```text
/// split_part('a,b,c', ',',  0) -- ERROR: field position must not be zero
/// split_part('a,b,c', ',',  1) = 'a'
/// split_part('a,b,c', ',',  4) = ''       -- past the end is empty, not an error
/// split_part('a,b,c', ',', -1) = 'c'      -- negative counts from the right (PG 14+)
/// split_part('a,b,c', ',', -3) = 'a'
/// split_part('a,b,c', ',', -4) = ''
/// split_part('a,b,c', '',   1) = 'a,b,c'  -- an empty delimiter makes ONE field,
/// split_part('a,b,c', '',  -1) = 'a,b,c'  -- reachable from either end,
/// split_part('a,b,c', '',   2) = ''       -- and nothing else
/// split_part('a::b::c', '::', 2) = 'b'    -- the delimiter is a string, not a set
/// ```
///
/// A field position of zero is the one input that errors, and it has to:
/// because out-of-range is legal in *both* directions, `0` would otherwise
/// have to mean either the first or the last field, and neither reading is
/// safe to guess.
///
/// The index arithmetic is `i64`. `field` arrives as a full `i32`, so a
/// negative field of `i32::MIN` would overflow a 32-bit negation; widening
/// first makes it fall out of range and return `''`, which is what the live
/// server does (`split_part('a,b,c', ',', -2147483648)` is `''`, not an error).
fn pg_split_part(s: &str, delim: &str, field: i32) -> Result<String, ExecError> {
    if field == 0 {
        return Err(ExecError::TypeMismatch(
            "field position must not be zero".to_string(),
        ));
    }
    if delim.is_empty() {
        return Ok(if field == 1 || field == -1 {
            s.to_string()
        } else {
            String::new()
        });
    }

    let parts: Vec<&str> = s.split(delim).collect();
    let field = i64::from(field);
    let index = if field > 0 {
        field - 1
    } else {
        parts.len() as i64 + field
    };
    Ok(usize::try_from(index)
        .ok()
        .and_then(|i| parts.get(i))
        .map(|p| (*p).to_string())
        .unwrap_or_default())
}

/// `replace(text, text, text) -> text`, `pg_proc` oid 2087: every occurrence
/// of `from` in the subject replaced with `to`. An ordinary strict function —
/// no NULL-skipping special case like `concat`'s — and no per-row error, so
/// unlike its neighbours it can zip iterators instead of walking indices.
pub struct Replace;

impl ScalarFunc for Replace {
    fn oid(&self) -> Oid {
        Oid(2087)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        let s = downcast_array::<StringArray>(arg(args, 0, 2087)?, "text")?;
        let from = downcast_array::<StringArray>(arg(args, 1, 2087)?, "text")?;
        let to = downcast_array::<StringArray>(arg(args, 2, 2087)?, "text")?;
        let out: StringArray = s
            .iter()
            .zip(from.iter())
            .zip(to.iter())
            .map(|((s, from), to)| match (s, from, to) {
                // An empty `from` is where Rust and PostgreSQL part ways.
                // `str::replace` treats "" as matching at every character
                // boundary, so `"hello".replace("", "0")` is `"0h0e0l0l0o0"`.
                // PostgreSQL 18.2 returns the subject unchanged:
                // `replace('hello world', '', '0') = 'hello world'` (verified
                // live for this port). Nothing to find means nothing to
                // replace, so the guard is what keeps Basin correct here.
                (Some(s), Some(from), Some(to)) if !from.is_empty() => Some(s.replace(from, to)),
                (Some(s), Some(_), Some(_)) => Some(s.to_string()),
                _ => None,
            })
            .collect();
        Ok(Arc::new(out))
    }
}

/// `strpos(text, text)` / `position(text, text)`, on already-unwrapped values:
/// the 1-based *character* position of the first occurrence, or `0`.
///
/// Character-based, not byte-based, verified against live PostgreSQL 18.2:
/// `strpos('héllo', 'llo') = 3` (the 2-byte `é` is one character) and
/// `strpos('héllo世界', '世') = 6`. `str::find` returns a *byte* offset, so it
/// is converted back through `s[..byte_idx].chars().count()` rather than used
/// directly — using it directly would answer 4 and 7.
///
/// An empty needle is found at position 1, including in an empty subject
/// (`strpos('', '') = 1` live), which `str::find("")` returning `Some(0)`
/// already gives.
fn pg_strpos(s: &str, needle: &str) -> i32 {
    match s.find(needle) {
        Some(byte_idx) => s[..byte_idx].chars().count() as i32 + 1,
        None => 0,
    }
}

fn eval_strpos(s: &ArrayRef, needle: &ArrayRef) -> Result<ArrayRef, ExecError> {
    let s = downcast_array::<StringArray>(s, "text")?;
    let needle = downcast_array::<StringArray>(needle, "text")?;
    let out: Int32Array = s
        .iter()
        .zip(needle.iter())
        .map(|(s, needle)| match (s, needle) {
            (Some(s), Some(needle)) => Some(pg_strpos(s, needle)),
            _ => None,
        })
        .collect();
    Ok(Arc::new(out))
}

/// `strpos(text, text) -> integer`, `pg_proc` oid 868.
pub struct Strpos;

impl ScalarFunc for Strpos {
    fn oid(&self) -> Oid {
        Oid(868)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_strpos(arg(args, 0, 868)?, arg(args, 1, 868)?)
    }
}

/// `position(text, text) -> integer`, `pg_proc` oid 849.
///
/// **The argument-order trap, and why this is a separate struct rather than a
/// second registration of [`Strpos`].** `POSITION(needle IN haystack)` is
/// grammar; the `pg_proc` row it desugars to takes `(haystack, needle)` — the
/// same order as `strpos`, so the functional spelling reads backwards relative
/// to the syntax. Confirmed live on 18.2:
///
/// ```text
/// position('b' in 'abc')          = 2
/// pg_catalog.position('abc', 'b') = 2   -- same call, arguments as stored
/// pg_catalog.position('b', 'abc') = 0
/// ```
///
/// Two `pg_proc` oids with one implementation, which is exactly what the
/// registry cannot express in one entry: [`crate::funcs::FuncRegistry`] panics
/// on a duplicate oid and a `Box<dyn ScalarFunc>` reports exactly one
/// [`ScalarFunc::oid`]. Both structs are zero-sized, so the duplication costs
/// nothing at runtime, and keying on oid is what makes a resolution bug show up
/// as *the wrong function called* rather than as a broken implementation.
pub struct Position;

impl ScalarFunc for Position {
    fn oid(&self) -> Oid {
        Oid(849)
    }

    fn invoke(&self, args: &[ArrayRef], _session: &EvalSession) -> Result<ArrayRef, ExecError> {
        eval_strpos(arg(args, 0, 849)?, arg(args, 1, 849)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::funcs::builtins;
    // `is_null`/`len` are `Array` trait methods, not inherent ones.
    use arrow_array::Array;

    /// The registry must actually be reached for this oid. Without this, a
    /// port that forgot step 2 would still pass every behavioural test,
    /// because the `match` arm would answer instead — the port would look
    /// done and have changed nothing.
    #[test]
    fn lower_is_hosted_by_the_registry() {
        assert!(
            builtins().scalar(Oid(870)).is_some(),
            "lower must be reachable through the registry, not only the match"
        );
    }

    /// Behaviour is preserved exactly across the port, INCLUDING the U+0130
    /// divergence from PostgreSQL. The assertion states what Basin does and
    /// what the server does, so the gap is pinned rather than latent.
    #[test]
    fn lower_preserves_behaviour_including_a_known_postgres_divergence() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("HeLLo"),
            Some("İ"),
            Some("ǄUNGLA"),
            None,
        ]));
        let out = Lower
            .invoke(&[input], &EvalSession::DEFAULT)
            .expect("lower over text");
        let s = out
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("text out");

        assert_eq!(s.value(0), "hello");
        assert_eq!(s.value(2), "ǆungla");
        assert!(s.is_null(3), "NULL in, NULL out");
        assert_eq!(s.len(), 4, "output length must match input length");

        // Measured on live PostgreSQL 18.2, datcollate en_US.UTF-8:
        //   encode(convert_to(lower('İ'),'UTF8'),'hex')  ->  69
        // Basin, via str::to_lowercase, produces 69 cc87.
        assert_eq!(
            s.value(1),
            "i\u{307}",
            "Basin appends COMBINING DOT ABOVE where PostgreSQL returns a \
             bare 'i' — a KNOWN divergence, unchanged by this port. Fixing it \
             means collation-aware case mapping for the whole string family, \
             not a special case here; if this assertion ever fails because \
             someone fixed it properly, change it to \"i\"."
        );
    }

    // ─── helpers for the second slice ───────────────────────────────────

    fn text(v: Vec<Option<&str>>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    fn int(v: Vec<Option<i32>>) -> ArrayRef {
        Arc::new(Int32Array::from(v))
    }

    fn call(f: &dyn ScalarFunc, args: Vec<ArrayRef>) -> ArrayRef {
        f.invoke(&args, &EvalSession::DEFAULT)
            .expect("invoke must succeed")
    }

    fn err(f: &dyn ScalarFunc, args: Vec<ArrayRef>) -> String {
        match f
            .invoke(&args, &EvalSession::DEFAULT)
            .expect_err("invoke must fail")
        {
            ExecError::TypeMismatch(m) => m,
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    fn strs(a: &ArrayRef) -> &StringArray {
        a.as_any().downcast_ref::<StringArray>().expect("text out")
    }

    fn ints(a: &ArrayRef) -> &Int32Array {
        a.as_any()
            .downcast_ref::<Int32Array>()
            .expect("integer out")
    }

    /// Registration asserts a `pg_proc` row exists and PANICS if it does not,
    /// so every oid in this slice is checked here — while `str_fns.rs` can
    /// still be edited — rather than at the moment someone applies
    /// `str_fns.integration.md` and gets a panic in `builtins()` instead of a
    /// working registry. The name check is the real content: an oid that
    /// resolves to the wrong catalog row would register happily and then
    /// answer calls to a different function.
    #[test]
    fn every_oid_in_this_slice_has_the_catalog_row_registration_will_demand() {
        let slice: Vec<(&str, Box<dyn ScalarFunc>)> = vec![
            ("lower", Box::new(Lower)),
            ("upper", Box::new(Upper)),
            ("initcap", Box::new(Initcap)),
            ("lpad", Box::new(Lpad2)),
            ("lpad", Box::new(Lpad3)),
            ("rpad", Box::new(Rpad2)),
            ("rpad", Box::new(Rpad3)),
            ("repeat", Box::new(Repeat)),
            ("split_part", Box::new(SplitPart)),
            ("replace", Box::new(Replace)),
            ("strpos", Box::new(Strpos)),
            ("position", Box::new(Position)),
        ];
        for (name, f) in slice {
            let oid = f.oid();
            let row = crate::funcs::catalog_row(oid).unwrap_or_else(|| {
                panic!(
                    "oid {} has no pg_proc row in FUNCS — register_scalar \
                     would panic on it",
                    oid.get()
                )
            });
            assert_eq!(
                row.name,
                name,
                "oid {} is tabulated as `{}`, not `{name}` — the impl would be \
                 hosted under the wrong function",
                oid.get(),
                row.name
            );
            f.return_type(&[]).unwrap_or_else(|e| {
                panic!("oid {} cannot be typed from the catalog: {e:?}", oid.get())
            });
        }
    }

    /// `lower` diverges from PostgreSQL a SECOND way, found while porting
    /// `upper` and pinned here because the module doc's table cites it.
    ///
    /// U+0130 is an *expansion* (one character in, two out). This is a
    /// *context* disagreement, which is worse: Rust applies Unicode's
    /// `Final_Sigma` conditional mapping and lowercases a word-final `Σ` to
    /// `ς`, while glibc's `towlower_l` has no notion of word position and
    /// always answers `σ`. Same character count, different character —
    /// invisible to any test that only checks lengths, and invisible to ASCII.
    ///
    /// Measured on live PostgreSQL 18.2, `datcollate = en_US.UTF-8`:
    ///
    /// ```text
    /// encode(convert_to(lower('ΟΔΟΣ'),'UTF8'),'hex') -> cebf ceb4 cebf cf83
    ///                                                                  ^^^^ σ
    /// str::to_lowercase("ΟΔΟΣ")                      -> …            U+03C2 ς
    /// ```
    #[test]
    fn lower_also_diverges_on_the_greek_final_sigma() {
        let out = call(&Lower, vec![text(vec![Some("ΟΔΟΣ"), Some("ΟΔΟΣ ΟΔΟΣ")])]);
        let s = strs(&out);
        assert_eq!(
            s.value(0),
            "\u{3bf}\u{3b4}\u{3bf}\u{3c2}",
            "Basin ends the word with FINAL SIGMA U+03C2; PostgreSQL 18.2 \
             returns U+03C3 (hex cf83). KNOWN divergence, unchanged by this \
             port — the fix is collation-aware case mapping for the whole \
             family. If someone fixes it properly, this becomes \\u{{3c3}}."
        );
        assert_eq!(
            s.value(1),
            "\u{3bf}\u{3b4}\u{3bf}\u{3c2} \u{3bf}\u{3b4}\u{3bf}\u{3c2}",
            "both words, so this is word position and not merely end-of-string"
        );
    }

    /// `upper` is preserved exactly, INCLUDING the full-case-mapping gap.
    ///
    /// Every value here was read off live PostgreSQL 18.2 as hex before the
    /// port, so the assertion states what Basin does and the message states
    /// what the server does.
    #[test]
    fn upper_preserves_the_full_case_mapping_divergence() {
        let out = call(
            &Upper,
            vec![text(vec![
                Some("HeLLo"),
                Some("héllo世界 abc"),
                Some("ß"),
                Some("ﬁ"),
                Some("ᾀ"),
                None,
            ])],
        );
        let s = strs(&out);

        // Agreement — every ASCII input, and the ordinary accented/CJK cases,
        // which is why this gap has never shown up in a test.
        assert_eq!(s.value(0), "HELLO");
        assert_eq!(s.value(1), "HÉLLO世界 ABC", "upper('héllo世界 abc') live");

        // Divergence, preserved.
        assert_eq!(
            s.value(2),
            "SS",
            "PostgreSQL 18.2 returns 'ß' unchanged (hex c39f): no single wide \
             character is its uppercase, so towupper_l has nothing to do"
        );
        assert_eq!(
            s.value(3),
            "FI",
            "PostgreSQL 18.2 returns 'ﬁ' unchanged (hex efac81)"
        );
        assert_eq!(
            s.value(4),
            "\u{1f08}\u{399}",
            "PostgreSQL 18.2 returns U+1F88 (hex e1be88), the TITLECASE \
             character — glibc has a one-to-one answer here, it is just not \
             Unicode's two-character one"
        );

        assert!(s.is_null(5), "NULL in, NULL out");
        assert_eq!(s.len(), 6, "output length must match input length");
    }

    /// `initcap` agrees with PostgreSQL on the sigma and disagrees on
    /// expansions, and the reason is one word: `char::to_lowercase` is
    /// per-character, so `Final_Sigma` cannot fire; `char::to_uppercase` is
    /// still the full mapping, so `ß` still expands.
    ///
    /// This is the test that will fail if someone "tidies" [`pg_initcap`] to
    /// use `str::to_lowercase` like [`Lower`] does. That tidy-up would be a
    /// regression against the live server, not a cleanup.
    #[test]
    fn initcap_agrees_with_postgres_on_sigma_but_not_on_expansion() {
        let out = call(
            &Initcap,
            vec![text(vec![
                Some("ΟΔΟΣ ΟΔΟΣ"),
                Some("İstanbul iSTANBUL"),
                Some("ǅungla ǅungla"),
                Some("ßeta"),
                Some("ﬁne w"),
            ])],
        );
        let s = strs(&out);

        // Agreement, all four measured live on 18.2.
        assert_eq!(
            s.value(0),
            "Οδοσ Οδοσ",
            "contextless lowercasing agrees with towlower_l — see the doc"
        );
        assert_eq!(
            s.value(1),
            "İstanbul Istanbul",
            "initcap('İstanbul iSTANBUL') live"
        );
        assert_eq!(
            s.value(2),
            "Ǆungla Ǆungla",
            "titlecase ǅ upcases 1:1 both sides"
        );

        // Divergence, preserved.
        assert_eq!(
            s.value(3),
            "SSeta",
            "PostgreSQL 18.2 returns 'ßeta' — char::to_uppercase('ß') is still \
             Unicode's two-character 'SS'"
        );
        assert_eq!(s.value(4), "FIne W", "PostgreSQL 18.2 returns 'ﬁne W'");
    }

    /// The word rule is "after any non-alphanumeric", not "after whitespace",
    /// and digits do not split. The last row is the `iswalnum` divergence.
    #[test]
    fn initcap_splits_on_any_non_alphanumeric_and_diverges_on_category_no() {
        let out = call(
            &Initcap,
            vec![text(vec![
                Some("hELLo-woRLD foo_bar"),
                Some("3abc a2b"),
                Some("世界abc"),
                Some("½abc"),
                None,
            ])],
        );
        let s = strs(&out);
        assert_eq!(s.value(0), "Hello-World Foo_Bar", "'-' and '_' both split");
        assert_eq!(
            s.value(1),
            "3abc A2b",
            "digits are alphanumeric, so they do not split"
        );
        assert_eq!(s.value(2), "世界abc", "nor do CJK letters");
        assert_eq!(
            s.value(3),
            "½abc",
            "PostgreSQL 18.2 returns '½Abc': glibc's iswalnum counts only Nd \
             as a digit, so '½' (category No) splits a word there and does not \
             here. KNOWN divergence — std exposes no Nd-only predicate"
        );
        assert!(s.is_null(4), "NULL in, NULL out");
    }

    /// The three pad steps, in the order that makes the awkward cases fall
    /// out: truncate, then check the fill, then cycle it. Every expectation is
    /// a live PostgreSQL 18.2 answer.
    #[test]
    fn pads_truncate_then_cycle_the_fill_by_character() {
        let subject = || text(vec![Some("a"), Some("héllo"), Some("hello"), Some("hello")]);
        let lens = || int(vec![Some(6), Some(8), Some(10), Some(3)]);
        let fills = || text(vec![Some("xyz"), Some("é"), Some(""), Some("")]);

        let l = call(&Lpad3, vec![subject(), lens(), fills()]);
        let l = strs(&l);
        assert_eq!(
            l.value(0),
            "xyzxya",
            "the fill wraps mid-string, it is not repeated whole"
        );
        assert_eq!(l.value(1), "éééhéllo", "len counts characters, not bytes");
        assert_eq!(l.value(2), "hello", "an empty fill cannot pad");
        assert_eq!(
            l.value(3),
            "hel",
            "…but truncation already happened, so this is NOT 'return the \
             input unchanged'"
        );

        let r = call(&Rpad3, vec![subject(), lens(), fills()]);
        let r = strs(&r);
        assert_eq!(r.value(0), "axyzxy");
        assert_eq!(r.value(1), "héllo\u{e9}\u{e9}\u{e9}");
        assert_eq!(
            r.value(3),
            "hel",
            "rpad truncates from the SAME end as lpad"
        );

        // Multibyte fill wrapping, and the two-argument default fill.
        let wrapped = call(
            &Lpad3,
            vec![
                text(vec![Some("a")]),
                int(vec![Some(6)]),
                text(vec![Some("世界")]),
            ],
        );
        assert_eq!(strs(&wrapped).value(0), "世界世界世a");

        let defaults = call(
            &Lpad2,
            vec![
                text(vec![Some("héllo"), Some("hello")]),
                int(vec![Some(2), Some(-3)]),
            ],
        );
        let d = strs(&defaults);
        assert_eq!(d.value(0), "hé", "a shorter len truncates");
        assert_eq!(
            d.value(1),
            "",
            "a negative len clamps to zero — not an error"
        );

        let right_default = call(&Rpad2, vec![text(vec![Some("ab")]), int(vec![Some(4)])]);
        assert_eq!(
            strs(&right_default).value(0),
            "ab  ",
            "default fill is one space"
        );
    }

    /// **NULL is tested before the ceiling, and the order is the behaviour.**
    /// `lpad(NULL, 2147483647)` is NULL on the live server, not `requested
    /// length too large`, because a strict function is never entered when an
    /// argument is NULL. A whole-array pre-check would turn that NULL into an
    /// error, which is why [`eval_pad`] walks indices.
    #[test]
    fn pad_checks_its_ceiling_per_row_and_only_after_the_null_test() {
        let over = PAD_MAX_LEN + 1; // 268435455 — errors live, 268435454 does not
        assert_eq!(over, 268_435_455, "the ceiling is (MaxAllocSize - 4) / 4");

        let m = err(
            &Lpad2,
            vec![text(vec![Some("hello")]), int(vec![Some(over)])],
        );
        assert_eq!(m, "requested length too large");

        let out = call(&Lpad2, vec![text(vec![None]), int(vec![Some(over)])]);
        assert!(
            strs(&out).is_null(0),
            "a NULL subject must stay NULL at a length that would otherwise error"
        );

        // A NULL fill is just as strict, and it is the third argument that is
        // easiest to forget.
        let out = call(
            &Lpad3,
            vec![text(vec![Some("a")]), int(vec![Some(5)]), text(vec![None])],
        );
        assert!(strs(&out).is_null(0), "lpad('a', 5, NULL) is NULL live");
    }

    /// `repeat` clamps a negative count to zero BEFORE the size check, so a
    /// negative count is an empty string rather than an error — including
    /// `i32::MIN`, where a 32-bit negation would have overflowed. The check
    /// itself is on bytes times count, not on the count alone.
    #[test]
    fn repeat_clamps_a_negative_count_before_it_checks_bytes() {
        let out = call(
            &Repeat,
            vec![
                text(vec![
                    Some("ab"),
                    Some("ab"),
                    Some("ab"),
                    Some("ab"),
                    None,
                    Some(""),
                ]),
                int(vec![
                    Some(3),
                    Some(0),
                    Some(-3),
                    Some(i32::MIN),
                    Some(3),
                    Some(i32::MAX),
                ]),
            ],
        );
        let s = strs(&out);
        assert_eq!(s.value(0), "ababab");
        assert_eq!(s.value(1), "");
        assert_eq!(s.value(2), "", "a negative count is not an error");
        assert_eq!(
            s.value(3),
            "",
            "repeat('ab', -2147483648) = '' live, no overflow"
        );
        assert!(s.is_null(4), "NULL in, NULL out");
        assert_eq!(
            s.value(5),
            "",
            "an empty subject is empty at any count. NOTE: the live server \
             agrees on the value but not on the cost — repeat('', 2147483647) \
             did not return within a 2s statement_timeout on 18.2, because \
             PostgreSQL loops `count` times copying nothing"
        );
        assert_eq!(s.len(), 6, "output length must match input length");

        // 1 byte * 1073741820 + 4 > MaxAllocSize; 1073741819 lands exactly on
        // it and is accepted (not exercised here — it would allocate a GiB).
        let m = err(
            &Repeat,
            vec![text(vec![Some("a")]), int(vec![Some(1_073_741_820)])],
        );
        assert_eq!(m, "requested length too large");
        let m = err(
            &Repeat,
            vec![text(vec![Some("世")]), int(vec![Some(536_870_910)])],
        );
        assert_eq!(
            m, "requested length too large",
            "three bytes moves the boundary by three"
        );

        let out = call(
            &Repeat,
            vec![text(vec![None]), int(vec![Some(1_073_741_820)])],
        );
        assert!(
            strs(&out).is_null(0),
            "NULL before the size error, as with pad"
        );
    }

    /// `split_part` indexes from either end, runs off both ends quietly, and
    /// errors on exactly one input. All measured live on 18.2.
    #[test]
    fn split_part_indexes_from_either_end_and_only_zero_errors() {
        let subject = |n: usize| text(vec![Some("a,b,c"); n]);
        let fields = int(vec![
            Some(1),
            Some(4),
            Some(-1),
            Some(-3),
            Some(-4),
            Some(i32::MIN),
        ]);
        let out = call(
            &SplitPart,
            vec![subject(6), text(vec![Some(","); 6]), fields],
        );
        let s = strs(&out);
        assert_eq!(s.value(0), "a");
        assert_eq!(s.value(1), "", "past the end is empty, not an error");
        assert_eq!(s.value(2), "c", "negative counts from the right (PG 14+)");
        assert_eq!(s.value(3), "a");
        assert_eq!(s.value(4), "");
        assert_eq!(
            s.value(5),
            "",
            "i32::MIN would overflow a 32-bit negation; widened to i64 it \
             falls out of range and returns '', which is what the server does"
        );

        // An empty delimiter makes exactly ONE field, reachable from either
        // end and nothing else.
        let out = call(
            &SplitPart,
            vec![
                subject(4),
                text(vec![Some(""); 4]),
                int(vec![Some(1), Some(-1), Some(2), Some(-2)]),
            ],
        );
        let s = strs(&out);
        assert_eq!(s.value(0), "a,b,c");
        assert_eq!(s.value(1), "a,b,c");
        assert_eq!(s.value(2), "");
        assert_eq!(s.value(3), "");

        // The delimiter is a string, not a character set.
        let out = call(
            &SplitPart,
            vec![
                text(vec![Some("a::b::c")]),
                text(vec![Some("::")]),
                int(vec![Some(2)]),
            ],
        );
        assert_eq!(strs(&out).value(0), "b");

        let m = err(
            &SplitPart,
            vec![subject(1), text(vec![Some(",")]), int(vec![Some(0)])],
        );
        assert_eq!(m, "field position must not be zero");

        // …but NULL wins over that error, same ordering as pad.
        let out = call(
            &SplitPart,
            vec![text(vec![None]), text(vec![Some(",")]), int(vec![Some(0)])],
        );
        assert!(
            strs(&out).is_null(0),
            "split_part(NULL, ',', 0) is NULL live"
        );
    }

    /// The empty-`from` guard is the whole point of `replace` not being
    /// `str::replace`. Without it Basin would answer `'0h0e0l0l0o0'` where the
    /// server answers `'hello'`.
    #[test]
    fn replace_leaves_an_empty_from_alone_unlike_str_replace() {
        let out = call(
            &Replace,
            vec![
                text(vec![
                    Some("hello world"),
                    Some("aaa"),
                    Some("abc"),
                    Some("héllo"),
                    None,
                ]),
                text(vec![Some(""), Some("aa"), Some("b"), Some("é"), Some("x")]),
                text(vec![Some("0"), Some("b"), Some(""), Some("X"), Some("y")]),
            ],
        );
        let s = strs(&out);
        assert_eq!(
            s.value(0),
            "hello world",
            "PostgreSQL 18.2: replace('hello world','','0') = 'hello world'. \
             \"hello world\".replace(\"\", \"0\") would be '0h0e0l0l0o0 0w…'"
        );
        assert_eq!(s.value(1), "ba", "matches do not overlap, left to right");
        assert_eq!(s.value(2), "ac", "an empty replacement deletes");
        assert_eq!(s.value(3), "hXllo", "multibyte needle");
        assert!(s.is_null(4), "strict in all three arguments");
        assert_eq!(s.len(), 5, "output length must match input length");
    }

    /// `strpos` and `position` are one implementation behind two oids, and the
    /// argument order is the trap: `POSITION(needle IN haystack)` is grammar,
    /// but the `pg_proc` row takes `(haystack, needle)`. Both directions are
    /// asserted, because a single example would pass either way round.
    #[test]
    fn strpos_and_position_are_character_positions_in_haystack_needle_order() {
        let out = call(
            &Strpos,
            vec![
                text(vec![
                    Some("héllo"),
                    Some("hello"),
                    Some("héllo世界"),
                    Some("abc"),
                    Some(""),
                    Some("abc"),
                ]),
                text(vec![
                    Some("llo"),
                    Some("xyz"),
                    Some("世"),
                    Some(""),
                    Some(""),
                    None,
                ]),
            ],
        );
        let p = ints(&out);
        assert_eq!(
            p.value(0),
            3,
            "characters, not bytes: 'llo' starts at BYTE offset 3, so using \
             str::find's answer directly would report 4"
        );
        assert_eq!(p.value(1), 0, "a non-match is 0, not NULL");
        assert_eq!(p.value(2), 6, "byte offset 6 is character 5, so position 6");
        assert_eq!(p.value(3), 1, "an empty needle is found at 1");
        assert_eq!(p.value(4), 1, "strpos('','') = 1 live");
        assert!(p.is_null(5), "NULL in, NULL out");
        assert_eq!(p.len(), 6, "output length must match input length");

        // Same answers through oid 849, and both orders, because `position`
        // exists precisely so the desugared grammar has a row to call.
        let forwards = call(
            &Position,
            vec![text(vec![Some("abc")]), text(vec![Some("b")])],
        );
        assert_eq!(
            ints(&forwards).value(0),
            2,
            "pg_catalog.position('abc','b') = 2 live"
        );
        let reversed = call(
            &Position,
            vec![text(vec![Some("b")]), text(vec![Some("abc")])],
        );
        assert_eq!(
            ints(&reversed).value(0),
            0,
            "pg_catalog.position('b','abc') = 0 live — arguments are \
             (haystack, needle), NOT the order POSITION(x IN y) is written in"
        );
    }

    /// Arity is guaranteed by resolution, so a short call is a planner bug
    /// and must say so rather than silently returning something.
    #[test]
    fn a_missing_argument_is_reported_as_a_planner_bug() {
        let err = Lower
            .invoke(&[], &EvalSession::DEFAULT)
            .expect_err("no arguments must fail");
        assert!(
            matches!(err, ExecError::Internal(ref m) if m.contains("planner bug")),
            "expected a planner-bug Internal error, got {err:?}"
        );
    }
}
